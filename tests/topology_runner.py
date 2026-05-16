"""
tests/topology_runner.py — Multi-process proxy topology substrate.

Brings up a graph of proxy processes in topological order, with
per-node port allocation, readiness gating, structured-log capture,
and orderly teardown. The foundation for Tier 3 multi-process
diagnostic scripts per `docs/notes/proxy-topology-testing-plan.md`
§§2.1–2.5.

Generalised from `frontend/scripts/run-selector-stack.py` (which the
plan §6 phase 1 originally scoped to relocate). The selector-stack
script was SELECTOR-shaped by accident of where its author needed it;
relocating to the proxy repo as a role-generic substrate is the
right home — proxy expertise lives here, not in umbrella tooling.

What this IS:

  - A topology declaration (`NodeSpec` / `TopologySpec`) + a runner
    (`TopologyRunner`) that brings the topology up against a pool of
    subprocess-spawned proxies and/or pre-existing endpoints.
  - The Tier 3 substrate per the plan's tier split: multi-process,
    real network sockets, real proxy processes.

What this IS NOT:

  - Not a deployment tool — production launch is `run_leaf.sh` /
    `run_relay.sh` / systemd. The substrate is for testing and
    debugging.
  - Not a load generator — driving traffic through the topology is
    each diagnostic script's concern. The substrate brings the
    topology up; what flows through it is the test's responsibility.
  - Not a substitute for the in-process `tests/test_*.py` pattern
    (Tier 2). The substrate covers what in-process testing can't
    observe — multi-process behaviours under real network sockets.

Spec invariants enforced at construction (`TopologySpec.__post_init__`
calls `NodeSpec._validate` on each member, asserts label uniqueness,
checks upstream references resolve, and computes topological order
to detect cycles). Failures at construction surface as `ValueError`
with the offending node/label named — ADR-0002's startup-time
loud-failure register.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import enum
import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Module-local constants
# ---------------------------------------------------------------------------


# `tests/topology_runner.py` → parent is the proxy root.
_PROXY_ROOT = Path(__file__).resolve().parent.parent
_PROXY_SERVER = _PROXY_ROOT / "proxy_server.py"

# Per-node readiness budget. LEAF spawn dominates: the proxy logs
# "listening on ws://..." only after KataGo's startup probe completes,
# which on slow disks / cold-load can be 30s+. The proxy's own
# `KATAGO_STARTUP_TIMEOUT_S` defaults to 60s, so 90s is the natural
# upper bound — if the node hasn't started listening by then, the
# fault is on the proxy side and surfacing it is better than waiting.
# Non-LEAF roles (RELAY / SELECTOR / ECHO / REDIRECT) start in well
# under a second; the same budget covers them with margin.
_DEFAULT_NODE_READINESS_TIMEOUT_S = 90.0
_READINESS_POLL_INTERVAL_S = 0.5


# ===========================================================================
# Spec types
# ===========================================================================


class ProxyRole(str, enum.Enum):
    """Mirror of `cfg.PROXY_ROLE`'s closed vocabulary; values match the
    env-var strings the proxy reads at startup."""
    LEAF = "LEAF"
    RELAY = "RELAY"
    SELECTOR = "SELECTOR"
    ECHO = "ECHO"
    REDIRECT = "REDIRECT"


@dataclass(frozen=True)
class NodeSpec:
    """One proxy process (or one pre-existing endpoint) in the topology.

    Two shapes share the same NodeSpec surface, discriminated by
    `is_spawned`:

      - **Spawned** (`pre_existing_url is None`): the substrate will
        `proxy_server.py` for this node, with port allocation and
        readiness gating. Role-specific fields (`model_path` for LEAF,
        `upstreams` for RELAY/SELECTOR/REDIRECT) supply the runtime
        configuration.
      - **Pre-existing** (`pre_existing_url is not None`): the
        substrate does NOT spawn anything for this node; it serves
        only to expose a labelled URL that other nodes' `upstreams`
        can reference. The operator is responsible for the
        pre-existing endpoint's lifecycle.

    Role-specific constraints (enforced by `_validate`):

      - LEAF: requires `model_path`; takes no upstreams.
      - RELAY / SELECTOR / REDIRECT: requires ≥1 upstream; takes no
        `model_path`.
      - ECHO: takes no `upstreams`, no `model_path`.
      - Pre-existing: takes no `model_path`, no `upstreams`; URL must
        be ws:// or wss://.
    """
    label: str
    role: ProxyRole
    upstreams: tuple[str, ...] = ()
    model_path: Optional[Path] = None
    pre_existing_url: Optional[str] = None
    advertise_capabilities: bool = True

    @property
    def is_spawned(self) -> bool:
        return self.pre_existing_url is None

    def _validate(self) -> None:
        if self.pre_existing_url is not None:
            if self.model_path is not None:
                raise ValueError(
                    f"NodeSpec {self.label!r}: pre_existing_url and "
                    f"model_path are mutually exclusive"
                )
            if self.upstreams:
                raise ValueError(
                    f"NodeSpec {self.label!r}: pre_existing_url nodes "
                    f"take no upstreams (their own topology is the "
                    f"operator's concern, not the substrate's)"
                )
            if not (
                self.pre_existing_url.startswith("ws://")
                or self.pre_existing_url.startswith("wss://")
            ):
                raise ValueError(
                    f"NodeSpec {self.label!r}: pre_existing_url must "
                    f"start with ws:// or wss:// "
                    f"(got {self.pre_existing_url!r})"
                )
            return
        if self.role == ProxyRole.LEAF:
            if self.model_path is None:
                raise ValueError(
                    f"NodeSpec {self.label!r}: spawned LEAF requires "
                    f"model_path"
                )
            if self.upstreams:
                raise ValueError(
                    f"NodeSpec {self.label!r}: LEAF takes no upstreams "
                    f"(it serves its own KataGo subprocess)"
                )
        elif self.role in (
            ProxyRole.RELAY, ProxyRole.SELECTOR, ProxyRole.REDIRECT,
        ):
            if not self.upstreams:
                raise ValueError(
                    f"NodeSpec {self.label!r}: {self.role.value} "
                    f"requires at least one upstream"
                )
            if self.model_path is not None:
                raise ValueError(
                    f"NodeSpec {self.label!r}: {self.role.value} takes "
                    f"no model_path (it's not the engine boundary)"
                )
        elif self.role == ProxyRole.ECHO:
            if self.upstreams:
                raise ValueError(
                    f"NodeSpec {self.label!r}: ECHO takes no upstreams "
                    f"(it serves synthetic responses)"
                )
            if self.model_path is not None:
                raise ValueError(
                    f"NodeSpec {self.label!r}: ECHO takes no model_path"
                )


@dataclass(frozen=True)
class TopologySpec:
    """A graph of proxy nodes plus the user-facing port.

    The user-facing node (the "leaf of the topology DAG", not to be
    confused with the LEAF role) is the one a test client connects
    to. Identified by `client_label`; that node listens on
    `client_port`. All other spawned nodes get OS-allocated free
    ports at start time.

    Validation at construction (`__post_init__`):
      1. Labels unique across the topology.
      2. Each NodeSpec's own role-specific constraints satisfied.
      3. Upstream labels reference declared nodes.
      4. No cycles in the upstream graph.
      5. `client_label` names a spawned node (a pre-existing client
         would defeat the substrate's purpose — its port is the
         operator's concern, not ours).
    """
    nodes: tuple[NodeSpec, ...]
    client_label: str
    client_port: int
    host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        labels_seen: set[str] = set()
        for n in self.nodes:
            if n.label in labels_seen:
                raise ValueError(
                    f"TopologySpec: duplicate label {n.label!r}"
                )
            labels_seen.add(n.label)
            n._validate()

        for n in self.nodes:
            for u in n.upstreams:
                if u not in labels_seen:
                    raise ValueError(
                        f"NodeSpec {n.label!r}: upstream {u!r} not "
                        f"declared in this topology"
                    )

        order = _topological_order(self.nodes)
        # Frozen dataclass: __setattr__ refuses; use object.__setattr__
        # to cache the computed order.
        object.__setattr__(self, "_topo_order", order)

        client_node = next(
            (n for n in self.nodes if n.label == self.client_label),
            None,
        )
        if client_node is None:
            raise ValueError(
                f"TopologySpec: client_label {self.client_label!r} "
                f"not in nodes"
            )
        if not client_node.is_spawned:
            raise ValueError(
                f"TopologySpec: client_label {self.client_label!r} is "
                f"a pre-existing node; the user-facing node must be "
                f"spawned so the substrate can bind it to client_port"
            )

    @property
    def topological_order(self) -> tuple[str, ...]:
        # Cached in __post_init__ via object.__setattr__.
        return self._topo_order  # type: ignore[no-any-return]


def _topological_order(nodes: tuple[NodeSpec, ...]) -> tuple[str, ...]:
    """Kahn's algorithm: upstreams before dependents.

    An edge `A → B` means "A is B's upstream" (B's `upstreams` tuple
    contains A); A must start before B so B can be configured against
    A's URL at spawn time. Returns labels in start order. Raises
    `ValueError` if a cycle is detected.
    """
    in_degree: dict[str, int] = {n.label: len(n.upstreams) for n in nodes}
    ready: list[str] = sorted(
        label for label, d in in_degree.items() if d == 0
    )
    order: list[str] = []
    seen: set[str] = set(ready)

    while ready:
        label = ready.pop(0)
        order.append(label)
        for n in nodes:
            if label in n.upstreams and n.label not in seen:
                in_degree[n.label] -= 1
                if in_degree[n.label] == 0:
                    ready.append(n.label)
                    seen.add(n.label)
        ready.sort()  # deterministic tie-breaking

    if len(order) != len(nodes):
        missing = [n.label for n in nodes if n.label not in seen]
        raise ValueError(
            f"TopologySpec: cycle detected; nodes unreachable after "
            f"topological sort: {missing}"
        )
    return tuple(order)


# ===========================================================================
# Runner
# ===========================================================================


@dataclass
class _RunningNode:
    """Per-node bookkeeping during a runner's lifetime."""
    spec: NodeSpec
    port: Optional[int] = None
    process: Optional[asyncio.subprocess.Process] = None


class TopologyRunner:
    """Orchestrates spawn / readiness / teardown for a TopologySpec.

    Usage:

        runner = TopologyRunner(spec, log_dir=Path("./logs"))
        await runner.start()
        try:
            # Drive the test against runner.client_url; observe via
            # structured JSON logs at <log_dir>/<label>.jsonl per node.
            ...
        finally:
            await runner.stop()

    `start()` is one-shot — calling it twice raises. `stop()` is
    idempotent. On `start()` failure (any node fails to become
    reachable), best-effort teardown of already-started nodes runs
    before the exception propagates.
    """

    def __init__(
        self,
        spec: TopologySpec,
        *,
        log_dir: Optional[Path] = None,
        node_readiness_timeout_s: float = _DEFAULT_NODE_READINESS_TIMEOUT_S,
        katago_path: str = "katago",
        katago_cfg: str = "analysis.cfg",
    ) -> None:
        self.spec = spec
        self._log_dir = log_dir
        self._node_readiness_timeout_s = node_readiness_timeout_s
        self._katago_path = katago_path
        self._katago_cfg = katago_cfg
        self._nodes: dict[str, _RunningNode] = {
            n.label: _RunningNode(spec=n) for n in spec.nodes
        }
        self._started = False
        self._stopped = False

    @property
    def client_url(self) -> str:
        """The ws:// URL the test client should connect to."""
        return f"ws://{self.spec.host}:{self.spec.client_port}"

    def url_for(self, label: str) -> str:
        """The ws:// URL of a named node. Valid after `start()`.

        For pre-existing nodes the URL is known at spec construction
        time and could be returned earlier, but the spawned-vs-
        pre-existing distinction is an internal detail callers
        shouldn't have to care about — keeping the precondition
        uniform avoids the "which kind of node is this" branch at
        the call site.
        """
        if not self._started:
            raise RuntimeError(
                "TopologyRunner.url_for() called before start()"
            )
        node = self._nodes[label]
        if node.spec.pre_existing_url is not None:
            return node.spec.pre_existing_url
        assert node.port is not None, (
            f"spawned node {label!r} has no port post-start "
            f"(invariant violation)"
        )
        return f"ws://{self.spec.host}:{node.port}"

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("TopologyRunner.start() called twice")
        if self._log_dir is not None:
            self._log_dir.mkdir(parents=True, exist_ok=True)

        # Allocate ports first so dependent nodes can include
        # upstream URLs in their spawn env.
        for label in self.spec.topological_order:
            node = self._nodes[label]
            if not node.spec.is_spawned:
                continue
            if label == self.spec.client_label:
                node.port = self.spec.client_port
            else:
                node.port = _allocate_free_port(self.spec.host)

        try:
            for label in self.spec.topological_order:
                node = self._nodes[label]
                if not node.spec.is_spawned:
                    continue
                await self._spawn_and_wait(node)
            self._started = True
        except Exception:
            await self._terminate_all()
            raise

    async def stop(self) -> None:
        if self._stopped:
            return
        await self._terminate_all()
        self._stopped = True

    async def _spawn_and_wait(self, node: _RunningNode) -> None:
        env = self._build_env(node)
        node.process = await asyncio.create_subprocess_exec(
            sys.executable, str(_PROXY_SERVER),
            env=env, cwd=str(_PROXY_ROOT),
        )
        ready = await self._wait_for_listen(node)
        if not ready:
            rc = node.process.returncode
            if rc is not None:
                raise RuntimeError(
                    f"node {node.spec.label!r} ({node.spec.role.value}) "
                    f"exited with code {rc} before becoming reachable. "
                    f"Check the proxy's stderr / log output for cause."
                )
            raise RuntimeError(
                f"node {node.spec.label!r} ({node.spec.role.value}) did "
                f"not start listening on port {node.port} within "
                f"{self._node_readiness_timeout_s}s"
            )

    def _build_env(self, node: _RunningNode) -> dict[str, str]:
        env = os.environ.copy()
        env["PROXY_ROLE"] = node.spec.role.value
        env["PROXY_HOST"] = self.spec.host
        assert node.port is not None, (
            f"spawned node {node.spec.label!r} has no port at spawn "
            f"time (invariant violation)"
        )
        env["PROXY_PORT"] = str(node.port)
        if node.spec.advertise_capabilities:
            env["PROXY_ADVERTISE_CAPABILITIES"] = "true"

        if node.spec.role == ProxyRole.LEAF:
            assert node.spec.model_path is not None
            env["KATAGO_PATH"] = self._katago_path
            env["KATAGO_MODEL"] = str(node.spec.model_path)
            env["KATAGO_CFG"] = self._katago_cfg
        elif node.spec.role in (ProxyRole.RELAY, ProxyRole.REDIRECT):
            env["UPSTREAM_URLS"] = ",".join(
                self._upstream_url(u) for u in node.spec.upstreams
            )
        elif node.spec.role == ProxyRole.SELECTOR:
            env["SELECTOR_MODELS"] = ",".join(
                f"{u}={self._upstream_url(u)}" for u in node.spec.upstreams
            )
        # ECHO: no role-specific env beyond PROXY_ROLE.

        if self._log_dir is not None:
            env["PROXY_LOG_FORMAT"] = "json"
            env["PROXY_LOG_DEST"] = (
                f"file:{self._log_dir / f'{node.spec.label}.jsonl'}"
            )
        return env

    def _upstream_url(self, label: str) -> str:
        node = self._nodes[label]
        if node.spec.pre_existing_url is not None:
            return node.spec.pre_existing_url
        assert node.port is not None, (
            f"upstream {label!r} has no port — topological order "
            f"should have spawned it before this dependent"
        )
        return f"ws://{self.spec.host}:{node.port}"

    async def _wait_for_listen(self, node: _RunningNode) -> bool:
        """Poll until the port is connectable, the proc dies, or
        timeout. Returns True iff connectable while the proc stayed
        alive."""
        assert node.process is not None
        assert node.port is not None
        loop = asyncio.get_event_loop()
        deadline = loop.time() + self._node_readiness_timeout_s
        while loop.time() < deadline:
            if node.process.returncode is not None:
                return False
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.spec.host, node.port),
                    timeout=1.0,
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return True
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(_READINESS_POLL_INTERVAL_S)
        return False

    async def _terminate_all(self, grace_s: float = 5.0) -> None:
        """SIGTERM in reverse topological order; SIGKILL stragglers
        past the grace budget. Pre-existing nodes are untouched —
        their lifecycle is the operator's concern."""
        for label in reversed(self.spec.topological_order):
            node = self._nodes[label]
            if node.process is None or node.process.returncode is not None:
                continue
            node.process.terminate()
        loop = asyncio.get_event_loop()
        deadline = loop.time() + grace_s
        for label in reversed(self.spec.topological_order):
            node = self._nodes[label]
            if node.process is None:
                continue
            remaining = max(0.0, deadline - loop.time())
            try:
                await asyncio.wait_for(
                    node.process.wait(),
                    timeout=remaining if remaining > 0 else 0.1,
                )
            except asyncio.TimeoutError:
                node.process.kill()
                try:
                    await asyncio.wait_for(node.process.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass


# ===========================================================================
# Port allocation
# ===========================================================================


def _allocate_free_port(host: str) -> int:
    """Bind to port 0 and return the OS-assigned port.

    TOCTOU caveat (same as `frontend/scripts/run-selector-stack.py`):
    there's a window between this returning and the proxy binding;
    in single-user testing the collision risk is negligible. If a
    collision does occur the proxy fails to bind and exits with
    `OSError` — `_wait_for_listen` detects this and surfaces.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])
