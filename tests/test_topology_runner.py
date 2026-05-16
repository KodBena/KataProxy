"""
tests/test_topology_runner.py — Pure-logic unit tests for the
topology substrate's spec validation and topological sort.

The runtime parts of the substrate (subprocess spawn, readiness
polling, structured-log capture, termination) require a real
`proxy_server.py` execution against real KataGo and are exercised
by the Tier 3 diagnostic scripts. This file covers what's testable
in-process: NodeSpec / TopologySpec validation and the
`_topological_order` algorithm.

Per ADR-0002 startup-time loud-failure register: bad specs raise
`ValueError` at construction with the offending node and reason
named. The negative-control tests pin that loud-failure surface.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from tests.topology_runner import (  # noqa: E402
    NodeSpec,
    ProxyRole,
    TopologySpec,
    _topological_order,
)


# ===========================================================================
# NodeSpec validation
# ===========================================================================


class TestNodeSpecValidation:
    def test_spawned_leaf_requires_model_path(self) -> None:
        with pytest.raises(ValueError, match="LEAF requires model_path"):
            TopologySpec(
                nodes=(
                    NodeSpec(label="leaf", role=ProxyRole.LEAF),
                ),
                client_label="leaf",
                client_port=4242,
            )

    def test_spawned_leaf_takes_no_upstreams(self) -> None:
        with pytest.raises(ValueError, match="LEAF takes no upstreams"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf",
                        role=ProxyRole.LEAF,
                        model_path=Path("/tmp/model.bin.gz"),
                        upstreams=("other",),
                    ),
                    NodeSpec(
                        label="other", role=ProxyRole.LEAF,
                        model_path=Path("/tmp/other.bin.gz"),
                    ),
                ),
                client_label="leaf",
                client_port=4242,
            )

    @pytest.mark.parametrize(
        "role", [ProxyRole.RELAY, ProxyRole.SELECTOR, ProxyRole.REDIRECT],
    )
    def test_fanout_role_requires_upstream(self, role: ProxyRole) -> None:
        with pytest.raises(
            ValueError, match="requires at least one upstream",
        ):
            TopologySpec(
                nodes=(
                    NodeSpec(label="r", role=role),
                ),
                client_label="r",
                client_port=4242,
            )

    @pytest.mark.parametrize(
        "role", [ProxyRole.RELAY, ProxyRole.SELECTOR, ProxyRole.REDIRECT],
    )
    def test_fanout_role_takes_no_model_path(self, role: ProxyRole) -> None:
        with pytest.raises(ValueError, match="takes no model_path"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="r", role=role,
                        upstreams=("leaf",),
                        model_path=Path("/tmp/m.gz"),
                    ),
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        model_path=Path("/tmp/leaf.gz"),
                    ),
                ),
                client_label="r",
                client_port=4242,
            )

    def test_echo_takes_no_upstreams_or_model(self) -> None:
        with pytest.raises(ValueError, match="ECHO takes no upstreams"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="e", role=ProxyRole.ECHO,
                        upstreams=("other",),
                    ),
                    NodeSpec(
                        label="other", role=ProxyRole.ECHO,
                    ),
                ),
                client_label="e",
                client_port=4242,
            )

    def test_pre_existing_excludes_model_path(self) -> None:
        with pytest.raises(
            ValueError, match="mutually exclusive",
        ):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf",
                        role=ProxyRole.LEAF,
                        pre_existing_url="ws://192.168.122.1:1235",
                        model_path=Path("/tmp/m.gz"),
                    ),
                    NodeSpec(
                        label="client", role=ProxyRole.RELAY,
                        upstreams=("leaf",),
                    ),
                ),
                client_label="client",
                client_port=4242,
            )

    def test_pre_existing_excludes_upstreams(self) -> None:
        with pytest.raises(
            ValueError, match="pre_existing_url nodes take no upstreams",
        ):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="x",
                        role=ProxyRole.RELAY,
                        pre_existing_url="ws://192.168.122.1:1235",
                        upstreams=("other",),
                    ),
                    NodeSpec(
                        label="other", role=ProxyRole.LEAF,
                        pre_existing_url="ws://192.168.122.1:1236",
                    ),
                ),
                client_label="x",
                client_port=4242,
            )

    def test_pre_existing_url_must_be_ws_scheme(self) -> None:
        with pytest.raises(ValueError, match="must start with ws://"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        pre_existing_url="http://192.168.122.1:1235",
                    ),
                    NodeSpec(
                        label="client", role=ProxyRole.RELAY,
                        upstreams=("leaf",),
                    ),
                ),
                client_label="client",
                client_port=4242,
            )


# ===========================================================================
# TopologySpec graph validation
# ===========================================================================


class TestTopologySpecValidation:
    def test_duplicate_labels_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate label"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        model_path=Path("/tmp/a.gz"),
                    ),
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        model_path=Path("/tmp/b.gz"),
                    ),
                ),
                client_label="leaf",
                client_port=4242,
            )

    def test_undeclared_upstream_rejected(self) -> None:
        with pytest.raises(
            ValueError, match=r"upstream 'ghost' not declared",
        ):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="relay", role=ProxyRole.RELAY,
                        upstreams=("ghost",),
                    ),
                ),
                client_label="relay",
                client_port=4242,
            )

    def test_cycle_detected(self) -> None:
        with pytest.raises(ValueError, match="cycle detected"):
            # Two RELAYs naming each other as upstream — pathological
            # but the substrate should refuse rather than hang.
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="r1", role=ProxyRole.RELAY,
                        upstreams=("r2",),
                    ),
                    NodeSpec(
                        label="r2", role=ProxyRole.RELAY,
                        upstreams=("r1",),
                    ),
                ),
                client_label="r1",
                client_port=4242,
            )

    def test_client_label_must_exist(self) -> None:
        with pytest.raises(ValueError, match="client_label 'missing'"):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        model_path=Path("/tmp/a.gz"),
                    ),
                ),
                client_label="missing",
                client_port=4242,
            )

    def test_client_label_must_be_spawned(self) -> None:
        with pytest.raises(
            ValueError, match="client_label.*is a pre-existing node",
        ):
            TopologySpec(
                nodes=(
                    NodeSpec(
                        label="leaf", role=ProxyRole.LEAF,
                        pre_existing_url="ws://192.168.122.1:1235",
                    ),
                ),
                client_label="leaf",
                client_port=4242,
            )


# ===========================================================================
# Valid specs (positive controls)
# ===========================================================================


class TestValidSpecs:
    def test_single_leaf(self) -> None:
        spec = TopologySpec(
            nodes=(
                NodeSpec(
                    label="leaf", role=ProxyRole.LEAF,
                    model_path=Path("/tmp/m.gz"),
                ),
            ),
            client_label="leaf",
            client_port=4242,
        )
        assert spec.topological_order == ("leaf",)

    def test_relay_over_pre_existing_leaves(self) -> None:
        """The Tier 3 RELAY-testing shape: a spawned RELAY pointing
        at the user-provided pre-existing LEAFs."""
        spec = TopologySpec(
            nodes=(
                NodeSpec(
                    label="leaf-a", role=ProxyRole.LEAF,
                    pre_existing_url="ws://192.168.122.1:1235",
                ),
                NodeSpec(
                    label="leaf-b", role=ProxyRole.LEAF,
                    pre_existing_url="ws://192.168.122.1:1236",
                ),
                NodeSpec(
                    label="leaf-c", role=ProxyRole.LEAF,
                    pre_existing_url="ws://192.168.122.1:1237",
                ),
                NodeSpec(
                    label="relay", role=ProxyRole.RELAY,
                    upstreams=("leaf-a", "leaf-b", "leaf-c"),
                ),
            ),
            client_label="relay",
            client_port=4242,
        )
        # All three pre-existing nodes come before the relay.
        order = spec.topological_order
        assert order.index("relay") == 3
        assert set(order[:3]) == {"leaf-a", "leaf-b", "leaf-c"}

    def test_selector_chained_over_relay_over_leafs(self) -> None:
        """A three-tier chain — exercises the topological-sort
        correctness on a non-trivial graph."""
        spec = TopologySpec(
            nodes=(
                NodeSpec(
                    label="leaf-a", role=ProxyRole.LEAF,
                    model_path=Path("/tmp/a.gz"),
                ),
                NodeSpec(
                    label="leaf-b", role=ProxyRole.LEAF,
                    model_path=Path("/tmp/b.gz"),
                ),
                NodeSpec(
                    label="relay", role=ProxyRole.RELAY,
                    upstreams=("leaf-a", "leaf-b"),
                ),
                NodeSpec(
                    label="selector", role=ProxyRole.SELECTOR,
                    upstreams=("relay",),
                ),
            ),
            client_label="selector",
            client_port=4242,
        )
        order = spec.topological_order
        # Leaves before relay before selector.
        assert set(order[:2]) == {"leaf-a", "leaf-b"}
        assert order[2] == "relay"
        assert order[3] == "selector"


# ===========================================================================
# Topological sort algorithm
# ===========================================================================


class TestTopologicalOrder:
    def test_no_upstreams_returns_all_in_label_order(self) -> None:
        """Tie-break is sorted by label for determinism — important so
        log files / test outputs stay reproducible across runs."""
        nodes = (
            NodeSpec(
                label="z", role=ProxyRole.LEAF,
                model_path=Path("/tmp/z.gz"),
            ),
            NodeSpec(
                label="a", role=ProxyRole.LEAF,
                model_path=Path("/tmp/a.gz"),
            ),
            NodeSpec(
                label="m", role=ProxyRole.LEAF,
                model_path=Path("/tmp/m.gz"),
            ),
        )
        order = _topological_order(nodes)
        assert order == ("a", "m", "z")

    def test_diamond_dependency(self) -> None:
        """Diamond: leaf → r1, r2; r1, r2 → top. Order must place
        leaf first, top last, r1/r2 in the middle in either order."""
        nodes = (
            NodeSpec(
                label="leaf", role=ProxyRole.LEAF,
                model_path=Path("/tmp/l.gz"),
            ),
            NodeSpec(
                label="r1", role=ProxyRole.RELAY,
                upstreams=("leaf",),
            ),
            NodeSpec(
                label="r2", role=ProxyRole.RELAY,
                upstreams=("leaf",),
            ),
            NodeSpec(
                label="top", role=ProxyRole.RELAY,
                upstreams=("r1", "r2"),
            ),
        )
        order = _topological_order(nodes)
        assert order[0] == "leaf"
        assert order[-1] == "top"
        assert set(order[1:3]) == {"r1", "r2"}
