"""
tests/test_identity_types.py — Namespace-contract regression tests for
the identity-type branding migration (Phase 3).

Per `proxy/docs/roadmap-identity-type-branding.md` §6 Rule 3, this
file pins the branding contract at the type level using
``typing.assert_type``. The tests below are evaluated at runtime as
``pass``-shaped assertions (assert_type is a no-op at runtime), but
fail typecheck if the branding contract regresses — e.g., if a future
refactor accidentally widens ``IdMapping.register``'s return from
``InternalId`` back to ``str``.

The tests are organised in three layers, mirroring the
ClientId → InternalId → CanonicalId → WireId namespace chain:

  1. Construction: the four NewType constructors return their
     branded type at runtime (str-identity) and at typecheck.
  2. Boundary translation: the framework's translation surfaces
     (IdMapping, ProxyLink) preserve namespace at construction
     and rebrand correctly on translation.
  3. Negative space: deliberate type errors are recorded in
     ``test_brand_confusion_fails_typecheck`` as commented-out
     lines with the expected mypy diagnostic, so future readers
     see the contract and the typechecker enforces it.

Run from the proxy directory:
  ``pytest tests/test_identity_types.py`` (runtime — should pass)
  ``mypy tests/test_identity_types.py`` (typecheck — should pass; the
    commented-out negative lines are documentation, not under-test)

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

# typing.assert_type is exported on Python 3.11+, but mypy's
# tracking of that vs. typing_extensions is finicky depending on
# interpreter version. typing_extensions ships a backport with
# identical semantics; importing from there keeps the typecheck path
# stable across Python versions.
from typing_extensions import assert_type

from AbstractProxy.proxy_core import (  # noqa: E402
    CanonicalId,
    ClientId,
    IdMapping,
    InternalId,
    WireId,
)
from katago.katago_proxy import katago_id_generator  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Construction: NewType constructors return branded types
# ---------------------------------------------------------------------------


def test_client_id_constructor_brands_str() -> None:
    raw = "client-123"
    branded = ClientId(raw)
    # Runtime: still a string.
    assert isinstance(branded, str)
    assert branded == raw
    # Typecheck: branded carries ClientId, not str.
    assert_type(branded, ClientId)


def test_internal_id_constructor_brands_str() -> None:
    raw = "internal-456"
    branded = InternalId(raw)
    assert isinstance(branded, str)
    assert_type(branded, InternalId)


def test_canonical_id_constructor_brands_str() -> None:
    raw = "canonical-789"
    branded = CanonicalId(raw)
    assert isinstance(branded, str)
    assert_type(branded, CanonicalId)


def test_wire_id_constructor_brands_str() -> None:
    raw = "wire-000"
    branded = WireId(raw)
    assert isinstance(branded, str)
    assert_type(branded, WireId)


# ---------------------------------------------------------------------------
# 2. Boundary translation: IdMapping preserves the (U, D) namespace pair
# ---------------------------------------------------------------------------


def test_id_mapping_register_preserves_namespaces() -> None:
    """A fresh ``IdMapping[ClientId, InternalId]`` register call:

      - accepts upstream ClientId,
      - mints a downstream InternalId via the generator,
      - returns the downstream brand (not the upstream, not bare str).
    """
    mapping: IdMapping[ClientId, InternalId] = IdMapping(
        generator=katago_id_generator,
    )
    upstream = ClientId("client-abc")
    downstream = mapping.register(upstream)
    # The return is an InternalId at typecheck — assert_type forces a
    # match. If the IdMapping or katago_id_generator ever widens to
    # str (or confuses U/D), this assertion fails.
    assert_type(downstream, InternalId)
    # Reverse lookup returns the upstream brand.
    looked_up = mapping.reverse(downstream)
    assert_type(looked_up, "ClientId | None")
    assert looked_up == upstream


def test_id_mapping_forward_preserves_downstream_brand() -> None:
    mapping: IdMapping[ClientId, InternalId] = IdMapping(
        generator=katago_id_generator,
    )
    upstream = ClientId("client-xyz")
    mapping.register(upstream)
    fwd = mapping.forward(upstream)
    assert_type(fwd, "InternalId | None")


# ---------------------------------------------------------------------------
# 3. Generator signature is U → D, not str → str
# ---------------------------------------------------------------------------


def test_katago_id_generator_signature_is_branded() -> None:
    """``katago_id_generator`` is the IdGenerator at the per-session
    boundary; its signature must be ``ClientId -> InternalId``.

    The assert_type below pins the return type at the call site.
    """
    upstream = ClientId("client-id-1")
    result = katago_id_generator(upstream)
    assert_type(result, InternalId)


# ---------------------------------------------------------------------------
# 4. Negative space: documented type errors
# ---------------------------------------------------------------------------
#
# The lines below are intentional type errors recorded as comments so
# future readers see what the brand discipline forbids. Uncommenting
# any of them must produce a mypy error of the indicated kind. They
# are documentation, not under-test — pytest doesn't run them.
#
#   def _negative_examples() -> None:
#       mapping: IdMapping[ClientId, InternalId] = IdMapping(
#           generator=katago_id_generator,
#       )
#       iid = mapping.register(InternalId("wrong-direction"))
#       # mypy: Argument 1 to "register" has incompatible type
#       # "InternalId"; expected "ClientId"  [arg-type]
#
#       cid: ClientId = InternalId("oops")
#       # mypy: Incompatible types in assignment (expression has type
#       # "InternalId", variable has type "ClientId")  [assignment]
#
#       wid: WireId = "raw-str"
#       # mypy: Incompatible types in assignment (expression has type
#       # "str", variable has type "WireId")  [assignment]
#
# The presence of these comments is itself the regression contract:
# the rules they document are the brand discipline this migration
# established.


# ---------------------------------------------------------------------------
# Sanity: brands are not implicitly assignable to each other
# ---------------------------------------------------------------------------


def test_brand_runtime_equality_is_string_identity() -> None:
    """The brands are runtime-identity over str — equal-string brands
    compare equal regardless of which namespace they're declared as.

    This is the runtime contract that makes branding zero-overhead:
    NewType is a no-op at runtime. The typecheck contract (forbids
    confusion) is the load-bearing layer; this test pins the runtime
    side as documented.

    The cross-brand comparisons are written via the underlying str
    value so mypy doesn't reject them as non-overlapping; the runtime
    contract is what's being pinned here, not the typecheck.
    """
    raw = "shared-string"
    cid = ClientId(raw)
    iid = InternalId(raw)
    can = CanonicalId(raw)
    wid = WireId(raw)
    # All compare equal at runtime because they're all str under the
    # hood. The typecheck still distinguishes them; comparing through
    # str() makes the equality check overlap-sound at type level.
    assert str(cid) == str(iid) == str(can) == str(wid) == raw
