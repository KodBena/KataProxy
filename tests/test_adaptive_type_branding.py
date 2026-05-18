"""
tests/test_adaptive_type_branding.py — Namespace-contract regression
tests for the move/turn brand migration (v1.0.22).

Per `proxy/docs/roadmap-adaptive-type-branding.md` §6, this file pins
the move/turn branding contract at the type level using
``typing.assert_type``. The tests below are evaluated at runtime as
``pass``-shaped assertions (assert_type is a no-op at runtime), but
fail typecheck if the branding contract regresses — e.g., if a future
refactor accidentally widens ``move_to_turn_pair``'s return from
``tuple[TurnIndex, TurnIndex]`` back to ``tuple[int, int]``.

Three layers, mirroring `test_identity_types.py`'s shape:

  1. Construction: the two NewType constructors return their branded
     type at runtime (int-identity) and at typecheck. Color admits
     only the two literal values.
  2. Translation seam: ``move_to_turn_pair`` returns a typed pair with
     the correct arithmetic (Black's m-th move → (2m, 2m+1); White's
     m-th move → (2m+1, 2m+2)).
  3. Negative space: deliberate type errors are recorded as
     commented-out lines with the expected mypy diagnostic, so future
     readers see the contract and the typechecker enforces it.

Run from the proxy directory:
  ``pytest tests/test_adaptive_type_branding.py`` (runtime — passes)
  ``mypy tests/test_adaptive_type_branding.py`` (typecheck — passes;
    the commented-out negative lines are documentation, not under-test)

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

# typing.assert_type is exported on Python 3.11+, but mypy's tracking
# of that vs. typing_extensions is finicky depending on interpreter
# version. typing_extensions ships a backport with identical semantics;
# importing from there keeps the typecheck path stable across Python
# versions (matches the precedent in tests/test_identity_types.py).
from typing_extensions import assert_type

from katago import (  # noqa: E402
    Color,
    MoveIndex,
    TurnIndex,
    move_to_turn_pair,
)


# ---------------------------------------------------------------------------
# 1. Construction: NewType constructors return branded types
# ---------------------------------------------------------------------------


def test_move_index_constructor_brands_int() -> None:
    raw = 3
    branded = MoveIndex(raw)
    # Runtime: still an int (NewType is identity at runtime).
    assert isinstance(branded, int)
    assert branded == raw
    # Typecheck: branded carries MoveIndex, not int.
    assert_type(branded, MoveIndex)


def test_turn_index_constructor_brands_int() -> None:
    raw = 7
    branded = TurnIndex(raw)
    assert isinstance(branded, int)
    assert branded == raw
    assert_type(branded, TurnIndex)


def test_color_admits_black_and_white() -> None:
    # Color is a Literal alias; the bare string literals "black" and
    # "white" satisfy it natively without an explicit constructor call.
    # That the assignments compile is the contract. (mypy narrows the
    # assignment target to the specific literal — Literal["black"] —
    # which is a subtype of the wider Color = Literal["black", "white"];
    # the wider-type assertion lives in test_color_function_signature
    # below.)
    black: Color = "black"
    white: Color = "white"
    assert black == "black"
    assert white == "white"


def test_color_function_signature() -> None:
    # A function parameter typed as Color exposes the full
    # Literal["black", "white"] union, not the narrowed singleton mypy
    # infers for a direct literal assignment. This is the canonical
    # way to pin the wider-type contract.
    def echo(c: Color) -> Color:
        return c

    result_b = echo("black")
    result_w = echo("white")
    assert_type(result_b, Color)
    assert_type(result_w, Color)


# ---------------------------------------------------------------------------
# 2. Translation seam: move_to_turn_pair
# ---------------------------------------------------------------------------


def test_move_to_turn_pair_signature() -> None:
    color: Color = "black"
    m = MoveIndex(2)
    pair = move_to_turn_pair(color, m)
    # Typecheck: returns tuple[TurnIndex, TurnIndex].
    assert_type(pair, tuple[TurnIndex, TurnIndex])


def test_move_to_turn_pair_emits_branded_elements() -> None:
    # Each element of the returned tuple carries TurnIndex, not int.
    before, after = move_to_turn_pair("black", MoveIndex(3))
    assert_type(before, TurnIndex)
    assert_type(after, TurnIndex)


def test_move_to_turn_pair_black_arithmetic() -> None:
    # Black's m-th move: turns (2m, 2m+1).
    before, after = move_to_turn_pair("black", MoveIndex(0))
    assert int(before) == 0
    assert int(after) == 1

    before, after = move_to_turn_pair("black", MoveIndex(2))
    assert int(before) == 4
    assert int(after) == 5

    before, after = move_to_turn_pair("black", MoveIndex(10))
    assert int(before) == 20
    assert int(after) == 21


def test_move_to_turn_pair_white_arithmetic() -> None:
    # White's m-th move: turns (2m+1, 2m+2).
    before, after = move_to_turn_pair("white", MoveIndex(0))
    assert int(before) == 1
    assert int(after) == 2

    before, after = move_to_turn_pair("white", MoveIndex(2))
    assert int(before) == 5
    assert int(after) == 6

    before, after = move_to_turn_pair("white", MoveIndex(10))
    assert int(before) == 21
    assert int(after) == 22


# ---------------------------------------------------------------------------
# 3. Negative space: deliberate type errors (commented-out)
# ---------------------------------------------------------------------------
#
# The following lines record contract violations that mypy --strict
# catches. Uncommenting any of them produces the documented mypy
# diagnostic; they live here as documentation, not under-test (mirrors
# `test_identity_types.py`'s discipline).
#
# def test_brand_confusion_fails_typecheck() -> None:
#     m = MoveIndex(3)
#     t = TurnIndex(7)
#
#     # Mismatched brands cannot be silently exchanged in dict/list
#     # contexts:
#     # x: TurnIndex = m
#     # ↳ mypy: error: Incompatible types in assignment
#     #         (expression has type "MoveIndex", variable has type "TurnIndex")
#
#     # y: MoveIndex = t
#     # ↳ mypy: error: Incompatible types in assignment
#     #         (expression has type "TurnIndex", variable has type "MoveIndex")
#
#     # Passing a TurnIndex where a MoveIndex is expected fails:
#     # pair = move_to_turn_pair("black", t)
#     # ↳ mypy: error: Argument 2 to "move_to_turn_pair" has
#     #         incompatible type "TurnIndex"; expected "MoveIndex"
#
#     # Color admits only "black" and "white":
#     # bad_color: Color = "red"
#     # ↳ mypy: error: Incompatible types in assignment
#     #         (expression has type "str", variable has type
#     #          "Literal['black', 'white']")
