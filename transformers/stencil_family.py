"""Single owner of stencil-family parsing and the active family definition."""
from __future__ import annotations

from pathlib import Path

from transformers.stencil_solver_geometry import Stencil


def parse_stencil_family(source: str) -> tuple[Stencil, ...]:
    """Parse and validate the repository's name:/ASCII-row stencil format."""
    lines = source.splitlines()
    family: list[Stencil] = []
    names: set[str] = set()
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        index += 1
        if not line:
            continue
        if not line.startswith("name:"):
            raise ValueError(f"expected 'name:' at family line {index}")
        name = line.partition(":")[2].strip()
        if not name or name in names:
            raise ValueError(f"empty or duplicate stencil name at family line {index}")
        rows: list[str] = []
        while index < len(lines) and lines[index].strip():
            row = lines[index].strip()
            if any(char not in "#." for char in row):
                raise ValueError(f"invalid stencil row at family line {index + 1}")
            rows.append(row)
            index += 1
        if not rows or not any("#" in row for row in rows):
            raise ValueError(f"stencil {name!r} has no occupied cells")
        width = max(map(len, rows))
        if any(len(row) != width for row in rows):
            raise ValueError(f"stencil {name!r} has ragged rows")
        cells = tuple(
            (y, x)
            for y, row in enumerate(rows)
            for x, char in enumerate(row)
            if char == "#"
        )
        pending = {cells[0]}
        reached: set[tuple[int, int]] = set()
        cell_set = set(cells)
        while pending:
            y, x = pending.pop()
            if (y, x) in reached:
                continue
            reached.add((y, x))
            pending.update(
                (ny, nx)
                for ny, nx in ((y - 1, x), (y + 1, x), (y, x - 1), (y, x + 1))
                if (ny, nx) in cell_set and (ny, nx) not in reached
            )
        if reached != cell_set:
            raise ValueError(f"stencil {name!r} is not 4-connected")
        family.append((name, cells))
        names.add(name)
    if not family:
        raise ValueError("stencil family is empty")
    return tuple(family)


STENCIL_FAMILY: tuple[Stencil, ...] = parse_stencil_family(
    Path(__file__).with_name("stencil_family.txt").read_text(encoding="utf-8")
)
