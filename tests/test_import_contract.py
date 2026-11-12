"""Repository-wide mechanical guard against function-local imports."""
from __future__ import annotations

import ast
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent
_IGNORED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
}


class _FunctionImportVisitor(ast.NodeVisitor):
    """Find imports whose execution is deferred until a function is called."""

    def __init__(self) -> None:
        self._function_depth = 0
        self.violations: list[tuple[int, str]] = []

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        # Decorators/defaults execute at definition time, not call time.
        for decorator in node.decorator_list:
            self.visit(decorator)
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if default is not None:
                self.visit(default)
        self._function_depth += 1
        for statement in node.body:
            self.visit(statement)
        self._function_depth -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_Import(self, node: ast.Import) -> None:
        if self._function_depth:
            names = ", ".join(alias.name for alias in node.names)
            self.violations.append((node.lineno, f"import {names}"))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if self._function_depth:
            module = "." * node.level + (node.module or "")
            names = ", ".join(alias.name for alias in node.names)
            self.violations.append((node.lineno, f"from {module} import {names}"))


def test_python_sources_have_no_function_local_imports() -> None:
    """Every runtime dependency is visible at the module boundary.

    Type-only dependencies belong under TYPE_CHECKING at module scope;
    optional dependencies use a module-scope try/except plus an explicit
    availability check at the point of use. Function-body imports conceal
    dependency edges and make module initialization order do architectural work.
    """
    violations: list[str] = []
    for path in sorted(_ROOT.rglob("*.py")):
        relative = path.relative_to(_ROOT)
        if any(part in _IGNORED_PARTS for part in relative.parts):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            violations.append(f"{relative}: could not parse source: {exc}")
            continue
        visitor = _FunctionImportVisitor()
        visitor.visit(tree)
        violations.extend(
            f"{relative}:{line}: {statement}"
            for line, statement in visitor.violations
        )

    assert not violations, (
        "Function-local imports hide dependency edges. Move runtime imports "
        "to module scope, and place type-only imports under TYPE_CHECKING:\n"
        + "\n".join(violations)
    )
