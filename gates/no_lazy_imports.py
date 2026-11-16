#!/usr/bin/env python3
"""Mechanical gate for the project law: lazy imports are forbidden.

Every runtime import belongs at module/class definition time.  An ``import``
or ``from ... import`` nested inside a function or method is a violation; the
module is mis-factored if some importers should not pay that dependency.  Split
the module at the dependency boundary instead of deferring the import.

Allowed by construction:
- imports at module scope, including imports under module-level ``if``/``try``;
- imports in class bodies (executed when the class is defined);
- imports in ``if TYPE_CHECKING:`` blocks (the body never executes at runtime).

No per-file or per-import allowlist exists.  Historical snapshots and generated
or vendored trees are excluded because they are evidence/artifacts, not live
source.  In a Git checkout, the default reads staged bytes for tracked Python
files; ``--tree`` reads working-tree bytes.  In an extracted source archive,
the gate falls back to scanning the complete working tree.

Usage:
    python gates/no_lazy_imports.py [root] [--tree]
"""
from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence, Tuple

REPO = Path(__file__).resolve().parents[1]
EXCLUDE_PARTS = {
    ".git", ".staging", ".venv", "venv", "venvs", "node_modules",
    "__pycache__", ".pytest_cache", "legacy",
}
EXCLUDE_PATH_PREFIXES: Tuple[str, ...] = ()
_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)


def _git_env() -> dict[str, str]:
    """Return an environment that does not inherit another worktree's Git path."""
    env = dict(os.environ)
    for name in (
        "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR",
        "GIT_PREFIX", "GIT_CEILING_DIRECTORIES",
    ):
        env.pop(name, None)
    env["GIT_OPTIONAL_LOCKS"] = "0"
    return env


def _run_git(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        env=_git_env(),
    )


def _excluded(relative_path: Path) -> bool:
    parts = set(relative_path.parts)
    if parts & EXCLUDE_PARTS:
        return True
    normalized = relative_path.as_posix()
    return any(normalized.startswith(prefix) for prefix in EXCLUDE_PATH_PREFIXES)


def _git_root(root: Path) -> Optional[Path]:
    result = _run_git(root, "rev-parse", "--show-toplevel")
    if result.returncode != 0:
        return None
    try:
        return Path(os.fsdecode(result.stdout.strip())).resolve()
    except (OSError, ValueError):
        return None


def _tracked_files(root: Path, repo_root: Path) -> list[Tuple[Path, str]]:
    """Return (worktree path, repository-relative index path) pairs."""
    result = _run_git(root, "ls-files", "-z", "--full-name", "--", "*.py")
    if result.returncode != 0:
        raise RuntimeError(
            "git ls-files failed: " + os.fsdecode(result.stderr).strip()
        )
    try:
        root_relative = root.resolve().relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeError(f"scan root {root} is outside Git root {repo_root}") from exc

    entries: list[Tuple[Path, str]] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        repo_relative = Path(os.fsdecode(raw))
        try:
            local_relative = repo_relative.relative_to(root_relative)
        except ValueError:
            continue
        if _excluded(local_relative):
            continue
        entries.append((repo_root / repo_relative, repo_relative.as_posix()))
    return entries


def _tree_files(root: Path) -> list[Tuple[Path, str]]:
    entries: list[Tuple[Path, str]] = []
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root)
        if not _excluded(relative):
            entries.append((path, relative.as_posix()))
    return entries


def _read_source(
    path: Path,
    repo_path: str,
    repo_root: Optional[Path],
    use_tree: bool,
) -> str:
    if use_tree or repo_root is None:
        return path.read_text(encoding="utf-8")
    result = _run_git(repo_root, "show", f":{repo_path}")
    if result.returncode != 0:
        detail = os.fsdecode(result.stderr).strip()
        raise OSError(f"cannot read staged blob for {repo_path}: {detail}")
    return result.stdout.decode("utf-8")


def _type_checking_names(tree: ast.Module) -> Tuple[set[str], set[str]]:
    """Resolve TYPE_CHECKING names and ``typing`` module aliases at file scope."""
    names = {"TYPE_CHECKING"}
    typing_modules = {"typing"}

    def visit_module_scope(node: ast.AST) -> None:
        # Imports nested in module-level if/try/with blocks are still eager.
        # Do not inspect function/class bodies: their imports are runtime-deferred.
        if isinstance(node, (_FUNCTIONS, ast.ClassDef)):
            return
        if isinstance(node, ast.ImportFrom) and node.module in {
            "typing", "typing_extensions"
        }:
            for alias in node.names:
                if alias.name == "TYPE_CHECKING":
                    names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "typing":
                    typing_modules.add(alias.asname or alias.name)
        for child in ast.iter_child_nodes(node):
            visit_module_scope(child)

    visit_module_scope(tree)
    return names, typing_modules


def _is_type_checking_test(
    node: ast.AST, names: set[str], typing_modules: set[str]
) -> bool:
    if isinstance(node, ast.Name):
        return node.id in names
    if (
        isinstance(node, ast.Attribute)
        and node.attr == "TYPE_CHECKING"
        and isinstance(node.value, ast.Name)
    ):
        return node.value.id in typing_modules
    return False


def _import_text(node: ast.Import | ast.ImportFrom) -> str:
    if isinstance(node, ast.ImportFrom):
        module = "." * node.level + (node.module or "")
        return f"from {module} import " + ", ".join(alias.name for alias in node.names)
    return "import " + ", ".join(alias.name for alias in node.names)


def violations_in_source(source: str, display_path: str = "<source>") -> list[str]:
    """Return every function/method-body import violation in source text."""
    try:
        tree = ast.parse(source, filename=display_path)
    except (SyntaxError, UnicodeDecodeError) as exc:
        return [
            f"{display_path}:0: UNPARSEABLE ({exc.__class__.__name__}) "
            "— gate cannot certify this file"
        ]

    type_names, typing_modules = _type_checking_names(tree)
    violations: list[str] = []

    def walk(
        node: ast.AST, scope_stack: Tuple[str, ...], function_depth: int
    ) -> None:
        if isinstance(node, ast.If) and _is_type_checking_test(
            node.test, type_names, typing_modules
        ):
            walk(node.test, scope_stack, function_depth)
            # The TYPE_CHECKING body is never executed; its entire subtree is exempt.
            for child in node.orelse:
                walk(child, scope_stack, function_depth)
            return
        if isinstance(node, ast.ClassDef):
            for field_name, value in ast.iter_fields(node):
                if field_name == "body":
                    for child in value:
                        walk(child, scope_stack + (node.name,), function_depth)
                elif isinstance(value, ast.AST):
                    walk(value, scope_stack, function_depth)
                elif isinstance(value, list):
                    for child in value:
                        if isinstance(child, ast.AST):
                            walk(child, scope_stack, function_depth)
            return
        if isinstance(node, _FUNCTIONS):
            # Defaults, decorators and annotations are evaluated at definition time;
            # the body is the only region whose imports are deferred until invocation.
            for field_name, value in ast.iter_fields(node):
                if field_name == "body":
                    for child in value:
                        walk(
                            child, scope_stack + (node.name,), function_depth + 1
                        )
                elif isinstance(value, ast.AST):
                    walk(value, scope_stack, function_depth)
                elif isinstance(value, list):
                    for child in value:
                        if isinstance(child, ast.AST):
                            walk(child, scope_stack, function_depth)
            return
        if isinstance(node, (ast.Import, ast.ImportFrom)) and function_depth:
            out_path = display_path
            out_context = ".".join(scope_stack)
            violations.append(
                f"{out_path}:{node.lineno}: {_import_text(node)}  "
                f"(inside {out_context})"
            )
            return
        for child in ast.iter_child_nodes(node):
            walk(child, scope_stack, function_depth)

    walk(tree, (), 0)
    return violations


def scan_tree(root: Path, use_tree: bool = False) -> list[str]:
    """Scan live Python files under *root*; staged bytes are the Git default."""
    root = root.resolve()
    repo_root = _git_root(root)
    if repo_root is None:
        entries = _tree_files(root)
        repo_root = None
    else:
        entries = _tracked_files(root, repo_root)

    violations: list[str] = []
    for path, repo_path in entries:
        try:
            source = _read_source(path, repo_path, repo_root, use_tree)
        except (OSError, UnicodeDecodeError) as exc:
            try:
                display = path.relative_to(root).as_posix()
            except ValueError:
                display = str(path)
            violations.append(
                f"{display}:0: UNREADABLE ({exc.__class__.__name__}) "
                "— gate cannot certify this file"
            )
            continue
        try:
            display = path.relative_to(root).as_posix()
        except ValueError:
            display = str(path)
        violations.extend(violations_in_source(source, display))
    return violations


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=REPO)
    parser.add_argument(
        "--tree", action="store_true",
        help="read working-tree bytes instead of staged Git blobs",
    )
    args = parser.parse_args(argv)
    try:
        violations = scan_tree(args.root, use_tree=args.tree)
    except (OSError, RuntimeError) as exc:
        print(f"NO-LAZY-IMPORTS GATE ERROR: {exc}", file=sys.stderr)
        return 2
    if violations:
        print(f"LAZY-IMPORT VIOLATIONS ({len(violations)})")
        print("\n".join(violations))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
