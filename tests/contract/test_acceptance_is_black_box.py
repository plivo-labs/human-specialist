"""Acceptance tests reach HS only through its public API and events: no `hs` import, no direct database handle."""

import ast
from pathlib import Path

ACCEPTANCE = Path(__file__).parents[1] / "acceptance"


def _breaks_the_boundary(node: ast.AST) -> bool:
    if isinstance(node, ast.Import):
        return any(a.name.split(".")[0] == "hs" for a in node.names)
    if isinstance(node, ast.ImportFrom):
        return node.level == 0 and (node.module or "").split(".")[0] == "hs"
    return isinstance(node, ast.Attribute) and node.attr == "_db"


def test_no_acceptance_test_imports_hs_or_touches_the_database():
    bad = [
        f"{path.name}:{node.lineno}"
        for path in sorted(ACCEPTANCE.rglob("*.py"))
        for node in ast.walk(ast.parse(path.read_text()))
        if _breaks_the_boundary(node)
    ]
    assert not bad, f"acceptance tests are black-box; use the public API instead: {bad}"
