import ast
from collections import Counter
from pathlib import Path

SRC = Path(__file__).parents[2] / "src" / "hs"


def test_no_module_assigns_a_top_level_name_twice():
    """Two branches can each add a constant, class or function of the same name to one module; the later one silently wins."""
    twice = {}
    for f in SRC.rglob("*.py"):
        tree = ast.parse(f.read_text())
        top = [n for n in tree.body if isinstance(n, (ast.Assign, ast.AnnAssign))]
        targets = [t for n in top for t in (n.targets if isinstance(n, ast.Assign) else [n.target])]
        names = Counter(
            x.id for t in targets for x in ast.walk(t) if isinstance(x, ast.Name) and isinstance(x.ctx, ast.Store)
        )
        names.update(n.name for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)))
        if dup := sorted(n for n, k in names.items() if k > 1):
            twice[str(f.relative_to(SRC))] = dup
    assert twice == {}
