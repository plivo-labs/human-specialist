"""An acceptance test proves an event's absence on the replay, never on a live stream.

fence() waits until an event is replayable, not until a stream has delivered it, so "fence, then no seen(stream)"
can pass after the event was written. Use `replayed(tenant, type, ...)`. On an agent's scoped stream, wait for a
later event by id on that stream first: a stream delivers in seq order.

Flagged, on any `seen(...)` call: `not seen(...)`, `seen(...) == []`, a count of it compared with 0 or 1 as
`== 0`, `< 1`, `<= 0`, and a count compared with a variable outside a wait lambda (a baseline that must not grow).
"""

import ast
from pathlib import Path

import pytest

ACCEPTANCE = Path(__file__).parents[1] / "acceptance"


def _is_seen(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and getattr(node.func, "id", None) == "seen"


def _is_count(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call) and getattr(node.func, "id", None) == "len" and any(_is_seen(a) for a in node.args)
    )


def _literal(node: ast.AST):
    return node.value if isinstance(node, ast.Constant) else None


def absence_lines(source: str) -> list[int]:
    found: list[int] = []

    def visit(node: ast.AST, in_lambda: bool) -> None:
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not) and _is_seen(node.operand):
            found.append(node.lineno)
        if isinstance(node, ast.Compare):
            parts = [node.left, *node.comparators]
            for left, op, right in zip(parts, node.ops, parts[1:], strict=False):
                for a, b in ((left, right), (right, left)):
                    if _is_seen(a) and isinstance(op, (ast.Eq, ast.NotEq)) and isinstance(b, ast.List) and not b.elts:
                        found.append(node.lineno)
                    if _is_count(a):
                        n = _literal(b)
                        zero = (type(op), n) in {(ast.Eq, 0), (ast.LtE, 0)} or (
                            type(op) is ast.Lt and n == 1 and a is left
                        )
                        baseline = isinstance(op, ast.Eq) and isinstance(b, ast.Name) and not in_lambda
                        if zero or baseline:
                            found.append(node.lineno)
        for child in ast.iter_child_nodes(node):
            visit(child, in_lambda or isinstance(node, ast.Lambda))

    visit(ast.parse(source), False)
    return sorted(set(found))


def test_no_acceptance_test_checks_absence_on_a_live_stream():
    bad = [
        f"{path.relative_to(ACCEPTANCE)}:{n}"
        for path in sorted(ACCEPTANCE.rglob("*.py"))
        for n in absence_lines(path.read_text())
    ]
    assert not bad, "absence checked on a live stream; use replayed(tenant, ...):\n" + "\n".join(bad)


@pytest.mark.parametrize(
    "snippet",
    [
        "assert not seen(s, 'x')",
        "assert not seen(\n    s,\n    'x',\n)",
        "assert len(seen(s, 'x')) == 0",
        "assert len( seen(s,'x') )==0",
        "assert 0 == len(seen(s, 'x'))",
        "assert seen(s, 'x') == []",
        "assert [] == seen(s, 'x')",
        "assert len(seen(s, 'x')) < 1",
        "assert len(seen(s, 'x')) <= 0",
        "assert len(seen(s, 'x')) == offers",
        "assert (\n    len(seen(s, 'x'))\n    == offers\n)",
    ],
)
def test_the_check_flags_absence_on_a_stream(snippet):
    assert absence_lines(snippet)


@pytest.mark.parametrize(
    "snippet",
    [
        "assert seen(s, 'x')",
        "assert len(seen(s, 'x')) == 2",
        "assert len(seen(s, 'x')) >= 1",
        "world.wait(lambda n=n: len(seen(s, 'x')) == n)",
        "assert not replayed(t, 'x')",
        "assert not other(s, 'x')",
    ],
)
def test_the_check_allows_presence_and_the_replay(snippet):
    assert not absence_lines(snippet)
