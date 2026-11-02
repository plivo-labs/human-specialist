"""SQL text may interpolate only module-level constants; bind every value.

An f-string, `%` format or `.format()` whose literal text holds SQL fails unless each interpolated
expression is a literal or an UPPER_CASE name bound at module level, or the statement carries
`# sql: constants only`. A format string held in a module-level name counts as a literal.
`ruff check` (S608) is the cheap first pass; this catches what it cannot see.
"""

import ast
import re
import textwrap
from pathlib import Path

import hs

SQL = re.compile(
    r"\b(?:SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM|FROM|WHERE|JOIN|VALUES|RETURNING|ORDER\s+BY|GROUP\s+BY"
    r"|HAVING|UNION|LIMIT|OFFSET)\b"
    r"|(?i:\bselect\b.+\bfrom\b|\binsert\s+into\b|\bdelete\s+from\b|\bupdate\s+\w+\s+set\b)",
    re.S,
)
EXEMPT = "# sql: constants only"
CONSTANT = re.compile(r"_*[A-Z][A-Z0-9_]*$")


def module_scope(tree: ast.Module) -> tuple[set[str], dict[str, str]]:
    """Names bound at module level, and the ones bound to a string literal."""
    names, strings = set(), {}
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names |= {(a.asname or a.name).split(".")[0] for a in node.names}
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                if isinstance(target, ast.Name):
                    names.add(target.id)
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        strings[target.id] = node.value.value
    return names, strings


def is_constant(node: ast.expr, names: set[str]) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.Name):
        return node.id in names and bool(CONSTANT.match(node.id))
    if isinstance(node, ast.Attribute):
        root = node.value
        while isinstance(root, ast.Attribute):
            root = root.value
        return isinstance(root, ast.Name) and root.id in names and bool(CONSTANT.match(node.attr))
    return False


def literal(node: ast.expr, strings: dict[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return strings.get(node.id) if isinstance(node, ast.Name) else None


def formatting(node: ast.AST, strings: dict[str, str]) -> tuple[str, list[ast.expr]] | None:
    """(literal text, interpolated expressions) when `node` formats a string."""
    if isinstance(node, ast.JoinedStr):
        text = "".join(v.value for v in node.values if isinstance(v, ast.Constant))
        return text, [v.value for v in node.values if isinstance(v, ast.FormattedValue)]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        text = literal(node.left, strings)
        if text is not None:
            right = node.right
            values = (
                right.elts if isinstance(right, ast.Tuple) else right.values if isinstance(right, ast.Dict) else [right]
            )
            return text, list(values)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        text = literal(node.func.value, strings)
        if text is not None:
            return text, [*node.args, *(k.value for k in node.keywords)]
    return None


def violations(source: str) -> list[int]:
    """Line numbers of SQL text built from anything but constants."""
    tree = ast.parse(source)
    names, strings = module_scope(tree)
    lines = source.splitlines()
    found = []
    for node in ast.walk(tree):
        fmt = formatting(node, strings)
        if fmt is None or not SQL.search(fmt[0]) or all(is_constant(e, names) for e in fmt[1]):
            continue
        if not any(EXEMPT in line for line in lines[node.lineno - 1 : node.end_lineno]):
            found.append(node.lineno)
    return sorted(set(found))


FIXTURE = textwrap.dedent(
    """
    from sqlalchemy import text

    TABLE = "agents"
    _JOIN = "JOIN teams t ON t.id = a.team"
    QUERY = "SELECT id FROM agents WHERE skill IN (%s)"

    def bad(skills, team, order):
        a = text("SELECT id FROM agents WHERE skill IN (%s)" % ",".join(skills))
        b = text(f"SELECT id FROM {TABLE} WHERE team = '{team}'")
        c = text("DELETE FROM agents WHERE id = {}".format(team))
        d = text(QUERY % skills)
        e = text(f"select id from agents order by {order}")
        return a, b, c, d, e

    def good(team, order):
        a = text(f"SELECT id FROM {TABLE} a {_JOIN} WHERE a.team = :team")
        b = text("SELECT id FROM %s" % TABLE)
        c = text(f"SELECT id FROM agents ORDER BY {order}")  # sql: constants only
        d = f"agent {team} left the queue"
        return a, b, c, d
    """
)


def test_fixture_flags_every_formatted_value():
    assert violations(FIXTURE) == [9, 10, 11, 12, 13]


def test_app_sql_interpolates_only_constants():
    app = Path(hs.__file__).parent
    bad = [f"{p.relative_to(app.parent)}:{n}" for p in sorted(app.rglob("*.py")) for n in violations(p.read_text())]
    assert not bad, "SQL built from non-constants (bind them, or mark `# sql: constants only`):\n" + "\n".join(bad)
