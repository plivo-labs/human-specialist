import re
from pathlib import Path

from tests.contract.routes import SPEC

UI = Path(__file__).resolve().parents[2] / "ui"
CALL = re.compile(r"""["'](GET|POST|PUT|PATCH|DELETE)["'],\s*(?:[`"'](/api/v1/[^`"'?#]*)|([^\s,)]+))""")
STREAM = re.compile(r"""(?:EventSource|fetch)\(`\$\{BASE\}(/api/v1/[^`?$]*)""")
SOURCES = [
    f for f in UI.rglob("*") if f.suffix in {".js", ".ts", ".tsx"} and not {"node_modules", "dist"} & set(f.parts)
]


def test_every_ui_call_is_a_listed_route():
    found = [m for f in SOURCES for m in CALL.findall(f.read_text())]
    found += [("GET", p, "") for f in SOURCES for p in STREAM.findall(f.read_text())]
    assert found, f"no API call found under {UI}"
    assert not [other for *_, other in found if other], "a UI call's path is not a literal /api/v1 path"
    calls = {(method, re.sub(r"\$\{[^}]*\}", "{}", path)) for method, path, _ in found}
    unknown = sorted(f"{method} {path}" for method, path in calls if (method, path) not in SPEC)
    assert not unknown, f"the UI calls routes the API does not serve: {unknown}"
