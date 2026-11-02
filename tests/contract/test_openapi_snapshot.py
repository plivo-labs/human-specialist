"""The committed OpenAPI document is the contract. `make snapshot` rewrites it; review the diff."""

import difflib
import json
from pathlib import Path

import pytest

from tests.contract import routes

SNAPSHOT = Path(__file__).with_name("openapi.json")


def render(app) -> str:
    return json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"


def test_openapi_matches_the_snapshot(app):
    assert SNAPSHOT.exists(), "no tests/contract/openapi.json: run `make snapshot`, review and commit it"
    committed, current = SNAPSHOT.read_text(), render(app)
    if committed != current:
        diff = difflib.unified_diff(committed.splitlines(), current.splitlines(), "snapshot", "app", lineterm="")
        pytest.fail("OpenAPI differs from the snapshot; run `make snapshot` and review:\n" + "\n".join(list(diff)[:80]))


if __name__ == "__main__":
    import tests.conftest  # noqa: F401  # the test env hs reads at import

    app = routes.import_app()
    if app is None:
        raise SystemExit("hs.app not built yet")
    SNAPSHOT.write_text(render(app))
