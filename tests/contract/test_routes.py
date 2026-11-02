from tests.contract.routes import ROUTES, SPEC, norm, served
from tests.harness import ALL_SCOPES

AUTH = {"op", "agent", "sup", "stream", "sig", "plivo", "none", *(f"key:{s}" for s in ALL_SCOPES)}


def test_spec_rows_are_well_formed():
    assert len(SPEC) == len(ROUTES), "duplicate (method, path) rows"
    bad = [(m, p, a) for m, p, a in ROUTES if not a or not set(a) <= AUTH or ("none" in a and len(a) > 1)]
    assert not bad, bad


def test_every_served_route_is_in_the_spec(app):
    extra = sorted(
        f"{method} {path}"
        for method, path, module in served(app)
        if (method, norm(path)) not in SPEC and not module.startswith("tests.")
    )
    assert not extra, "add these to tests/contract/routes.py (or stop serving them):\n" + "\n".join(extra)
