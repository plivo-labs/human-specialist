import pytest

from tests.contract import routes


@pytest.fixture(scope="session")
def app():
    app = routes.import_app()
    if app is None:
        pytest.skip("hs.app not built yet")
    return app


def pytest_terminal_summary(terminalreporter):
    app = routes.import_app()
    if app is not None:
        built, total = routes.coverage(app)
        terminalreporter.write_line(f"spec routes built: {built}/{total}")
