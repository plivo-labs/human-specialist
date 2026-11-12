import pytest

from tests.contract import routes


@pytest.fixture(scope="session")
def app():
    return routes.import_app()


def pytest_terminal_summary(terminalreporter):
    built, total = routes.coverage(routes.import_app())
    terminalreporter.write_line(f"spec routes built: {built}/{total}")
