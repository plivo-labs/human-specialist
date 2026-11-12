"""Fixtures for suites that run against a real HS: `from tests.harness.fixtures import *`."""

import pytest
from xdist.plugin import pytest_xdist_auto_num_workers as cores

from tests.harness import SEARCH_URL, Tenant, World, require_search

__all__ = ["per_test", "pytest_collection_modifyitems", "pytest_xdist_auto_num_workers", "tenant", "world"]
SEARCH = pytest.StashKey[World]()


@pytest.fixture(scope="session")
def world():
    w = World()
    yield w
    w.close()


@pytest.fixture(scope="session")
def search_world(request: pytest.FixtureRequest) -> World:
    """One World per run, kept in the stash as each module binding it as `world` holds a fixture of its own, with HS
    searching the OpenSearch index at SEARCH_URL; skips when nothing answers there."""
    require_search()
    if SEARCH not in request.config.stash:
        request.config.stash[SEARCH] = World(search_url=SEARCH_URL)
        request.addfinalizer(request.config.stash[SEARCH].close)
    return request.config.stash[SEARCH]


@pytest.fixture
def tenant(world: World) -> Tenant:
    return world.new_tenant()


@pytest.fixture(autouse=True)
def per_test(world: World):
    world.begin_test()
    yield
    world.plivo.reset_faults()
    world.end_test()


def pytest_xdist_auto_num_workers(config: pytest.Config) -> int:
    """`-n auto`, at most 4 workers: a worker holds up to 21 Postgres connections, more with a replica, and the test
    stack's Postgres allows 100."""
    return min(cores(config), 4)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Group the search-World modules on one worker, so a run starts one OpenSearch-backed service."""
    for item in items:
        if getattr(item.module, "world", None) is search_world:
            item.add_marker(pytest.mark.xdist_group("search"))
