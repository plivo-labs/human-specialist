"""Fixtures for suites that run against a real HS: `from tests.harness.fixtures import *`."""

import pytest

from tests.harness import SEARCH_URL, Tenant, World, require_search

__all__ = ["per_test", "tenant", "world"]


@pytest.fixture(scope="session")
def world():
    w = World()
    yield w
    w.close()


@pytest.fixture(scope="session")
def search_world():
    """A World whose HS searches the OpenSearch index at SEARCH_URL; skips when nothing answers there."""
    require_search()
    w = World(search_url=SEARCH_URL)
    yield w
    w.close()


@pytest.fixture
def tenant(world: World) -> Tenant:
    return world.new_tenant()


@pytest.fixture(autouse=True)
def per_test(world: World):
    world.begin_test()
    yield
    world.plivo.reset_faults()
    world.end_test()
