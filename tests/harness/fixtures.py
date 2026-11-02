"""Fixtures for suites that run against a real HS: `from tests.harness.fixtures import *`."""

import pytest

from tests.harness import Tenant, World

__all__ = ["tenant", "world"]


@pytest.fixture(scope="session")
def world():
    w = World()
    yield w
    w.close()


@pytest.fixture
def tenant(world: World) -> Tenant:
    return world.new_tenant()
