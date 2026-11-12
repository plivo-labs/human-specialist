import atexit
import os
from urllib.parse import urlsplit

import pytest

from hs import jobs
from tests.harness import _Redis

# Started at import: a World's free port may reuse the released port tests/conftest.py picked.
atexit.register(_Redis(urlsplit(os.environ["REDIS_URL"]).port).stop)


@pytest.fixture(scope="session", autouse=True)
def _migrated_db(db_engine):
    pass


@pytest.fixture
def pool_first(monkeypatch):
    """jobs.run_now as a route meets it once the job pool ran the job first: the pool's run, whose result reaches no
    one, then the route's own claim."""
    run_now = jobs.run_now

    def claimed_first(kind, key, *where, wait=False):
        run_now(kind, key, wait=True)
        return run_now(kind, key, *where, wait=wait)

    monkeypatch.setattr(jobs, "run_now", claimed_first)
