"""Regression: the closed-conversation search model must be preloaded at app startup.

`hs.conversations.search_store.ClosedConversationSearch` is a concrete
subclass of `DB` (an `AbstractConcreteBase`) whose primary-key columns (id, uuid)
come from the `CoreDBBase` mixin. SQLAlchemy assembles those columns only if the
class is mapped BEFORE the first `configure_mappers()`. In the app that first
configure happens implicitly on the first ORM query of any request; the model is
then reached only via *lazy* (function-local) imports on the closed-conversation
read/write paths — i.e. AFTER configure. The result was every
closed-conversation search returning 500:

    ArgumentError: Mapper mapped class ClosedConversationSearch->closed_conversation_search
        could not assemble any primary key columns for mapped table
    (and, on the evicted-module re-import, "Table ... is already defined for this MetaData")

The fix is to import the adapter eagerly in `hs/app.py` (and the conftest
mirror), exactly like every other DB-mapped adapter. This test reproduces the
the app's import ordering in a clean interpreter so it FAILS on the unfixed code
and PASSES once the adapter is preloaded.
"""

import os
import subprocess
import sys

_ORDER_REPRO = """
import sys
import hs.app  # uvicorn entrypoint: must preload every DB-mapped adapter
assert "hs.conversations.search_store" in sys.modules, (
    "hs/app.py must eagerly import hs.conversations.search_store "
    "(AbstractConcreteBase model registered after configure_mappers cannot map)"
)
from sqlalchemy.orm import configure_mappers
configure_mappers()  # what the first real ORM query triggers
# request-time lazy import path (read side: hs.conversations.management:2009):
import hs.conversations.search  # noqa: F401
from hs.conversations.search_store import ClosedConversationSearch
pk = sorted(c.name for c in ClosedConversationSearch.__table__.primary_key.columns)
assert pk == ["id", "uuid"], pk
print("PRELOAD_OK")
"""


def test_app_startup_preloads_closed_conv_search_model():
    # The test env from tests/conftest.py; create_engine is lazy, so the import does not connect.
    proc = subprocess.run(
        [sys.executable, "-c", _ORDER_REPRO], capture_output=True, text=True, env=os.environ, timeout=120
    )
    # Surface ANY startup failure (ImportError, circular import, mapper error, a real
    # missing-preload regression, ...) as a test FAILURE — never skip, or a genuine
    # regression would hide. conftest.py imports hs.app for the whole suite, so it
    # is always importable wherever this test runs.
    assert proc.returncode == 0, (
        "closed_conversation_search not preloaded at startup / hs.app failed to import:\n"
        f"STDOUT:\n{proc.stdout[-1000:]}\nSTDERR:\n{proc.stderr[-2000:]}"
    )
    assert "PRELOAD_OK" in proc.stdout, proc.stdout[-1000:]
