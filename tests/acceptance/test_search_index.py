"""The search specs of test_search.py and the handoff transcript's absence from search again, with HS searching the
OpenSearch index at the harness's SEARCH_URL, and the one only an index has: a document it loses comes back. They skip
when nothing answers there."""

import httpx

from tests.acceptance.conftest import connect, finish, run_jobs_until
from tests.acceptance.test_handoff_transcript import (
    test_a_transcript_is_in_no_event_webhook_list_row_search_document_or_export,  # noqa: F401
)
from tests.acceptance.test_search import *  # noqa: F403
from tests.acceptance.test_search import indexed, search
from tests.harness.fixtures import search_world as world  # noqa: F401


def test_a_document_the_index_lost_is_indexed_again_by_the_next_sweep(world, tenant):  # noqa: F811
    closed = finish(connect(tenant))["conversation_id"]
    indexed(tenant, [closed])
    lost = httpx.delete(f"{world.search_url}/hs-conversations-{tenant.id}")
    assert lost.status_code == 200 and search(tenant) == []
    world.run_jobs("search_reconcile")
    run_jobs_until(world, lambda: search(tenant) == [closed], "the sweep to index the lost document")
