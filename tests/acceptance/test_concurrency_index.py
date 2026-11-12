"""The search spec of test_concurrency.py again, with HS searching the OpenSearch index at the harness's SEARCH_URL.
It skips when nothing answers there."""

from tests.acceptance.test_concurrency import (  # noqa: F401
    test_notes_and_a_label_added_at_once_to_a_closed_conversation_are_all_found_by_search,
)
from tests.harness.fixtures import search_world as world  # noqa: F401
