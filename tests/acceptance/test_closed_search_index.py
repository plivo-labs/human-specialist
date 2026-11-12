"""The search specs of test_closed_search.py again, with HS searching the OpenSearch index at the harness's
SEARCH_URL. They skip when nothing answers there."""

from tests.acceptance.test_closed_search import *  # noqa: F403
from tests.harness.fixtures import search_world as world  # noqa: F401
