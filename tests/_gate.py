"""Skip a test whose service is down, or fail it when HS_REQUIRE_STACK=1 says the stack must be up."""

import os

import pytest


def gate(missing, what: str = "test stack"):
    """Skip (or fail, under HS_REQUIRE_STACK=1) when `missing` names any unavailable dependency."""
    if not missing:
        return
    detail = f"{what} unavailable: {', '.join(missing)}"
    if os.getenv("HS_REQUIRE_STACK", "").strip().lower() in ("1", "true", "yes", "on"):
        pytest.fail(f"{detail} (HS_REQUIRE_STACK=1)", pytrace=False)
    pytest.skip(f"{detail}; set HS_REQUIRE_STACK=1 to make this a failure")
