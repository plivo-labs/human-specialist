"""Log-level hygiene.

(a) get_active_conversations_for_agent_v2 logs the idle-agent "no active conversations"
    line at INFO (was ERROR) — it's the normal steady state, not an error.
(b) the two unexpected-error 500 handlers pass exc_info=True; assert the custom logger
    actually forwards the traceback onto the record (the mechanism those fixes rely on).
"""

import logging

import tests.conftest  # noqa: F401 — eager adapter imports (mapper registry)


def test_custom_logger_forwards_exc_info_for_500_handlers(caplog):
    from hs.infra.log import logger

    with caplog.at_level(logging.ERROR, logger="human-specialist"):
        try:
            raise ValueError("boom")
        except Exception:
            logger.error(msg="unexpected error -> 500", exc_info=True)
    rec = [r for r in caplog.records if "unexpected error" in r.getMessage()]
    assert rec, "expected the handler error log"
    assert rec[0].exc_info is not None and rec[0].exc_info[0] is ValueError, (
        "exc_info=True must attach the traceback so the 500 root cause is debuggable"
    )
