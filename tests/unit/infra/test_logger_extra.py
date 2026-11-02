"""hs/infra/log.py must not drop extra= (correlation id / org context).

_Logger._emit used to do kwargs.pop("extra", None) and throw the value away, so
every one of the dozens of call sites passing extra=context_req_meta.get() (api_id,
org_name, aom_uuid, ...) never actually reached the emitted log line. Fix renders
extra as appended key=value pairs on the same line (the formatter has no custom
fields to hook into, so this is the minimal-change way to make them visible).

Pins three things:
(a) extra fields now appear in the emitted line,
(b) calls WITHOUT extra are byte-identical to before (other tooling parses these
    lines, so the no-extra format must not shift),
(c) an extra= dict containing a key that collides with a reserved LogRecord
    attribute (e.g. "message", "name") is sanitized (prefixed) instead of raising.
"""

import importlib
import logging
from io import StringIO

import pytest

import hs.infra.log as logger_mod


@pytest.fixture()
def captured_log():
    """Reload the logger module fresh and swap its handler for an in-memory buffer
    with a formatter that only emits the level + message (mirrors the real
    formatter's "%(message)s" tail, without the timestamp noise)."""
    from hs.infra.log_context import context_req_meta

    importlib.reload(logger_mod)
    clean = context_req_meta.set(None)  # an earlier test may leave a request context behind
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger_mod._base.handlers = [handler]
    logger_mod._base.setLevel(logging.DEBUG)
    yield logger_mod.logger, buf
    context_req_meta.reset(clean)
    importlib.reload(logger_mod)


def test_no_extra_output_is_unchanged(captured_log):
    """Calls without extra= must emit exactly the same line as before the fix."""
    logger, buf = captured_log
    logger.info(msg="plain message")
    assert buf.getvalue() == "INFO plain message\n"


def test_no_extra_with_positional_args_is_unchanged(captured_log):
    """%-style positional args (no extra=) must still be substituted as before."""
    logger, buf = captured_log
    logger.warning("value is %s", "ok")
    assert buf.getvalue() == "WARNING value is ok\n"


def test_extra_fields_now_appear_in_output(captured_log):
    """extra= context (api_id/org_name/...) must reach the emitted line, not be dropped."""
    logger, buf = captured_log
    logger.error(extra={"api_id": "req-123", "org_name": "acme"}, msg="something failed")
    out = buf.getvalue()
    assert "something failed" in out
    assert "api_id=req-123" in out
    assert "org_name=acme" in out


def test_extra_none_behaves_like_no_extra(captured_log):
    """extra=None (the ContextVar default before a request sets it) must not add noise."""
    logger, buf = captured_log
    logger.info(extra=None, msg="no context yet")
    assert buf.getvalue() == "INFO no context yet\n"


def test_extra_empty_dict_behaves_like_no_extra(captured_log):
    logger, buf = captured_log
    logger.info(extra={}, msg="empty context")
    assert buf.getvalue() == "INFO empty context\n"


def test_reserved_key_collision_does_not_raise(captured_log):
    """extra= keys that collide with LogRecord's own attributes (e.g. 'message', 'name')
    must be sanitized (prefixed) rather than crashing the logging call."""
    logger, buf = captured_log
    logger.error(extra={"message": "clobber attempt", "name": "also-reserved"}, msg="boom")
    out = buf.getvalue()
    assert "boom" in out
    assert "extra_message=clobber attempt" in out
    assert "extra_name=also-reserved" in out


def test_reserved_key_collision_does_not_shadow_non_colliding_keys(captured_log):
    """A mix of reserved and normal keys: only the colliding ones get prefixed."""
    logger, buf = captured_log
    logger.error(extra={"asctime": "clobber", "api_id": "req-9"}, msg="mixed")
    out = buf.getvalue()
    assert "extra_asctime=clobber" in out
    assert "api_id=req-9" in out


def test_request_context_is_the_default_extra(captured_log):
    """Without extra=, a line carries the request context."""
    from hs.infra.log_context import context_req_meta

    log, buf = captured_log
    token = context_req_meta.set({"api_id": "ctx-1"})
    try:
        log.info(msg="with context")
    finally:
        context_req_meta.reset(token)
    log.info(msg="outside a request")
    assert buf.getvalue().splitlines() == ["INFO with context [api_id=ctx-1]", "INFO outside a request"]
