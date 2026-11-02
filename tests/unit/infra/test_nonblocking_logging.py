"""Non-blocking log sink (hs/infra/log_nonblocking.py).

With one synchronous StreamHandler, a stdout pipe the host stops draining blocks every thread
that logs (the event-loop thread included) behind the handler lock.

Pinned here: (1) a blocked sink never blocks producers, and the event loop keeps servicing
callbacks while the sink is frozen (with the synchronous handler it does not — the control
case); (2) output bytes are identical to the synchronous handler, asctime included;
(3) the queue is bounded — overflow drops+counts instead of blocking; (4) a slow write is
reported once, with stats; (5) gunicorn/uvicorn loggers get the same treatment (uvicorn
aliases gunicorn's handler list); (6) install/uninstall are idempotent; (7) the at-fork hook
rebuilds the writer thread in a child.
"""

import asyncio
import copy
import io
import logging
import os
import signal
import threading
import time

import pytest

import hs.infra.log_nonblocking as nb

_FMT = "%(asctime)s %(levelname)s [human-specialist] %(message)s"


_SINKS = []  # every frozen sink is released at fixture teardown: a failed test must not leave the
# writer thread stuck inside handle() holding the handler lock, or logging.shutdown() hangs at exit


class _BlockableHandler(logging.Handler):
    """Downstream sink whose write can be frozen (gate cleared) like a full stdout pipe."""

    def __init__(self, fmt="%(levelname)s %(message)s"):
        super().__init__()
        self.setFormatter(logging.Formatter(fmt))
        self.gate = threading.Event()
        self.gate.set()
        self.lines = []
        _SINKS.append(self)

    def emit(self, record):
        self.gate.wait()
        self.lines.append(self.format(record))

    def payload(self):
        return [line for line in self.lines if not line.startswith("WARNING log sink blocked")]


def _wait_until(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.005)
    return pred()


@pytest.fixture()
def fresh_logger():
    lg = logging.getLogger(f"hs-test-nonblocking-{time.time_ns()}")
    lg.setLevel(logging.DEBUG)
    lg.propagate = False
    yield lg
    for sink in _SINKS:
        sink.gate.set()
    nb.uninstall(lg)
    lg.handlers.clear()


# ------------------------------------------------------------------ 1. producers never block
def test_blocked_sink_does_not_block_producers_and_preserves_order(fresh_logger):
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger, block_warn_seconds=100.0)
    sink.gate.clear()  # freeze the sink: the writer thread hangs inside emit()
    t0 = time.monotonic()
    for i in range(200):
        fresh_logger.info("line %d", i)
    assert time.monotonic() - t0 < 0.5, "producers waited on a frozen sink"
    assert listener.queue.qsize() >= 190  # queued, not written, not lost
    sink.gate.set()
    assert _wait_until(lambda: len(sink.payload()) == 200)
    assert sink.payload() == [f"INFO line {i}" for i in range(200)]


def test_event_loop_keeps_running_while_sink_is_frozen_but_not_with_sync_handler(fresh_logger):
    """The logging mechanism, end to end: an asyncio loop that logs while the sink is frozen.

    Non-blocking sink: a watchdog-style ping (call_soon_threadsafe -> Event) is serviced
    promptly. Control: with the synchronous handler restored, the loop's first log call
    blocks on the frozen sink and the same ping is NOT serviced — the stall this fixes."""
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    nb.install(fresh_logger, block_warn_seconds=100.0)

    loop = asyncio.new_event_loop()
    started = threading.Event()

    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(started.set)
        loop.run_forever()

    threading.Thread(target=_run, daemon=True).start()
    assert started.wait(5)

    def _ping(timeout):
        ev = threading.Event()
        loop.call_soon_threadsafe(ev.set)
        return ev.wait(timeout)

    try:
        sink.gate.clear()
        for i in range(50):
            loop.call_soon_threadsafe(fresh_logger.info, "loop log %d", i)
        assert _ping(1.0), "event loop stalled behind a frozen NON-blocking sink"
        sink.gate.set()
        assert _wait_until(lambda: len(sink.payload()) == 50)

        # Control: a plain synchronous handler + frozen sink => the loop blocks in write().
        nb.uninstall(fresh_logger)
        assert fresh_logger.handlers == [sink]
        sink.gate.clear()
        loop.call_soon_threadsafe(fresh_logger.info, "this blocks the loop")
        assert not _ping(0.3), "control case broken: sync handler did not block the loop"
        sink.gate.set()
        assert _ping(2.0)
    finally:
        sink.gate.set()
        loop.call_soon_threadsafe(loop.stop)


# ------------------------------------------------------------------ 2. identical bytes
def test_output_is_byte_identical_to_the_synchronous_handler(fresh_logger):
    sync_buf, queued_buf = io.StringIO(), io.StringIO()
    sync_lg = logging.getLogger(f"{fresh_logger.name}-sync")
    sync_lg.propagate = False
    sync_lg.setLevel(logging.DEBUG)
    for lg, buf in ((sync_lg, sync_buf), (fresh_logger, queued_buf)):
        h = logging.StreamHandler(buf)
        h.setFormatter(logging.Formatter(_FMT))
        lg.handlers = [h]
    listener = nb.install(fresh_logger)
    try:
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            exc = sys.exc_info()
        record = logging.LogRecord(fresh_logger.name, logging.ERROR, "x.py", 7, "hello %s", ("world",), exc)
        record.created, record.msecs = 1_700_000_000.123, 123.0  # fixed clock: asctime must match
        fresh_logger.handle(record)
        sync_lg.handle(copy.copy(record))
        assert _wait_until(lambda: listener.queue.qsize() == 0 and queued_buf.getvalue())
        time.sleep(0.02)
        assert queued_buf.getvalue() == sync_buf.getvalue()
        assert "ERROR [human-specialist] hello world" in queued_buf.getvalue()
        assert "ValueError: boom" in queued_buf.getvalue()
        assert queued_buf.getvalue().count("Traceback") == 1
    finally:
        sync_lg.handlers.clear()


# ------------------------------------------------------------------ 3. bounded queue
def test_full_queue_drops_and_counts_instead_of_blocking(fresh_logger):
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger, maxsize=50, block_warn_seconds=100.0)
    sink.gate.clear()
    t0 = time.monotonic()
    for i in range(300):
        fresh_logger.info("burst %d", i)
    assert time.monotonic() - t0 < 0.5
    assert listener.queue.qsize() <= 50
    assert listener.queue_handler.dropped_total >= 249  # 300 - 50 queued - 1 in flight
    assert nb.sink_snapshot()["dropped"] >= 249
    sink.gate.set()
    assert _wait_until(lambda: listener.queue.qsize() == 0)


# ------------------------------------------------------------------ 6. idempotence
def test_install_is_idempotent_and_uninstall_restores_handlers(fresh_logger):
    h = logging.StreamHandler(io.StringIO())
    h.setLevel(logging.WARNING)
    fresh_logger.addHandler(h)
    before = threading.active_count()
    first = nb.install(fresh_logger)
    second = nb.install(fresh_logger)
    assert second is not first and not first._thread  # old listener stopped, one live writer
    assert threading.active_count() == before + 1
    assert [type(x) for x in fresh_logger.handlers] == [nb.CxQueueHandler]
    assert fresh_logger.name in nb._installed
    # respect_handler_level: the downstream WARNING level still filters
    fresh_logger.info("dropped by level")
    fresh_logger.warning("kept")
    assert _wait_until(lambda: "kept" in h.stream.getvalue())
    assert "dropped by level" not in h.stream.getvalue()
    nb.uninstall(fresh_logger)
    assert fresh_logger.handlers == [h] and not nb.is_installed(fresh_logger)
    assert fresh_logger.name not in nb._installed
    assert threading.active_count() == before


def test_install_noop_without_handlers(fresh_logger):
    assert nb.install(fresh_logger) is None and not nb.is_installed(fresh_logger)


def test_app_logger_is_wrapped_by_default():
    """hs/infra/log.py installs the sink on the real app logger."""
    import hs.infra.log as logger_mod

    assert nb.is_installed(logger_mod._base)


# ------------------------------------------------------------------ 7. shutdown + fork safety
def test_stop_all_is_bounded_and_counts_leftovers_when_the_writer_is_frozen(fresh_logger, monkeypatch):
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger, block_warn_seconds=100.0)
    monkeypatch.setattr(nb, "_installed", {fresh_logger.name: listener})  # process-wide call: scope it to this logger
    sink.gate.clear()
    for i in range(5):
        fresh_logger.info("pending %d", i)
    assert _wait_until(lambda: listener.queue.qsize() == 4)  # one record is in flight inside emit()
    t0 = time.monotonic()
    lost = nb.stop_all(timeout=0.2)
    assert time.monotonic() - t0 < 1.0  # bounded, even though the sink is still frozen
    assert lost == 4 and listener.queue_handler.dropped_total == 4
    # the frozen writer still owns the stream: it is NOT handed back to synchronous producers
    assert nb.is_installed(fresh_logger) and [type(h) for h in fresh_logger.handlers] == [nb.CxQueueHandler]


def test_stop_all_drains_cleanly_when_the_writer_is_idle(fresh_logger, monkeypatch):
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger)
    monkeypatch.setattr(nb, "_installed", {fresh_logger.name: listener})
    fresh_logger.info("flushed before exit")
    assert nb.stop_all(timeout=1.0) == 0
    assert "INFO flushed before exit" in sink.lines
    assert fresh_logger.handlers == [sink] and not nb.is_installed(fresh_logger)  # synchronous again


def test_reinstall_never_starts_a_second_writer_on_a_frozen_sink(fresh_logger):
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    first = nb.install(fresh_logger)
    sink.gate.clear()
    fresh_logger.info("stuck in flight")
    assert _wait_until(lambda: first.queue.empty())  # dequeued, now frozen inside emit()
    before = threading.active_count()
    assert nb.uninstall(fresh_logger, timeout=0.1) is False  # writer still inside the write: keep the wrap
    assert nb.is_installed(fresh_logger) and [type(h) for h in fresh_logger.handlers] == [nb.CxQueueHandler]
    assert nb.install(fresh_logger) is first and threading.active_count() == before  # reused, not doubled
    fresh_logger.info("queued behind the frozen write")
    sink.gate.set()  # the sink thaws: the SAME writer drains everything, in order, and keeps serving
    assert _wait_until(lambda: sink.payload() == ["INFO stuck in flight", "INFO queued behind the frozen write"])
    assert nb.uninstall(fresh_logger, timeout=1.0) is True and fresh_logger.handlers == [sink]


def test_stop_never_returns_false_after_the_writer_committed_to_exit(fresh_logger):
    """The boundary: the writer accepts the token, then is descheduled before it actually exits,
    while stop()'s join times out. stop() must NOT cancel (the writer WILL exit) — it must wait
    and return True, so uninstall() never retains a listener whose writer is gone."""
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger)
    paused = threading.Event()  # models the deschedule between accept and exit
    orig_accept = listener._accept_stop

    def _accept_then_stall(token):
        ok = orig_accept(token)
        if ok:
            paused.wait()
        return ok

    listener._accept_stop = _accept_then_stall
    threading.Timer(0.3, paused.set).start()  # the writer is "rescheduled" after stop()'s bound expires
    writer = listener._thread
    t0 = time.monotonic()
    assert listener.stop(timeout=0.05) is True  # accepted → waited for the exit → never cancelled
    assert time.monotonic() - t0 >= 0.25 and not writer.is_alive() and listener._stop_token is not None
    nb.uninstall(fresh_logger)


def test_timed_out_stop_does_not_poison_a_slowly_draining_writer(fresh_logger):
    """A timeout case: the stop token is already queued behind a backlog when the bound
    expires. The retained writer must skip that stale token and keep consuming afterwards."""
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    listener = nb.install(fresh_logger)
    sink.gate.clear()
    for i in range(3):
        fresh_logger.info("backlog %d", i)
    assert nb.uninstall(fresh_logger, timeout=0.05) is False  # token queued behind the backlog, then cancelled
    assert nb.is_installed(fresh_logger)
    sink.gate.set()
    assert _wait_until(lambda: len(sink.payload()) == 3)
    fresh_logger.info("after the stale token")  # would be stranded forever if the writer had exited
    assert _wait_until(lambda: sink.payload()[-1] == "INFO after the stale token")
    assert listener._thread.is_alive()
    assert nb.uninstall(fresh_logger, timeout=1.0) is True and fresh_logger.handlers == [sink]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork-only")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")  # the fork is the point
def test_real_fork_child_gets_a_live_writer_without_touching_inherited_locks(fresh_logger):
    """Fork while THIS thread holds the queue's lock: in the child that lock stays locked
    forever (its owner does not exist there). The hook must rebuild from fresh objects and
    never touch the inherited queue/lock/thread — a child that hangs here hangs in os.fork()."""
    sink = _BlockableHandler()
    fresh_logger.addHandler(sink)
    old = nb.install(fresh_logger)
    r, w = os.pipe()
    with old.queue.mutex:
        pid = os.fork()
    if pid == 0:  # child: report through the pipe, never return into pytest
        try:
            new = getattr(fresh_logger, nb._ATTR, None)
            ok = new is not None and new is not old and new._thread.is_alive() and list(new.handlers) == [sink]
            fresh_logger.info("hello from the child")
            ok = ok and _wait_until(lambda: "INFO hello from the child" in sink.lines, 2.0)
            os.write(w, b"1" if ok else b"0")
        finally:
            os._exit(0)
    os.close(w)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        done, _ = os.waitpid(pid, os.WNOHANG)
        if done:
            break
        time.sleep(0.02)
    else:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("child hung inside the at-fork hook (inherited lock)")
    verdict = os.read(r, 1)
    os.close(r)
    assert verdict == b"1"
    assert nb.is_installed(fresh_logger) and getattr(fresh_logger, nb._ATTR) is old  # parent untouched
