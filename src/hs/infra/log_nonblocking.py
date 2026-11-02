"""Non-blocking log sink: keep a slow stdout/stderr consumer from stalling the process.

Why: a synchronous ``StreamHandler`` writes every log line to stdout under the stdlib handler
lock. When the host stops draining the pipe, the 64 KiB pipe fills, the writing thread blocks
inside ``write()`` holding the lock, and every other thread that logs (the asyncio event-loop
thread included) blocks behind it.

Fix: producers enqueue (``queue.Queue.put_nowait`` — O(1), never blocks) and ONE daemon
listener thread does the blocking write. A frozen sink now costs memory (bounded by the
queue) instead of the event loop, and its health becomes observable: queue depth, dropped
records, and how long a single write took (a >1s write IS the host-freeze signal the
watchdog could never name from inside).

Guarantees:
- Formatting still happens on the producer (stdlib ``QueueHandler.prepare``), so
  ``%(asctime)s`` keeps the record's creation time — the create-vs-ship gap stays measurable.
- Output bytes are identical to the synchronous handler (same formatter, same stream).
- Bounded memory: 20000 records; overflow DROPS the newest
  record and counts it rather than blocking.
- Fork-safe: a listener thread does not survive ``fork()``; ``os.register_at_fork`` restarts
  it in the child (gunicorn ``--preload`` / any fork after import).
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import logging.handlers
import os
import queue
import threading
import time

_ATTR = "_hs_nonblocking_listener"
_DEFAULT_MAXSIZE = 20000
_DEFAULT_BLOCK_WARN_SECONDS = 1.0
_BLOCK_REPORT_COOLDOWN_SECONDS = 60.0
_STOP_JOIN_SECONDS = 2.0

_registry_lock = threading.Lock()
_installed: dict[str, MonitoredQueueListener] = {}  # logger name -> listener (at-fork restart)


class _StopToken:
    """A stop request the writer honours only while it is still wanted (see stop())."""


class CxQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler that never blocks: a full queue drops the record and counts it."""

    dropped_total = 0

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped_total += 1  # unlocked: a diagnostics counter, off by a hair under contention is fine


class MonitoredQueueListener(logging.handlers.QueueListener):
    """QueueListener that times every downstream write and reports sink stalls.

    ``handle`` runs on the listener thread only, so the stats need no lock beyond the
    single-writer discipline; readers get a consistent-enough snapshot for diagnostics."""

    def __init__(
        self,
        q: queue.Queue,
        *handlers: logging.Handler,
        queue_handler: CxQueueHandler,
        block_warn_seconds: float = _DEFAULT_BLOCK_WARN_SECONDS,
    ):
        super().__init__(q, *handlers, respect_handler_level=True)
        self.queue_handler = queue_handler
        self.block_warn_seconds = block_warn_seconds
        self.blocked_writes_total = 0
        self.last_block_seconds = self.max_block_seconds = 0.0
        self._last_block_report_ts = -1e9
        self._stop_lock = threading.Lock()  # makes "writer accepts the token" and "stop() cancels it" exclusive
        self._stop_token: _StopToken | None = None
        self._stop_accepted = False

    def snapshot(self) -> dict:
        return {
            "depth": self.queue.qsize(),
            "dropped": self.queue_handler.dropped_total,
            "blocked_writes": self.blocked_writes_total,
            "last_block_s": round(self.last_block_seconds, 3),
            "max_block_s": round(self.max_block_seconds, 3),
        }

    def _accept_stop(self, token: _StopToken) -> bool:
        """Writer side: commit to exiting on ``token`` iff it is still the current stop request.
        Under the same lock stop() uses to cancel, so exactly one of {accept, cancel} wins."""
        with self._stop_lock:
            if token is self._stop_token:
                self._stop_accepted = True
                return True
            return False

    def _monitor(self) -> None:
        """Writer loop. A stop token is honoured only if it is still the CURRENT one: a stop that
        timed out cancels its token, so the retained writer skips it and keeps serving instead of
        exiting mid-queue and stranding every later record (stdlib _monitor exits on any stop marker)."""
        while True:
            record = self.queue.get(True)
            if isinstance(record, _StopToken):
                if self._accept_stop(record):
                    break
                continue
            self.handle(record)

    def handle(self, record: logging.LogRecord) -> None:
        t0 = time.monotonic()
        try:
            super().handle(record)
        finally:
            took = time.monotonic() - t0
            if took >= self.block_warn_seconds:
                self.blocked_writes_total += 1
                self.last_block_seconds = took
                self.max_block_seconds = max(self.max_block_seconds, took)
                if time.monotonic() - self._last_block_report_ts >= _BLOCK_REPORT_COOLDOWN_SECONDS:
                    self._last_block_report_ts = time.monotonic()
                    self._emit_block_report(took)

    def _emit_block_report(self, took: float) -> None:
        # Straight to the handlers, not logger.warning: lands right after the write that just
        # unblocked, stays off the queue, and cannot re-enter the listener.
        record = logging.LogRecord(
            "human-specialist",
            logging.WARNING,
            __file__,
            0,
            f"log sink blocked: one write to the log stream took {took:.3f}s "
            f"(queue_depth={self.queue.qsize()} dropped_total={self.queue_handler.dropped_total} "
            f"blocked_writes_total={self.blocked_writes_total}) — the container's stdout consumer "
            "(docker json-file / host) stalled; request threads were NOT blocked (queued sink)",
            None,
            None,
        )
        for handler in self.handlers:
            with contextlib.suppress(Exception):  # a diagnostics line must never kill the listener
                if record.levelno >= handler.level:
                    handler.handle(record)

    def stop(self, timeout: float = _STOP_JOIN_SECONDS) -> bool:  # type: ignore[override]
        """Bounded stop; True iff the writer thread has exited.

        The stdlib version joins forever and raises ``queue.Full`` on a saturated queue —
        exactly the state a blocked sink leaves behind. If the writer has not exited within
        ``timeout`` (frozen sink, or a backlog draining slower than the bound) the stop is
        CANCELLED: the writer stays alive, skips the stale token when it reaches it, and keeps
        serving in FIFO order — nothing queued before or after the token is lost."""
        thread = self._thread
        if thread is None:
            return True
        with self._stop_lock:
            self._stop_token = token = _StopToken()
        with contextlib.suppress(queue.Full):
            self.queue.put(token, timeout=timeout)
        thread.join(timeout)
        with self._stop_lock:
            if thread.is_alive() and not self._stop_accepted:
                self._stop_token = None  # cancel: the writer has not accepted and now cannot
                return False
        # Accepted (possibly right at the boundary): the writer has committed to exiting and no
        # blocking call lies between that commit and its exit — wait for it, so True means "gone".
        thread.join()
        self._thread = None
        return True


def sink_snapshot() -> dict:
    """Aggregate sink health across every non-blocking logger in this process; ``{}`` when
    nothing is installed (tests) so callers can tell healthy from no-visibility."""
    with _registry_lock:
        snaps = [lst.snapshot() for lst in _installed.values()]
    if not snaps:
        return {}
    return {
        "depth": sum(s["depth"] for s in snaps),
        "dropped": sum(s["dropped"] for s in snaps),
        "blocked_writes": sum(s["blocked_writes"] for s in snaps),
        "last_block_s": max(s["last_block_s"] for s in snaps),
        "max_block_s": max(s["max_block_s"] for s in snaps),
    }


def is_installed(target: logging.Logger) -> bool:
    return getattr(target, _ATTR, None) is not None


def install(
    target: logging.Logger, *, maxsize: int | None = None, block_warn_seconds: float | None = None
) -> MonitoredQueueListener | None:
    """Move ``target``'s current handlers behind a bounded queue + one listener thread.

    Idempotent: re-installing on an already-wrapped logger tears the old listener down
    first (module reloads in tests). No-op (None) when the logger has no
    handlers to protect. Handler lists are mutated IN PLACE on purpose: uvicorn aliases
    ``uvicorn.error.handlers`` to gunicorn's ``error_log.handlers`` list object, so wrapping
    ``gunicorn.error`` transparently covers ``uvicorn.error`` too."""
    if is_installed(target) and not uninstall(target):
        return getattr(target, _ATTR)  # writer still stuck in a write: keep it, never start a second one
    downstream = [h for h in target.handlers if not isinstance(h, CxQueueHandler)]
    if not downstream:
        return None
    qh = CxQueueHandler(queue.Queue(maxsize=maxsize or _DEFAULT_MAXSIZE))
    listener = MonitoredQueueListener(
        qh.queue, *downstream, queue_handler=qh, block_warn_seconds=block_warn_seconds or _DEFAULT_BLOCK_WARN_SECONDS
    )
    for h in downstream:
        target.removeHandler(h)
    target.addHandler(qh)
    listener.start()
    setattr(target, _ATTR, listener)
    with _registry_lock:
        _installed[target.name] = listener
    return listener


def uninstall(target: logging.Logger, timeout: float = _STOP_JOIN_SECONDS) -> bool:
    """Stop the listener (bounded) and put the original handlers back on the logger.

    False — with the queue left in place — when the writer is still inside a write after
    ``timeout``: handing its downstream handlers back to synchronous producers would let two
    parties write the same stream, and a later install would start a second writer on it."""
    listener: MonitoredQueueListener | None = getattr(target, _ATTR, None)
    if listener is None:
        return True
    if not listener.stop(timeout):
        return False
    target.__dict__.pop(_ATTR, None)
    if listener.queue_handler in target.handlers:
        target.removeHandler(listener.queue_handler)
        for h in listener.handlers:
            target.addHandler(h)
    with _registry_lock:
        if _installed.get(target.name) is listener:
            del _installed[target.name]
    return True


def stop_all(timeout: float = _STOP_JOIN_SECONDS) -> int:
    """Worker exit (atexit): drain every listener within ``timeout`` and restore the synchronous
    handlers. Records still queued past the deadline are lost — drained, counted into
    dropped_total and returned, so the loss shows in the sink stats instead of vanishing
    silently. (A stdout still frozen at exit can also block logging.shutdown(); that is the
    pre-existing synchronous behaviour and gunicorn's kill bounds it.)"""
    lost = 0
    for name in list(_installed):
        target = logging.getLogger(name)
        listener = getattr(target, _ATTR, None)
        if listener is None:
            continue
        if uninstall(target, timeout):
            left = 0
            with contextlib.suppress(queue.Empty):
                while True:
                    if not isinstance(listener.queue.get_nowait(), _StopToken):
                        left += 1
        else:  # writer still frozen at exit: whatever is queued will not make it out
            with listener.queue.mutex:
                left = sum(1 for item in listener.queue.queue if not isinstance(item, _StopToken))
        listener.queue_handler.dropped_total += left
        lost += left
    return lost


def _restart_listeners_after_fork() -> None:
    """Child-side at-fork hook. The listener thread dies with fork and any lock a vanished
    thread held stays locked, so the child must not touch the inherited queue, lock or thread
    object — it rebuilds from fresh ones only (Logger.add/removeHandler are safe: logging
    re-inits its own locks in its at-fork hook). The parent's queued records stay the
    parent's to write."""
    global _registry_lock
    _registry_lock = threading.Lock()
    for name, old in list(_installed.items()):
        target = logging.getLogger(name)
        del _installed[name]
        target.__dict__.pop(_ATTR, None)
        if old.queue_handler in target.handlers:
            target.removeHandler(old.queue_handler)
            for h in old.handlers:
                target.addHandler(h)
        install(target, maxsize=old.queue.maxsize or None, block_warn_seconds=old.block_warn_seconds)


atexit.register(stop_all)
if hasattr(os, "register_at_fork"):  # pragma: no branch — CPython on POSIX
    os.register_at_fork(after_in_child=_restart_listeners_after_fork)
