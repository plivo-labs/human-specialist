"""Detached background-thread spawner — the one way to run fire-and-forget work off a request.

WHY: usecase modules used to hand-roll a ``_spawn`` that copied the caller's FULL contextvars
context (or none at all) into a daemon thread. Copying the context also copies
``context_db_session`` — the REQUEST's SQLAlchemy Session — so the worker and the request
thread then drive ONE Session (one psycopg2 connection) CONCURRENTLY. Sessions are not
thread-safe: when the request thread's COMMIT lands between the worker's implicit BEGIN and
its SAVEPOINT, the worker dies with ``psycopg2.errors.NoActiveSqlTransaction: SAVEPOINT can
only be used in transaction blocks``, silently losing best-effort side-writes
(recording links, closed-conv search rows, call legs).

HOW: keep the copied context so org/api_id log metadata survives into the worker, but DETACH
the DB session inside it — ``bg_db_guard`` sets ``context_db_session`` to None (so
``get_db_session()`` falls back to the worker thread's own scoped session) and guarantees
commit-on-success / rollback-on-error / remove ON the worker thread. Every plain-thread spawner
gets the same guarantee by delegating here.
"""

import contextvars
import threading

from hs.infra.log import logger


def spawn_detached(target, *args, log_context: str = "bg_spawn") -> None:
    """Run ``target(*args)`` on a daemon thread with logging context but a DETACHED DB session."""
    ctx = contextvars.copy_context()

    def _guarded():
        # Runs INSIDE the copied context, except path included: ctx.run restores the previous
        # context before it returns/raises, so an outer handler would log the failure WITHOUT
        # the caller's request/org metadata.
        from hs.infra.context import bg_db_guard

        try:
            # bg_db_guard: context_db_session=None + commit/rollback + remove_db_session, all on
            # THIS thread (the session's owner). It re-raises on error -> logged just below.
            bg_db_guard(target)(*args)
        except Exception as e:
            logger.error(f"{log_context} background task error: {e}")

    threading.Thread(target=lambda: ctx.run(_guarded), daemon=True).start()
