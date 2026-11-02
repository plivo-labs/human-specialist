import contextlib
from contextvars import ContextVar

from fastapi import BackgroundTasks
from sqlalchemy.orm import Session

from hs.infra.db import db_session
from hs.infra.log import context_req_meta, logger

context_db_session: ContextVar[Session] = ContextVar("db_session", default=None)
context_db_commit: ContextVar[bool] = ContextVar("db_commit", default=True)
context_org_name: ContextVar[str] = ContextVar("org_name", default=None)
context_api_id: ContextVar[str] = ContextVar("api_id")
context_aom_uuid: ContextVar[str] = ContextVar("aom_uuid", default=None)
context_org_uuid: ContextVar[str] = ContextVar("org_uuid", default=None)
context_request_id: ContextVar[str] = ContextVar("request_id", default=None)
context_background_tasks: ContextVar[BackgroundTasks] = ContextVar("background_tasks", default=None)


class _InlineBackgroundTasks:
    def add_task(self, fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception as e:
            logger.error(f"inline background task {getattr(fn, '__name__', fn)} failed: {e}")


def add_background(fn, *args, **kwargs) -> None:
    """Queue `fn` after the response, or run it now when the path has no request (a job, a callback task)."""
    (context_background_tasks.get() or _InlineBackgroundTasks()).add_task(fn, *args, **kwargs)


def clear_context():
    context_aom_uuid.set(None)
    context_org_name.set(None)
    context_api_id.set(None)
    context_org_uuid.set(None)
    context_req_meta.set(None)
    context_db_session.set(None)
    context_db_commit.set(True)
    context_background_tasks.set(None)


def get_context_org_name() -> str:
    from hs.admin.account import Organization

    org_name = context_org_name.get()
    if (not org_name) or org_name == "":
        org_uuid = context_org_uuid.get()
        if org_uuid:
            org = Organization.get_by_uuid(org_uuid)
            if org:
                context_org_name.set(org.name)
    return context_org_name.get()


def get_db_session() -> Session:
    db = context_db_session.get()
    if db:
        return db
    return db_session


def _end_session(db) -> None:
    """scoped_session → remove() (close + clear the thread slot); plain Session → close().
    If close() itself dies, the slot must STILL be cleared or the next request on this AnyIO
    worker thread inherits the wreck."""
    try:
        (getattr(db, "remove", None) or db.close)()
    except Exception as close_err:
        logger.error(f"remove_db_session: session close failed ({close_err!r}); clearing the thread slot")
        with contextlib.suppress(Exception):  # plain Session: no registry, nothing to clear
            db.registry.clear()


def remove_db_session(db=None):
    """Commit-or-rollback, then ALWAYS release the current session.

    A commit on a dead connection raises after SQLAlchemy has moved the transaction to PREPARED;
    if the session stayed registered, the next request on that worker thread would fail on its
    first statement. Any commit failure (PendingRollbackError included) rolls back, releases the
    session and still propagates, so the caller sees the failed write."""
    db = get_db_session() if db is None else db
    try:
        if should_commit_db_session():
            try:
                db.commit()
            except Exception:
                with contextlib.suppress(Exception):  # the connection may already be dead
                    db.rollback()
                raise
        else:
            db.rollback()
    finally:
        _end_session(db)


def fallback_session_active() -> bool:
    """True iff the CURRENT thread's slot in the module-level scoped_session holds a live
    session. Lets guards skip cleanup entirely instead of lazily CREATING a session just to
    commit-and-remove it (scoped_session attribute access instantiates on first touch)."""
    try:
        return bool(db_session.registry.has())
    except Exception:
        return True


def threadpool_db_guard(fn):
    """Worker-thread-scoped teardown for the FALLBACK db_session.

    The module-level ``db_session`` is a scoped_session keyed by OS thread. A sync endpoint
    creates its session on the AnyIO worker thread that runs it, but the app-level
    ``_scoped_db_session_guard`` tears down on the EVENT-LOOP thread, so its ``remove()``
    hits the loop thread's (empty) slot and the worker session leaks across requests on the
    reused thread. This wrapper runs ``remove_db_session()`` ON the thread that owns the session,
    mirroring ``bg_db_guard``'s semantics for background tasks. No-ops when the thread never
    touched the DB. When the request uses a contextvar-scoped session its owner tears it down,
    but the ROLLBACK marker is honored here: the wrapped call runs in a COPIED context, so a
    set_db_session_rollback() inside it can never reach the loop-side teardown, and without a
    worker-thread rollback the poisoned transaction would commit."""
    import functools

    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        if context_db_session.get() is not None:
            # contextvar-scoped session: owned by its guard. Honor the copied context's
            # rollback flag on THIS thread before returning; the loop-side teardown then
            # commits/rolls back an already-clean session (both no-ops, double-rollback safe).
            try:
                return fn(*args, **kwargs)
            finally:
                if not should_commit_db_session():
                    try:
                        context_db_session.get().rollback()
                    except Exception as e:
                        logger.error(f"threadpool_db_guard: worker-thread rollback of contextvar session failed: {e}")
        failed = False
        try:
            return fn(*args, **kwargs)
        except Exception:
            failed = True
            raise
        finally:
            if fallback_session_active():  # only if THIS thread actually opened a session
                if failed:
                    set_db_session_rollback()
                remove_db_session()

    return _wrapped


def bg_db_guard(fn):
    import functools

    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        context_db_session.set(None)
        context_db_commit.set(True)
        try:
            return fn(*args, **kwargs)
        except Exception:
            set_db_session_rollback()
            raise
        finally:
            remove_db_session()

    return _wrapped


def should_commit_db_session():
    return context_db_commit.get()


def db_session_commit():
    get_db_session().commit()


def set_db_session_rollback():
    context_db_commit.set(False)
