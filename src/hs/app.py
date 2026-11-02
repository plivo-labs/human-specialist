import time
import uuid
from contextlib import asynccontextmanager

import prometheus_client
from fastapi import Depends, FastAPI, Request, Response

# Mapped-model registration: every ORM module must import before the first configure_mappers().
import hs.admin.account
import hs.admin.dispositions
import hs.conversations.search_store
import hs.conversations.store
import hs.setup.number
import hs.voice.call_store  # noqa: F401
from hs import api, events, jobs
from hs.errors import status_of
from hs.infra import status
from hs.infra.context import context_request_id
from hs.infra.counters import record_request_outcome
from hs.infra.log import logger
from hs.infra.metrics import http_request_response_metric
from hs.infra.settings import settings


def _shutdown_cleanup():
    try:
        from hs.infra.redis import Cache

        Cache.get_instance().close_redis()
    except Exception:
        logger.warning("_shutdown_cleanup:: failed to close redis", exc_info=1)
    try:
        from hs.infra.db import core_engine

        core_engine.dispose()
    except Exception:
        logger.warning("_shutdown_cleanup:: failed to dispose core_engine", exc_info=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from hs.admin.heartbeat_sweeper import start_heartbeat_sweeper

    start_heartbeat_sweeper()
    import hs.conversations.timers  # noqa: F401  registers the job kinds the runner claims

    metrics_server = prometheus_client.start_http_server(settings.metrics_port)[0] if settings.metrics_port else None
    stops = [jobs.start(), events.start()]
    yield
    for stop in stops:
        stop()
    if metrics_server:
        metrics_server.shutdown()
        metrics_server.server_close()
    _shutdown_cleanup()


async def _scoped_db_session_guard():
    from hs.infra.context import context_db_session, fallback_session_active, remove_db_session, set_db_session_rollback

    failed = False
    try:
        yield
    except Exception:
        failed = True
        raise
    finally:
        # Tear down only an existing fallback session on this loop thread; worker sessions have their own guard.
        if context_db_session.get() is None and fallback_session_active():
            if failed:
                set_db_session_rollback()
            remove_db_session()


app = FastAPI(
    title="human-specialist",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    dependencies=[Depends(_scoped_db_session_guard)],
    generate_unique_id_function=api.operation_id,
)


def _patch_background_tasks_db_guard():
    from starlette.background import BackgroundTasks as _BT

    if getattr(_BT, "_hs_db_guarded", False):
        return
    from starlette._utils import is_async_callable

    from hs.infra.context import bg_db_guard

    _orig_add_task = _BT.add_task

    def add_task(self, func, *args, **kwargs):
        if not is_async_callable(func):
            func = bg_db_guard(func)
        return _orig_add_task(self, func, *args, **kwargs)

    _BT.add_task = add_task
    _BT._hs_db_guarded = True


_patch_background_tasks_db_guard()


def _patch_def_endpoint_db_guard():
    # Run fallback session teardown on the same worker thread as the sync endpoint.
    # The guard must remain harmless for response validation when no fallback session exists.
    import fastapi.routing as _fr

    if getattr(_fr, "_hs_db_guarded", False):
        return
    from hs.infra.context import threadpool_db_guard

    _orig_run_in_threadpool = _fr.run_in_threadpool

    async def _guarded_run_in_threadpool(func, *args, **kwargs):
        return await _orig_run_in_threadpool(threadpool_db_guard(func), *args, **kwargs)

    _fr.run_in_threadpool = _guarded_run_in_threadpool
    _fr._hs_db_guarded = True


_patch_def_endpoint_db_guard()


def _observe_request(request: Request, status_code: int, start_time: float) -> None:
    response_time = time.time() - start_time
    path = request.url.path
    for key, val in request.path_params.items():
        path = path.replace(val, f":{key}", 1)
    # Bound label cardinality: an UNMATCHED request (404 — no route in scope) would otherwise
    # mint a new Prometheus series per unique scanner/typo URL, growing the registry (and
    # /metrics payload) for the life of the process. Matched routes keep the exact same
    # labels as before.
    route = request.scope.get("route")
    if route is None:
        path = "<unmatched>"
    http_request_response_metric.observe(
        {"endpoint": path, "method": request.method, "response_code": status_code}, response_time
    )
    record_request_outcome(getattr(route, "path", "<unmatched>"), request.method, status_code)


# An uncaught exception unwinds this middleware before the Exception handler (outermost
# ServerErrorMiddleware) builds its 500/499, so observe with the status it will pick, then re-raise.
async def add_response_time_metric(request: Request, call_next):
    start_time = time.time()
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        _observe_request(request, status_of(exc), start_time)
        raise
    _observe_request(request, response.status_code, start_time)
    return response


REQUEST_ID_HEADER = "X-Request-Id"


# Registered AFTER add_response_time_metric so it wraps OUTERMOST (Starlette runs the
# last-added @app.middleware("http") first) — it must see/stamp every request before
# add_response_time_metric (and any handler) runs, and see the response last on the
# way out so the echoed header survives untouched. Pure-async, O(1): no I/O, no DB.
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex
    context_request_id.set(request_id)
    request.state.request_id = request_id
    response: Response = await call_next(request)
    response.headers[REQUEST_ID_HEADER] = request_id
    return response


app.include_router(status.router)
api.mount(app)
# Register last so request metrics include every middleware's failures.
app.middleware("http")(add_response_time_metric)
