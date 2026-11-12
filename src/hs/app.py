"""The service: every router once, the lifespan, `/status` and the plugin loader."""

import logging
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib.metadata import entry_points

import prometheus_client
from fastapi import APIRouter, Depends, FastAPI, Security
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer
from sqlalchemy.exc import SQLAlchemyError

from hs import access, agents, config, conversations, core, events, jobs, plivo, reports, routing, search, tasks, text
from hs.export import router as export
from hs.text import plivo as text_plivo
from hs.text import templates
from hs.views import router as views
from hs.voice import calls, flow, media, mpc, outbound

log = logging.getLogger(__name__)
VOICE = (flow, calls, outbound, media)
API = (access, config, agents, conversations, routing, *VOICE, text, templates, tasks, search, reports, events)
VOICE_CALLBACKS = (flow, mpc, media)
# Declares the scheme in OpenAPI only; access.require checks the credential.
BEARER = HTTPBearer(
    auto_error=False, description="A tenant API key, the operator key, or an agent, supervisor or stream token."
)


@dataclass(frozen=True)
class Host:
    """What a plugin's `setup(hs)` receives: the three hooks."""

    plugin: str

    @property
    def app(self) -> FastAPI:
        return app

    def on_event(self, fn):
        events.HOOKS[f"{self.plugin}:{fn.__module__}.{fn.__qualname__}"] = fn
        return fn

    def add_resolver(self, fn):
        access.RESOLVERS.append(fn)
        return fn


@asynccontextmanager
async def lifespan(app: FastAPI):
    core.init()
    core.check_versions()
    core.check_eviction()
    for ep in entry_points(group="hs.plugins"):  # setup(hs) of each plugin; one that raises stops startup
        try:
            ep.load()(Host(ep.name))
        except Exception:
            log.exception("plugin %s failed to set up", ep.name)
            raise
    port = core.settings.metrics_port
    metrics = prometheus_client.start_http_server(port)[0] if port else None
    stops = [jobs.start(), events.start()]
    yield
    for stop in stops:
        stop()
    if metrics:
        metrics.shutdown()
        metrics.server_close()
    core.engine.dispose()


def operation_id(route: APIRoute) -> str:
    """Method and path without /api/v1, e.g. post_conversations_conversation_id_notes: unique and readable."""
    return re.sub(r"\W+", "_", f"{min(route.methods)} {route.path_format.removeprefix(core.PUBLIC)}").strip("_").lower()


core.setup_logging()
app = FastAPI(
    title="human-specialist",
    version="0.2.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    generate_unique_id_function=operation_id,
)
# Added first, so it runs inside the request id and CORS middleware that install adds.
app.middleware("http")(access.idempotency)
core.install(app)
api = APIRouter(
    prefix=core.PUBLIC, responses={"default": {"model": core.ErrorEnvelope}}, dependencies=[Security(BEARER)]
)
for module in API:
    api.include_router(module.router)
api.include_router(export)
api.include_router(views)
app.include_router(api)
for module in VOICE_CALLBACKS:
    app.include_router(module.callbacks, prefix="/plivo/{tenant_id}", dependencies=[Depends(plivo.voice_signed)])
app.include_router(text_plivo.callbacks, prefix="/plivo/{tenant_id}")
app.webhooks.include_router(events.webhook_docs)


@app.get("/status", tags=["status"])
def status() -> JSONResponse:
    """Liveness for load balancers: only booleans, never exception text or hosts."""
    try:
        with core.read() as db:
            db.exec_driver_sql("SELECT 1")
    except SQLAlchemyError as e:
        log.warning("status: the database did not answer (%s)", type(e).__name__)
        return JSONResponse({"db": False}, 503)
    return JSONResponse({"db": True})
