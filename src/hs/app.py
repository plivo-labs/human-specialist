"""The service: every router once, the lifespan, `/status` and the plugin loader."""

import logging
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path

import prometheus_client
from fastapi import APIRouter, Depends, FastAPI, Security
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer
from fastapi.staticfiles import StaticFiles
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
UI = Path("/app/ui")
# The console's own routes (ui/src/main.tsx), each with plain sub-path segments: a GET of one that names no file gets its page.
CONSOLE = r"(signin|you|desk|floor|conversations|tasks|teams|queues|skills|statuses|contacts|outbound|hours|workload|dispositions|agents|numbers|plivo)(/[A-Za-z0-9_-]+)*/?$"
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


class _UI(StaticFiles):
    def __init__(self, missing) -> None:
        super().__init__(directory=UI, html=True)
        self.missing = missing

    async def __call__(self, scope, receive, send) -> None:
        web = scope.get("method") in ("GET", "HEAD") and not scope["path"].startswith(("/api/", "/plivo/"))
        await (super().__call__ if web else self.missing)(scope, receive, send)

    def lookup_path(self, path: str):
        found = super().lookup_path(path)
        return found if found[1] or not re.match(CONSOLE, path) else super().lookup_path("index.html")

    def file_response(self, full_path, stat_result, scope, status_code=200) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        # Only Vite's assets carry their hash in the name. Judged by the file served, not the path asked:
        # /assets/%2e%2e%2findex.html serves index.html.
        hashed = Path(full_path).is_relative_to(Path(self.directory).resolve() / "assets")
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable" if hashed else "no-cache"
        return response


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
    # Not app.mount("/"): a root Mount full-matches every path, so a route's 405 would become a 404.
    if UI.is_dir():
        app.router.default = _UI(app.router.default)
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
    version="1.0.0",
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
for router in (*(module.router for module in API), export, views):
    api.include_router(router)
app.include_router(api)
for module in VOICE_CALLBACKS:
    app.include_router(module.callbacks, prefix="/plivo/{tenant_id}", dependencies=[Depends(plivo.voice_signed)])
app.include_router(text_plivo.callbacks, prefix="/plivo/{tenant_id}")
app.webhooks.include_router(events.webhook_docs)


@app.get("/status", tags=["status"])
def status() -> JSONResponse:
    """Liveness for load balancers: only booleans, never exception text or hosts. `setup` is true while a registration
    needs no operator key, so a console knows before it asks for one."""
    try:
        with core.read() as db:
            db.exec_driver_sql("SELECT 1")
            setup = access.unclaimed(db)
    except SQLAlchemyError as e:
        log.warning("status: the database did not answer (%s)", type(e).__name__)
        return JSONResponse({"db": False}, 503)
    return JSONResponse({"db": True} | ({"setup": True} if setup else {}))
