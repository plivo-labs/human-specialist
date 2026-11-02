"""The public routers (/api/v1 and Plivo's /plivo/{tenant_id}) and what every /api/v1 route shares: the error
envelope, CORS and idempotency."""

import logging
import re

from fastapi import APIRouter, Depends, FastAPI, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer

from hs import errors
from hs.access import api as access_api
from hs.access import plivo_signed, redact_access_token
from hs.access.idempotency import idempotency
from hs.admin import api as admin_api
from hs.conversations import api as conversations
from hs.conversations import outcomes
from hs.errors import PUBLIC, ErrorEnvelope
from hs.events import api as events
from hs.infra.settings import settings
from hs.intake import api as intake
from hs.reports import api as reports
from hs.setup import api as setup
from hs.voice import api as calls
from hs.voice import recordings

API_ROUTERS: tuple[APIRouter, ...] = (
    access_api.router,
    access_api.sessions_router,
    admin_api.router,
    events.router,
    conversations.router,
    outcomes.router,
    reports.router,
    setup.router,
    recordings.router,
    calls.router,
    intake.public,
)
PLIVO_ROUTERS: tuple[APIRouter, ...] = (intake.router,)

# Declares the scheme in OpenAPI only; access.require checks the credential.
BEARER = HTTPBearer(
    auto_error=False, description="A tenant API key, the operator key, or an agent, supervisor or stream token."
)
api_router = APIRouter(prefix=PUBLIC, responses={"default": {"model": ErrorEnvelope}}, dependencies=[Security(BEARER)])
plivo_router = APIRouter(prefix="/plivo/{tenant_id}", dependencies=[Depends(plivo_signed)])
for _router in API_ROUTERS:
    api_router.include_router(_router)
for _router in PLIVO_ROUTERS:
    plivo_router.include_router(_router)


def operation_id(route: APIRoute) -> str:
    """Method and path without /api/v1, e.g. post_conversations_conversation_id_notes: unique and readable."""
    return re.sub(r"\W+", "_", f"{min(route.methods)} {route.path_format.removeprefix(PUBLIC)}").strip("_").lower()


class _ApiCors(CORSMiddleware):
    """CORS for /api/v1 only: exact origins, bearer tokens (no credentials mode)."""

    def __init__(self, app) -> None:
        super().__init__(
            app,
            allow_origins=list(settings.cors_allowed_origins),
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "Last-Event-ID"],
            expose_headers=["Retry-After", "X-Request-Id"],
        )

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http" and scope["path"].startswith(PUBLIC):
            return await super().__call__(scope, receive, send)
        return await self.app(scope, receive, send)


def mount(app: FastAPI) -> None:
    if "*" in settings.cors_allowed_origins:
        raise RuntimeError("CORS_ALLOWED_ORIGINS takes exact origins, never *")
    errors.install(app)
    app.include_router(api_router)
    app.include_router(plivo_router)
    app.webhooks.include_router(events.catalog)
    app.middleware("http")(idempotency)
    app.add_middleware(_ApiCors)
    logging.getLogger("uvicorn.access").addFilter(redact_access_token)
