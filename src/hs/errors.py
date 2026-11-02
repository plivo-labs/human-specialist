"""Every error answers here: the envelope `{"error": {code, message, details}, "request_id"}`, except a Plivo
route's bare status code and a bare 499 when the client went away."""

import asyncio
import logging
import uuid
from http import HTTPStatus

import anyio
import redis
import sqlalchemy.exc
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from hs.infra.counters import MetricName, increase_counter

PUBLIC = "/api/v1"
log = logging.getLogger(__name__)
# A dropped client or load balancer; CancelledError surfaces inside exception groups.
_DISCONNECTS = (
    ClientDisconnect,
    asyncio.CancelledError,
    anyio.EndOfStream,
    anyio.BrokenResourceError,
    BrokenPipeError,
    ConnectionResetError,
)
UNAVAILABLE = (503, "dependency_unavailable", "Plivo or a data store did not answer; try again")
_STORE_DOWN = (redis.ConnectionError, redis.TimeoutError, sqlalchemy.exc.OperationalError)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str = "", details: dict | None = None):
        super().__init__(message or code)
        self.status, self.code, self.message, self.details = status, code, message, details or {}


class Error(BaseModel):
    code: str
    message: str
    details: dict = {}


class ErrorEnvelope(BaseModel):
    error: Error
    request_id: str


def render(request: Request, status: int, code: str, message: str = "", details: dict | None = None) -> Response:
    rid = getattr(request.state, "request_id", None) or uuid.uuid4().hex
    body = {"error": {"code": code, "message": message, "details": details or {}}, "request_id": rid}
    return JSONResponse(body, status, headers={"X-Request-Id": rid})


def _api_error(request: Request, exc: ApiError) -> Response:
    response = render(request, exc.status, exc.code, exc.message, exc.details)
    if exc.status == 429:
        response.headers["Retry-After"] = str(exc.details.get("retry_after", 1))
    return response


def _validation(request: Request, exc: RequestValidationError) -> Response:
    if any(e["type"] == "json_invalid" for e in exc.errors()):
        return render(request, 400, "invalid_json", "the body is not valid JSON")
    fields = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
    return render(request, 422, "validation_failed", "the request is invalid", {"fields": fields})


def _http(request: Request, exc: HTTPException) -> Response:
    code = HTTPStatus(exc.status_code).phrase.lower().replace(" ", "_")
    return render(request, exc.status_code, code, str(exc.detail))


def _unhandled(request: Request, exc: Exception) -> Response:
    log.error("unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
    increase_counter(MetricName.UNHANDLED_ERROR, {"error_type": type(exc).__name__})
    if isinstance(exc, _STORE_DOWN):
        return render(request, *UNAVAILABLE)
    return render(request, 500, "internal_error", "internal server error")


def _disconnect(exc: BaseException) -> bool:
    """Every leaf is a disconnect: a group mixing in a real fault still answers 500."""
    if isinstance(exc, BaseExceptionGroup):
        return bool(exc.exceptions) and all(map(_disconnect, exc.exceptions))
    return isinstance(exc, _DISCONNECTS)


def _chained_disconnect(exc: BaseException) -> bool:
    """A disconnect in the cause/context chain: FastAPI wraps one during the body read in HTTPException(400).
    `raise ... from None` is honoured, and the walk is bounded since a hand-built chain can cycle."""
    for _ in range(10):
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
        if exc is None:
            return False
        if _disconnect(exc):
            return True
    return False


def status_of(exc: Exception) -> int:
    """The status an error answers with; 499 when the client went away."""
    if _disconnect(exc) or (isinstance(exc, HTTPException) and _chained_disconnect(exc)):
        return 499
    if isinstance(exc, ApiError):
        return exc.status
    if isinstance(exc, _STORE_DOWN):
        return 503
    return getattr(exc, "status_code", 422 if isinstance(exc, RequestValidationError) else 500)


def install(app: FastAPI) -> None:
    for exc_class, public in {
        ApiError: _api_error,
        RequestValidationError: _validation,
        HTTPException: _http,
        Exception: _unhandled,
    }.items():
        app.add_exception_handler(exc_class, _scoped(public))


def _scoped(public):
    async def handle(request: Request, exc: Exception) -> Response:
        status = status_of(exc)
        if status == 499 or request.url.path.startswith("/plivo/"):
            # Exception's handler runs outside the request-id middleware, so the id is stamped here too.
            rid = getattr(request.state, "request_id", None)
            return Response(status_code=status, headers={"X-Request-Id": rid} if rid else None)
        return public(request, exc)

    return handle
