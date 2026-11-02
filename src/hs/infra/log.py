import logging
import sys
from collections.abc import Mapping

from hs.infra.settings import settings

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [human-specialist] %(message)s"))
_base = logging.getLogger("human-specialist")
if not _base.handlers:
    _base.addHandler(_handler)
_base.setLevel(getattr(logging, settings.log_level, logging.INFO))
# Writes go through a bounded queue and one writer thread: a stalled stdout consumer costs memory, not the event loop.
from hs.infra.log_nonblocking import install as _install_nonblocking_sink  # noqa: E402

_install_nonblocking_sink(_base)
from hs.infra.log_context import context_req_meta  # noqa: E402  (own module: survives a reload of this one)

# Attribute names LogRecord itself sets (plus "message"/"asctime", which the
# stdlib also special-cases) — an extra= key matching one of these makes
# logging.Logger.makeRecord raise KeyError. Sanitize by prefixing collisions
# so extra context is never silently dropped and never crashes logging.
_RESERVED_RECORD_KEYS = frozenset(vars(logging.LogRecord("", logging.INFO, "", 0, "", None, None)).keys()) | {
    "message",
    "asctime",
}

_SAFE_REQUEST_HEADER_NAMES = frozenset(
    {
        "accept",
        "api-id",
        "client-type",
        "client-version",
        "content-length",
        "content-type",
        "traceparent",
        "user-agent",
        "x-correlation-id",
        "x-request-id",
    }
)
_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "api-key",
        "apikey",
        "auth",
        "authentication",
        "credential",
        "credentials",
        "auth-token",
        "authorization",
        "cookie",
        "password",
        "password-hash",
        "passwordhash",
        "proxy-authorization",
        "secret",
        "set-cookie",
        "signature",
        "token",
        "x-api-key",
        "x-auth-token",
        "x-client-secret",
        "x-plivo-signature-v2",
        "x-plivo-signature-v3",
        "x-signature",
    }
)
_REDACTED = "[REDACTED]"
_CIRCULAR = "[CIRCULAR]"
_TRUNCATED = "[TRUNCATED]"
_SANITIZATION_FAILED = "[SANITIZATION_FAILED]"
_MAX_LOG_NESTING_DEPTH = 8


def safe_request_headers(headers) -> dict:
    """Return bounded, non-secret request headers suitable for logs."""
    try:
        items = headers.items()
    except AttributeError:
        return {}
    result = {}
    for key, value in items:
        normalized = str(key).strip().lower()
        if normalized in _SAFE_REQUEST_HEADER_NAMES:
            result[normalized] = str(value)[:512]
    return result


def _normalized_field_name(key) -> str:
    return str(key).strip().lower().replace("_", "-")


def _sanitize_log_value(key, value, *, _depth=0, _active_container_ids=None):
    normalized = _normalized_field_name(key)
    if normalized == "headers":
        return safe_request_headers(value)
    if (
        normalized in _SENSITIVE_FIELD_NAMES
        or normalized.endswith("-password")
        or normalized.endswith("-secret")
        or normalized.endswith("-token")
        or normalized.endswith("-signature")
        or normalized.endswith("-auth")
        or normalized.endswith("-credential")
        or normalized.endswith("-credentials")
        or normalized.endswith("-password-hash")
    ):
        return _REDACTED

    is_container = isinstance(value, (Mapping, list, tuple, set, frozenset))
    if not is_container:
        return value
    if _depth >= _MAX_LOG_NESTING_DEPTH:
        return _TRUNCATED

    if _active_container_ids is None:
        _active_container_ids = set()
    container_id = id(value)
    if container_id in _active_container_ids:
        return _CIRCULAR
    _active_container_ids.add(container_id)
    try:
        if isinstance(value, Mapping):
            return {
                nested_key: _sanitize_log_value(
                    nested_key, nested_value, _depth=_depth + 1, _active_container_ids=_active_container_ids
                )
                for nested_key, nested_value in value.items()
            }
        if isinstance(value, list):
            # Never reconstruct an arbitrary list subclass: its constructor may
            # require application-specific arguments or execute user code.
            return [
                _sanitize_log_value(key, nested_value, _depth=_depth + 1, _active_container_ids=_active_container_ids)
                for nested_value in value
            ]
        if isinstance(value, tuple):
            # Namedtuples and tuple subclasses cannot generally be reconstructed
            # from a single iterable. A plain tuple retains ordering safely.
            return tuple(
                _sanitize_log_value(key, nested_value, _depth=_depth + 1, _active_container_ids=_active_container_ids)
                for nested_value in value
            )
        if isinstance(value, set):
            return {
                _sanitize_log_value(key, nested_value, _depth=_depth + 1, _active_container_ids=_active_container_ids)
                for nested_value in value
            }
        if isinstance(value, frozenset):
            return frozenset(
                _sanitize_log_value(key, nested_value, _depth=_depth + 1, _active_container_ids=_active_container_ids)
                for nested_value in value
            )
    finally:
        _active_container_ids.remove(container_id)


def _safe_log_text(value) -> str:
    try:
        return str(value)
    except Exception:
        return _SANITIZATION_FAILED


class _Logger:
    def _emit(self, level, *args, **kwargs):
        msg = kwargs.pop("msg", None)
        if msg is None and args:
            msg, args = args[0], args[1:]
        extra = kwargs.pop("extra", None) or context_req_meta.get()
        exc_info = kwargs.pop("exc_info", None)
        if extra:
            safe_extra = {}
            try:
                extra_items = extra.items()
            except Exception:
                extra_items = (("log_context", _SANITIZATION_FAILED),)
            for key, value in extra_items:
                safe_key = f"extra_{key}" if key in _RESERVED_RECORD_KEYS else key
                try:
                    safe_extra[safe_key] = _sanitize_log_value(key, value)
                except Exception:
                    # Logging context is diagnostic. It must never take down the
                    # request path, and falling back must not serialize the raw
                    # value that failed sanitization.
                    safe_extra[safe_key] = _SANITIZATION_FAILED
            suffix = " ".join(f"{_safe_log_text(k)}={_safe_log_text(v)}" for k, v in safe_extra.items())
            msg = f"{msg} [{suffix}]"
        _base.log(level, msg, *args, exc_info=exc_info if exc_info is not None else False)

    def info(self, *a, **k):
        self._emit(logging.INFO, *a, **k)

    def error(self, *a, **k):
        self._emit(logging.ERROR, *a, **k)

    def warning(self, *a, **k):
        self._emit(logging.WARNING, *a, **k)

    def debug(self, *a, **k):
        self._emit(logging.DEBUG, *a, **k)


logger = _Logger()
