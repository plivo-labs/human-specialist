"""Plivo REST as one account: tenant setup (account read, Application create/update/delete) and endpoints."""

import re

import httpx

from hs.errors import ApiError
from hs.infra.log import logger
from hs.infra.settings import settings


def call(auth: tuple[str, str], method: str, path: str = "", **body) -> httpx.Response:
    """One request as the account `auth` = (auth_id, auth_token), to `path` under /v1/Account/{auth_id}/."""
    url = f"{settings.plivo_api_url}/v1/Account/{auth[0]}/{path}"
    r = httpx.request(method, url, auth=auth, json=body or None, timeout=10)
    if 400 <= r.status_code < 500:
        r.extensions["reason"] = reason(r, auth[1])
        logger.warning(f"Plivo {method} {path} answered {r.status_code}: {r.extensions['reason']}")
    return r


# HS sends Plivo the token only in the auth header, so this guards an accidental echo, not a hostile Plivo.
_FIELD = re.compile(r"(?!.*(?:key|token|secret|signature|password|auth))[a-z_]{1,19}")


def reason(r: httpx.Response, token: str) -> dict:
    """Plivo's own error fields (field name to message), bounded, minus secret-named fields and messages with the token."""
    try:
        body = r.json()
    except ValueError:
        return {}
    if not isinstance(body, dict):
        return {}
    out = {}
    for k, v in list(body.items())[:10]:
        msgs = [v] if isinstance(v, str) else v if isinstance(v, list) else []
        safe = [m[:200] for m in msgs[:5] if isinstance(m, str) and token not in m]
        if safe and _FIELD.fullmatch(k) and k != "api_id":
            out[k] = safe if isinstance(v, list) else safe[0]
    return out


def refused(r: httpx.Response) -> ApiError:
    """The 502 for a Plivo 4xx, with Plivo's reason in `details`."""
    details = {"status": r.status_code, "errors": r.extensions.get("reason", {})}
    return ApiError(502, "plivo_error", f"Plivo answered {r.status_code}", details)


def ok(auth: tuple[str, str], method: str, path: str = "", **body) -> dict:
    """`call`, answering the API caller when Plivo fails: 503 unreachable, 422 credentials refused (401 only), 502 other."""
    try:
        r = call(auth, method, path, **body)
    except httpx.HTTPError as e:
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    if r.status_code == 401:
        raise ApiError(422, "plivo_credentials_rejected", "Plivo refused these credentials")
    if not r.is_success:
        raise refused(r)
    return r.json() if r.content else {}
