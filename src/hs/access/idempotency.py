"""Idempotency-Key on /api/v1 writes: Redis keeps (tenant, principal, key) -> fingerprint + response for 24 h.

The principal is the API key or the agent, not the token, which changes on re-mint. A route that returns a
secret (`require(..., secret=True)`) keeps only the status and resource id, and its replay is 409. Only a success
is kept: an error response frees the key for a retry.
"""

import hashlib
import json

from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response

from hs import access
from hs.errors import PUBLIC, render

DAY, CLAIM = 86400, 120


async def idempotency(request: Request, call_next) -> Response:
    key = request.headers.get("idempotency-key")
    if not key or request.method == "GET" or not request.url.path.startswith(PUBLIC):
        return await call_next(request)
    token = access.bearer(request)
    principal = await run_in_threadpool(access.resolve, token) if token else None
    if principal is None:
        return await call_next(request)
    who = principal.key_id or principal.agent_id or principal.kind
    slot = f"IDEM:{principal.tenant_id}:{who}:{access.digest(key)}"
    body = await request.body()
    target = f"{request.method} {request.scope['path']}?{request.scope['query_string'].decode()} "
    fingerprint = hashlib.sha256(target.encode() + body).hexdigest()
    r = access.redis_client()
    # A short claim: a first request that dies mid-way frees its key soon.
    if not r.set(slot, json.dumps({"fp": fingerprint}), nx=True, ex=CLAIM):
        return _replay(request, json.loads(r.get(slot) or "{}"), fingerprint)
    try:
        response = await call_next(request)
    except BaseException:
        r.delete(slot)
        raise
    if response.status_code >= 400:
        r.delete(slot)
        return response
    content = b"".join([chunk async for chunk in response.body_iterator])
    stored = {"fp": fingerprint, "status": response.status_code}
    if getattr(request.state, "secret", False):
        sent = json.loads(content or b"{}")
        stored["id"] = sent.get("id") or sent.get("tenant_id")
    else:
        stored |= {"body": content.decode(), "type": response.headers.get("content-type")}
    r.set(slot, json.dumps(stored), ex=DAY)
    return Response(content, response.status_code, headers=dict(response.headers))


def _replay(request: Request, stored: dict, fingerprint: str) -> Response:
    if stored.get("fp") != fingerprint:
        return render(request, 409, "idempotency_conflict", "this Idempotency-Key was used with another request")
    if "status" not in stored:
        return render(request, 409, "idempotency_in_progress", "the first request with this key is still running")
    if "body" not in stored:
        msg = "the response held a secret and is not replayed"
        return render(request, 409, "idempotency_replay_secret", msg, {"id": stored.get("id")})
    return Response(stored["body"], stored["status"], media_type=stored["type"])
