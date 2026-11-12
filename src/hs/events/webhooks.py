"""The webhook delivery job: signed, never redirected, sent only to a checked public address."""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import socket
import time
from datetime import UTC, datetime
from urllib.parse import urlsplit, urlunsplit

import httpx
from sqlalchemy import select, update
from sqlalchemy.engine import Row

from hs import core, events, hot, jobs, plivo

log = logging.getLogger(__name__)
DEADLINE = 10.0  # seconds for the whole delivery: connect, send, and the answer's status line
NAT64 = ipaddress.ip_network("64:ff9b::/96")


def destination(url: str) -> str | None:
    """The checked address to connect to, or None for a WEBHOOK_PRIVATE_HOSTS host; ValueError if refused."""
    parts = urlsplit(url)
    host, port = parts.hostname, parts.port
    if not host or parts.username is not None:
        raise ValueError("a webhook URL needs a host and no credentials")
    if host in {h.lower() for h in core.settings.webhook_private_hosts} and parts.scheme in ("http", "https"):
        return None
    if parts.scheme != "https":
        raise ValueError("webhook URLs must use https")
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        try:
            found = socket.getaddrinfo(host, port or 443, type=socket.SOCK_STREAM)
        except OSError as e:
            raise ValueError(f"cannot resolve {host}") from e
        addresses = [ipaddress.ip_address(info[4][0]) for info in found]
    for address in addresses:
        address = getattr(address, "ipv4_mapped", None) or address
        if address in NAT64:
            address = ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
        if not address.is_global or address.is_multicast:
            raise ValueError(f"{host} is not a public address")
    return str(addresses[0])


def signature(timestamp: int, body: bytes, *secrets: str) -> str:
    signed = str(timestamp).encode() + b"." + body
    return ",".join("v1=" + hmac.new(s.encode(), signed, hashlib.sha256).hexdigest() for s in secrets)


def pinned(url: str) -> tuple[str, dict, dict]:
    """(URL, extra headers, request extensions) aimed at the address `destination` checked, keeping the name for
    the Host header and TLS SNI, so a DNS answer that changes after the check cannot move the request."""
    address, parts = destination(url), urlsplit(url)
    if address is None:
        return url, {}, {}
    netloc = f"[{address}]" if ":" in address else address
    url = urlunsplit(parts._replace(netloc=f"{netloc}:{parts.port}" if parts.port else netloc))
    return url, {"Host": parts.netloc}, {"sni_hostname": parts.hostname}


async def post(url: str, body: bytes, headers: dict[str, str]) -> int:
    """The answer's status. The body is never read, and DEADLINE bounds the whole exchange, so neither a huge
    answer nor one trickled a byte at a time can hold the worker."""
    url, host, extensions = pinned(url)
    async with asyncio.timeout(DEADLINE), httpx.AsyncClient(timeout=DEADLINE, trust_env=False) as client:
        sending = client.stream("POST", url, content=body, headers=headers | host, extensions=extensions)
        async with sending as answer:
            return answer.status_code


@jobs.handler("webhook", backoff=events.RETRIES, lane="webhook")
def deliver(job: Row) -> core.ApiError | None:
    """Payload {webhook_id, body}: the envelope rides on the job, so a parked delivery outlives its event. Only a 5xx
    or a failed exchange is retried; any other answer but a 2xx or a 410 is a refusal another try would get again."""
    W, body = core.T.webhooks, job.payload["body"]
    with core.read() as db:
        hook = db.execute(select(W).where(W.c.id == job.payload["webhook_id"])).first()
    if hook is None or not hook.enabled:
        return None
    if hot.tripped(hook.tenant_id, hook.id):
        raise jobs.NotYet(f"webhook {hook.id} rests after a failed exchange")
    keys = [plivo.decrypt(hook.secret)]
    if hook.previous_secret and hook.previous_secret_expires_at > datetime.now(UTC):
        keys.append(plivo.decrypt(hook.previous_secret))
    raw, now = json.dumps(body, separators=(",", ":")).encode(), int(time.time())
    headers = {
        "Content-Type": "application/json",
        "X-HS-Event-Id": body["id"],
        "X-HS-Timestamp": str(now),
        "X-HS-Signature": signature(now, raw, *keys),
    }
    try:
        status = asyncio.run(post(hook.url, raw, headers))
    except Exception:
        hot.trip(hook.tenant_id, hook.id)
        raise
    if status == 410:
        with core.tx() as db:
            db.execute(update(W).where(W.c.id == hook.id).values(enabled=False))
    elif 500 <= status <= 599:
        raise RuntimeError(f"webhook answered {status}")
    elif not 200 <= status < 300:
        log.warning("webhook %s answered %s: parked, not retried", hook.id, status)
        return core.ApiError(502, "webhook_refused", f"the webhook answered {status}")
    return None
