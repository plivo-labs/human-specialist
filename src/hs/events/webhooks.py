"""Webhook subscriptions and their delivery job: signed, never redirected, sent only to a checked public address."""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import socket
import time
from datetime import UTC, datetime
from urllib.parse import urlsplit, urlunsplit

import httpx
from sqlalchemy import Boolean, Column, Index, Table, Text, func, select, true, update
from sqlalchemy.dialects.postgresql import ARRAY, TIMESTAMP, UUID

from hs import events
from hs.access import decrypt
from hs.infra.db import DBBase, core_engine
from hs.infra.settings import settings
from hs.jobs import handler

webhooks = Table(
    "webhooks",
    DBBase.metadata,
    Column("id", UUID(as_uuid=False), primary_key=True, server_default=func.gen_random_uuid()),
    Column("tenant_id", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("secret", Text, nullable=False),  # sealed with hs.access.encrypt, as is previous_secret
    Column("previous_secret", Text),
    Column("previous_secret_expires_at", TIMESTAMP(timezone=True)),
    Column("event_types", ARRAY(Text), nullable=False, server_default="{}"),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_webhooks_tenant", "tenant_id"),
)

RETRIES = (10, 60, 300, 1800, 7200, 21600, 43200)
DEADLINE = settings.webhook_deadline  # seconds for the whole delivery: connect, send, and the answer's status line
NAT64 = ipaddress.ip_network("64:ff9b::/96")


def destination(url: str) -> str | None:
    """The checked address to connect to, or None for a WEBHOOK_PRIVATE_HOSTS host; ValueError if refused."""
    parts = urlsplit(url)
    host, port = parts.hostname, parts.port
    if not host or parts.username is not None:
        raise ValueError("a webhook URL needs a host and no credentials")
    if host in {h.lower() for h in settings.webhook_private_hosts} and parts.scheme in ("http", "https"):
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


@handler("webhook", backoff=RETRIES)
def deliver(payload: dict) -> None:
    """Payload {webhook_id, event_id} for an outbox event, or {webhook_id, body} for a test event."""
    with core_engine.connect() as conn:
        hook = conn.execute(select(webhooks).where(webhooks.c.id == payload["webhook_id"])).first()
        body = payload.get("body") or events.one(conn, payload["event_id"])
    if hook is None or not hook.enabled or body is None:
        return
    keys = [decrypt(hook.secret)]
    if hook.previous_secret and hook.previous_secret_expires_at > datetime.now(UTC):
        keys.append(decrypt(hook.previous_secret))
    raw, now = json.dumps(body, separators=(",", ":")).encode(), int(time.time())
    headers = {
        "Content-Type": "application/json",
        "X-HS-Event-Id": body["id"],
        "X-HS-Timestamp": str(now),
        "X-HS-Signature": signature(now, raw, *keys),
    }
    status = asyncio.run(post(hook.url, raw, headers))
    if status == 410:
        with core_engine.begin() as conn:
            conn.execute(update(webhooks).where(webhooks.c.id == hook.id).values(enabled=False))
    elif not 200 <= status < 300:
        raise RuntimeError(f"webhook answered {status}")
