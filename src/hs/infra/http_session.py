"""The process-wide httpx clients for Plivo's REST API. A client is thread-safe and pools its connections (100
covers the AnyIO threadpool, about 40), and each refuses cookies, so an upstream Set-Cookie is never replayed on a
later request."""

import time
from http.cookiejar import DefaultCookiePolicy

import httpx

RETRY_STATUSES = (502, 503, 504)


def _client(connect_retries: int) -> httpx.Client:
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    client = httpx.Client(transport=httpx.HTTPTransport(retries=connect_retries, limits=limits))
    client.cookies.jar.set_policy(DefaultCookiePolicy(allowed_domains=[]))
    return client


# Idempotent reads retry a failed connect twice; writes get one attempt, since a retried write could repeat a
# provider effect.
READS, WRITES = _client(2), _client(0)


def read(method: str, url: str, **kwargs) -> httpx.Response:
    """An idempotent read on READS that also rides out a 502/503/504, twice, 0.2 s then 0.4 s apart."""
    for attempt in range(3):
        if attempt:
            time.sleep(0.2 * attempt)
        resp = READS.request(method, url, **kwargs)
        if resp.status_code not in RETRY_STATUSES:
            break
    return resp
