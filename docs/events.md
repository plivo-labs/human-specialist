# Events and webhooks

`events.emit` is the only outbound notification path. Each event is written to a Postgres table, stamped with a per-service sequence number by one relay, then sent to the stream and to webhook jobs. The event id is that sequence number. Event types are listed in `src/hs/events/catalog.py` and in OpenAPI under `webhooks`.

## Stream

`GET /api/v1/events/stream` is server-sent events. Each frame has `id: <seq>` and a `data:` JSON envelope (`id`, `type`, `created_at`, `tenant_id`, `conversation_id`, `agent_id`, `direction`, `context`, `data`). There is no `event:` field, so `EventSource.onmessage` receives every event. The server sends `: ping` every 15 seconds, and `auth.expired` then closes the stream when the token ends. To resume, send `Last-Event-ID` or `?after=<seq>`; the server replays everything after it. Agents see their own events, supervisors those of agents they can access, and tenant keys see all events.

## Webhooks

Create one with `POST /webhooks {url, event_types}`; the secret is returned once. Deliveries are at-least-once, so dedupe on the event id. A delivery succeeds on any 2xx within 10 seconds; redirects are not followed; a 410 disables the subscription. Failed deliveries are retried after 10 s, 1 min, 5 min, 30 min, 2 h, 6 h and 12 h, then marked failed. Each attempt carries `X-HS-Event-Id`, `X-HS-Timestamp` and `X-HS-Signature: v1=<hex>`, where the value is an HMAC-SHA256 over `timestamp + "." + raw body`. During the 24 hours after `POST /webhooks/{id}/rotate-secret` the header holds both `v1=` values, comma separated.

```python
import hmac, hashlib, time

def verify(secret: str, timestamp: str, body: bytes, header: str) -> bool:
    if abs(time.time() - int(timestamp)) > 300:
        return False
    expected = "v1=" + hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, part.strip()) for part in header.split(","))
```

Webhook URLs must be https and must resolve to public addresses. `WEBHOOK_PRIVATE_HOSTS` lists the hosts that may use http or a private address, for a receiver on your own machine or network.

## Replay

`GET /api/v1/events?after=<seq>&type=` returns every retained event after a sequence number, in order, with `next_cursor`. Use it to recover after downtime or after a delivery has failed. Events older than `EVENT_RETENTION_DAYS` are deleted. Order across events is not guaranteed; `GET /conversations/{id}` is the source of truth. One call can emit `call.ended` more than once, under different event ids, when Plivo replays one of its callbacks, so keep one call record per `conversation_id`. `POST /webhooks/{id}/test` sends a `webhook.test` event.
