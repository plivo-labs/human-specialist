# Self-hosting

The compose stack in the README is for evaluation. For production, run the image on its own against databases you operate.

## Run the image

```sh
docker run --rm --env-file prod.env ghcr.io/plivo-labs/human-specialist:0.1.0 alembic upgrade head
docker run -d --restart unless-stopped -p 6004:6004 --env-file prod.env ghcr.io/plivo-labs/human-specialist:0.1.0
```

The first command is the migration. It is a one-shot step that fails closed, and the service never migrates at boot, so run it on every upgrade before you start the new version. The variables are in [Configuration](configuration.md).

## Postgres and Redis

- Point `DATABASE_URL` and `REDIS_URL` at servers you run (Postgres 13 or later, Redis 6 or later).
- Back up Postgres. It holds tenants, configuration, conversations and events.
- Redis holds live call state: agent capacity, offers and per-call context.
- Keep a copy of `HS_SECRET_KEY`. Without it the stored Plivo tokens cannot be decrypted.

## Public URL and TLS

Plivo calls the service back at `PUBLIC_BASE_URL`, so it must be a public `https://` URL. Terminate TLS at your load balancer or reverse proxy and forward to port 6004. Do not buffer the response of `GET /api/v1/events/stream`: it is a server-sent-events stream with a ping every 15 seconds.

## Health and replicas

- `GET /status` is public and answers 200 only if Postgres and Redis answer. Use it as the load balancer and container health check; the image already uses it.
- Run as many identical replicas as you need behind the load balancer. The job runner and the event relay run in every replica and take locks, so replicas never run the same job at once. Webhook deliveries are at least once, so consumers dedupe by event id. See [Architecture](architecture.md#process-model).
- Prometheus metrics are on `METRICS_PORT` (9100). Publish it only where your monitoring can reach it.
