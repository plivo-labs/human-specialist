# Configuration

Environment variables only. With Docker Compose, copy `docker/.env.example` to `docker/.env`; compose passes it to the service.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | yes | | Postgres URL; Postgres 13 or later (the schema uses the built-in `gen_random_uuid()`) |
| `REDIS_URL` | yes | | Redis URL; Redis 6 or later (the client speaks RESP3) |
| `PUBLIC_BASE_URL` | yes | | Absolute http or https URL with a host that reaches this service on port 6004; Plivo calls it back. Startup refuses anything else |
| `HS_OPERATOR_KEY` | yes | | Bearer key for the operator routes (`/api/v1/tenants`) |
| `HS_SECRET_KEY` | yes | | Key material for Plivo tokens at rest and signed recording URLs, 32 bytes or more |
| `PLIVO_API_URL` | no | `https://api.plivo.com` | Plivo REST host (the test simulator overrides it) |
| `PLIVO_MEDIA_URL` | no | `https://media.plivo.com` | Host recordings are fetched from |
| `CORS_ALLOWED_ORIGINS` | no | empty | Comma-separated browser origins allowed on `/api/v1` |
| `EVENT_RETENTION_DAYS` | no | `90` | How long events are kept |
| `WEBHOOK_PRIVATE_HOSTS` | no | empty | Hosts whose webhooks may use http or a private address (local receivers) |
| `WEBHOOK_DEADLINE` | no | `10` | Seconds a webhook delivery may take in all before it counts as failed and is retried |
| `METRICS_PORT` | no | `9100` | Prometheus metrics port; `0` turns it off. `docker/compose.yaml` does not publish it; uncomment its line there to scrape it |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `HOLD_MUSIC_URL` | no | `https://s3.amazonaws.com/plivocloud/music.mp3` | What callers hear while queued or on hold, and agents on hold, when a queue sets no `hold_music`: Plivo's sample track, the one its [Multiparty Call coaching example](https://plivo.com/docs/voice/use-cases/supervisor-coaching) plays. Empty plays nothing: callers hear silence |
| `PORT` | no | `6004` | Host port `docker/compose.yaml` publishes the service on; the service itself always listens on 6004 |

Generate the two secrets with `openssl rand -base64 32`, and keep a copy of `HS_SECRET_KEY`: without it, the stored Plivo tokens cannot be decrypted.

Plivo must be able to reach the service, so `PUBLIC_BASE_URL` has to be a public URL (a tunnel is enough for development). Registering a tenant creates a Plivo Application whose answer and hangup URLs point at it. If the URL changes, update the environment, restart, and `PATCH /api/v1/tenants/{id}` to re-apply it at Plivo.

HS calls Plivo's REST API from its own host. An account that restricts API access by IP must list that host's egress IP, or Plivo answers 403 and HS reports `plivo_error` with Plivo's reason.

