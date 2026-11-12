# Deploy

human-specialist is one container image, `ghcr.io/plivo-labs/human-specialist:<version>` (a release tag publishes it; until the first one, build it with `docker build -f docker/Dockerfile -t human-specialist .`), and it runs on any platform that runs containers: Docker or Docker Compose, AWS ECS or Fargate, Kubernetes, Google Cloud Run and the like. The optional UI builds to a folder of static files. This page lists what every deployment needs and the settings that differ by platform.

## What HS needs

- **Postgres 14 or later.** HS keeps its tables in the schema `hs`. Point `DATABASE_URL` at it.
- **Redis 7 or later, set to `maxmemory-policy noeviction`.** HS keeps hot state there and rebuilds it from Postgres after a loss, but never a key Redis evicted. Point `REDIS_URL` at it. See [Run Redis](configuration.md#run-redis).
- **A public HTTPS URL that reaches HS.** Plivo calls HS back at `PUBLIC_BASE_URL`, so a load balancer, an ingress or a tunnel must route it to the container's port 6004.
- **Two secrets.** `HS_OPERATOR_KEY` guards the tenant routes and `HS_SECRET_KEY` encrypts Plivo tokens at rest. Load both from your platform's secret store.
- **One migration step before each start and each upgrade.** Run `alembic upgrade head` once, with the service's environment, and start the service only after it succeeds.
- **The legacy time zone names, if you run HS outside the image.** The image installs them. On Debian 13, Ubuntu 24.04 or later, install `tzdata-legacy`, or business hours refuse names that browsers still offer, such as `Asia/Calcutta`.

[Configuration](configuration.md) lists every setting.

## Run it on your platform

Every platform runs the same image twice: once as a one-off task for the migration, and once as the long-running service.

| Platform | Migration | Service |
|---|---|---|
| Docker Compose | the `migrate` service in `docker/compose.yaml` | the `hs` service |
| AWS ECS or Fargate | a one-off task (`aws ecs run-task`) with the command `alembic upgrade head` | an ECS service behind an Application Load Balancer |
| Kubernetes | a Job with the command `alembic upgrade head` | a Deployment behind a Service and an Ingress |
| Google Cloud Run | a Cloud Run job with the command `alembic upgrade head` | a Cloud Run service |

The settings that differ by platform:

- **Health check.** Point it at `GET /status`, which answers 200 while the database answers.
- **Background work.** Each HS process runs the job runner, the event relay and the sweeper next to the API. On a platform that throttles CPU between requests, such as Cloud Run, keep CPU always allocated and at least one instance running, or timers and webhooks stall.
- **Event streams.** `GET /api/v1/events/stream` stays open and sends a ping while idle, so the default idle timeouts of load balancers hold. A platform that caps request length, such as Cloud Run, ends the stream at the cap, and clients reconnect with `Last-Event-ID`.
- **Scale out.** Run two or more replicas behind the load balancer. They share Postgres and Redis and need no sticky sessions. Size the Postgres connections for the replica count, as [Size the database connections](configuration.md#size-the-database-connections) shows.

## Host the UI

The UI in `ui/` is optional. Your own UI uses the same API, event stream and webhooks. The UI builds to static files in `ui/dist`, and you host them one of two ways.

**On the same origin as HS, with Docker Compose.** The `ui` profile builds the app and serves it from a Caddy container that passes `/api`, `/plivo`, `/status` and `/openapi.json` to HS on one port, `UI_PORT` (6005 by default).

```sh
docker compose -f docker/compose.yaml --profile ui up -d --build --wait
```

Point `PUBLIC_BASE_URL` at that port, so one URL serves the UI and Plivo's callbacks. To run the same image outside Compose, build it from the repo root with `docker build -f docker/ui.Dockerfile -t human-specialist-ui .` and run it with `HS_UPSTREAM` set to HS's address.

**On any static host, on another origin.** Build the app with the Node version in `ui/.nvmrc`:

```sh
cd ui
npm ci
npm run build
```

Then:

1. Set the HS URL in `ui/dist/config.js`, for example `window.HS_URL = "https://hs.example.com";`.
2. Upload `ui/dist` to S3 behind CloudFront, Netlify, Vercel, GitHub Pages or any web server.
3. Add the UI's origin to HS's `CORS_ALLOWED_ORIGINS`, for example `https://desk.example.com`, and restart HS.

Serve the UI over HTTPS. The softphone needs a secure context, and browsers allow the microphone only on HTTPS or `localhost`.
