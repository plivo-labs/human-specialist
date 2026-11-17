# Deploy

human-specialist is one container image, `ghcr.io/plivo-labs/human-specialist:<version>` (a release tag publishes it; until the first one, build it with `docker build -t human-specialist .`), and it runs on any platform that runs containers: Docker or Docker Compose, AWS ECS or Fargate, Kubernetes, Google Cloud Run and the like. This page lists what every deployment needs and the settings that differ by platform.

## One image, one URL

The image serves the [reference UI](ui.md) at `/`, on port 6004 beside the API, so the one URL in `PUBLIC_BASE_URL` serves the UI, the API, the event stream and Plivo's callbacks. The UI routes by path (`/desk`). A path that a route of HS or of a plugin answers keeps that answer. A GET or HEAD of any other path outside `/api/` and `/plivo/` answers a file of the UI. A path of one of the UI's own screens that names no file, such as `/desk` or `/desk/{id}`, with only letters, digits, hyphens and underscores in the segments under the screen, answers the UI's `index.html`, so a link to any of its screens opens it; any other answers 404. The screens are the routes `CONSOLE` in `src/hs/app.py` lists, which a test keeps equal to the UI's own. Every other request answers as it would without the UI. `index.html` and `config.js` go out with `Cache-Control: no-cache`, so a browser picks up a new release at once, and the files under `assets/`, whose names carry their content's hash, as immutable for a year. An image derived from this one that wants no UI at `/` deletes `/app/ui` in its own Dockerfile.

## What HS needs

- **Postgres 14 or later, which you back up.** HS keeps its tables in the schema `hs`. Point `DATABASE_URL` at it.
- **Redis 7 or later, set to `maxmemory-policy noeviction`.** HS keeps hot state there and rebuilds it from Postgres after a loss, but never a key Redis evicted. Point `REDIS_URL` at it. See [Run Redis](configuration.md#run-redis).
- **A public HTTPS URL that reaches HS.** Plivo calls HS back at `PUBLIC_BASE_URL`, so a load balancer, an ingress or a tunnel must route it to the container's port 6004.
- **Two secrets.** `HS_OPERATOR_KEY` guards the tenant routes and `HS_SECRET_KEY` encrypts Plivo tokens at rest. Load both from your platform's secret store.
- **One migration step before each start and each upgrade.** Run `alembic upgrade head` once, with the service's environment, and start the service only after it succeeds. Runs that start together take turns, so a run that waited finds nothing left to do. On a platform with no step before the start, make `/app/migrate-and-serve` the service's command: it runs the migration, then starts HS on `PORT`, 6004 when unset. With plain Docker, run `docker run --rm --env-file docker/.env ghcr.io/plivo-labs/human-specialist:<version> alembic upgrade head`.
- **The legacy time zone names, if you run HS outside the image.** The image installs them. On Debian 13, Ubuntu 24.04 or later, install `tzdata-legacy`, or business hours refuse names that browsers still offer, such as `Asia/Calcutta`.

[Configuration](configuration.md) lists every setting.

## The first run

Open `PUBLIC_BASE_URL` in a browser and choose **Register**. Enter the Plivo account's Auth ID and Auth Token, your email and a password. When `HS_SETUP_PLIVO_AUTH_ID` names that Auth ID and no organization was registered yet, that is all: the install needs no operator key, as [The first registration](configuration.md#the-first-registration) explains. Otherwise the form asks for `HS_OPERATOR_KEY`. The desk then opens, signed in as you, the organization's first administrator.

Setup ends at the first tenant row; a registration with no key that was already under way may still complete. Like every route, `POST /api/v1/tenants` reads the request body before it checks the credential, so a body that is not JSON answers 400 even without one. Setup mode adds no way past that check: with no key, only the Auth ID in `HS_SETUP_PLIVO_AUTH_ID` gets as far as HS asking Plivo, and only 10 times from one address in 15 minutes.

## Run it on your platform

Every platform runs the same image twice: once as a one-off task for the migration, and once as the long-running service. Render's free plan has no step before the start, so there the service's own command runs both.

| Platform | Migration | Service |
|---|---|---|
| Docker Compose | the `migrate` service in `docker/compose.yaml` | the `hs` service |
| AWS ECS or Fargate | a one-off task (`aws ecs run-task`) with the command `alembic upgrade head` | an ECS service behind an Application Load Balancer |
| Kubernetes | a Job with the command `alembic upgrade head` | a Deployment behind a Service and an Ingress |
| Google Cloud Run | a Cloud Run job with the command `alembic upgrade head` | a Cloud Run service |
| Render | the service's own command, `/app/migrate-and-serve`, as [Render](#render) sets it | a web service |
| Heroku | the release phase in `heroku.yml`, as [Heroku](#heroku) describes | the `web` dyno |
| Railway | the pre-deploy command `alembic upgrade head`, which [a template](#railway) sets | a service built from this repository |

The compose stack in `docker/` is for trying things out, as [Quick start](quickstart.md) runs it. Its Postgres runs in a container with a fixed password.

The settings that differ by platform:

- **Health check.** Point it at `GET /status`, which answers 200 while the database answers.
- **Background work.** Each HS process runs the job runner, the event relay and the sweeper next to the API. On a platform that throttles CPU between requests, such as Cloud Run, keep CPU always allocated and at least one instance running, or timers and webhooks stall.
- **Event streams.** `GET /api/v1/events/stream` stays open and sends a ping while idle, so the default idle timeouts of load balancers hold. A platform that caps request length, such as Cloud Run, ends the stream at the cap, and clients reconnect with `Last-Event-ID`.
- **Scale out.** Run two or more replicas behind the load balancer. They share Postgres and Redis and need no sticky sessions. Size the Postgres connections for the replica count, as [Size the database connections](configuration.md#size-the-database-connections) shows.

## Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/plivo-labs/human-specialist)

**Status.** Not yet deployed on a live Render account. The first deploy should check that its log shows `alembic upgrade head` finishing before uvicorn starts, with no `permission denied for database`, and that HS does not stop on a missing `PUBLIC_BASE_URL`.

The button reads `render.yaml` at the root of this repository, a Blueprint of three resources on Render's free plans: the web service `human-specialist`, built from the root `Dockerfile`, the Postgres database `hs-postgres` and the Key Value instance `hs-redis`.

1. Press the button and sign in to Render.
2. In `HS_SETUP_PLIVO_AUTH_ID`, the one value the Blueprint asks for, enter the Auth ID of the Plivo account that will own this install, and apply the Blueprint.
3. Wait for the web service to go live. Its command, `/app/migrate-and-serve`, runs `alembic upgrade head` and then starts HS, and Render waits for `GET /status` to answer.
4. Open the service's `onrender.com` URL and register as [The first run](#the-first-run) describes.

| Setting | Value |
|---|---|
| `PORT` | `6004`, the port Render routes to and HS listens on |
| `METRICS_PORT` | `0`, because Render forwards one port |
| `DATABASE_URL`, `REDIS_URL` | the internal connection strings of `hs-postgres` and `hs-redis` |
| `PUBLIC_BASE_URL` | the service's own `RENDER_EXTERNAL_URL`, its `onrender.com` URL |
| `HS_OPERATOR_KEY`, `HS_SECRET_KEY` | generated by Render, each a random 256-bit value in base64 |
| Health check path | `/status` |
| Automatic deploys | off, so a push to this repository does not redeploy your service |
| Key Value `maxmemoryPolicy` | `noeviction`; Render's default evicts keys |
| `ipAllowList` | empty on both data stores, so only Render's private network reaches them |

Keep a copy of `HS_SECRET_KEY` from the service's environment. The stored Plivo tokens cannot be decrypted without it.

What the free plans leave out:

- **Sleep.** A free web service spins down after 15 minutes with no inbound traffic and takes about a minute to wake. While it sleeps, timers do not run and Plivo's calls may fail. An open desk sends a heartbeat every 15 seconds, which keeps it awake.
- **Data.** The free database expires 30 days after it is created, with 14 more days of grace, and has no backups. The free Key Value instance keeps its data in memory only; HS rebuilds what a restart takes, as [Run Redis](configuration.md#run-redis) describes. A workspace gets one free database and one free Key Value instance.
- **Pre-deploy.** A free web service has no pre-deploy command, so the migration runs in the service's own start.

On paid plans, Render runs a pre-deploy command on a separate instance before the new version starts. On the web service, set `plan: 0.5c-512mb` and `preDeployCommand: alembic upgrade head`, and delete the `dockerCommand` line: the image's own command then starts HS on 6004, which `PORT` names. Give the database `plan: 0.1c-256mb` with `diskSizeGB: 1`, and the Key Value instance `plan: 256mb` to keep its data. At Render's November 2026 prices that is about $13 a month with the free Key Value instance, or $23 with `256mb`, plus $0.30 per GB of database storage.

HS sees Render's proxy as the address of every client, so all clients share the sign-in limits, as [What it does not do](limitations.md) describes.

## Heroku

[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.svg)](https://www.heroku.com/deploy?template=https://github.com/plivo-labs/human-specialist)

**Status.** Not yet deployed on a live Heroku account. The first deploy should check that the build completes, that the release log shows `alembic upgrade head`, and that once `PUBLIC_BASE_URL` is set the dyno answers `/status` with no R10 error.

The button reads `app.json` and `heroku.yml` at the root of this repository. It creates one app on Heroku's container stack, built from the root `Dockerfile`, with a Basic web dyno, Heroku Postgres Essential-0 and Heroku Key-Value Store Mini. Heroku has no free plan: these cost about $15 a month, billed by the hour, and Heroku asks for a card to verify the account.

1. Press the button and sign in to Heroku. Name the app, enter the Auth ID of the Plivo account that will own this install in `HS_SETUP_PLIVO_AUTH_ID`, leave `PUBLIC_BASE_URL` empty, and deploy the app.
2. Heroku builds the image, runs `alembic upgrade head` in the release phase and starts the dyno. The dyno stops at once with `missing required environment variables: PUBLIC_BASE_URL`: a new app's URL carries a random suffix, so no file can name it.
3. Open the app's **Settings**, copy its URL from **Domains**, and paste it into the config var `PUBLIC_BASE_URL`. Heroku starts a new release, whose migration has nothing left to do, and the dyno comes up. This is the one step by hand.
4. Open the app's URL and register as [The first run](#the-first-run) describes.

What the two files set:

- **Build.** `heroku.yml` builds the root `Dockerfile` for `web`.
- **Release.** The release phase runs `/app/heroku release`, which runs `alembic upgrade head`, before the release's dynos start, and a failed migration leaves the running release in place. Heroku creates a release on each build, rollback, pipeline promotion and config var change, and runs the release phase for each, except for a change to an add-on's own config vars, such as a rotated `DATABASE_URL`. Releases can run at once, so the migrations take turns.
- **Run.** The `web` dyno runs `/app/heroku web`. Both modes of `/app/heroku` hand HS the add-ons' URLs as its drivers take them: Heroku names Postgres `postgres://`, which SQLAlchemy refuses, so it becomes `postgresql://`. Key-Value Store takes TLS with a self-signed certificate, so a `rediss://` URL gets `ssl_cert_reqs=none`, as Heroku's Python guide sets it, unless the URL already names that option. HS listens on the `PORT` Heroku sets.
- **Keys.** Heroku generates `HS_OPERATOR_KEY` and `HS_SECRET_KEY` once, each 64 hex characters. Keep a copy of `HS_SECRET_KEY` from the app's config vars: the stored Plivo tokens cannot be decrypted without it.
- **Connections.** `DB_POOL_SIZE` 12 and `DB_MAX_OVERFLOW` 4 keep the dyno at 18 of Essential-0's 20 connections, which leaves one for the release phase. `METRICS_PORT` is `0`, because Heroku routes only `PORT`.

A Basic dyno never sleeps. Heroku's router ends a response that sends nothing for 55 seconds, and HS sends the event stream a ping every 15. HS sees Heroku's router as the address of every client, so all clients share the sign-in limits, as [What it does not do](limitations.md) describes.

## Railway

**Status.** No Railway template exists yet, so nothing here has run on Railway. Whoever creates the template should check that its deploy form asks for `HS_SETUP_PLIVO_AUTH_ID`, that `PUBLIC_BASE_URL` holds the service's domain at the first start, and that the pre-deploy command reaches Postgres.

Railway reads no deploy file from a repository for a new service; services that already used `railway.json` or `railway.toml` keep reading them until 2026-12-01. A Deploy on Railway button opens a template, which someone with a Railway account creates once in Railway's template composer, and the button needs that template's code. This repository has no template yet, so it has no button. A template for HS builds this repository's root `Dockerfile` beside Railway's own Postgres and Redis, and sets:

| Setting | Value |
|---|---|
| Source | `https://github.com/plivo-labs/human-specialist`, branch `main` |
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}` |
| `REDIS_URL` | `${{Redis.REDIS_URL}}` |
| `PUBLIC_BASE_URL` | `https://${{RAILWAY_PUBLIC_DOMAIN}}` |
| `HS_OPERATOR_KEY`, `HS_SECRET_KEY` | `${{secret(48, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")}}` |
| `HS_SETUP_PLIVO_AUTH_ID` | required, no default |
| `PORT` | `6004`, the port Railway health-checks |
| `METRICS_PORT` | `0` |
| Pre-deploy command | `alembic upgrade head` |
| Health check path | `/status` |
| Public networking | a Railway domain on port 6004 |

Postgres and Redis keep the settings and volumes of Railway's own templates, and neither has a public TCP proxy. Railway's Redis starts with no `maxmemory-policy`, so it keeps Redis's default, `noeviction`, which HS needs.

- **Migration.** The pre-deploy command runs in its own container, with the service's variables, before the new version takes traffic. A failure stops the deploy.
- **Serverless.** Leave it off: the job runner, the event relay and the sweeper work between requests.
- **Event streams.** Railway ends a request after 15 minutes even while data flows, so the desk's event stream reconnects every 15 minutes and resumes with `Last-Event-ID`.
- **Trial accounts.** Railway's Limited Trial restricts outbound traffic, so HS may not reach Plivo there. Use a verified trial or the Hobby plan.
- **Client addresses.** HS sees Railway's edge as the address of every client, so all clients share the sign-in limits, as [What it does not do](limitations.md) describes.

## Google Cloud

[![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run/?git_repo=https://github.com/plivo-labs/human-specialist&revision=v1.0.0&dir=deploy/gcp)

Status: built from Google's documentation, the button's source and reference projects, and checked offline; it has not been run against a live Google Cloud account yet.

The button needs the release tag `v1.0.0` and its public image `ghcr.io/plivo-labs/human-specialist:1.0.0`. Until both exist it stops before it creates anything billed: without the tag it cannot check out the release, and without the public image its build fails, after it has turned on the Cloud Run and Artifact Registry APIs and made an empty `cloud-run-source-deploy` repository. Making the tag (`cz bump`) and the public image package are the maintainer's steps.

The Cloud Run Button opens Cloud Shell, clones this repository at the release tag and deploys that release's image. On a press that finds no service yet it runs `deploy/gcp/precreate.sh` as your Google account, which creates what the table below lists. The button is Google's open-source project, not an officially supported Google product.

1. [Create a project](https://console.cloud.google.com/projectcreate) for human-specialist alone, with billing enabled. You tear the install down by deleting this project, so keep nothing else in it. The project needs the `default` network with its subnet in the region you choose: a new project gets it when the Compute Engine API is turned on, unless an organization policy prevents that. Without it the script stops at Redis, before anything billed.
2. Click **Run on Google Cloud**. When Cloud Shell asks whether to trust the button's image, choose **Trust**, then **Authorize**.
3. Choose the project and a region.
4. When the button asks for `HS_SETUP_PLIVO_AUTH_ID`, enter your Plivo account's Auth ID.
5. Wait for the button to finish. Creating Redis and Cloud SQL takes most of the time. If it stops with an error, fix what the error names and click the button again: a press skips what an earlier one made and never replaces a secret's first version.
6. Open the URL at the end of the output and follow [The first run](#the-first-run).

The script creates the following, each named after the service, `human-specialist`. Prices are Google's list prices for us-central1, read on 2026-11-13 unless the row says otherwise, for 730 hours a month.

| Resource | Settings | Per month |
|---|---|---|
| Cloud Run service | 1 vCPU, 1 GiB, port 6004, instance-based billing, one instance (minimum and maximum), 3600 s request timeout, concurrency 1000, Direct VPC egress to the `default` network, the Cloud SQL socket. Each revision runs the image's `/app/migrate-and-serve`, which runs `alembic upgrade head` before it serves | $52.56, less up to $5.22 from the Cloud Run free tier |
| Memorystore for Redis | Basic tier, 1 GB, Redis 7.2, `maxmemory-policy noeviction`, AUTH on | $35.77, from Google's pricing page as archived on 2026-05-20: the live page did not load on 2026-11-13 |
| Cloud SQL for PostgreSQL 16 | Enterprise edition, db-f1-micro (shared core, outside the Cloud SQL SLA), 10 GB SSD, `max_connections=50`, database and user `hs` | $7.67, $1.70 for storage, $0.08 per GiB-month of backups |
| Secret Manager | `human-specialist-operator-key`, `human-specialist-secret-key`, `human-specialist-database-url` and `human-specialist-redis-url`, one version each, which HS reads | free up to 6 active versions |
| Service account | Cloud SQL Client and Secret Manager Secret Accessor on the project | free |
| Artifact Registry | the release image, in the `cloud-run-source-deploy` repository | free up to 0.5 GiB |

That comes to $97.70 a month, about $3.21 a day, and $92.48 with the whole Cloud Run free tier. The button's last lines say the app is billed only while it handles requests; this service keeps its instance running and is billed for all of it.

**Upgrade.** Click the button of the newer release. It is expected to deploy that release's image as a new revision, which runs the migration before it serves, and a revision whose migration fails should never become ready, so traffic stays on the one before it. A live run has not shown this yet.

**The secrets.** The service reads version 1 of each secret. A new version changes nothing until the service is redeployed with it. Disabling version 1 stops new instances from starting. Changing the database password alone breaks the stored URL: change the user `hs`'s password and the secret's value together, then redeploy the service with the new version.

**The operator key.** Registering another organization, a new key for one and re-applying Plivo URLs take `HS_OPERATOR_KEY`:

```sh
gcloud secrets versions access 1 --secret=human-specialist-operator-key --project=<project id>
```

**Tear down.** Delete the project, which deletes everything in it:

```sh
gcloud projects delete <project id>
```

**Redis on the network.** Memorystore takes an address on the project's `default` network. Anything on that network that holds the AUTH string can read and write HS's hot state, and the traffic is not encrypted in transit, which is one more reason to keep the project to HS alone.

**Client addresses.** The service leaves `FORWARDED_ALLOW_IPS` unset. Until it is set to the value that lets HS read each client's address from Cloud Run's forwarding header, the sign-in and setup limits count every client together, as [API](api.md#auth) explains.

## AWS

**Status.** `cfn-lint` checks this template on every change, but it has not yet been run against a live AWS account. [Check the first AWS run](#check-the-first-aws-run) lists what to watch.

[`deploy/aws/template.yaml`](../deploy/aws/template.yaml) is a CloudFormation template for one install: human-specialist as one Fargate task behind CloudFront and an internal Application Load Balancer, with RDS for PostgreSQL 17 and ElastiCache for Valkey 8 set to `maxmemory-policy noeviction`. CloudFront's `*.cloudfront.net` address is the install's HTTPS URL and its `PUBLIC_BASE_URL`, so no domain is needed. CloudFront reaches the load balancer only through a VPC origin, so no request crosses the internet unencrypted, and the load balancer, Postgres and Valkey have no public address. The template names a release's image in **Image**, so use the template from a release. Either way below needs only an AWS account.

**Four Regions can refuse the VPC origin.** CloudFront creates no VPC origin in one Availability Zone of us-east-1 (`use1-az3`), us-west-1 (`usw1-az2`), ap-northeast-1 (`apne1-az3`) and ca-central-1 (`cac1-az3`). The template takes the Region's first two zones, and which zone ids those are differs by account, so in these four Regions the stack can fail at `VpcOrigin` and roll back. Then delete the stack and create it in another Region, or change the `!Select` index of all four subnets in the template from 0 and 1 to two other zones, and create it again. The Regions where new accounts start, us-east-2, eu-north-1 and ap-southeast-2, are not among the four.

**In the console.**

1. Download `deploy/aws/template.yaml` from the release you deploy.
2. Open the CloudFormation console in your Region, choose **Create stack**, **With new resources (standard)**, **Upload a template file**, and choose the template.
3. Enter your Plivo account's Auth ID in **PlivoAuthId**, select the IAM acknowledgment, and choose **Create stack**.
4. When the stack shows `CREATE_COMPLETE`, open its **ConsoleUrl** output and choose **Register**, as [The first run](#the-first-run) describes.

**From AWS CloudShell**, in your Region:

```sh
curl -fsSLO https://raw.githubusercontent.com/plivo-labs/human-specialist/v<version>/deploy/aws/template.yaml
aws cloudformation deploy --stack-name human-specialist --template-file template.yaml --capabilities CAPABILITY_IAM --parameter-overrides PlivoAuthId=<your Plivo Auth ID>
aws cloudformation describe-stacks --stack-name human-specialist --query "Stacks[0].Outputs"
```

- **Size and cost.** 0.5 vCPU and 1 GB on ARM, `db.t4g.micro` with 20 GB and `cache.t4g.micro`: about $60 a month in us-east-1 or us-east-2 at AWS's prices on 2026-11-14, before data transfer. For more load, raise the task's `Cpu` and `Memory` and the instance classes, and update the stack.
- **Upgrades.** Update the stack with the new release's template. Each new task runs `alembic upgrade head` in its `migrate` container before human-specialist starts, while the old task keeps serving.
- **The operator key** is in the Secrets Manager secret that the **OperatorKeySecret** output names.
- **Client addresses.** HS sees the load balancer's address for every client, so the per-address limits on sign-in and setup count all clients together. `FORWARDED_ALLOW_IPS` does not separate them, because the address before the load balancer is CloudFront's.
- **Delete the stack** to remove the install. It keeps a final snapshot of the database and the `HS_SECRET_KEY` secret, which the snapshot needs to decrypt Plivo tokens. Delete both when you no longer need them.

### Check the first AWS run

The stack takes about 20 to 30 minutes. If it fails, its **Events** tab names the first resource that failed.

- **`CannotPullContainerError`** in the ECS service's events: the release image is not public, or its tag does not exist. After three failed tasks the deployment stops and the stack rolls back.
- **An explicit deny in a service control policy** at the first resource: the stack is in a Region the account cannot use. A new account on the Free plan creates resources in one Region chosen by its country: us-east-2, eu-north-1 or ap-southeast-2.
- **`Database` fails with "Cannot find version"**: set `EngineVersion` to `"18"` in the template and create the stack again.
- **"Unable to assume the service linked role"** on a new account: delete the stack and create it again.
- **`CERTIFICATE_VERIFY_FAILED`, or "did not answer the EXPIRE GT probe"**, in the `hs` log stream: HS cannot use Valkey. `GET /status` checks only Postgres, so the stack can complete anyway.
- **`VpcOrigin` fails** in us-east-1, us-west-1, ap-northeast-1 or ca-central-1: one of the two zones is the excluded one, as above. Creating a VPC origin can take up to 15 minutes.
- **The event stream.** The desk holds `GET /api/v1/events/stream` open through CloudFront, which waits up to 60 seconds between packets, and the load balancer, which closes a connection idle for 60 seconds. HS pings every 15. A desk that reconnects every minute means one of them closed it.
- **After a failed create or a delete**, look in Secrets Manager and in the RDS snapshots for the kept `HS_SECRET_KEY` secret and snapshot. A delete that stalls at the VPC may be waiting on the security group CloudFront made for its VPC origin. Delete the stack again once that group is gone.

## Host the UI

The UI in `ui/` is optional. Your own UI uses the same API, event stream and webhooks. The image serves it, as [One image, one URL](#one-image-one-url) says. To host it apart from HS, build it to static files in `ui/dist` and host them one of two ways.

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
2. Upload `ui/dist` to the root of a site on S3 behind CloudFront, Netlify, Vercel or any web server, at the root of its origin, not under a sub-path, and have it answer the paths of the UI's screens (the routes `CONSOLE` in `src/hs/app.py` lists, such as `/desk` and `/desk/{id}`) with `index.html`, so a link to a screen opens the UI, while a missing file still answers 404.
3. Add the UI's origin to HS's `CORS_ALLOWED_ORIGINS`, for example `https://desk.example.com`, and restart HS.

Serve the UI over HTTPS. The softphone needs a secure context, and browsers allow the microphone only on HTTPS or `localhost`.

## Where the deploy files live

Four files sit at the repository root because a platform reads them only there. The rest sit under `deploy/` for hosted platforms and `docker/` for Docker Compose.

| File | Read by | Why it is there |
|---|---|---|
| `Dockerfile` | `docker build .`, Heroku, Render, Railway, the compose stack and the release workflow | Heroku builds with the Dockerfile's own folder as the context, and the image copies `src/`, `ui/` and `alembic/` from the root. Render and Railway also look for it at the root by default. |
| `render.yaml` | Render's Deploy button | The button reads the Blueprint at the repository root. |
| `app.json` | Heroku's Deploy button | The button reads it at the repository root. |
| `heroku.yml` | Heroku's container stack | Heroku reads it at the repository root. |
| `deploy/gcp/` | The Run on Google Cloud button | The button's `dir` names this folder, so its link and the release tags fix the path. |
| `deploy/aws/template.yaml` | CloudFormation | You download and upload it yourself; [AWS](#aws) links this path. |
| `deploy/migrate-and-serve` | Render and Cloud Run, as `/app/migrate-and-serve` | The image copies it to `/app`, and the platforms name only that path. |
| `deploy/heroku` | Heroku's release phase and `web` dyno, as `/app/heroku` | The image copies it to `/app`, and `heroku.yml` names only that path. |
| `docker/` | Docker Compose on your machine | `docker/start.sh` starts `docker/compose.yaml` with `docker/.env`; `docker/compose.test.yaml` is the test stack. |
