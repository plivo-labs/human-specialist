# human-specialist

[![CI](https://github.com/plivo-labs/human-specialist/actions/workflows/ci.yml/badge.svg)](https://github.com/plivo-labs/human-specialist/actions/workflows/ci.yml) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**The contact center engine behind your own agent desk.**

human-specialist answers calls on Plivo, runs menus and business hours, queues callers, offers calls to agents and controls the live call. You build your own agent UI, dialer and CRM sync on its HTTP API, a live event stream and signed webhooks. Open source and self-hosted. By [Plivo](https://plivo.com).

Version 0.1 handles voice calls. [What it does not do](docs/limitations.md) lists every gap the release ships with.

## Why human-specialist

A contact center on raw telephony needs the same hard parts every time: queues that hold up in a busy hour, agent state and capacity, offers that ring several agents and give the call to the first one who answers, timers for wrap-up and missed calls, and a live call that can be held, transferred and monitored while Plivo's callbacks arrive out of order. Hosted contact center suites have these parts, but they also own your agent screen and your data.

human-specialist is that engine as one service you run, with an API you build on.

- **One service.** A FastAPI process with Postgres, Redis and a Plivo account. Nothing else.
- **Multi-tenant.** Each tenant is one Plivo sub-account, and every request is scoped to one tenant by its credential.
- **Routing is configuration.** Queues, business hours, holidays, menus, overflow and voicemail are resources, not a drawn call flow.
- **Agents in a browser or on a SIP phone.** Agents take calls with the Plivo Browser SDK or any SIP softphone. Hold, transfer, conference, recording and supervisor listen, whisper and barge are API calls.
- **Every change is an event.** State changes go out on a server-sent-events stream and to signed webhooks, with replay.
- **Your data stays yours.** It runs on your servers with your Plivo account, and calls and reports stay in your database. HS stores recording metadata and serves a signed `audio_url`. The audio itself stays with Plivo.

## How it works

1. **A call arrives.** Plivo requests the service's answer URL. The called number's entry point checks business hours and runs its action: a menu, a queue, voicemail or a forward.
2. **The caller waits in a queue.** Routing picks agents by the queue's strategy, teams, skills and capacity and rings them. The first agent to answer gets the call.
3. **The agent handles the call.** Your agent desk calls the API to hold, transfer, record or hang up. A supervisor can listen, whisper or barge.
4. **Wrap-up.** The agent sets a disposition and a note. Reports and the live floor read the result, and every step is an event on the stream and your webhooks.

```
 your agent UI / dialer / CRM sync
        |  HTTPS /api/v1      ^ SSE stream, signed webhooks
        v                     |
 +---------------------------------------+       +----------------+
 |  human-specialist (FastAPI)           | <---> | Postgres,Redis |
 +---------------------------------------+       +----------------+
        ^ Plivo callbacks (/plivo/...)   |  Plivo REST
        |                                v
 +---------------------------------------+
 |  Plivo: numbers, MultiPartyCall, SIP  |  <--- callers and agent phones
 +---------------------------------------+
```

## Quick start

You need Docker, `jq`, a Plivo sub-account with one number, its auth id and token, and a public URL that reaches port 6004 (a tunnel is enough for development).

```sh
git clone https://github.com/plivo-labs/human-specialist && cd human-specialist
cp docker/.env.example docker/.env   # set PUBLIC_BASE_URL, HS_OPERATOR_KEY, HS_SECRET_KEY
docker compose -f docker/compose.yaml up -d --wait   # migrates the database, then starts the service, Postgres and Redis
```

Then register a tenant and take a first call:

```sh
set -a; . docker/.env; set +a
HS=http://localhost:6004; J='Content-Type: application/json'

# 1. Register the tenant with the operator key. The first tenant key is returned once.
REG=$(curl -sS -X POST $HS/api/v1/tenants -H "Authorization: Bearer $HS_OPERATOR_KEY" -H "$J" \
  -d '{"name":"Demo","plivo_auth_id":"SA...","plivo_auth_token":"..."}')
echo "$REG"   # the tenant and its key, or the error (for example plivo_credentials_rejected)
KEY=$(echo "$REG" | jq -r .api_key)
A="Authorization: Bearer $KEY"

# 2. Create an agent.
AGENT=$(curl -s -X POST $HS/api/v1/agents -H "$A" -H "$J" \
  -d '{"name":"Agent One","external_id":"u-1","role":"agent"}' | jq -r .id)

# 3. Mint SIP credentials and register any SIP softphone with them.
curl -s -X POST $HS/api/v1/agents/$AGENT/sip-credentials -H "$A"

# 4. Set the agent to Available (SIP devices send no heartbeat).
AVAILABLE=$(curl -s $HS/api/v1/agent-statuses -H "$A" | jq -r '.data[] | select(.name=="Available") | .id')
curl -s -X PUT $HS/api/v1/agents/$AGENT/status -H "$A" -H "$J" -d '{"status_id":"'$AVAILABLE'"}'

# 5. Create a queue.
QUEUE=$(curl -s -X POST $HS/api/v1/queues -H "$A" -H "$J" \
  -d '{"name":"Sales","agents":["'$AGENT'"]}' | jq -r .id)

# 6. Route a number you own in that sub-account to the queue.
curl -s -X PUT $HS/api/v1/numbers/+14155550100 -H "$A" -H "$J" \
  -d '{"open_action":{"type":"queue","queue_id":"'$QUEUE'"}}'

# 7. Call +14155550100 from any phone. The softphone rings.
#    To dial out, POST /api/v1/calls with {"agent_id": ..., "to": ...}.
```

Registering a tenant also creates a Plivo Application whose answer and hangup URLs point at `PUBLIC_BASE_URL`. Registering the same sub-account twice returns 409 `tenant_exists`.

To upgrade after pulling new commits, rebuild: `docker compose -f docker/compose.yaml up -d --wait --build`. Compose reuses the local `0.1.0` image tag otherwise and keeps running the old code. To run a published image instead, set `image:` to it and use `docker compose -f docker/compose.yaml pull` before `up -d --wait`.

The compose stack is for evaluation: Postgres and Redis run in containers with a fixed password, reachable only on the stack's own network. In production, point `DATABASE_URL` and `REDIS_URL` at databases you run and back up (Postgres 13 or later, Redis 6 or later), and run the image on its own.

## Docs

- [Configuration](docs/configuration.md)
- [API](docs/api.md)
- [Events and webhooks](docs/events.md)
- [Architecture](docs/architecture.md)
- [Self-hosting](docs/self-hosting.md)
- [What it does not do](docs/limitations.md)
- [Security](SECURITY.md)

## Develop

```sh
uv sync
docker compose -f docker/compose.test.yaml up -d   # Postgres and Redis for the tests
make check                                         # lint, format, unit, contract and acceptance tests
```

[CONTRIBUTING.md](CONTRIBUTING.md) covers the checks, the code rules and the commit conventions.

## Security

To report a vulnerability, use GitHub's private vulnerability reporting, as [SECURITY.md](SECURITY.md) describes. Don't open a public issue.

## License

Apache-2.0; see [LICENSE](LICENSE).
