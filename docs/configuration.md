# Configuration

Environment variables only. With Docker Compose, copy `docker/.env.example` to `docker/.env`; compose passes it to the service.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DATABASE_URL` | yes | | Postgres URL; Postgres 14 or later (queries use the `CYCLE` clause of recursive queries and the built-in `gen_random_uuid()`) |
| `REDIS_URL` | yes | | Redis URL, such as `redis://redis:6379/0`, for hot state; Redis 7 or later (offers use `EXPIRE ... NX` and `GT`). Redis must run with `maxmemory-policy noeviction`; see [Run Redis](#run-redis) |
| `DB_POOL_SIZE` | no | `20` | Database connections each replica keeps open, at least 1; a lower value stops the start, as does a total with `DB_MAX_OVERFLOW` under 16 (see [Size the database connections](#size-the-database-connections)) |
| `DB_MAX_OVERFLOW` | no | `10` | Extra database connections a replica opens when every pooled one is busy, closed again once returned, at least 0; a negative value, which SQLAlchemy reads as unlimited, stops the start |
| `PUBLIC_BASE_URL` | yes | | Absolute http or https URL with a host, and optionally a port or a path prefix, that reaches this service on port 6004; Plivo calls it back. Startup refuses anything else |
| `HS_OPERATOR_KEY` | yes | | Bearer key for the operator routes (`/api/v1/tenants`), 32 bytes or more |
| `HS_SECRET_KEY` | yes | | Key material for Plivo tokens and webhook secrets at rest and for signed URLs, 32 bytes or more |
| `SEARCH_URL` | no | empty | Base URL of an OpenSearch node for conversation search. Empty means search reads Postgres |
| `PLIVO_API_URL` | no | `https://api.plivo.com` | Plivo REST host (the test simulator overrides it) |
| `PLIVO_MEDIA_URL` | no | `https://media.plivo.com` | Host recordings are fetched from |
| `CORS_ALLOWED_ORIGINS` | no | empty | Comma-separated browser origins allowed on `/api/v1` |
| `EVENT_RETENTION_DAYS` | no | `90` | Age in days after which events and parked jobs are deleted (a refused Plivo command's parked row goes after 5 minutes); an event waits until every plugin hook has passed it, and one of a conversation not yet closed until it closes. The agent status history the availability reports read is kept for good |
| `WEBHOOK_PRIVATE_HOSTS` | no | empty | Hosts whose webhooks may use http or a private address (local receivers) |
| `METRICS_PORT` | no | `9100` | Prometheus metrics port; `0` turns it off. `docker/compose.yaml` does not publish it; uncomment its line there to scrape it |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `PORT` | no | `6004` | Host port `docker/compose.yaml` publishes the service on; the service itself always listens on 6004 |

Both keys must be at least 32 bytes, or the start fails. Generate them with `openssl rand -base64 32`, and keep a copy of `HS_SECRET_KEY`. Without it, the stored Plivo tokens and webhook secrets cannot be decrypted.

`POST /api/v1/tenants` registers a tenant with your Plivo account's Auth ID and Auth Token. A Plivo sub-account's Auth ID and Auth Token work the same way, and keep one organization's numbers and usage apart from another's.

Plivo must be able to reach the service, so `PUBLIC_BASE_URL` has to be a public URL (a tunnel is enough for development). Registering a tenant creates a Plivo Application whose answer, hangup and message URLs point at it, unless the tenant [keeps its own](#keep-your-own-plivo-setup). If the URL changes, update the environment, restart, and `PATCH /api/v1/tenants/{id}` to re-apply it at Plivo.

HS calls Plivo's REST API from its own host. A Plivo account can limit API access to the IP addresses it lists. Plivo then answers 403 to any other host, and HS reports `plivo_refused` with Plivo's reason.

The service keeps its tables in the Postgres schema `hs`, and its hot state in Redis.

## Size the database connections

Each replica opens at most `DB_POOL_SIZE` + `DB_MAX_OVERFLOW` connections for its work, 30 at the defaults, and 2 more that only take conversation numbers, so 32. Postgres keeps `superuser_reserved_connections` (3 by default) of its `max_connections` for superusers, so HS can use the rest: 97 at the defaults. Keep replicas × (`DB_POOL_SIZE` + `DB_MAX_OVERFLOW` + 2) under that, with room left for migrations and your own sessions. Three replicas at the defaults use 96 of the 97, which leaves one connection for a migration. To run more replicas, or to keep more room, raise `max_connections` or lower the two settings.

The last 8 of a replica's connections are kept for its timers (offer, queue and wrap-up timeouts, routing), one per timer worker, so a burst of other work never makes a timer wait for a connection. Everything else shares the rest, 22 at the defaults. A total under 16 stops the start, so the rest is never fewer than the timers' 8. When the connections it may use are all busy, a request waits up to 10 seconds for one. If none frees up, it answers 503 `dependency_unavailable`, and the client can retry it.

Point `DATABASE_URL` at the Postgres primary itself, never at a transaction-mode pooler such as PgBouncer in transaction mode. HS sets its schema and its statement, lock and idle-transaction timeouts as startup options on each connection. A transaction-mode pooler shares one server connection among many clients, so it cannot keep those options for HS.

A managed Postgres that fails over demotes the old primary. A write it then refuses as read-only (SQLSTATE `25006`) fails that request once and discards the connection pool, so the next requests connect to the new primary. HS opens no read-only transaction, so a refused write means the server became a standby.

## Sharing a database

The migrations create every table in the Postgres schema `hs` and nothing anywhere else. They keep their revision in `hs.alembic_version`, so another application's tables and its own Alembic version table are left alone. To keep the revision in another schema, set `HS_VERSION_SCHEMA` on every `alembic` run.

A missing schema is created, which needs `CREATE` on the database. A migration role without that privilege works once a database owner has created the schemas for it, for example with `CREATE SCHEMA hs AUTHORIZATION <role>`.

`alembic upgrade head` changes nothing and exits with an error when schema `hs` holds tables but no recorded revision.

## Capacity

One benchmark run held 200 concurrent calls across 10 tenants and 280 agents. Two replicas ran with 1 vCPU each, against Postgres 16 with 2 vCPU and Redis 7 with 0.5 vCPU, on one 8-core laptop under Docker. The Plivo simulator in `tests/harness` stood in for Plivo.

| Measure | Result |
|---|---|
| Answer, p99 | 106 ms |
| Accept, p99 | 119 ms |
| Plivo callbacks, p99 | 103 ms |
| Wrap-up, p99 | 128 ms |
| Longest row-lock wait | 110 ms |
| HS CPU per replica, p95 | 35% of its vCPU |
| Errors | no 5xx, no retries, no deadlocks |

This is a single-machine run against a simulator, with no real carrier latency. Treat it as evidence that the design holds at that load, and size your own deployment for your traffic: replicas, `DB_POOL_SIZE` and Postgres `max_connections` (see above).

Two limits sit outside that run, which spreads its arrivals:

- Enqueues into one queue take that queue's lock in turn, each through its first offer round. About 50 calls arriving into one queue at the same instant wait 1 to 2 s on that machine. An enqueue that waits past Postgres's 3 s `lock_timeout` answers 503, and Plivo retries the answer URL a second later.
- Conversation reports aggregate one stored row per conversation. Over 90 days of a tenant at 200 concurrent calls, about 2 million conversations, the bucketed reports took a median of 1.4 to 2.5 s, single runs up to 4 s, and under heavy database load they can reach the 5 s statement timeout. That test data carried no dispositions or handle times, so reports that group by them may take longer on real data. Narrow the window or add filters.

## Run Redis

Redis holds hot state. Every key starts with `hs:{`, and a tenant's with `hs:{<tenant id>}:`, so a shared Redis can hold other keys too, and in cluster mode a tenant's keys share one slot. The one key no tenant owns, `hs:{probe}:pxat`, is the version check startup writes, and it expires after a second. A tenant's `epoch` key says its keys are whole. Postgres keeps every durable fact, and the families below are what a Redis loss takes:

| Family | Holds | After a Redis loss |
|---|---|---|
| Presence (`hb`, `beats`) | each desk's heartbeats | The next sweep gives each signed-in agent 40 seconds to beat, then Offline as usual. |
| Admission (`cap`, `full`) | each agent's slots per channel, and the agents at their cap | The sweep rebuilds them from the stints and open offers in Postgres, then sets the epoch. Until then no offer takes a slot. |
| Offers (`offers`, `desk`, `offer_conversation`) | each conversation's open offers and each agent's | Retired, not rebuilt: a live ring cannot be rebuilt from Postgres without Plivo's leg state. The sweep ends each open offer and starts the conversation's round again, so the next agent is offered the work. A leg rung for a lost offer that answers while its caller still waits takes the call. |
| Rounds (`route`, `tried`, wait prompt) | the agents each queued conversation tried, its round and the prompt it has played | Restarted: a queued conversation begins its rounds over. The rebuild restores the agents the current stay declined or missed, from the stored `offer.rejected` and `offer.missed` events, so they are not offered it again. A waiting call forgets them 300 s after its last offer, so for a call it restores only those since the last offer that came more than 300 s after the one before it. An open offer still ends and its round restarts. |
| Sticky (`sticky`, `osticky`) | each customer's sticky agent per queue, and the agent of the last outbound contact | Rebuilt on the slow lane once the rebuild commits, 500 customers per job, one tenant at a time, from the newest primary stints. Until the job reaches a customer, that customer routes without a sticky agent. |
| Unread (`unread`) | each open conversation's unread count | Not rebuilt. A read counts a missing one from Postgres, and an agent's read or write creates it again. |
| Config cache (`cfg`) | a 60-second cache of each tenant's config and WhatsApp templates | Config reads go to Postgres until Redis answers. |
| Webhook rest (`wh`) | each subscription resting for 30 seconds after a timeout or a failed connection | Its next delivery is sent at once. |

The sweep runs every 15 seconds on one replica, and a run that fails runs again at the next interval. It rebuilds every tenant that is not deleted, whether or not an agent is signed in. A transaction rebuilds at most 500 tenants and holds one advisory lock for each, so the rebuild fits Postgres's shared lock table at any tenant count.

A rebuild takes about 2 ms a tenant: on one loaded 8-core laptop, 10,000 tenants with no open work took about 25 seconds and 40,000 about 80. A sweep holds its job for 60 seconds, so past about 30,000 tenants another replica can start the next sweep while one still runs; it rebuilds the tenants still missing, which repeats some work but reaches the same state.

Work the sweep restarts, a queued conversation's next round for one, runs as jobs on the control lane, and the sticky rebuild queues on the slow lane behind search indexing and reports, so on a busy install the sticky agents return after the queued work, not at once.

- Startup refuses Postgres older than 14 and Redis older than 7, which refuses `EXPIRE ... GT`. When Redis refuses the probe for another reason, such as an ACL or a renamed `EXPIRE`, or does not answer it, HS logs a warning and starts.
- Set `maxmemory-policy noeviction`. Redis then refuses writes when it is full instead of dropping keys, and HS answers the refused request with an error. An evicted key is lost without a trace. HS logs an error at startup when Redis reports another policy.
- Persistence is optional. Every agent signed in keeps its status after a loss and has 40 seconds to send a heartbeat before it goes Offline.
- While Redis is unreachable, `POST /me/heartbeat` answers 503 `heartbeat_failed` and no agent is swept Offline. Config reads go to Postgres. `/status` does not check Redis.
- HS waits at most 0.5 seconds for a Redis reply. When one times out, every Redis call fails fast for 2 seconds, then one call probes; HS logs once when Redis stops answering and once when it answers again. A request that needs hot state answers 503 meanwhile, and config reads go to Postgres.
- A config change is seen at once on every replica. When Redis fails at the moment a config write commits, a job retries the change within a second, on any replica, until Redis takes it.

## Set what each role sees

The `permissions` setting (`PATCH /api/v1/settings`) holds each role's grants. `view` is the work the role sees in conversation lists, search, the conversation rows report, recordings, tasks and the event stream (every other report counts the whole tenant): `org` the whole tenant, `team` its own work, every teammate's and what no agent took on a route to one of its teams (the route it was queued with), `self` its own work.

`download_list`, `download_conversation` and `download_recording` allow the downloads, of what the role sees. `monitor` and `barge` allow listening and barging. The floor (`GET /api/v1/floor`) and `GET /api/v1/queues/{id}/stats` show a role only the queues its view reaches: every queue at `org`, the queues routing to one of its teams at `team`, none at `self`. The defaults:

| Role | `view` | Downloads | `monitor`, `barge` |
|---|---|---|---|
| `admin` | `org`, fixed | on, fixed | on |
| `supervisor` | `team` | on | on |
| `agent` | `self` | on | off, fixed |

A change applies to the next request, except that a signed `audio_url` issued before it plays until it expires, 5 minutes after it was issued, because the audio route checks only the URL's signature. An open stream takes a change within its next check: a narrower view ends the stream, so the desk reconnects and resumes with `Last-Event-ID`, and the replay holds only what the new view sees. `GET /api/v1/me` lists the agent's grants, so a desk can hide what the agent cannot use. A tenant key sees the whole tenant and holds every grant its scopes allow.

## Name the WhatsApp accounts

Plivo's API reads WhatsApp templates by WhatsApp Business Account id only, so a tenant names its accounts in the `whatsapp.accounts` setting (`PATCH /api/v1/settings`), each with its `id` and its `senders`:

```json
{"whatsapp": {"accounts": [{"id": "104561283945", "senders": ["+14155550100"]}]}}
```

With accounts named, `GET /api/v1/whatsapp/templates` lists their templates, a WhatsApp start and `default_from.whatsapp` must use one of their senders, and a template message must fit an approved template of its sender's account (see [Text](api.md#text)). With none named, HS checks no WhatsApp sender or template and passes each template to Plivo as given, as it did before this setting existed.

HS reads an account's templates from Plivo with one request per 20 templates and one per template, then keeps them for 60 seconds, or until the tenant's config changes. The read runs before the send, outside any database transaction, so a slow Plivo delays only that request. When Plivo does not answer, the list and a template send answer 503 `plivo_unavailable`.

## Keep your own Plivo setup

A tenant that already runs on Plivo can keep its ids, endpoints and applications. Each of these is an opt-in. Without them, HS makes and points its own Plivo resources.

- **Ids.** Creating a queue, team, skill, agent status, disposition, business hours, holiday group, menu or agent takes an optional `id`, a UUID. The document keeps it, so the ids your other systems hold still name it. An id already in use answers 409 `id_taken`.
- **Endpoints.** `POST /api/v1/agents` with `endpoint_id` puts the agent on that endpoint of the tenant's Plivo account instead of a new one. HS rings it, but never changes its password or deletes it. Rotating its SIP credentials answers 409 `endpoint_adopted`, and deleting the agent or the tenant leaves the endpoint at Plivo. An endpoint the Plivo account does not hold answers 422 `endpoint_not_in_account`, and one another agent has answers 409 `agent_exists`.
- **No application.** `POST /api/v1/tenants` with `"plivo_app": false` registers the tenant without a Plivo Application. Its `plivo_app_id` is null, and HS creates, repoints and deletes none. Your own application answers the tenant's numbers and hands a call to HS by redirecting it to a queue's `enqueue_url`. The endpoints HS creates for the tenant, and the browser tokens it mints, name no application.
- **Numbers.** `PUT /api/v1/numbers/{number}?repoint=false` registers a number without pointing it at the tenant's application, for example as an agent's caller id. A tenant with no application never repoints a number.

## Fill the search index

With `SEARCH_URL` set, `POST /conversations/search` reads only the OpenSearch index. The index holds the conversations that closed or changed after you set it, so fill it with the older ones once:

1. Set `SEARCH_URL` on every replica and restart them.
2. Queue the backfill. It queues 200 `search_index` jobs every 10 seconds, once the last 200 ran:

	```sql
	INSERT INTO hs.jobs (kind, lane, key, run_at, payload)
	VALUES ('search_backfill', 'slow', 'search_backfill', now(), '{"after": "00000000-0000-0000-0000-000000000000"}')
	ON CONFLICT (kind, key) WHERE key IS NOT NULL DO NOTHING;
	```

3. Wait for the job runner to work through them. The backfill is done when this query returns 0:

	```sql
	SELECT count(*) FROM hs.jobs WHERE kind IN ('search_backfill', 'search_index') AND locked_until IS DISTINCT FROM 'infinity';
	```

A job that fails for hours parks. Its `locked_until` is `infinity`, and its error is in `last_error`. Every 15 minutes, the `search_reconcile` pass runs parked `search_index` jobs again, as many as keep fewer than 200 index writes waiting, and with an index it writes again each conversation of the last 7 days the index lacks, so the index catches up on its own once it answers.

Each write carries the conversation's version, so an older write never replaces a newer one. Running the backfill twice, or while new conversations close, is safe, and a backfill that stops resumes after the last conversation it queued. No migration queues the backfill. After a release that adds a search field, queue it with the statement above once the rollout ends, on either backend. A replica still on the release before writes documents without the new field until it stops.
