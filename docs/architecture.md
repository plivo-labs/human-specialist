# Architecture

human-specialist is one FastAPI process backed by Postgres and Redis, talking to Plivo for telephony. This page describes how the parts fit.

## Process model

- One container image, run as several identical replicas behind a load balancer. uvicorn only.
- `alembic upgrade head` is a separate one-shot step (the `migrate` service in compose). It fails closed, and the app never migrates at boot.
- Background loops run inside every replica: the job runner, the event relay and the agent heartbeat sweep. Jobs use `FOR UPDATE SKIP LOCKED`; the relay takes a Postgres advisory lock, so one replica stamps events at a time.
- Postgres holds durable state. Redis holds live call state: agent capacity, offers, per-call context, tokens and idempotency records.
- `/status` is public and reports only whether the database and Redis answer. Prometheus metrics are served on `METRICS_PORT`.

## Modules

`src/hs/` is one package grouped by domain. Each domain owns its router, logic and storage.

| Package | Owns |
|---|---|
| `hs` | the app (`app.py`), the router list (`api.py`), the error envelope (`errors.py`), paging |
| `src/hs/access` | tenants, API keys, agent sessions and their agent and stream tokens, principal dependencies, `load_call`, the Plivo V3 signature check, idempotency, CORS, secret encryption |
| `src/hs/admin` | agents and their SIP endpoints, teams, skills, roles, presence statuses and the heartbeat, dispositions, settings, status map, tenant seeds, the agent's desk (`/me`) |
| `src/hs/setup` | queues, business hours, menus, number entry points, the action renderer and its Plivo XML |
| `src/hs/intake` | Plivo's answer and hangup URLs, the call-flow XML (consent, actions, overflow), conversation open, enqueue, queue exit, closing a call that never reached a queue |
| `src/hs/routing` | agent selection, capacity (Redis Lua), offers and invites, sticky, reverse routing, RONA |
| `src/hs/voice` | the MultiPartyCall XML and its control, Plivo's call callbacks, agent legs, outbound calls, in-call controls, transfer, monitor, recording, hangup processing |
| `src/hs/conversations` | conversation model and reads, notes, wrap-up, closed-conversation search table |
| `src/hs/events` | `emit`, outbox relay, SSE, webhooks, replay, timeline, event models |
| `src/hs/jobs` | the `jobs` table runner: timers, async work, webhook deliveries, retention |
| `src/hs/reports` | reports and the live floor |
| `src/hs/plivo` | Plivo REST clients: account and Application setup, number lookup, MultiPartyCall and call requests, callback URLs |
| `src/hs/infra` | settings, db, redis, logging, metrics |

The dependencies between them run one way:

- `access` authenticates every request and resolves the tenant. Every other router depends on it.
- `admin` and `setup` hold configuration: agents, teams, skills, statuses, dispositions, settings, queues, hours, menus and numbers.
- `intake` handles the Plivo answer URL and turns a call into a conversation and a queue entry.
- `routing` picks agents and manages capacity and offers. `voice` controls the call through Plivo MultiPartyCall (MPC).
- `conversations` stores conversations, notes and wrap-up. `reports` reads them.
- `events` and `jobs` are the only outbound side channels. A state change calls `events.emit`; a timer or async task is a `jobs` row.

## Inbound call path

1. Plivo requests `POST /plivo/{tenant_id}/answer`. The signature is checked first. A repeat for the same call id returns the same XML.
2. The called number is looked up as an entry point. It carries a business-hours schedule, an open action and a closed action.
3. A conversation is opened and `conversation.started` is emitted. If the schedule says closed, the closed action runs and `conversation.after_hours` is emitted. If consent is on, a consent announcement plays first.
4. The open action is rendered. Entry points, menu options, queue overflow and queue key presses all use the same action shape, and one renderer turns it into Plivo XML. The types are `queue`, `menu`, `voicemail`, `forward`, `redirect` and `hangup`. A `redirect` hands the call to your own Plivo application after consent and hours have run, and you send it back to a queue's enqueue URL.
5. Enqueue opens the queue entry from the queue's configuration and returns an MPC document to Plivo. If the queue is full, the overflow action runs instead. `conversation.queued` is emitted.
6. Routing selects agents by the queue's strategy, teams, skills and capacity, and creates offers (`offer.created`). Each offer is a call from Plivo to the agent with `Hs*` SIP headers (`HsKind`, `HsConversation`, `HsOffer`, `HsAutoAnswer`): to its Browser SDK login (`<endpoint username>_<auth id>`) while it holds a session, else to its endpoint, where a SIP device registers. The first agent to answer is added to the MPC; the other offers are withdrawn.
7. An agent that does not answer in time counts a missed call. After the configured number of strikes the agent's status moves to an unavailable one (RONA) and `agent.status_changed` is emitted with reason `rona`. A decline or a lost race does not count. A ring that ends any other way while the caller is still on (a busy line, a phone that cannot be reached, a cancelled transfer) is an `offer.missed` that leaves the count as it was.
8. The queue exit URL continues the caller when they leave a queue: a wait timeout or a key press renders the overflow or key action, and `conversation.overflowed` is emitted. Any other exit hangs up.
9. When the call ends, `call.ended` is emitted and each agent who handled it enters wrap-up (`conversation.wrap_up_started`). The agent submits a disposition and note, or the deadline closes the wrap-up with the status-map disposition. `conversation.closed` fires once, after the last wrap-up, and writes the closed-conversation row that reports read.

Plivo callbacks are asynchronous and unordered. The service branches on hangup cause codes and event names only, and it cancels its own ringing invites when the caller leaves.

A call that ends before any queue (a hangup during a menu, a forward, a voicemail) is closed by a delayed job from the application's hangup URL, so a late voicemail recording callback is not lost.

## Outbound path

1. `POST /calls` with a tenant key, or with an agent token only when the tenant enables `agent_direct_dial`. The `from` number must be registered with `PUT /numbers/{number}`.
2. The service opens an outbound conversation and places a Plivo call to the agent, as an offer rings it, keeping the destination, caller id and `context` under a request id. `call.started` is emitted when the agent leg is up.
3. When the agent answers, the service returns an MPC document in the same shape as the inbound one, so hold, transfer, recording and hangup work the same way. It then adds the customer to the MPC and emits `call.answered`.
4. A failed agent or customer leg emits `call.failed`, then `call.ended`, and the conversation closes with the disposition mapped to the result.
5. A dial that starts on a device without a pending request id is refused at the answer URL when `agent_direct_dial` is off.

## Events

1. `events.emit(type, data)` inserts an unstamped row into the `events` table. For database changes it uses the request's session, so the event commits with the change. For changes in Redis or at Plivo it commits its own short transaction right after the effect.
2. The relay polls every 0.2 seconds. Under an advisory lock it stamps unstamped rows with `seq` in id order, inserts one webhook job per matching subscription in the same transaction, then publishes the new sequence numbers on a Redis channel for the tenant.
3. Only one relay stamps at a time, so `seq > cursor` is exact for every reader. A transaction that commits late gets a later number.
4. The stream (SSE) reads events after its cursor, filtered to what the token may see, and wakes on the Redis channel. Resume is `Last-Event-ID` or `?after=`. It pings every 15 seconds and re-checks its token about once a second.
5. Webhook delivery is a `jobs` row per event and subscription. See [Events and webhooks](events.md) for the headers and retry schedule.
6. Replay is `GET /events?after=`. A daily job deletes events older than `EVENT_RETENTION_DAYS`.

Delivery is at-least-once. Events inside a conversation are not ordered by delivery, so consumers dedupe on the event id and read `GET /conversations/{id}` for the current state. Every event type is a pydantic model in `src/hs/events/catalog.py` and appears in OpenAPI.

## Jobs

The `jobs` table is the only timer and async-work mechanism: `kind`, `payload`, `run_at`, `attempts`, `locked_until`, `last_error`, and an optional unique `key` per kind. Scheduling with an existing key replaces the row, so a timer can be moved (for example, extending wrap-up). A worker leases a due row for 60 seconds. A failing handler retries on a per-kind backoff, then the row parks with its error. Kinds in use: wrap-up deadlines, delayed closes, missed-call timers, webhook deliveries and event retention.

## Data model

Postgres, one Alembic chain, no foreign keys to tables the service does not own.

| Area | Tables |
|---|---|
| Tenants and settings | `organization` (one per Plivo sub-account), `api_keys`, `org_setting`, `routing_settings` (ring timeout, wrap-up, capacity), `org_event_actions` (the status a missed offer moves an agent to), `org_statuses` (presence statuses) |
| People | `account` (a person), `account_org_mapping` (that person as an agent of one tenant: the "aom"), `role`, `aom_role_profile`, `team`, `aom_team`, `skills`, `aom_skills`, `aom_routing_params` (per-agent counters used by routing) |
| Setup | `queues`, `hours_schedules`, `menus`, `entry_points` (a number's open and closed actions), `number`, `dispositions`, `conversation_priority`, `conversation_custom_status_mapping` |
| Waiting calls | `active_queue_entry` and its `agents`, `teams` and `skills_filter` rows: one queued call and who may take it |
| Conversations | `conversation_parent`, `conversation` (a child: one agent's or the flow's part), `conversation_chunk` (timeline entries), `conversation_secondary_aom_mapping` (other agents on the call), `customer_conversation_data`, `closed_conversation_search` (one row per closed conversation for search and reports) |
| Calls and media | `call_leg`, `recording`, `transcription`, `transcription_segment` |
| Events and jobs | `events`, `webhooks`, `jobs` |

A tenant is an `organization` row keyed by the Plivo sub-account auth id. Queue, menu and entry-point configuration is JSON validated by pydantic models. Schema changes are additive inside v1.

## Tenancy and auth

- Credentials: the operator key (environment), tenant API keys (SHA-256 hashed, scoped), agent tokens and stream tokens (opaque, random, stored in Redis with a time to live), signed URLs for recording audio, and the Plivo V3 signature on callbacks.
- The tenant comes only from the credential. A path or body id is looked up inside that tenant; a foreign id is 404 in a path and 422 in a body.
- Every `/calls/{id}/*`, `/conversations/{id}/*` and `/recordings/{id}*` route goes through one dependency, `load_call`, which resolves the conversation, checks its tenant and, for an agent token, checks that the agent took part in it.
- Agents and supervisors see only their own and their accessible agents' data. Admin keys see the tenant.
- Plivo callbacks rebuild the signed URL from `PUBLIC_BASE_URL`, not the Host header.

## API conventions

- Base path `/api/v1`. Changes inside v1 are additive: new fields, endpoints and event types. A breaking change would ship as `/api/v2` alongside.
- Ids are UUIDs, numbers are E.164, timestamps are UTC ISO 8601. `conversation_id` is stable across overflow and transfer, and both `/conversations/{id}` and `/calls/{id}/*` take it.
- Errors use one envelope. 422 covers any validation failure, 400 only unparseable JSON, 401 a missing or bad credential, 403 a scope or role refusal, 409 a state conflict, 429 a Plivo rate rejection with `Retry-After`.
- Lists use `?limit=` (at most 200) and `?cursor=`.
- `Idempotency-Key` is optional and recommended on `POST /calls` and `POST /tenants`. Redis keeps the response for 24 hours per tenant, principal and key. Routes that return a secret store only the status and resource id, so a replay returns 409 with that id.
- CORS applies to `/api/v1` only, with exact origins from `CORS_ALLOWED_ORIGINS` and bearer tokens rather than cookies.
- The service has no rate limiter of its own; put one at your ingress.

## Glossary

The code and the schema keep some older names.

| Term | Meaning |
|---|---|
| organization | A tenant. |
| aom | An agent: one person's membership in one tenant (`account_org_mapping`, joining `account` to `organization`). An agent id in the API is an aom uuid. |
| contact uuid | The caller's Plivo CallUUID. It keys a call's rows in Postgres and its state in Redis. |
| parent conversation | A `conversation_parent` row: one customer contact from start to end. Its uuid is the API's `conversation_id`. |
| child conversation | A `conversation` row under a parent: one stretch of the contact with its own agent (`primary_aom`), such as a queue entry, a transfer or an outbound dial (`generation_type`). |
| qnr run | One queue-and-route run: a caller's stay in one queue while routing offers the call to agents (`active_queue_entry.qnr_run_uuid`). The call's MultiPartyCall is named `mpc_<qnr run uuid>`. |
| chunk | One entry in a conversation's stored history (`conversation_chunk`), such as a note, a disposition, a recording or a transfer. The API's `/timeline` reads events, not chunks. |

Redis key prefixes:

| Prefix | Holds |
|---|---|
| `LST:<aom>` | the agent's last heartbeat; its expiry marks the agent offline |
| `AOMPC:<aom>` | the agent's heartbeat count |
| `{<org>}AOMN:<aom>` | the agent's display name |
| `{<org>}CCSM` | the tenant's conversation states, name to row id |
| `bts:<contact uuid>:<channel>` | a child conversation's state before a transfer, restored if the transfer fails |
| `ACS:<conversation number>:<setting>` | a conversation's copy of a tenant routing setting (release capacity on resolve) |
| `CASM:<digits>:<queue>`, `OCASM:<auth id>:<digits>` | a caller's sticky agent, inbound per queue and outbound per tenant |

## Design choices

| Choice | Rejected alternative | Reason |
|---|---|---|
| Routing as resources plus one action renderer | A node-based call-flow engine | Integrators need settings, not a visual editor; the engine and its validators would be code with no user |
| Your logic enters as a redirect after the service answers | Your application answers first and redirects into the service | Consent and hours then run on every call, and one Plivo application owns each number |
| One outbound path: call the agent, then build the MPC at answer | Dial the customer first, or separate API and browser paths | It reuses the inbound MPC shape, so every in-call control works unchanged, and there is one path to test |
| SSE and webhooks fed from a Postgres outbox | A WebSocket server, or a hosted pub/sub service | The UI only needs server to client; SSE resume gives replay; no extra vendor account |
| Reports as SQL over the service's own tables | A separate analytics database | One fewer hard dependency; the report set is small |
| Opaque tokens in Redis; one Postgres `jobs` table for timers and retries; a plain `events` table | A JWT library; a Redis sorted set or a separate scheduler; a second delivery table; monthly partitions | Tokens are revocable with no new dependency; one durable queue; a missing partition would fail state changes at a month boundary |
| Dialing is a server right (`agent_direct_dial` off) | Letting agent tokens place calls | Your server can run do-not-call and calling-window checks before every dial |
| Tenant per Plivo sub-account, credentials per tenant | A shared account with a tenant column on Plivo data | Plivo's own account boundary backs the isolation, and tokens never cross tenants |
| `jobs` and the event relay run in every replica | A separate worker deployment | One image to run; locks make duplicates harmless |
