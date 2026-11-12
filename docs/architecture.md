# Architecture

human-specialist is one FastAPI process backed by Postgres and Redis. It talks to Plivo for calls, SMS and WhatsApp, and to an optional OpenSearch node for conversation search. This page describes how the parts fit.

## Process model

- One container image, run as several identical replicas behind a load balancer. uvicorn only.
- `alembic upgrade head` is a separate one-shot step (the `migrate` service in compose). It fails closed, and the app never migrates at boot. A revision that alters a table traffic reads waits at most 5 seconds for its lock, then rolls the whole upgrade back; run it again when traffic is lighter.
- Every replica runs the same background threads: the job runner (four lanes, each with its own workers: `plivo` 8 for Plivo commands and room checks, `webhook` 4 for webhook deliveries, `slow` 4 for plugin hook replays, search indexing, report numbers, transcript fetches and retention, `control` 8 for timers and routing), the event relay and stream fan-out, the stream credential check, and one thread per plugin hook. Each job row carries its lane, and a lane claims its due rows with `FOR UPDATE SKIP LOCKED` through an index on lane and due time, so a backlog in one lane never slows another's claim. The relay takes a Postgres advisory lock, so one replica stamps events at a time.
- Postgres holds the durable state, live calls included: assignments, agent statuses, tokens, idempotency records and timers, an open offer's timer among them. The service keeps its tables in the schema `hs`.
- Redis holds hot state, one source per datum: each desk's heartbeats, each conversation's open offers and each agent's (`offers`, `desk`, and `offer_conversation`, which finds an ended offer's conversation), each queued conversation's offer rounds, the agents they tried and its wait prompt, each open conversation's unread count, and a 60-second cache of each tenant's config.

  Only `hot.py` sends commands; a tenant's `epoch` key holds a token each rebuild mints and says its keys are whole, and the presence sweep rebuilds a tenant whose epoch a Redis loss took.

  A Redis loss ends each open offer and starts each queued conversation's rounds over; a leg rung for a lost offer that answers while its caller still waits takes the call for the agent it rang, and a timer whose offer alone Redis lost gets its conversation's round at the next sweep. A slot taken for a transaction that rolls back is given back before the rollback.

  Every other write only creates (an offer, sent just before the commit, which counts only with the timer its transaction commits) or runs after the commit and applies only while Redis holds what its transaction read, under the epoch it read; the end of a queued caller's wait prompts applies whatever it read and holds for the queue stint.

  No sweep rebuilds an unread count: only an agent's read or write creates it, a message adds to it only once it exists, and a read counts a missing one from Postgres without writing it back.
- `/status` is public and reports only whether the database answers. Prometheus metrics are served on `METRICS_PORT`.

## Modules

`src/hs/` is one package grouped by domain. Each module owns its routes, logic and storage.

| Module | Owns |
|---|---|
| `core.py` | settings, the database (the reflected `hs` tables, `tx`, `read`), the error envelope, request middleware, paging, logging |
| `jobs.py` | the `jobs` table runner: timers, recurring passes and provider commands |
| `hot.py` | the Redis hot-state store: every key template and command, the rebuild after a Redis loss, and the config cache |
| `plivo.py` | Plivo REST as a tenant, Plivo writes as `plivo` jobs, the V3 and V2 signature checks, callback URLs and XML |
| `access.py` | the operator key, tenant API keys, agent and stream tokens, password hashes, plugin resolvers, scopes and roles, tenant-scoped lookups, `Idempotency-Key` |
| `events/` | the event catalog, the outbox and its relay, the stream, webhooks, replay, the timeline and plugin hooks |
| `config/` | tenants and their configuration documents: queues, business hours, menus, numbers, teams, skills, statuses, dispositions, settings and the status map. `config/provision.py` holds what HS makes and deletes at Plivo: the durable delete (a request never waits on it: the job pool runs it at once and retries), the intent an endpoint or application create commits first, and a deleted tenant's teardown |
| `agents.py` | agents and their Plivo endpoints, sessions, sign-in and renewal, presence, the heartbeat and its offline sweep, the agent's desk (`/me`) |
| `conversations/` | the conversation and assignment state machines, entries, notes, attributes and wrap-up. `conversations/read_model.py` holds the read model, `conversations/transfers.py` the transfer status events and `conversations/api.py` the routes |
| `routing.py` | queues, offer rounds, the four strategies, skills, capacity, sticky agents, reverse routing, RONA, and the text and task offer routes |
| `voice/flow.py` | the answer URL, a queue's enqueue URL, entry points, menus, consent, voicemail, a caller's moves between rooms and the hangup URL |
| `voice/mpc.py` | Plivo MultiPartyCall and call-status callbacks, ringing agents, and the room check |
| `voice/calls.py` | hold, hangup, warm and cold transfers, extra participants, and supervisor listen, whisper and barge |
| `voice/outbound.py` | agent-first outbound calls |
| `voice/media.py` | recordings, transcripts, signed audio URLs, and pausing or resuming a recording |
| `text/` | chat, SMS, WhatsApp and email conversations: messages in and out, closing, transfers, a second agent and the session timeout. `text/plivo.py` holds Plivo's message callbacks, the `send` command and the media proxy, and `text/templates.py` the WhatsApp template list and the sender and template checks |
| `tasks.py` | agent tasks and callbacks |
| `search.py` | search over closed conversations, in OpenSearch or in Postgres |
| `reports.py` | the fixed reports and the live floor |
| `views.py` | saved conversation views: filters, columns, sharing, favorites and each agent's order |
| `export.py` | CSV exports of closed conversations: the summary list and each conversation's content |
| `app.py` | the router list, the lifespan, `/status` and the plugin loader |

Imports run one way, in this order: `core`; `hot`; `jobs` and `plivo`; `access`; `events`; `config`; `agents` and `conversations`; `routing`; the voice modules, `text` and `tasks`; `search` and `reports`; `views`; `export`; `app`. Voice, text and tasks never import each other. They meet through `routing.CHANNELS`, `plivo.OPS`, job kinds and the public API. A lower module asks a higher one for work by scheduling a job.

## One model for every channel

A conversation is one contact with a human, on one channel: `call`, `chat`, `sms`, `whatsapp`, `email`, `task` or `callback`. Three tables hold it:

- `conversation`: the contact, its state, its customer and HS addresses, its queue, `context` and `attributes`.
- `assignment`: one stint on it, from an accepted offer (or an outbound dial) to wrap-up. Its role is `primary`, `secondary`, `external` (a phone) or `monitor` (a supervisor).
- `entry`: a message, note, signal, recording, track or voicemail.

A conversation moves through `open`, `queued`, `live`, `wrap_up` and `closed`. An offer is open while its `offer_timeout` timer row exists and its Redis field is open, and every end deletes the timer in its own transaction: an accept inserts the `active` assignment under the offer's id, and a decline, miss or withdrawal leaves no row, except a `missed` one for a leg that left before it joined. An assignment moves from `active` to `wrap_up` and `done`; an outbound dial's starts `offered` while it rings. Every write takes the conversation's row lock, and each transition is one guarded `UPDATE` or `DELETE`. When no row comes back, the event was a duplicate or arrived late, and the caller does nothing. That is how repeated and unordered Plivo callbacks converge.

Every transaction takes its locks in one order and never goes back for an earlier one: the tenant's configuration lock first (a configuration write holds it alone, and other writes that name configuration ids share it), then conversation rows, a queue row, agents' presence rows (several in agent order, in one statement), status rows, agent and other configuration rows, and last the tenant row, credentials, passwords, job rows and events. A status delete therefore locks every presence row of its tenant before the status row, so no agent moves into the status while its agents move out.

Capacity lives in Redis: each agent's slots per channel (`cap`), each held by the offer or stint that took it and counted once per conversation, so two offers of one conversation hold one slot, and the agents at their cap (`full`), whom routing skips.

An offer takes a slot, and its stint gives it back once its end commits, or its wrap-up's end while wrap-up blocks routing (a cold transfer to a queue holds it at least until the room the caller left ends). A call offer HS's own timer missed keeps it until its leg's hangup reaches HS, 60 s at most, since that timer runs ahead of Plivo's ring and the leg may still ring or have answered unheard.

A slot is taken in one script that refuses past the cap, so two replicas never fill one agent's last slot, and that refuses while the tenant's `epoch` is missing or a rebuild holds the tenant's `slots` advisory lock, so no transaction that took a slot under a lost state commits into a rebuilt count.

An accept holds the same lock to its commit and then checks its offer is still open, so a rebuild either counts its stint or ran first and left it no offer to take. A slot is given back only after the commit, since a rollback could not take back a slot another replica took meanwhile. A give names its own holder, so one that comes late or twice leaves a newer offer's slot.

A give that has not reached Redis, or never will, leaves the agent full until the agent's own `route_agent` run, or the sweep. A run that finds the agent full ends only while Postgres holds the agent at its cap; otherwise it settles the agent's slots against Postgres and picks again.

The 15 s sweep rebuilds a tenant's slots from its stints and open offers after a Redis loss, and in the same transaction schedules the tenant's follow-up rounds and offers its signed-in agents work; it sets the epoch only once that commits, so a rebuild that fails anywhere is the next sweep's to run again. One transaction rebuilds at most 500 tenants, as it holds each one's advisory lock to its commit. The sweep then settles any disagreement under the conversation's lock, after a commit of its own has scheduled the `route_agent` run of each agent the settle frees, so a settle that fails once it has changed Redis loses no agent's wakeup.

A slot taken for a conversation that never committed, as when its process died before COMMIT, is given back by the first sweep after the transaction that opened the conversation ended: that transaction holds the conversation's advisory key to its end, so a slow transaction keeps its slot however long it runs.

A dead replica's transaction ends once Postgres notices its closed socket, or by `idle_in_transaction_session_timeout` 10 s after its last statement, which `statement_timeout` ends within 5 s; the slot then comes back within one 15 s sweep interval plus the length of a sweep already running, since each pass is scheduled when the one before it ends, or after the sweep's 60 s lease when the dead replica held it.

The key is the last 64 bits of the conversation's id, 62 of them random, so another opener holding the same key, at odds of about 2^62 to 1 per pair, only delays the give-back until its own transaction ends.

Each customer's sticky agent also lives in Redis: one key per sticky queue (`sticky`), set after the commit of a primary stint's start, and one for the agent of its last outbound (`osticky`), keyed by the address dialed, set after the commit of an agent's call out, a call from a text conversation included, or of a primary start on an outbound contact before any enqueue.

Each is kept for the queue's expiry or the outbound hours, counted from that start or dial. Each value carries that time and the stint's id, and one script replaces only an older value, so a write that reaches Redis late never undoes a newer stint's. A queue's write ends an older outbound agent by leaving its own time, with no agent, in `osticky`.

After a Redis loss a job rebuilds every queue key and the outbound key from the newest such stints through the same script, so it agrees with the live writes; a queue key only from stints whose queue and agent are unchanged since, and the outbound key by the tenant's outbound settings as they are now. Until the job reaches a customer, the customer routes without one. The strategies' counters are read from assignment rows.

## Inbound call path

1. Plivo requests `POST /plivo/{tenant_id}/answer`. The V3 signature is checked first. A repeat for the same call uuid returns the same XML.
2. The called number is looked up as an entry point. It carries a business-hours schedule, an open action and a closed action.
3. A conversation opens and `conversation.started` is emitted. If the schedule says closed, the closed action runs and `conversation.after_hours` is emitted. If consent is on, the consent notice plays first.
4. The action is rendered as Plivo XML. Entry points, menu options, queue overflow and queue key presses all take the same action shape: `queue`, `menu`, `voicemail`, `forward`, `redirect` or `hangup`. A `redirect` hands the call to your own Plivo application after consent and hours have run. Send it back to a queue's `enqueue_url` to queue it.
5. A queue action puts the caller in a MultiPartyCall room with the queue's hold music, or Plivo's default track when the queue sets none, and emits `conversation.queued`. If the queue is full, its overflow action runs instead.
6. Routing offers the conversation in rounds. It ranks the agents by its route's strategy, teams and skills: the route it was queued with, the queue's as it was then or the one its enqueue brought, so a queue edit reaches only later contacts, skipping those at their cap, share-locks their presence rows in agent order to recheck their status, and creates up to `agents_at_once` offers (`offer.created`), each taking a slot.

   One add-participant request rings every offer of the round, with the `HsKind`, `HsOffer` and `HsConversation` SIP headers, plus `HsAutoAnswer` when the route auto-accepts. Plivo's first answer wins, and the other offers are withdrawn. A ring Plivo refuses runs the overflow action as `assignment_failed`: at once, or, when another agent was offered the call before that ran, once that offer ends untaken.

   A ring that ends busy or unregistered frees its agent's slot, but pulls no queued work to that agent until its status or load changes, while new enqueues still rank it. While a caller waits in a queue, an agent it was offered to is not offered it again until 300 s pass with no new offer. When every agent it could go to was tried, its next round runs once those 300 s pass and rings them again, until its `1 + retries` rounds are spent and the overflow action runs, or `wait_timeout_sec` ends the wait.

   An agent who frees up takes the contacts of `fifo` queues before those of `lifo` ones, then the most urgent queue's first.
7. A call offer that rings out unanswered counts a strike. At `rona.strikes` (0 turns RONA off) the agent moves to `rona.status_id`, and `agent.status_changed` is emitted with reason `rona`. A decline does not count, and neither does a ring Plivo never confirmed or a text or task offer.
8. The queue exit URL continues a caller who leaves the queue unserved. A wait timeout or a key press runs the overflow or key action, and `conversation.overflowed` is emitted.
9. `call.ended` is emitted once per room the caller leaves. A move out of a room (a transfer to a queue, menu, voicemail or redirect, a key press, an overflow, or the queue's after-agent action) tells it when an agent has the call. The caller's hangup tells it unless one already told of the last room the caller sat in. It names the agent of the newest primary stint still on the call, or in wrap-up from that room, or no agent.

   Each agent who handled the call enters wrap-up (`conversation.wrap_up_started`). An agent who cold-transfers the call to a queue wraps up manually, whatever the tenant's mode. The agent submits a disposition and a note, once for every wrap-up it has on the conversation, or the deadline closes the wrap-up with the status-map disposition. `conversation.closed` fires once, after the last wrap-up, and queues the `search_index` job.

Each agent leg's uuid comes from Plivo's reply or from a call-status callback naming the ring, whichever arrives first. After each ring, a `missed_mpc_event` job reads Plivo's view of the room, for callbacks HS may never get.

A seat in a queue with wait prompts arms an `mpc_initialized` job, which 10 seconds later stands in for a room's `MPCInitialized` that never came: if the caller still sits in that room and no prompt started, it reads Plivo's view of the room and starts the first.

Wait prompts start at `MPCInitialized`, play in order with their repeats, then cycle the `loop` prompts, and end for the queue stint at `MPCStart`, a successful add-member, the caller's departure from its room, or the agent's join, which also cuts off the one playing. A prompt reaches Plivo only while Redis still holds the prompt state its transaction chose it from, so a late or repeated callback never speaks twice or after the end.

Its command claims it in Redis just before the request, so a retry after a lost reply does nothing: a prompt may be missed, never doubled. Only a send Plivo refused or that never left gives the claim back.

A call that ends before any queue (a hangup in a menu, a forward or a voicemail) closes from a delayed job after the hangup URL, so a late voicemail recording still finds its conversation.

## Text path

1. A text contact starts with `POST /queues/{id}/enqueue` and a JSON body whose `channel` is `chat`, `sms`, `whatsapp` or `email`, or with an inbound SMS or WhatsApp message at `POST /plivo/{tenant_id}/message`.
2. A customer message joins the open conversation for its channel and its two addresses, or the email thread its headers name. Later chat and email messages arrive at `POST /api/v1/messages`, which never opens a conversation (404 `no_open_conversation`).
3. Routing offers the conversation in rounds, as it does a call. The agent takes an offer with `POST /offers/{id}/accept` or turns it down with `POST /offers/{id}/decline`, and a declined or missed offer moves the conversation to its next round. No round offers it again to an agent it was offered in this queue stay. Its rounds are spent after `1 + retries`, or once every agent who could take it was offered it and the queue's `text.wait_timeout_sec` sets no wait for its channel; it then closes as `assignment_failed`, with that result's system disposition. Every configuration write runs the next round of each queued text or task that was offered to an agent and has no wait timer, so one closes the same way once the last agent not yet offered it is deleted or loses the team or skill its route needs. An email's rounds are never spent: it waits for an agent. A queue's lines go to the customer on the conversation's channel: `waiting` each time it queues, `assigned` when the first agent takes it and `closed` when it closes. `text.messages` holds them keyed by channel: `chat`, `sms` or `whatsapp`.
4. An agent reply is an entry. SMS and WhatsApp replies leave through Plivo unless the tenant set that channel's delivery to `external`. Every other reply goes out as `message.sent` with `delivery: external`. Your own gateway delivers it and reports its status at `POST /messages/{id}/status`.
5. `POST /conversations/{id}/close` ends the conversation, and so does the channel's session timeout. A chat queue's `closed` line goes to the customer first. Wrap-up and close then run as they do for a call, but a text's wrap-up, automatic or not, waits for the agent until 300 s past its deadline: the desk submits an automatic one itself.

HS owns no chat socket, email provider, media store or template store.

## Tasks and callbacks

A task is a conversation of its own (channel `task` or `callback`) plus one `task` row. A task sent to a queue is routed like text, and waits as long as the queue's `text.wait_timeout_sec` gives its channel, `task` or `callback`. A task sent to a team is offered at once to its free members but the one who made it, at most the `simultaneous_agent_limit` setting. A task's offer, from a queue or a team, waits 71 hours for an answer.

One sent to an agent is assigned within the agent's capacity, or past it, holding none of its slots, when the agent made it; with no free slot it reads `failed`. It routes now or at a future `scheduled_at`.

Each routing step writes its `routing_status`: `scheduled` until its time, `pending` while it is due, queued or offered, then `accepted` once an agent took it, `declined` when its offers all ended untaken, or `failed` when no agent could take it or its queue gave up on it, until it routes again: a new target or schedule, or `reroute`. A queue that gives up on a task leaves it open, whatever its type. A queue that refuses it when it comes due, disabled, full or deleted since, closes it as `enqueue_failed`. `GET /tasks?routing_status=` filters on it.

An accept, a direct assignment included, a decline and a failure are each a `routing_status` change; an accept's `task.updated` names the agent the task was taken from, else, as a decline's or a failure's does, the agent who last made or changed it (`updated_by`).

Its `status` is the work status apart from that: `todo` until an agent sets `in_progress`, `waiting_on_customer` or `todo` again, which changes nothing else, an accept included; resolving or cancelling it ends its conversation. Routing a task again is a `routing_status` change too. Its assignee is read from the conversation's primary assignment.

Its history is the conversation's notes plus one `signal` entry per change, an agent taking it included. A taken task left idle 15 days closes as `session_timeout` and reads `resolved`. A callback is a task that the agent works by calling the customer with `POST /calls`. A task's `type` is `task`, `callback`, `internal`, `follow_up` or `others`.

An `internal` task takes no schedule and no `channel`; a `follow_up` names the `channel` it follows up on, needs a schedule when an agent makes it, and keeps its agent when its target changes, routing again only when its schedule does. Every type but `callback` routes on the `task` channel.

A task raised from a conversation (`conversation_id`, refused once that conversation closed) takes its direction and, unless it names one, its customer; it tells its events on that conversation, and whoever worked that conversation or a task raised from it reads the task. An agent with an open offer for a task reads it too, and so may comment on it before taking it.

Its target or type changes only while no offer or queue holds it; a type change moves the conversation to the new channel, and a slot its agent already holds stays on the channel it was taken on until that stint ends.

## Outbound path

1. `POST /calls` with a tenant key, or with an agent token only when the tenant enables `agent_direct_dial`. A `from` number must be registered with `PUT /numbers/{number}`.
2. The service opens an outbound conversation and rings the agent's endpoint (`call.started`).
3. When the agent answers, the agent's leg takes its seat in the conversation's room, and the service dials the customer into it. The room has the inbound shape, so hold, transfer, recording and hangup work the same way, and a hold plays Plivo's default track. `call.answered` is emitted when the customer answers.
4. A call that ends before the customer answers emits `call.failed`, then `call.ended`, and closes with the disposition mapped to its result.
5. With a `conversation_id`, the call joins the agent's live text, task or callback conversation instead: the agent's stint on it is secondary, the conversation's state never moves, and the call's end only tells `call.ended`. That stint is the call's agent for hold, recording and hangup, and the conversation's end ends the call. It takes no transfer, participant or supervisor (409 `call_joined`), since those would move the conversation itself.

The conversation outlives each call joined to it, so the call is the agent's dial stint, and its `call_on` mark on the conversation stands until the call's end is told:

| Dial | Its stint | What each input does |
|---|---|---|
| requested | offered, no request id yet | An answer seats the agent. A hangup, or the conversation's end, ends the dial now, and the request id cancels the call when Plivo's reply brings it. |
| ringing | offered, with the request id | An answer seats the agent. The ring's end, a hangup or the conversation's end ends the dial and cancels the request. |
| answered | active, with the agent's leg | The customer is dialed into the room. Hold and recording act on the call. A hangup hangs up the agent's leg, and either leg's end ends the dial. |
| ended | withdrawn or done | An answer is rejected, the request id that arrives late cancels the call, and a leg Plivo reports late (in the customer add's reply, in a status, or as a join in the dial's room) is hung up under the dial's own key, never bound to the conversation or to a later dial. A hold or recording that Plivo confirms late tells nothing. |

Every move of the stint out of offered or active, from a hangup, a leg's end, a close, a task's resolve or cancel, a session timeout or an agent going offline, passes through `conversations.move`. It tells `call.failed` (when the customer never answered) and `call.ended` once, cancels a ring and hangs up both legs. A new call from the conversation may start as soon as the last one ended. It takes the next room, and its customer's add is its own Plivo command, keyed by its dial, so the old call's late legs never reach it. The old call's recordings stay the conversation's history, and its transcript names its own customer.

## Plivo writes

Each Plivo write is a `plivo` job, scheduled in the transaction that decided it. It runs once that transaction commits. A create (ring, dial, send, record) is sent only on its first claim. A later claim cannot know whether an earlier one sent it, so it reconciles from callbacks, timers or a Plivo read instead of sending again. Other writes, such as hangup, hold, kick and redirect, retry on a backoff and then park. A route that answers with Plivo's verdict runs its command itself and waits for it. A refusal is that route's error, and a Plivo outage is a 503 while the command keeps retrying.

## Events

1. `events.emit(type, data)` inserts an unstamped row in the caller's transaction, so the event commits with the change.
2. The relay polls every 0.2 seconds. Under an advisory lock it takes the unstamped rows that have committed, stamps them with `seq` in `id` order, copies the conversation's direction and context onto them, and inserts one webhook job per matching subscription. A transaction that commits after a later `id` was stamped gets a later `seq`, so `seq` is the order the relay saw events, not the order they were emitted.
3. Each replica reads the newly stamped rows once per pass and hands each one to the open streams that may see it. A stream resumes from `Last-Event-ID` or `?after=`. It pings every 15 seconds, and its credential is checked again every 2 seconds.
4. Each plugin hook follows its own cursor, in `seq` order. An event that still fails after the hook's in-place retries is parked as a `hook` job and replayed on a backoff.
5. Replay is `GET /events?after=`. A daily job deletes events older than `EVENT_RETENTION_DAYS` once every hook has passed them.

Delivery is at-least-once. Consumers dedupe on the event id and read `GET /conversations/{id}` for the current state. Every event type is a pydantic model in `src/hs/events/catalog.py` and appears in OpenAPI under `webhooks`. See [Events and webhooks](events.md) for the envelope, who sees what, and webhook signing.

## Jobs

The `jobs` table is the only timer and async-work mechanism: `kind`, `lane`, `key`, `run_at`, `payload`, `attempts`, `locked_until`, `last_error` and `rev`. A kind and key hold one row, and scheduling the same key again replaces it, so a timer can move (for example, an extended wrap-up). A worker leases a due row for 60 seconds. A failing handler retries on its kind's backoff, and the last failure parks the row with its error.

Kinds: `plivo` (provider commands), `offer_timeout`, `next_round`, `queue_timeout`, `route_agent`, `config_rounds` (after a configuration write, the next round of each queued text or task that was offered to an agent and has no wait timer), `task_route`, `wrap_up_timeout`, `missed_leg` (the slot of a leg whose offer HS's timer missed), `session_timeout` (the text session timeout), `delayed_close`, `missed_mpc_event`, `dial_check`, `transcript_fetch`, `transcript_settle` (a room recording's wait for the tracks Plivo sends beside it), `search_index`, `search_backfill`, `webhook`, `hook`, `sweep` (the 15-second presence pass), `agent_offline`, `event_retention`, `report_facts` (the closed conversations' report numbers) and `retention` (parked jobs, idempotency records and tokens expired over a day ago; it keeps text callback nonces, since Plivo's V2 signature never expires).

## Search

`POST /conversations/search` reads closed conversations only. The `search_index` job writes a conversation each time it closes, and again on each change after that. A chat no one wrote in is never written. With `SEARCH_URL` set, the job writes to one OpenSearch index per tenant (`hs-conversations-<tenant id>`), and search reads only that index.

An index that does not answer, or answers a timed-out, partial or malformed page or another index's conversation, is a 503 `search_unavailable`, never a fallback to Postgres or a short page. Without `SEARCH_URL`, the job writes the `search` table, and search reads Postgres. A write that fails backs off for about 4.5 hours, then parks.

Every 15 minutes, a `search_reconcile` pass starts by running parked writes again, as many as keep fewer than 200 index writes waiting. With an index, the pass then walks the conversations closed in the 7 days before it started, 200 at a time, asks the index in one request which of them it holds, and queues an index write for each one it lacks that no write waits to index, a parked one included.

An index that cannot say whether it holds a document, on a shard's error for one, ends the pass at that page, and its retry asks again. A document the index loses, to a deleted index or a lost node, comes back within 15 minutes with no operator. The pass moves on to the next 200 only once fewer than 200 index writes wait, so a lost index refills at the backfill's pace.

The `search` table needs no such check: its rows live in the database the conversations do. No migration queues `search_backfill`. Queue it with the statement in [Fill the search index](configuration.md#fill-the-search-index) once after you turn OpenSearch on, and once after each release that adds a field to the document, on either backend. It indexes every closed conversation again at its version, 200 at a time.

The index and the table keep what they had meanwhile, so every filter that existed before keeps finding them, and the new filters find each conversation once the backfill reaches it.

Both backends read the same document: the conversation, the numbers it is filtered and sorted by (duration, talk, wrap and first-response seconds, and the message count), the last wrap-up's disposition and note, who ended a call, and the texts `text` searches. Who ended a call is kept on the conversation, so event retention never takes it.

A `team_id` filter, and a team's name in `text`, find the teams the agents who owned the conversation were in when it closed; a listening supervisor, a consult that never completed and a participant add none. It is a snapshot: a later change to an agent's teams or to the queue's moves nothing, and the queue's own teams never count. A conversation no agent took has none.

A `disposition_id` filter matches the last disposition: a parent only when it has no sub-disposition, and a sub-disposition by its own id. An `agent_id` filter keeps only the agents the caller may see, so asking for another agent finds nothing. One word of `text` matches inside any word or whole text, of any length, in any case. Several words match as a phrase whose last word may be cut short.

Talk runs from the first agent's start to the last wrap-up's start, so a transferred call counts the time between agents too, and an outbound call or text the customer never got has none. Each wrap-up counts until its agent submits it; one never submitted counts until its deadline or the close, whichever came first. Filter groups compare dates and times of day in the caller's `time_zone`.

Sorting by `created_at` orders by the creation time, then the conversation number. A text sort (`number`, `disposition`, `state` or `csat`) orders the texts in lower case by their characters' codes and reads an absent text as the empty one; on OpenSearch, a conversation the backfill has not reached yet sorts as an empty text. A search's `fields` names the filter fields whose values each hit carries in its own `fields`.

A `message` filter reads the texts of the conversation's messages, notes, email subjects and wrap-up notes, each a whole text to `is` and words to `contains`; `type` reads the kinds of item it holds (`message`, `notes`, `voicemail`, `recording`, `feedback` and `transfer`), as the export's `types` names them.

The backends differ in two places, and a unit spec pins each one on both. A Postgres phrase can span the end of one text and the start of the next, and OpenSearch keeps texts apart. While the backfill runs, a row it has not reached matches one word as a whole word on Postgres, as before the upgrade, and inside a word on OpenSearch; that ends when the backfill does.

`GET /conversations` always reads Postgres, for live and recent conversations.

## Data model

Postgres schema `hs`, created and changed by Alembic revisions only. The service reflects the tables at startup and never restates the DDL.

| Table | Holds |
|---|---|
| `tenant` | one per Plivo account or sub-account: the encrypted Plivo token, the Plivo Application id and the tenant settings document |
| `credential` | tenant API keys, agent tokens and stream tokens, as SHA-256 hashes with scopes and expiry; a token is deleted a day after it expires |
| `idempotency` | stored responses per tenant, principal and `Idempotency-Key`, kept for a day, and the nonce of every text callback, kept for good: one row per inbound text callback |
| `config` | every configuration document (queue, business hours, menu, number, team, skill, status, disposition and agent), one row each |
| `presence` | one row per agent: its status and RONA strikes. The desk's heartbeats are in Redis |
| `conversation`, `assignment`, `entry` | the contacts, their stints and their history |
| `status_history` | one row per agent status change: the agent, when it began and ended, the status's name and whether it was available; kept for good. The trigger `write_status_history` on `events` writes it from each `agent.status_changed` event, in that event's transaction |
| `task` | a task's title, type, status, priority, destination and schedule |
| `events`, `webhooks`, `hook_cursor` | the event outbox, webhook subscriptions and each plugin hook's position |
| `jobs` | timers, recurring passes and provider commands |
| `search` | the Postgres search sink: one row per closed conversation |

Queue, menu and entry-point configuration is JSON validated by pydantic models. Schema changes are additive inside v1.

## Plugins

A plugin is a Python package registered under the entry-point group `hs.plugins` that adds routes, receives every event or accepts your own credentials. [Plugins](plugins.md) has the hooks, what a plugin may rely on, and how to put one on the image.

## Tenancy and auth

- Credentials: the operator key (environment), tenant API keys (scoped), agent passwords (salted scrypt hashes, with failed tries counted in Redis), agent tokens and stream tokens (opaque, expiring after an hour, renewed through a 12-hour chain), signed URLs for recording audio and message media, the Plivo V3 signature on voice callbacks, and the Plivo V2 signature on message callbacks.
- The tenant comes only from the credential. A path or body id is looked up inside that tenant. A foreign id is 404 in a path and 422 in a body.
- An agent sees by its role's view level (`access.seen`, one rule for every reader). At `self` it sees the conversations it is on or has handled, and reads by id one it holds an open offer on. At `team` it also sees those of every agent who shares one of its teams, and reads the ones no agent took on a route to one of its teams, the route each was queued with. At `org`, as a tenant key, it sees the whole tenant. Defaults: admins `org`, supervisors `team`, agents `self`.
- Plivo callbacks rebuild the signed URL from `PUBLIC_BASE_URL`, not from the Host header. The V2 message signature does not cover the body, so a delivery report changes only the message its URL names.

## API conventions

- Base path `/api/v1`. Changes inside v1 are additive: new fields, endpoints and event types. A breaking change would ship as `/api/v2` alongside.
- Ids are UUIDs, numbers are E.164, timestamps are UTC ISO 8601. `conversation_id` is stable across overflow and transfer, and both `/conversations/{id}` and `/calls/{id}/*` take it.
- Errors use one envelope. 422 covers any validation failure and a Plivo refusal, 400 only unparseable JSON, 401 a missing or bad credential, 403 a scope or role refusal, 409 a state conflict, 429 a Plivo rate rejection or too many failed sign-ins, with `Retry-After`, and 503 a dependency that did not answer.
- Lists use `?limit=` (at most 200) and `?cursor=`.
- `Idempotency-Key` is optional on every `/api/v1` write. A repeat answers the stored response, and a different body is 409. A response that held a secret keeps only its status and resource id, so its replay is a 409 with that id. Once a route has recorded what it made, its answer is the outcome for good: a refusal or a 5xx replays as it was answered. Before that, an error answer frees the key. A create whose process died replays as its resource once its claim is two minutes old. Neither runs again.
- CORS applies to `/api/v1` only, with exact origins from `CORS_ALLOWED_ORIGINS` and bearer tokens rather than cookies.
- The service has no rate limiter of its own. Put one at your ingress.

## Glossary

| Term | Meaning |
|---|---|
| tenant | One Plivo account or sub-account and everything configured for it. |
| config document | A `config` row of one kind (`queue`, `agent`, `number` and others). Its `key` is the lookup the kind has besides its id: a number's E.164, an agent's SIP username, a status's system role. |
| presence | An agent's status and RONA strikes, and its desk's heartbeats in Redis. An agent's slots on each channel are in Redis too, checked against its assignments by the sweep. |
| conversation | One contact with a human on one channel. Its id is the API's `conversation_id`. |
| assignment | One stint on a conversation: an agent's, a phone's or a supervisor's time on it, through wrap-up. An accepted offer's id becomes its assignment's id. |
| entry | One item of a conversation's history: a message, note, signal, recording, track or voicemail. `/timeline` reads events, not entries. |
| round | One pass of offers for a queued conversation. The queue's `retries` sets how many more rounds follow an unanswered one. |
| room | The caller's MultiPartyCall, named `hs-<conversation id>-<room>`. The number goes up each time the caller moves to a new room. |
| ring | One add-participant request that rings a round's agents. Its id comes back on call-status callbacks as `g`. |
| track | One leg's own recording beside the room recording, kept for its transcript and never listed. |

## Design choices

| Choice | Rejected alternative | Reason |
|---|---|---|
| Routing as resources plus one action renderer | A node-based call-flow engine | Integrators need settings, not a visual editor; the engine and its validators would be code with no user |
| Your logic enters as a redirect after the service answers | Your application answers first and redirects into the service | Consent and hours then run on every call, and one Plivo application owns each number |
| One outbound path: ring the agent, then dial the customer into the agent's room | Dial the customer first, or separate API and browser paths | It reuses the inbound room shape, so every in-call control works unchanged, and there is one path to test |
| One conversation, assignment and entry model for every channel | A model per channel | Routing, offers, wrap-up, events and reports are written once |
| Postgres holds every durable fact, and a conversation's state-machine changes take its row lock; Redis holds only hot state (heartbeats, slots, offers, rounds, sticky agents, unread counts, the config cache), each datum in one store, written after the commit or fenced on the tenant's epoch | Live call state and timers in a second store, or everything in Postgres | A change and the event it emits share one transaction, and the hot reads that run on every heartbeat and every routing pass stay off Postgres at 200 concurrent calls; a Redis loss costs only state the sweep rebuilds from Postgres or restarts |
| SSE and webhooks fed from a Postgres outbox | A WebSocket server, or a hosted pub/sub service | The UI only needs server to client; SSE resume gives replay; no extra vendor account |
| Plivo writes as jobs in the deciding transaction | Calling Plivo inline | A crash after commit cannot lose a write, and a create is never sent twice |
| Reports as SQL over the service's own tables | A separate analytics database | One fewer dependency; the report set is small |
| Search in OpenSearch when configured, Postgres otherwise | OpenSearch required | A small install runs on Postgres alone, and a busy one moves search reads off Postgres |
| Dialing is a server right (`agent_direct_dial` off) | Letting agent tokens place calls | Your server can run do-not-call and calling-window checks before every dial |
| Tenant per Plivo account or sub-account, credentials per tenant | A shared account with a tenant column on Plivo data | Plivo's own account boundary backs the isolation, and tokens never cross tenants |
| `jobs` and the event relay run in every replica | A separate worker deployment | One image to run; locks make duplicates harmless |
