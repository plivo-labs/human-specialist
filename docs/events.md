# Events and webhooks

`events.emit` is the only outbound notification path. Each event is written to a Postgres table in the transaction of the change it reports, stamped with a per-service sequence number by one relay, then sent to the stream, to webhook jobs and to plugin hooks. `webhook.test` is the exception: a synthetic delivery to the one webhook it tests, never stored, streamed or passed to a hook. The event id is that sequence number. Event types are listed in `src/hs/events/catalog.py` and in OpenAPI under `webhooks`.

Events carry no signed media URL, because an event outlives any signed link. `recording.ready` and `voicemail.received` name the recording; read `GET /recordings/{id}` for a fresh `audio_url`. A message attachment hosted at Plivo has `url: null` in its event; read `GET /conversations/{id}/messages` for a signed link.

## Who sees what

A tenant key, and an agent whose role's view is `org`, see every event of the tenant (the `permissions` setting, [Set what each role sees](configuration.md#set-what-each-role-sees)). Any other agent sees the events that name it, and every event of a conversation it is offered, live on or wrapping up. At the `team` view it also sees the events of every agent who shares one of its teams, and of their conversations, and every event of a conversation no agent took on a route to one of its teams, the route it was queued with. A stream token sees what its agent sees, and a plugin's caller what an HS token of the role it was given sees.

An event names an agent by the envelope's `agent_id`:

- An offer, a wrap-up, a status change, a participant who joins or leaves, and a voicemail left for an agent name that agent. `task.created` names the task's target agent. `task.updated` names the previous assignee, and for a routing step the previous assignee or the agent who last updated the task.
- A call's own events name its agent, the primary on the call: `call.started`, `call.answered`, `call.failed`, `call.held`, `call.resumed`, `call.recording_paused` and `call.recording_resumed`. On an agent's own outbound call or text, that agent is named from `conversation.started` on.
- A transfer names the agent it came from. `call.ended` and `conversation.closed` name the agent who last had the call.
- A `supervisor.*` event names the agent being monitored. `supervisor.left` and `supervisor.failed` name the supervisor, who is off the call by then and would not hear them otherwise.
- `message.status_changed` names the agent who sent the message. Menu and queue events, other message events, notes, recordings, transcripts and CSAT name no agent. Whoever acted is `data.by_agent_id` where the type has it.

A task's events go no further than its readers. `task.created` and `task.updated` are on the conversation the task was raised from, else on the task's own. They reach a scoped reader only when they name an agent it sees, or when an agent it sees took that conversation or a task raised from it as the primary agent, at any time, or is on one of them now in another role. An offer or a queue never carries them.

A note's events reach exactly the readers of its conversation. `note.added`, `note.updated` and `note.deleted` reach a scoped reader when an agent it sees took the conversation in any role, at any time, or, while no agent has taken it, when its queue routes to one of the reader's teams. An offer never carries them. The note's author is `data.by_agent_id`, and the envelope names no agent. Notes on a task's conversation go to that conversation's readers, which can be fewer than the task's readers: the task's creator, its target agent before it takes the task, its target team's supervisors and the agents of the conversation it was raised from read its notes with `GET /tasks/{id}` but do not hear them.

A `supervisor.*` event names the supervisor in `data.supervisor_id`, and its envelope `agent_id` names the agent it is about: the call's primary agent, except for `supervisor.left` and `supervisor.failed`, which name the supervisor. A `supervisor.*` event whose `visible` is false, about a supervisor listening without having revealed itself, reaches that supervisor, and another scoped reader only when the reader sees that supervisor, never took the conversation and is not offered it, so the agent on the call never hears of it, then or on a later replay. A tenant key and an `org` view still do.

The service decides this when it sends or replays an event, from the conversation's assignments and queue at that moment. Once an agent's stint on a conversation has ended, a later replay shows that agent only the conversation's events that name it.

`agent.status_changed` carries `available`, whether the new status takes offers. Events stored before it was added omit it.

`call.ended` names the agent who has the call when the caller's room ends: the newest primary stint still on the call, or in wrap-up from that room, or no agent. It comes at most once per room the caller sits in, and each move to a new room can tell it again.

A recorded call records from the caller's seat in the queue, so its recording holds the wait, and a caller who leaves unanswered is recorded too. `conversation.closed` carries the `recording_ids` kept by then. Plivo sends a call's recording and hangup after its room ends, so a call that closes as the caller leaves, such as an unanswered one, can tell `conversation.closed` before `call.ended` and before its `recording.ready`. `GET /conversations/{id}` lists every recording.

`call.transferred` tells the agent a call transfer came from where it stands, by `data.status`. `initiated` comes once Plivo has taken the target's ring, before any other status of that transfer. A ring Plivo refuses fails only the transfer request, so it is told by no status. `accepted` comes when a target answers, cold or warm. `completed` comes when the target takes the call.

`failed` comes when no target answers, or when a warm target leaves before the transfer completes. `cancelled` comes when the agent stops it. Nothing follows `completed`, `failed` or `cancelled`. A transfer none of whose targets still rings or is on the call when the caller is queued again ends there, and nothing more of it is told.

A transfer's `target` has the shape the transfer request names it in: `{type: agent, agent_id}`, `{type: team, team_id}` or `{type: number, number}`. A team transfer's `target` names the team, from start to completion. A transfer to a queue, voicemail, a menu or a redirect is told only `initiated`, once Plivo has moved the caller, and its `target` is that action as the request named it.

A transfer to a saved destination is told as the number or redirect the destination holds. `conversation.transferred` tells a text transfer to a queue the same way: only `initiated`, once the queue takes the conversation, with the request's `target`; the conversation never reads `transferring`.

`offer.created` carries the `conversation_number`, the business number the contact reached (`to`) and `via`: `queue`, `transfer` (also a queue's offer of a conversation an agent had already taken) or `participant`. `conversation.wrapped_up` carries `duration_sec`, the seconds from the conversation's start to that agent's wrap-up. `GET /me` lists the pending text offers newest first, each with its `conversation_number`, customer (`from`), `via`, `offered_at` and `expires_at`, and `GET /conversations` rows carry the calling agent's `role` and `via` on the conversation.

`offer.missed` says why the offer ended unanswered, by `data.reason`: `timeout` when its timer ran out, `ring_unconfirmed` when the timer ran out on a call ring Plivo never confirmed (its ring request met an error or no reply, or was not sent in time), `ring_failed` when Plivo refused the ring, `unsuccessful` when none of the ring's legs joined, or Plivo's hangup cause code when the agent's leg ended unanswered; null when the leg left the room before it joined. On a call, `strike` is the agent's RONA strike count after the miss, and only `timeout` and Plivo's ring timeout (`6010`) add a strike.

`call.participant_added` comes when a participant joins the call, never for one that does not answer. `call.participant_removed` says why a participant is off the call while the caller is still on, by `data.reason`: `no_answer` for a ring that ended unanswered, `left` when its leg hung up, `removed` when the agent or HS took it off the call. A warm transfer's agent target that joined and comes off without taking the conversation, by a cancel or, on a text, a close, is told `call.participant_removed` (`removed`) or `conversation.participant_removed`, its `participant_id` the transfer's id. `call.held` and `call.resumed` come for each member whose hold changes, and `data.participant_id` names the participant, or the transfer for its target, or is null for the caller.

## Event types

Every type the service emits, with the keys of its `data`. A key marked `?` can be null.

| Type | Fires when | `data` |
|---|---|---|
| `conversation.started` | a contact reaches the service | `channel`, `number?`, `entry_point_id?`, `hours` |
| `conversation.menu_input` | on every menu attempt, a valid answer, an invalid one or no input | `menu_id`, `input`, `attempt`, `action?` |
| `conversation.redirected` | the contact is redirected to a URL | `url` |
| `conversation.queued` | the contact joins a queue | `from?`, `to?`, `queue_id` |
| `conversation.after_hours` | the contact arrives outside business hours | `number?`, `action?` |
| `conversation.overflowed` | the contact leaves a queue by its overflow | `queue_id`, `reason`, `action?` |
| `offer.created` | an agent is offered the contact | `offer_id`, `conversation_number`, `via`, `channel`, `queue_id?`, `from?`, `to?`, `ring_timeout_sec`, `auto_accept` |
| `offer.accepted`, `offer.rejected` | the agent accepts or declines the offer | `offer_id` |
| `offer.missed` | the offer times out unanswered | `offer_id`, `strike?`, `reason?` |
| `offer.withdrawn` | the offer is taken back | `offer_id`, `reason` |
| `call.started` | a call begins | `from?`, `to?` |
| `call.answered` | the call is answered, once: by the customer's leg of an outbound call, or by the leg of the first agent who takes an inbound call | `call_uuid` |
| `call.failed` | a call attempt fails | `result`, `hangup_cause_code?` |
| `call.held`, `call.resumed` | the call is put on hold or taken off it | `by_agent_id?`, `participant_id?` |
| `call.recording_paused`, `call.recording_resumed` | the recording is paused or resumed | `by_agent_id?` |
| `call.transferred` | a call transfer changes status | `transfer_id`, `mode`, `from_agent_id?`, `target`, `status` |
| `conversation.transferred` | a text transfer changes status | `transfer_id`, `mode`, `from_agent_id?`, `target`, `status`, `to_agent_id?` |
| `call.participant_added`, `conversation.participant_removed` | a participant joins the call, or leaves a conversation | `participant_id`, `participant`, `role` |
| `call.participant_removed` | a participant is off the call | `participant_id`, `participant`, `role`, `reason` |
| `supervisor.joined`, `supervisor.mode_changed`, `supervisor.left`, `supervisor.revealed`, `supervisor.assistance_requested`, `supervisor.assistance_rejected`, `supervisor.failed` | a supervisor's monitoring changes; `supervisor.left` fires on every way a supervisor stops monitoring: leaving, moving to another call, being displaced, or a transfer taking over | `supervisor_id`, `mode`, `visible` |
| `call.ended` | the caller's room ends | `from?`, `to?`, `result`, `queue_id?`, `hangup_cause?`, `hangup_cause_code?`, `duration_sec?`, `talk_sec?`, `ended_by` |
| `conversation.wrap_up_started` | an agent's wrap-up begins | `mode`, `deadline_at?` |
| `conversation.wrapped_up` | an agent submits or skips wrap-up | `disposition_id?`, `disposition?`, `sub_disposition?`, `note?`, `system`, `skipped`, `duration_sec` |
| `conversation.closed` | the conversation closes | `result`, `dispositions`, `recording_ids`, `duration_sec?` |
| `conversation.csat_recorded` | a customer's CSAT score is recorded | `score` |
| `recording.ready` | a recording is available | `recording_id`, `duration_sec?` |
| `transcript.ready` | a recording's transcript is available | `recording_id` |
| `voicemail.received` | a caller leaves a voicemail | `recording_id`, `mailbox`, `from?`, `transcript?` |
| `agent.status_changed` | an agent's status changes, by the agent, an admin, a missed heartbeat, RONA or the idle rule (`reason`) | `from_status?`, `to_status`, `available?`, `reason`, `reason_for_change?` |
| `message.received` | a customer's message arrives | `from?`, `to?`, `message_id`, `channel`, `text?`, `attachments`, `email?`, `whatsapp?`, `reply_to?` |
| `message.sent` | an agent's message goes out: on handover to your gateway when `delivery` is `external` (with no delivery confirmation), on Plivo's acceptance when it is `plivo` | the `message.received` keys, and `template?`, `delivery`, `by_agent_id?`, `provider_id?` |
| `message.status_changed` | a message's delivery status changes | `message_id`, `status`, `error_code?`, `error_title?`, `error_details?`, `provider_id?`, `reported_at?` |
| `note.added`, `note.updated`, `note.deleted` | a note changes | `note_id`, `text`, `attachments`, `by_agent_id?` |
| `task.created` | a task is created | `task_id`, `type`, `title`, `priority`, `due_at?`, `scheduled_at?` |
| `task.updated` | a task changes | `task_id`, `changes`, `by_agent_id?` |
| `webhook.test` | you call `POST /webhooks/{id}/test` | `webhook_id` |

## Stream

`GET /api/v1/events/stream` is server-sent events. Each frame has `id: <seq>` and a `data:` JSON envelope (`id`, `type`, `created_at`, `tenant_id`, `conversation_id`, `agent_id`, `direction`, `context`, `data`). The handoff `context` reaches an agent's stream, and a supervisor's conversation timeline, only while that agent is on the conversation: active on it, or in wrap-up as its primary. Otherwise it reads null.

`GET /conversations/{id}` gives it to an agent that took the conversation as primary or is active on it, a monitoring supervisor included. An agent holding an open offer on the conversation reads it and its messages until the offer ends or its ring timeout passes, with `context` null. A tenant credential, webhooks and plugin hooks always get it. There is no `event:` field, so `EventSource.onmessage` receives every event.

The server sends `: ping` every 15 seconds. When the token expires or is revoked, the server sends `auth.expired` and closes the stream. To resume, send `Last-Event-ID` or `?after=<seq>`, and the server replays everything after it. A client that falls 1,000 frames behind, or whose credential the server could not recheck for about a minute, is disconnected without `auth.expired` and resumes the same way.

## Webhooks

Create one with `POST /webhooks {url, event_types, queue_ids}`. With `queue_ids`, a webhook gets the events of a conversation that was in one of those queues when the event happened; one with none gets every event, and an event stored before queues were recorded reaches only such a webhook. The secret is returned once. Deliveries are at-least-once, so dedupe on the event id.

A delivery succeeds on any 2xx within 10 seconds, and a 410 disables the subscription. A 5xx, a timeout or a failed connection is retried after 10 s, 1 min, 5 min, 30 min, 2 h, 6 h and 12 h. A timeout or a failed connection also rests the subscription for 30 seconds: each of its deliveries due meanwhile counts a failed attempt without being sent, and retries on the same schedule.

After the last retry the delivery is parked in the `jobs` table, where it stays for `EVENT_RETENTION_DAYS`, and nothing resends it. Any other answer, a 4xx, a redirect or a status past 599, parks it at once, and a redirect is never followed. To catch up, read [Replay](#replay). Each attempt carries `X-HS-Event-Id`, `X-HS-Timestamp` and `X-HS-Signature: v1=<hex>`, where the value is an HMAC-SHA256 over `timestamp + "." + raw body`.

During the 24 hours after `POST /webhooks/{id}/rotate-secret` the header holds both `v1=` values, comma separated.

```python
import hmac, hashlib, time

def verify(secret: str, timestamp: str, body: bytes, header: str) -> bool:
    if abs(time.time() - int(timestamp)) > 300:
        return False
    expected = "v1=" + hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, part.strip()) for part in header.split(","))
```

Webhook URLs must be https and must resolve to public addresses. The sender connects to the address it checked. `WEBHOOK_PRIVATE_HOSTS` lists the hosts that may use http or a private address, for a receiver on your own machine or network.

## Replay

`GET /api/v1/events?after=<seq>&type=` returns every retained event after a sequence number, in order, with `next_cursor`. Use it to recover after downtime or after a delivery has failed. It returns the events themselves to the caller and does not resend any webhook.

An event older than `EVENT_RETENTION_DAYS` becomes eligible for deletion, and a daily job deletes it once its conversation, if it has one, has closed and the cursor of every plugin hook still running has passed it. Those are the hooks registered on the replica that runs the job, and any other hook whose cursor moved or was held in the last 60 seconds, as an old hook name is while a replica still runs it during a rolling deploy.

A hook retired by removing its plugin or renaming its function stops holding events back 60 seconds after it last ran. Agent status changes are also kept, for good, in the status history the availability reports read. Order across events is not guaranteed; `GET /conversations/{id}` is the source of truth. `POST /webhooks/{id}/test` sends a `webhook.test` event.
