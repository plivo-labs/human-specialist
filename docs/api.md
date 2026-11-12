# API

The public API is under `/api/v1`. Plivo callbacks (`/plivo/{tenant_id}/...`), `GET /status` and `GET /openapi.json` sit outside it, and `/openapi.json` describes all of them. There is no `/docs` or `/redoc` page, so load the document into any OpenAPI viewer or client generator. Collection routes take `?limit=&cursor=` and return `{"data": [...], "next_cursor": ...}`.

`GET /events` and `GET /conversations/{id}/timeline` page by `?after=<seq>&limit=` instead. Reports return their whole result as `{"data": [...]}`, except `GET /reports/conversations/rows` and `GET /reports/agent-status-log`, which page by `?limit=&cursor=` like a collection. Errors are `{"error": {"code", "message", "details"}, "request_id"}`.

When Plivo, the database or the search index does not answer, the request is a 503 (`dependency_unavailable`, `plivo_unavailable` or `search_unavailable`), not a 500; retry it. [`tests/contract/routes.py`](../tests/contract/routes.py) is the authoritative list of every method and path with its auth.

| Group | Paths |
|---|---|
| Tenants (operator) | `/tenants`, `/tenants/{id}`, `/tenants/{id}/api-keys`, `/tenants/{id}/api-keys/{key_id}` |
| Agents | `/agents`, `/agents/{id}`, `/agents/{id}/sessions`, `/agents/{id}/status`, `/agents/{id}/sip-credentials`, `/agents/{id}/password` |
| Sign-in | `/sessions`, `/me/sessions`, `/me/password` |
| People and settings | `/teams`, `/teams/{id}`, `/skills`, `/skills/{id}`, `/roles`, `/agent-statuses`, `/agent-statuses/{id}`, `/agent-statuses/reorder`, `/settings`, `/dispositions`, `/dispositions/{id}`, `/dispositions/reorder`, `/status-map` |
| Routing setup | `/queues`, `/queues/{id}`, `/queues/{id}/enqueue`, `/queues/{id}/stats`, `/business-hours`, `/business-hours/{id}`, `/business-hours/{id}/status`, `/holiday-groups`, `/holiday-groups/{id}`, `/menus`, `/menus/{id}`, `/numbers`, `/numbers/{number}`, `/transfer-destinations`, `/transfer-destinations/{id}` |
| Agent desk | `/me`, `/me/status`, `/me/heartbeat`, `/me/idle`, `/transfer-targets`, `/offers/{id}/accept`, `/offers/{id}/decline` |
| Conversations | `/conversations`, `/conversations/counts`, `/conversations/search`, `/conversations/export`, `/conversations/export/content`, `/conversations/{id}`, `/conversations/{id}/notes`, `/conversations/{id}/notes/{note_id}`, `/conversations/{id}/viewed`, `/conversations/{id}/wrap-up`, `/conversations/{id}/wrap-up/extend`, `/conversations/{id}/csat`, `/conversations/{id}/timeline` |
| Text | `/conversations/{id}/messages`, `/conversations/{id}/close`, `/conversations/{id}/transfers`, `/conversations/{id}/transfers/{tid}/complete`, `/conversations/{id}/transfers/{tid}/cancel`, `/conversations/{id}/participants`, `/conversations/{id}/participants/{participant_id}`, `/messages`, `/messages/{id}/status`, `/messages/{id}/media/{n}`, `/whatsapp/templates` |
| Tasks | `/tasks`, `/tasks/{id}` |
| Calls | `/calls`, `/calls/{id}/hold`, `/calls/{id}/switch`, `/calls/{id}/merge`, `/calls/{id}/recording`, `/calls/{id}/transfers`, `/calls/{id}/transfers/{tid}/complete`, `/calls/{id}/transfers/{tid}/cancel`, `/calls/{id}/participants`, `/calls/{id}/participants/{participant_id}`, `/calls/{id}/hangup`, `/calls/{id}/feedback`, `/calls/{id}/monitor`, `/calls/{id}/monitor/mute`, `/calls/{id}/monitor/reveal`, `/calls/{id}/monitor/assistance` |
| Recordings | `/recordings`, `/recordings/{id}`, `/recordings/{id}/audio`, `/recordings/{id}/transcript` |
| Reports | `/floor`, `/reports/conversations`, `/reports/conversations/rows`, `/reports/agent-activity`, `/reports/available-agents`, `/reports/agent-status-log` |
| Views | `/views`, `/views/{id}`, `/views/order`, `/views/{id}/favorite`, `/views/{id}/conversations` |
| Events | `/webhooks`, `/webhooks/{id}`, `/webhooks/{id}/rotate-secret`, `/webhooks/{id}/test`, `/events`, `/events/stream` |
| Plivo callbacks | `/plivo/{tenant_id}/answer`, `hangup`, `mpc-status`, `call-status`, `recording`, `transcription`, `actions/menu`, `actions/voicemail`, `actions/queue-exit`, `actions/move`, `message`, `message-status/{id}` |
| Public | `GET /status` (whether the database answers), `GET /openapi.json` |

`POST /conversations` starts an agent's SMS, WhatsApp or email to a customer, under the same rules as `POST /calls`. A call starts with `POST /calls`, and a task or callback with `POST /tasks`.

A queue holds a route: `strategy`, `teams`, `agents`, `skills`, `agents_at_once`, `retries`, `sticky` and `auto_accept`. A JSON `POST /queues/{id}/enqueue` may bring its own `route` of that shape, which must name agents or teams and is loosely sticky with no extra round unless it says otherwise: the conversation is offered by it in place of the queue's, and keeps the queue's place in line, capacity, timeouts and overflow.

`agents` narrows either route to those agents. A conversation keeps the route it was queued with, so a queue edit reaches only the contacts queued after it. `availability` offers the agent available longest, from its first sign-in or available status of a 12-hour window or its last wrapped-up text or task, whichever is later. A break or a call keeps an agent's place.

`fewest_handled` and `least_handle_time` count the texts and tasks an agent wrapped up as their last primary since its desk signed in, never a call or one it transferred on, and break ties the same way. A queue name is unique in its tenant (409 `queue_name_taken`). A queue with `enabled` false refuses new contacts as `enqueue_failed`, as a full queue does, and keeps routing those it holds.

A call is offered again to an agent it was offered once 300 s pass with no new offer, until its `1 + retries` rounds are spent. A text or task is offered to each agent once per queue stay. Once every agent who could take it was offered it, it waits out the queue's `text.wait_timeout_sec` for its channel, or with none leaves the queue at once as `assignment_failed`. It leaves the same way once a configuration change deletes the last agent not yet offered it, or takes away the team or skill its route needs; a queue edit reaches only contacts queued after it. An email waits for an agent instead.

`PATCH /conversations/{id}` merges `attributes` and, with `customer`, corrects the customer's identifier: the agent's replies and the customer's next messages follow it (422 `invalid_customer` for one that is no address of an SMS, WhatsApp or email conversation's channel; 409 `conversation_open` when that customer already has another open conversation on the line). `POST /calls` with a `conversation_id` rings `to`, any number of the customer of the agent's own live text conversation (a chat visitor's phone, or another number than the thread's), and the call joins that conversation: it ends without closing it, and 409 `conversation_not_joinable` refuses any other conversation. A conversation number is ten digits, counted per tenant.

## Text

- **The sender.** A start with no `from` takes the tenant's `default_from` setting for its channel (`call`, `sms` or `whatsapp`). A call with no `from` takes the agent's caller id first, then that setting. The sender must be registered with `PUT /numbers/{number}`. An email sender is an address registered the same way, with no Plivo check. Any other sender answers 422 `from_not_allowed`. An agent email's `from` and `to` must be addresses with a domain, bare or as `Name <local@domain>`; HS keeps the bare address. An SMS or WhatsApp number may carry spaces or leave out its plus; HS keeps it as E.164. An inbound email's addresses are taken as the gateway sends them.
- **SMS size.** An SMS's text and its files' `size` come to at most 5,000,000 bytes, else 422 `message_too_large`. An SMS or MMS to any E.164 number is sent, and Plivo's refusal of a destination or media it cannot deliver answers 422 `message_refused`.
- **WhatsApp.** An agent message may carry `interactive` (reply buttons, or a list behind a `button` with its `sections`) or a `location`. Reply buttons take a text `header` or a file's `{"url"}`; a list takes a text one. An interactive missing its buttons, button or sections answers 422. HS gives each button and row an id. A message may also name the message it answers in `reply_to`. A customer's tap, list pick or location reads as `whatsapp` on the message and its event, and a customer's quote of a message reads as `reply_to`. A WhatsApp of any other content type answers Plivo 422 and opens nothing.
- **WhatsApp templates.** `GET /whatsapp/templates` lists the templates of the WhatsApp accounts in the `whatsapp.accounts` setting, ordered by name and language, each with its account, status, components and `namespace`. `sender` keeps those of that number's account (404 `sender_not_found` when no account has it), and `status` those in that status.

  An account Plivo fails to read is left out of the list; the list fails with Plivo's reason only when no account reads. A WhatsApp start carries a `template`, else 422. With accounts listed, a WhatsApp start goes only from one of their senders, else 422 `from_not_allowed`.

  A message's `template` names an approved template of its sender's account by `name` and `language`, and fills it: one parameter of the header's format (`text`, `media` or `location`), and one per placeholder of a text header or the body, each with its value. Its `namespace` is the one the list names, `{account id}-{name}-{language}`. Any other answers 422 `invalid_template`, and HS sends it to Plivo as given.

  With no accounts listed, the list is empty and HS checks neither. See [WhatsApp accounts](configuration.md#name-the-whatsapp-accounts).
- **Wrap-up.** An agent in wrap-up may still send files, but a message with text answers 409 `in_wrap_up`.
- **Another channel or address.** A message's `channel`, `to` and `from` may move it off the conversation's own line: to SMS, WhatsApp or email, or to another address of the customer. A message on a call must name one, else 422 `channel_not_allowed`.

  `to` defaults to the conversation's customer and `from` to its address when the channel takes the same kind of address (phone numbers for calls, SMS and WhatsApp), else `to` is required (422 `to_required`) and `from` defaults to `default_from`. A switch's `from` must be registered with `PUT /numbers/{number}`, the conversation's own address included, else 422 `from_not_allowed`.

  A conversation's family is the call or text conversation its switches started from and every conversation linked from it. The message goes in the family's live conversation on that line that the agent holds, else in a new one the agent holds, linked to this one: it takes a slot on the channel (409 `agent_at_capacity`), and 409 `conversation_open` while another conversation is open on that line.

  Replies that switch in one family take turns, so two never open two conversations on a line. A new WhatsApp conversation needs a `template`, else 422 `template_required`. On each channel, this conversation and the family's live conversations that the agent holds write from one sender, whatever the agent's role: another `from` answers 422 `sender_mismatch`, with that sender in `details.from`.

  In wrap-up an agent may send files only to the customer's own address, and they stay in this conversation; any other switch answers 409 `in_wrap_up`.
- **Delivery.** A message moves through `pending`, `queued`, `sent`, `delivered`, `read` and, on WhatsApp, `deleted`, or ends `undelivered` or `failed` with Plivo's `error_code`. A gateway's `POST /messages/{id}/status` may add `error_title`, `error_details` and `reported_at`, and `message.status_changed` carries them.

  A Plivo-delivered message is told as `message.sent` only once Plivo takes it, so a refused one never reads sent; an answer with no message id counts as a refusal. The message and `message.status_changed` carry Plivo's id as `provider_id`. The event's `reported_at` is Plivo's time for the status, as an ISO time or epoch seconds, milliseconds, microseconds or nanoseconds.

  When Plivo gives none, or one more than 5 minutes outside the message's life, it is the time HS heard it. HS waits up to 30 seconds for Plivo to accept a message.
- **Files.** A message's file is its `url` with an optional `name`, `content_type`, `size` in bytes and `id`, which the message and its events show back. A customer's WhatsApp file shows Plivo's `content_type`.
- **Email.** An agent email takes `to`, `cc`, `bcc` and `reply_to` address lists beside the customer, and an inbound email keeps them. A reply joins a thread by its headers only on the address the thread is on.
- **No disposition.** A text or task that closes as `session_timeout`, `agent_closed`, `failed`, `resolved` or `cancelled` with no disposition takes the Auto-closed system disposition, through those rows of the status map.
- **From a call.** An agent's first SMS or WhatsApp from a call opens the caller's conversation on that channel, held by that agent, and the message the route returns names that conversation. It takes one of the agent's slots on the channel, and without a free one the message answers 409 `agent_at_capacity` and nothing is sent. The agent's later messages from the call and the caller's answers join it, whether or not the call has ended.

  Once it closes, the caller's next text routes as a new contact, and the agent's next message from the call opens a new conversation. Until the call closes, its wrap-up included, its session timeout extends once, by the same length. Files from a call the agent is wrapping up stay on the call and open no conversation. Each SMS in a conversation a call opened passes the size check of a first SMS.

  While another conversation is open on that channel and number, one the caller started included, a message from the call answers 409 `conversation_open`.
- **Replies.** Message text is trimmed, and text that is only whitespace answers 422 `empty_message`. A tenant key's reply is the primary agent's, and answers 409 `not_live` when no agent holds the conversation. A gateway message may give `sent_at`, the time the customer sent it, and the message keeps that time. A gateway or Plivo message id is deduplicated per receiving address, also when two deliveries of it race.
- **Session timeout.** A text closes as `session_timeout`, with no wrap-up, its channel's session timeout after an agent took it: from the accept, a transfer's included, or from the first message of a text the agent started. No message moves it, except a chat customer's that ends a waiting mark, and neither does a second agent's accept. Unmarking a waiting text starts it over. A text waiting in a queue has none, after a transfer to it too.
- **Whose turn.** An open text reads `awaiting`: `agent` until an agent message reaches the customer, then `customer` until the customer writes again. A chat message reaches the customer when it is sent; an SMS, WhatsApp or email when it is reported `delivered` or `read`, and a report that comes after the customer wrote again still hands them the turn. A transfer target's accept gives the agent the turn. Past live, `awaiting` is null.
- **Waiting.** `PATCH /conversations/{id}` with `waiting: true` marks a live text the caller is on, an agent's own or a tenant key's (409 `not_text` on another channel, 409 `not_live` past live), and `waiting: false` unmarks it. A `waiting` that matches the read is a no-op for any caller that reads the conversation. The next message from the agent ends the mark, and so does the customer's next message, except on a chat marked more than 25 hours ago, which stays marked until an agent writes or unmarks it. A transfer target's accept ends it too.
- **Inbox.** Each conversation reads `our_address`, the tenant's number or address it is on; `last_message`, its newest message, with its WhatsApp `template` or `whatsapp` content; and `last_activity_at`, the latest of its entries, its agents' starts and ends and its own state changes. `GET /conversations` takes several `number`s, comma-separated, so `?number=a,b&channel=sms&limit=1` reads the number last used with any of the customer's.
- **Who is on it.** Each conversation reads `assignments`, its stints active or in wrap-up, oldest first. Each gives its `id`, its offer's `offer_id` and a participant's `participant_id`; a transfer target's `transfer_id`, which hold and switch take as its `participant_id`; the `agent_id`, or an external party's `number`; its `role` (`primary`, `secondary`, `monitor` or `external`), `via` and `mode` (a supervisor's `listen`, `whisper` or `barge`, a transfer's `warm` or `cold`); its `state`; and `started_at`, `ended_at` (the start of its wrap-up) and `wrap_up_until`. `GET /conversations?participant_agent_id=` keeps the conversations that agent is on now, wrap-up included, so a desk that reloads lists what it still holds and not what it handed on.
- **Attributes.** `GET /conversations?attribute=case_id:C-4521` keeps the conversations whose `attributes` hold that string value at that key, such as a case or CRM contact id a caller kept there with `PATCH`; repeated, it keeps those that hold every one. A filter without its `:` answers 422 `invalid_attribute`. A call's own id is `?call_uuid=`, which matches any of its legs.
- **A customer's history.** With `number`, `GET /conversations?sort=last_activity_at` lists the customer's conversations by last activity, newest first, and `active_after` and `active_before` bound it. `GET /conversations/counts` takes the list's filters and answers `total`, `open` and `closed`. These read one customer's history, so without `number` they answer 422 `number_required`.
- **Unread.** Each conversation reads `unread_count`, the messages, notes and voicemails no agent wrote since an agent last wrote in it, read the last page of its messages or sent `POST /conversations/{id}/viewed`. It is the same for every viewer, and 0 once the conversation closes. After a Redis loss it counts again from the last entry an agent wrote.

## Views

A view saves a `/conversations/search` filter and the columns to show. It is `private` (its owner), `shared` (the agents and teams it names) or `public` (every agent of the tenant, including ones created later). An agent makes only private views; only the owner edits or deletes a view. A tenant key reads every view, and the views it makes have no owner, so only keys edit them. A view the caller may not see answers 403.

Favorites and order belong to the calling agent. `GET /views` lists views by type (private, shared, public), new ones first, then the agent's order of that type; `PUT /views/order` with a `type` of `private`, `shared`, `public` or `favorite` replaces the order of that type only, and answers 422 for a view of another type.

`?favorite=true` lists favorites, set with `PUT` and `DELETE /views/{id}/favorite`; favoriting a favorite, or unfavoriting a view that is not one, answers 400.

Turning a view private clears it from every agent's favorites and order, the owner's too; changing its type clears it from every order; updating a shared view clears it from the favorites and order of every agent it does not name in `agents`, the owner and team members included; a shared view that names its owner in `agents` answers 422 (`owner_in_shares`). A deleted agent or team is left out of the `filter` a view returns.

`GET /views/{id}/conversations` runs the stored filter through `/conversations/search`, with the caller's search scope and its page shape, so a deleted agent's conversations still match; each hit carries the values of the view's columns that are filter fields, such as `disposition` or `talk_sec`, in its `fields`.

## Reports

Reports take a window of whole days, `from` through `to`, in the IANA zone `tz` (default `UTC`). A bucket starts at its local hour, day, week or month in `tz`, as an instant, so the hour a daylight saving change repeats is two buckets. A supervisor reads the whole tenant's numbers, as an admin or a `reports` key does; only `/reports/conversations/rows` keeps a supervisor to the conversations its view level sees.

- **Conversations.** `GET /reports/conversations` counts the conversations opened in the window, filtered by any of the repeated `channel`, `direction`, `state` and `csat` values, per `bucket` of their open time (`hour`, `day`, `week` from Sunday, or `month`) and per value of up to two `by` keys: `channel`, `direction`, `state`, `disposition` (the last wrap-up's, else the result's system disposition) or `handle_time` (`none`, `under_1m`, `1m_to_5m`, `over_5m`).

  Each row holds the counts and the averages: `answered` once an agent took the customer, then `failed` for a carrier failure and `unanswered` for any other close; `abandoned` for the results `enqueue_failed`, `after_hours` and `assignment_failed`; `unassigned` when no agent took it; `replied` when an agent wrote; `waiting` for a live text whose last message is the agent's; `short` under 5 minutes of handle time; `transfers` the agents who took a call or text over, and `callbacks` the callback tasks raised from it.

  Handle time is talk plus wrap, as search counts them, and none when no agent took the conversation. The average CSAT reads only numeric answers. A task is a conversation of channel `task` or `callback`, so leave those channels out to count customer contacts alone.
- **Rows.** `GET /reports/conversations/rows` lists the same conversations newest first, each with its agent (the last one who took it), result, resolution, disposition, numbers, holds (`hold_count` and `hold_sec`, the caller's own, a hold the call ended in included), who ended the call and the offers declined or missed. A filter's total is the conversation report's `total` for the same filter.
- **Agents.** `GET /reports/agent-activity` gives each agent's seconds in each status and in available and away statuses inside the window, a status begun before `from` counting from `from`. `GET /reports/available-agents` counts per bucket the agents who changed to an available status in it, and `GET /reports/agent-status-log` lists each status change, newest first, with how long it lasted inside the window.

  Each takes `team_id`, `agent_id` and `agent_name` (part of the name, in any case). They read the status history, which keeps every change for good, past `EVENT_RETENTION_DAYS`, with the status's name and availability as they were.

  A change made before the upgrade that added the history comes from the events retention left, and one of those stored before events said whether the status was available reads its status's availability as it was at the upgrade.

HS keeps each closed conversation's numbers on its row once a pass reaches it, about a minute after the close, and works them out until then, so a report never waits for the pass. This release counts each wrap-up to its own submit and works out the kept wrap and handle times again, 500 conversations a run; the other kept numbers stay, since the events some were counted from may be gone.

## Export

`POST /conversations/export` and `POST /conversations/export/content` take the `/conversations/search` body and stream a CSV file of what that search finds, in its order, up to the first 10,000 conversations. To export a saved view, send its `filter` fields. Dates and times are in the body's `time_zone`.

- **The summary list** has one row per conversation, under the search document's field names in this order: `conversation_number`, `channel`, `number` (the customer's address), `agents`, `agent_count`, `teams`, `our_address`, `created_at`, `direction`, `conversation_state`, `wrap_up_note`, `disposition`, `message_count`, `duration_sec`, `wrap_sec`, `talk_sec`, `answered`, `types`, `csat` and `ended_by`.

  `columns` names the fields to write, in its order, with `conversation_number` added last unless named. `agents` lists the agents by name in the order they took the conversation, and `teams` the teams search files it under, those of the agents who owned it as they were at the close, in name order, each joined by commas. `agent_count` is the number of agents in `agents`, so a name with a comma in it never changes the count.

  `created_at` is ISO 8601 with the `time_zone` offset. `direction` is `inbound` or `outbound`, `answered` `true` or `false`, and `conversation_state` the state search filters on, such as `Customer answered`. The durations are whole seconds, and `talk_sec` is the talk time. `types` lists the kinds of item the conversation holds (`message`, `notes`, `voicemail`, `recording`, `feedback`, `transfer`), and `ended_by` is `customer`, `agent` or `system`.

  An empty cell is a value the conversation does not have.
- **The content** has one row per item of each conversation under `conversation_number`, `channel`, `id`, `from`, `agent_name`, `to`, `meta`, `type`, `text`, `number` (the customer's address) and `at` (ISO 8601 with the `time_zone` offset).

  The `type` is `message` (from and to the customer's and our address; `meta` is the id of the message it answers), `note`, `voicemail` or `recording` (the `text` is its id for `GET /recordings/{id}`), `feedback`, `signal` (task activity), `joined` (an agent took the conversation; `meta` says how, such as `queue` or `transfer`), `transfer`, `disposition` (`meta` is the sub-disposition) or `wrap_up_note`.

  A transfer has one row, by the agent it came from: `meta` is the target's type (`agent`, `team`, `queue`, `number`, `menu`, `voicemail` or `redirect`), `text` names the target, and `to` is the agent who took an agent or team transfer, or the number dialed.

  A message or note with files has as its `text` a JSON list of `{"file_url", "description"}`, one per file: the link the message or note API returns, so a Plivo-hosted file is a signed media URL, and the item's text. A row by an agent the caller does not see is left out, unless it is a transfer or a note. `columns` does not apply.

  The 10,000-conversation cap is the content's only bound: it writes every item of each conversation, however many, and HS reads them 1,000 at a time, so its memory does not grow with the item count.
- **Grants.** The summary list needs the role's `download_list`, the content its `download_conversation`; without it the answer is 403 `download_not_allowed`. A tenant key needs the `calls` scope.

Each page of the search is read on a connection of its own, so a slow download holds none between pages.

## Auth

The codes used in `routes.py`:

| Code | Credential |
|---|---|
| `op` | The operator key (`HS_OPERATOR_KEY`). Tenants and their keys only. |
| `key:<scope>` | A tenant API key with that scope: `admin`, `sessions`, `calls`, `reports`, `events`, `recordings` or `messages`. Keys are shown once and stored as SHA-256 hashes. |
| `agent` | An agent token, for that agent's own resources. |
| `sup` | A supervisor token, for the agents the supervisor can access. An admin's token holds it too. |
| `admin` | An admin's agent token, for the tenant's settings that a `key:admin` key runs. |
| `stream` | A stream token. Accepted only on `GET /events/stream`, and the only credential allowed in `?access_token=`. |
| `sig` | A short-lived signed URL (recording audio and message media). |
| `plivo` | A valid Plivo signature for the tenant: V3 on voice callbacks, V2 on message callbacks. |
| `none` | No credential. |

A route lists several codes when any one of them is accepted. An agent token reads the conversation of its own outbound dial while its leg rings, and one it holds an open offer on, with `GET /conversations/{id}` and its messages only, whatever its role's view, until the offer ends or its ring timeout passes; the read shows no handoff `context`. A `team` view also reads, the same way, the conversations offered to its teammates, but not their dials. A `messages` key belongs to your chat, email or message gateway. It posts customer messages and delivery reports, and it cannot read conversations.

An agent gets a session one of two ways. A company whose own app signs people in mints it from its server with `POST /agents/{id}/sessions` and hands it to the browser. Otherwise the agent signs in with `POST /sessions` and its email and password, which answers the same tokens. A session is an agent token and a stream token, valid an hour, and a Plivo access token. The browser then logs in the Plivo SDK, opens the stream and sends `POST /me/heartbeat` every 15 seconds. A desk that stays silent for 40 seconds goes Offline. Plivo mints that access token, and until the session ends or expires HS rings the agent at the SDK login (`sip:<username>_<auth id>@phone.plivo.com`), not at the endpoint, so a SIP softphone on the endpoint rings only while the agent has no such session.

Passwords: an agent's `email` is unique in its tenant, ignoring case (409 `agent_exists`). `POST /agents/{id}/password` with `{"password": ...}` sets the agent's password, and with no body makes one and answers it once in `password`. The agent changes it with `PUT /me/password` and its HS agent token, sending `current_password` (403 `invalid_credentials` when it is wrong). A reset or a revoke that lands while the change runs wins, and the change answers 401. HS keeps only a salted scrypt hash. A new password, like a new role, a revoke or a deleted agent, ends every session of the agent, the one that changed it included, so the desk signs in again.

Sign-in: `POST /sessions` takes `email`, `password` and `tenant_id`, and signs in to that tenant only. An install with one tenant, as most self-hosted ones are, may leave `tenant_id` out. On an install with more tenants, a sign-in without it answers 409 `tenant_required` before HS checks anything, so the answer says nothing about the email. Such an install gives each organization a sign-in link that carries its tenant id, and the sign-in page sends that id. A wrong email and a wrong password are one answer, 401 `invalid_credentials`, and take the same time. Each try counts against its email in that tenant (10) and its client (100) in a 15-minute window from the first try. The client is its IPv4 address, or the /64 network of its IPv6 address, since one host can hold a whole /64. Past either limit, the answer is 429 `too_many_attempts` with `Retry-After` until the window ends. A right password clears the count of that email in that tenant. The client address is the one uvicorn reports: behind a proxy, set `FORWARDED_ALLOW_IPS` to the proxy's address so it reads `X-Forwarded-For`.

Renewal: before the tokens expire, the desk sends `POST /me/sessions` with its agent token and gets the next session, with a new Plivo access token. The old tokens stay valid until their own expiry, so a call or a request in flight goes on. The desk reopens the stream with the new stream token. Renewals go on until `renewable_until`, 12 hours after the first session of the chain, sign-in or mint. Then the route answers 403 `sign_in_required`, and the agent signs in again. Revoking the agent's sessions ends the whole chain at once. `DELETE /me/sessions` signs the agent out as `DELETE /agents/{id}/sessions` does.

An admin who signed in runs every `key:admin` route with its agent token, so a console needs no tenant key. API keys, the Plivo credentials and tenants stay with the operator key, and minting a session for another agent, webhooks, deleting recordings and the message gateway stay with tenant keys: those belong to a company's server, and a key made from a sign-in would outlive the person and the 12 hours. An admin's token also lists the disabled and system dispositions, which a desk below admin does not see. Writes to the tenant's settings and agents take turns per tenant, and each checks the admin's token again on its turn, so a write that waited for its admin's demotion or deletion answers 401. A SIP password rotation whose admin is demoted before Plivo answers is 401 too, and shows no password.

The heartbeat rule: an agent goes Offline after 40 seconds without a heartbeat, and comes back Available on the second heartbeat after that. Going Offline resets the count, so a desk that beats only once a minute is reset before each second beat and never recovers. Browsers throttle timers in hidden tabs to about once a minute, so send the heartbeat from a Web Worker, or otherwise keep it under 40 seconds. The desk should watch `agent.status_changed` to show the agent when HS has marked them Offline.

Idle desk: when the desk sees no activity for `settings.idle.after_min` minutes, it calls `POST /me/idle`. HS applies `settings.idle.status_id` only when `settings.idle.enabled` is on, the agent's status is available and the agent has no conversation offered or active on any channel; otherwise the agent keeps their status and gets 200 with it. With tracking off the call is 409. The server applies these gates, so every desk behaves the same. `idle.status_id` must name an unavailable status, and tracking cannot be enabled without it.

## Call control errors

A control under `/calls/{id}` that the call's current state refuses answers 409 with a code that names the state:

| Code | Meaning |
|---|---|
| `call_not_live` | The call is not live, or it has ended. |
| `call_not_connected` | No agent is connected to the call. |
| `transfer_in_progress` | The call already has a transfer in progress. |
| `transfer_not_open` | Only a warm transfer whose target is on the call can complete. |
| `transfer_completing` | A complete waits on Plivo to hang up the agent's leg, so the transfer cannot be cancelled. |
| `agent_unavailable` | No agent of the transfer target is available with a free call slot. |
| `transfer_mode_not_allowed` | The tenant's `transfer.modes` setting does not allow this mode, on a call or a text. A warm transfer to a number needs `both`, and a cold one is always allowed. |
| `too_many_participants` | A call takes at most two extra participants. |
| `participant_not_allowed` | The call's agent took it by transfer, from an agent or through a queue, and such a call takes no participants. |
| `supervisor_present` | A supervisor is already on the call. |
| `supervisor_busy` | The supervisor is on a call of its own, barging in on another, or still joining another. |
| `omnichannel_not_supported` | The call also texts its caller, and such a call cannot be monitored. |
| `monitor_not_allowed`, `barge_not_allowed` | The caller's role lacks the `monitor` grant (any mode) or the `barge` grant (a barge, by start or switch). A tenant key holds both. |
| `pause_not_allowed` | The tenant does not allow pausing a recording. |

Feedback on a call no agent took (`POST /calls/{id}/feedback`) answers 422 `call_not_answered`.

A warm transfer ends in one of two ways, and each waits on Plivo. `POST /calls/{id}/transfers/{tid}/complete` answers only once Plivo has hung up the agent's leg: a hangup Plivo refuses answers 422 `plivo_refused` and changes nothing, so the transfer stays open and the same request can be sent again.

A `.../cancel` sent while a complete waits answers 409 `transfer_completing`, since the agent's leg is already going, and a cancel sent after it answers 422 `transfer_completed`. When Plivo does not answer a complete's hangup, the complete answers 503 `dependency_unavailable` and HS tries the hangup again 1, 5 and 30 seconds after successive failures; the target takes the call once a try succeeds.

Until then, or until the fourth try fails, a cancel answers 409 `transfer_completing`: about 36 seconds when Plivo answers each try with an error, and up to about 76 seconds when each try waits out the 10-second request timeout.

Only the call's own agent, or a tenant key, may transfer the call or add or remove a participant. Only the agent a transfer came from, or a tenant key, may cancel or complete it, also once that agent has left the call while the transfer rang. On a text conversation the same rule holds, and a warm target that joined may also cancel the transfer, which takes it off the conversation. Any other agent gets 403 `not_conversation_owner`, and adding the call's own agent as a participant answers 422 `already_on_call`. A participant's phone rings for 45 seconds, as a supervisor's does, whatever the tenant's `ring_timeout_sec`. A transfer answers only once Plivo has taken its ring, so a ring Plivo refuses is the request's error, and the transfer ends with it.

A control that Plivo refuses answers 422 `plivo_refused`, or 429 `carrier_rate_limited` with `Retry-After`. When Plivo does not answer, it answers 503 `dependency_unavailable` and the command keeps retrying, so a retry of the same request converges on the same state. Each code has one fixed message.

Every refusal's `details` carries Plivo's `status` and its reason as `errors`, Plivo's own error fields with any field named like a credential dropped. The credentials are those the request sent to Plivo: the auth ID, the auth token, the `Authorization` header's credential, and the value of every parameter whose name contains `key`, `token`, `secret`, `signature`, `password` or `auth` in any letter case.

A string, integer or float value is taken as its text, and an empty one is skipped. HS reads each credential, field name and message as written and after each URL-decoding, with `+` read as a space, until one changes nothing, at most three times, and ignores letter case. A field whose name contains a credential in any of these forms is dropped.

A message that contains one, or is still encoded after three decodes, is relayed as `[REDACTED]` in full. So is a message whose URL-decoded form holds anything on the list that follows. In any other message, a URL's user and password, an HS token, a Bearer or Basic credential, and the value of any name that contains `password`, `secret`, `signature`, `token`, `credential`, `api_key` or `api-key` in any letter case and with any of its characters percent-encoded, or that is `key` or `sig` with no letter, digit or underscore on either side, as in `?key=` or `X-Auth-Key`, read `[REDACTED]`. So a signed URL's `token`, `key`, `X-Amz-Credential` and `X-Amz-Security-Token` values read `[REDACTED]`, and `key_id`, `idempotency_key` and the `Idempotency-Key` header keep their values. `errors` is empty when the kept field names and messages contain a credential joined in order, the names alone or the messages alone. It is also empty when any credential has another type, is still encoded after three decodes, or is shorter than 8 characters after decoding, because HS cannot look for it reliably.

The service log gets exactly the `errors` the response carries, never Plivo's raw ones. HS does not catch a credential that Plivo splits into pieces in any other order, or writes in another encoding such as base64 or HTML entities. A 429 adds `retry_after`, the seconds its `Retry-After` header gives. Plivo's 401 alone is `plivo_credentials_rejected`, with the same `details`. Any other cause goes to the service log, never to the response.

A hold, a switch, a merge, a recording pause or resume, and a monitor switch each answer with their own Plivo commands' outcome. One that a later request of the same kind on the call replaced before HS sent it answers 204, and the later request answers for itself. A monitor request, a supervisor's mute and its leave also answer with Plivo's verdict, so a supervisor whose leave Plivo refuses stays on the call.

## Routing setup errors

- **Built-in statuses.** The seven statuses a tenant starts with (`default: true`) can be renamed, recolored and reordered, but not deleted (409 `status_default`) and not flipped between available and unavailable (409 `status_default`): the fallback for a deleted status, and the idle, sign-in and sign-out settings, rely on Available being available and Break unavailable.

Deleting a queue or a menu that a number, another queue or another menu names answers 409 `queue_in_use` or `menu_in_use`; point the reference elsewhere first. Only a reference counts: a greeting or a name that contains the id does not block the delete, and neither does an action of the row itself. Business hours that a number names answer 409 `hours_in_use`, and a holiday group that business hours select from answers 409 `holiday_group_in_use`. Each of these refusals lists what names the document in `details.used_by`, 50 at most: each entry's `kind` and `id`, and for a number its `number`.

Deleting a queue that conversations wait in, offered or not, or that an open task or callback is due to route to, scheduled or about to route, answers 409 `queue_in_use`. `details.waiting` counts the conversations waiting and `details.scheduled` the tasks and callbacks, each only when it is not zero. A disabled queue still offers the conversations it holds and takes no new ones, so disable the queue and delete it once agents take them or they close. Give a scheduled task or callback another target, or cancel it. One routed again after its queue was deleted, by a new schedule or `reroute`, closes as `enqueue_failed` when it comes due, as a disabled queue's refusal closes it.

Dispositions are two levels deep. A sub-disposition's parent is a top-level disposition other than itself, and a disposition with sub-dispositions cannot become one: either answers 422 `invalid_parent`. Deleting a disposition deletes its sub-dispositions too. They leave every list, read and wrap-up, while the conversations filed under them keep them in reports, search and exports, and their ids stay taken.

A deleted queue leaves every list and read and frees its name, and its id stays taken. A configuration write that names it answers 422 `foreign_id`, while a search, an export or a saved view may still filter on it. The conversations it took keep its name in exports.

A menu option takes a key from `0` to `9` or `*`. An option on `#` answers 422 `validation_failed`, because `#` ends Plivo's digit input. A menu saved with a `#` option before this rule still reads, and a PATCH that leaves `options` out keeps it.
