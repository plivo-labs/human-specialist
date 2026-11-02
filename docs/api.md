# API

Everything is under `/api/v1` and described by `/openapi.json`; there is no `/docs` or `/redoc` page, so load the document into any OpenAPI viewer or client generator. Lists take `?limit=&cursor=` and return `{"data": [...], "next_cursor": ...}`. Errors are `{"error": {"code", "message", "details"}, "request_id"}`. A request Redis or Postgres does not answer is a 503 `dependency_unavailable`, not a 500 (outside call controls a Redis read can still answer like a missing record; see [limitations](limitations.md)); retry it. [`tests/contract/routes.py`](../tests/contract/routes.py) is the authoritative list of every method and path with its auth.

| Group | Paths |
|---|---|
| Tenants (operator) | `/tenants`, `/tenants/{id}`, `/tenants/{id}/api-keys`, `/tenants/{id}/api-keys/{key_id}` |
| Agents | `/agents`, `/agents/{id}`, `/agents/{id}/sessions`, `/agents/{id}/status`, `/agents/{id}/sip-credentials` |
| People and settings | `/teams`, `/teams/{id}`, `/skills`, `/skills/{id}`, `/roles`, `/agent-statuses`, `/agent-statuses/{id}`, `/settings`, `/dispositions`, `/dispositions/{id}`, `/dispositions/reorder`, `/status-map` |
| Routing setup | `/queues`, `/queues/{id}`, `/queues/{id}/enqueue`, `/queues/{id}/stats`, `/business-hours`, `/business-hours/{id}`, `/business-hours/{id}/status`, `/menus`, `/menus/{id}`, `/numbers`, `/numbers/{number}` |
| Agent desk | `/me`, `/me/status`, `/me/heartbeat`, `/transfer-targets` |
| Conversations | `/conversations`, `/conversations/{id}`, `/conversations/{id}/notes`, `/conversations/{id}/wrap-up`, `/conversations/{id}/wrap-up/extend`, `/conversations/{id}/timeline` |
| Calls | `/calls`, `/calls/{id}/hold`, `/calls/{id}/recording`, `/calls/{id}/transfers`, `/calls/{id}/transfers/{tid}/complete`, `/calls/{id}/transfers/{tid}/cancel`, `/calls/{id}/participants`, `/calls/{id}/participants/{participant_id}`, `/calls/{id}/hangup`, `/calls/{id}/monitor` |
| Recordings | `/recordings`, `/recordings/{id}`, `/recordings/{id}/audio`, `/recordings/{id}/transcript` |
| Reports | `/floor`, `/reports/calls`, `/reports/dispositions`, `/reports/agent-activity` |
| Events | `/webhooks`, `/webhooks/{id}`, `/webhooks/{id}/rotate-secret`, `/webhooks/{id}/test`, `/events`, `/events/stream` |
| Plivo callbacks | `/plivo/{tenant_id}/answer`, `hangup`, `mpc-status`, `call-status`, `recording`, `transcription`, `actions/menu`, `actions/voicemail`, `actions/queue-exit`, `actions/move` |
| Public | `GET /status` (database and Redis reachable), `GET /openapi.json` |

## Auth

The codes used in `routes.py`:

| Code | Credential |
|---|---|
| `op` | The operator key (`HS_OPERATOR_KEY`). Tenants and their keys only. |
| `key:<scope>` | A tenant API key with that scope: `admin`, `sessions`, `calls`, `reports`, `events` or `recordings`. Keys are shown once and stored as SHA-256 hashes. |
| `agent` | An agent token, for that agent's own resources. |
| `sup` | A supervisor token, for the agents the supervisor can access. |
| `stream` | A stream token. Accepted only on `GET /events/stream`, and the only credential allowed in `?access_token=`. |
| `sig` | A short-lived signed URL (recording audio). |
| `plivo` | A valid Plivo V3 signature for the tenant. |
| `none` | No credential. |

A route lists several codes when any one of them is accepted. A server mints agent and stream tokens with `POST /agents/{id}/sessions` and hands them to the browser together with a Plivo access token; the browser then logs in the Plivo SDK, opens the stream and sends `POST /me/heartbeat` every 15 seconds. Plivo mints that access token. While a session lives, calls ring the agent's Browser SDK login, not a SIP device; an agent on a SIP device holds no session.

The heartbeat rule: an agent goes Offline after 40 seconds without a heartbeat, and comes back Available on the second heartbeat after that. Going Offline resets the count, and the offline check runs every 15 seconds, so a desk that beats only once a minute is reset before each second beat and never recovers. Browsers throttle timers in hidden tabs to about once a minute, so send the heartbeat from a Web Worker, or otherwise keep it under 40 seconds. The desk should watch `agent.status_changed` to show the agent when HS has marked them Offline.

## Routing setup rules

A menu option key is `0` to `9` or `*`; `#` answers 422 `validation_failed`, because it ends Plivo's digit input. Deleting a queue or a menu that a number, another queue or another menu names answers 409 `queue_in_use` or `menu_in_use`; point the reference elsewhere first. A menu option that names its own menu is allowed and does not block that menu's delete. Only a reference counts: a greeting or name that merely contains an id does not block the delete. A menu saved with a `#` option before this rule still reads, and an unrelated PATCH keeps the option; a PATCH that sends `options` without `#` removes it. At call time `#` still ends the caller's input.

## Call control errors

A control under `/calls/{id}` that the call's current state refuses answers 409 with the code of its kind: `call_control_failed` (hold, hangup), `recording_control_failed`, `transfer_failed`, `participant_failed`, `too_many_participants`, `monitor_failed` or `supervisor_present`. When Plivo or a data store does not answer, it answers 503 `dependency_unavailable`. A transfer or an added participant may still have reached Plivo, so read the call's events before retrying one. Each code has one fixed message; the cause goes to the service log, never to the response.

`POST /calls/{id}/hangup` hangs up the agent's own leg, as when the agent hangs up their phone; a tenant key hangs up the leg of the agent who holds the call. What follows is the agent leaving: a transfer target who already answered keeps the caller, otherwise the call ends for everyone on it. On a call more than two hours old the leave can no longer end the call for others, so a participant or a supervisor stays on with the caller. It answers 204 once the leg is hung up, 409 `call_not_connected` when the agent has no live leg on the call, and 503 `dependency_unavailable` when Plivo or a data store does not answer.

A call runs one transfer at a time, so `POST /calls/{id}/transfers` while another transfer is still in progress answers 409 `transfer_failed`. A transfer stays in progress while its target rings or while anyone but the caller, the call's agent and a supervisor is on the call; one with neither, more than a ring timeout after it started (a lost callback or a restart left it), gives way to the new request, and two requests that arrive together then can both dial (see [limitations](limitations.md)). Completing a warm transfer to a number answers 409 `transfer_failed` until that number has joined, and hands the caller to that number only. On an outbound call, a transfer to a queue or to voicemail answers 409 `transfer_failed`. A transfer never rings the call's own agent: a transfer to that agent answers 409 `transfer_failed`, and a team transfer offers only the team's other agents.

A transfer emits `call.transferred` once, with the `mode` its request named (`warm` or `cold`) and `status` `completed` when the target takes the call, or `failed` when the target's leg never connects (declined, unanswered or busy). A warm transfer to a busy number whose hangup reaches HS before Plivo's dial result emits none; the caller stays on hold as described below. A cancelled transfer, a move to a queue or to voicemail, and a warm consult the target leaves before it is completed emit no `call.transferred`.

A transfer to an agent, a team or a number holds the caller while its target rings. When it ends without a hand-over (cancelled, unanswered or declined, or the target hangs up during a warm consult), the caller stays on hold until the agent sends `POST /calls/{id}/hold` with `{"on": false}`. Adding a participant holds nobody.

