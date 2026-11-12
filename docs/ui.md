# The reference UI

`ui/` is an optional browser UI for human-specialist: a React app built with Vite, TypeScript and Tailwind CSS. It talks to HS only through the public API, the event stream and the Plivo Browser SDK, so your own UI can do everything it does in the same way. Run it as it is, copy parts of it, or leave it out. HS runs the same without it.

To host it, see [Deploy](deploy.md#host-the-ui).

## Screens

The app routes in the URL fragment, so any static host serves it.

A sidebar lists the screens the browser's credentials open: **My Inbox** with a session, the floor when that person is a supervisor or admin, and setup with an admin's session or the organization key. With a session, the header shows the agent's status menu and the dialer on every page, and a call or text offer rings on any page; a conversation that connects or reaches wrap-up opens in **My Inbox**. The header also holds the theme switch and the account menu. Setup screens are lists with a search box; a row opens its form in a side sheet.

| Route | Who uses it | What it does |
|---|---|---|
| `#/signin` | anyone | Signs a person in with email and password, taking the organization ID from a sign-in link (`#/signin?org=<id>`). Its **Organization key** tab signs in with an API key. On first run, **Register an organization** calls `POST /tenants` with the operator key and the Plivo account's Auth ID and Auth Token, then opens **Get started**. A Plivo account already registered answers 409 `tenant_exists` with its `tenant_id`, and the form then offers **Make a new organization key**: it mints a key with every scope through `POST /tenants/{id}/api-keys` using the operator key, shows it once and signs in with it. |
| `#/you` | an organization key holder | **Who are you?** Pick an admin or supervisor, or add yourself as an admin by name, email and password, so you sign in without the key too. It mints that user's session, and from then on the browser holds that session, not the key. **Not now** keeps the key alone; **Take calls** in the header comes back here. |
| `#/you`, right after registration | the new organization | **Get started** shows the organization key once and the sign-in link to hand to agents, and asks for your name, email, a password and a number on the Plivo account. It adds you as an admin who signs in with that email and password, a queue named General with you in it (default strategy), routes the number to General (`PUT /numbers/{number}`), turns on `agent_direct_dial` and sets `default_from.call` to the number (`PATCH /settings`), adds the dispositions Resolved, Follow-up needed and Not resolved when the organization has none an agent can pick, then opens your desk. A failure names its step. Each step looks up what exists first, so pressing **Get started** again adds no second agent or queue. **Skip** leaves the organization as it is. |
| `#/tasks` | an admin | Lists tasks, with **Refresh**. A task's sheet changes its name, priority and status; a resolved or cancelled task opens read-only. |
| `#/conversations` | an admin | Lists conversations, with **Refresh**. Each one shows its number (`#1000000000`), timeline with a text's messages, wrap-up, handoff context and recording playback; a recording whose signed URL expired fetches a fresh one when played. |
| `#/teams`, `#/skills` | an admin | Adds, renames and deletes teams and skills. A team name another team has asks before it saves. |
| `#/queues` | an admin | Adds queues, picks their members, disables or enables them, and deletes them. A queue's sheet names the numbers, menus and queues that route to it, and disabling it then asks again. A queue that conversations wait in, or that tasks are scheduled for, is not deleted: the error says how many wait, and to disable the queue and delete it once they are taken or closed, and how many are scheduled, and to give them another target or cancel them. A disabled queue is marked in the number pickers and offered only where already chosen. |
| `#/statuses` | an admin | Adds, edits and orders the statuses agents pick. Drag a row by its handle, or focus the handle and press the up or down arrow. Its **Configurations** tab sets the status on sign-in, on sign-out and when idle. |
| `#/contacts` | an admin | Adds the saved numbers agents transfer calls to (`/transfer-destinations`). A contact made with a URL through the API lists and edits its URL here. |
| `#/outbound` | an admin | Sets the default caller ID and senders, whether agents may dial out, and which calls record. |
| `#/hours` | an admin | Adds business hours with their open hours and time zone, and holiday lists. Business hours that use a holiday list close on the holidays checked in their sheet (`selected_holidays`); a list picked anew starts with all of them checked, and a holiday added to the list later starts unchecked. |
| `#/workload` | an admin | Sets how long an offer rings on each channel, how many conversations an agent holds on each, and whether wrap-up holds a slot. |
| `#/dispositions` | an admin | Sets text timeouts and the wrap-up timer. Its **Dispositions** tab adds, edits, orders and turns off the outcomes agents pick at wrap-up, with their sub-dispositions. |
| `#/agents` | an admin | Adds and lists users, with **Refresh**, under the organization's sign-in link. A name another user has asks before it saves. A user's sheet edits their name, role, email, teams and caller ID, deletes them, sets or generates their password (both stay off until the user has a saved email; an email another user has names that user; a password set for the signed-in person says it ends their session and then returns to sign-in with the reason), signs them out everywhere, and resets the SIP password. |
| `#/numbers` | an admin | Adds numbers and routes each one to a queue. A number's sheet also sets its business hours and what callers get when closed: hang up, a voicemail to the open queue, or another queue. Every save is a `PUT /numbers/{number}` of the number's current entry point with only what you changed, so a re-route keeps the hours. Adding a number already listed says where it goes now and asks before moving it. The sheet also deletes the number. |
| `#/plivo` | an admin | Re-applies the organization's Plivo URLs (`PATCH /tenants/{id}` with `{}`) after `PUBLIC_BASE_URL` changes. |
| `#/desk` | an agent session | **My Inbox**: incoming calls and texts, the softphone, mute, hold, recording, warm and cold transfer, hang-up, a text's thread and replies, wrap-up and each conversation's activity. `#/desk/{id}` opens one conversation the agent holds. |
| `#/floor` | a supervisor or admin session | Agents and their calls, waiting callers, and listen, coach and barge-in. |

Every action shows that it started, then that it worked or why it did not, with the API's `error.message` and code. A 422 also names each field in `details.fields` and its reason. Deleting and signing a user out everywhere ask for a second press in the sheet. When the organization key stops working (401), the UI drops it and returns to sign-in with the reason, unless a session is open. When the session stops working, the UI returns to sign-in with the reason.

## Sign-in and sessions

- The UI says organization for what the API calls a tenant: one company, with its own Plivo account and API key. The API, its routes and its settings keep the word tenant. Each organization sees only its own data, and inside it the user's role decides what shows.
- People sign in with their email and password on the first tab of `#/signin` (`POST /sessions`). An admin gives each user an email in their sheet, then sets a password there or generates one, which shows once. A server with one organization needs nothing more. On a server with more, each organization hands out its sign-in link, `#/signin?org=<organization ID>`, and the page sends that ID as `tenant_id`; without it, the page says to open that link. Too many failed tries say how long to wait. **Get started** and **Users** show the link.
- A person who signs in holds only their own session. An agent sees **My Inbox**, a supervisor also the floor, and an admin also Conversations, Tasks, Settings and Connect, which run on the admin's session with no organization key in the browser. Signing a user out everywhere (`DELETE /agents/{id}/sessions`) needs the organization key, so the sheet offers it only while the browser holds one; a new password also ends every session of the user.
- The organization key keeps its tab for the first setup and for integrators. It needs the `admin` and `sessions` scopes, as the key from registration has. After it signs in, **Who are you?** mints your own session with `POST /agents/{id}/sessions`, and the browser holds both: setup calls use the key, and desk calls use the session.
- Every desk renews its session with `POST /me/sessions`, however it signed in: six minutes before the session's hour ends, and again each minute if that fails. The API calls, the heartbeat and the event stream move to the new tokens at once; the stream resumes after the last event it saw (`?after=`). The softphone signs in with the new Plivo token once no call rings, dials or is live, so a renewal never cuts a call. Renewals go on for 12 hours from the sign-in. Then HS answers 403 `sign_in_required`: the desk stops renewing, warns five minutes before the session ends, and the person signs in again.
- A session also ends when an admin signs the user out everywhere, sets their password, changes their role or deletes them. The next request answers 401, and the UI reloads to sign-in with the reason, so no softphone of the ended session stays signed in.
- **Change password** in the account menu sends `PUT /me/password`. It ends every session of the person, this one too, so the UI returns to sign-in for the new password. **Sign out** sends `DELETE /me/sessions` and clears the browser's key and session.
- The operator key typed on **Plivo Account** or **Register an organization** is used for its request and kept nowhere. A user with no password cannot renew an older session or sign in, and the desk says to ask an administrator for one under Settings, Users. A company's own app can mint a session on its server and hand it to the browser in the URL fragment, `#session=<the session's JSON in base64>`, which the browser never sends to a server. The desk renews it as above.
- Keys and tokens live in `sessionStorage`. They end with the tab, and other tabs do not see them.

## How the desk works

- The softphone is Plivo's Browser SDK, `plivo-browser-sdk` from npm at one pinned version, loaded when the desk opens. It needs a secure context: HTTPS, or `localhost`.
- The desk asks for the microphone once when it opens, then stops the tracks. If the browser blocks the microphone, the desk says so and does not sign the phone in.
- While a desk session lives, HS rings the agent in this browser, not on a SIP phone.
- The desk sends `POST /me/heartbeat` every 15 seconds from a Web Worker, because a hidden tab slows the page's own timers to once a minute, past HS's 40-second limit.
- It reads events from `GET /events/stream` with `fetch`, the session's stream token in the `Authorization` header, so the token stays out of URLs and access logs. HS sends a frame at once and a ping at least every 15 seconds. After 35 seconds without a byte, the desk says that live updates stopped and reconnects after the last event it saw (`?after=`); once frames flow again, it rereads the call and **My Conversations**.
- It reads `GET /conversations/{id}` for where the agent stands on each conversation it holds. If that read fails, the desk says so and marks the conversation on screen as possibly out of date. On reload, it reads every conversation the agent holds with `GET /conversations?participant_agent_id=`, and the text offers still open from `pending_offers` in `GET /me`.
- **My Conversations** lists the offers ringing and the conversations the agent holds, wrap-up included, read with `GET /conversations?participant_agent_id=` when the desk opens, when live updates resume, and on **Refresh**. Past conversations are on the Conversations screen.
- Each call, text, task or callback the agent holds is a line in **My Conversations**, with its channel and state, and a text in the background shows how many new messages it has. The line on screen is marked, and picking another opens it at `#/desk/{id}`. A line that connects or reaches wrap-up takes the screen. The call card stays on top whichever line is open. The agent holds as many calls, texts and tasks at once as their capacities allow; HS decides what it offers.
- A text (SMS, WhatsApp, chat or email), task or callback offer shows in the same card as a call: the customer or the task's title, a text's latest message (`last_message` in `GET /conversations/{id}`), how long the customer has waited and the time left. **Accept** and **Decline** call `POST /offers/{id}/accept` and `POST /offers/{id}/decline`. Each offer has its own card, in a strip above the screen, so no offer covers the open conversation.
- A text's thread reads every page of `GET /conversations/{id}/messages`, and reads it again whenever the stream brings a `message.*` event for it, so customer messages and delivery statuses show live. The customer's messages sit on the left, the agent's on the right with their delivery status, and the conversation's events between them.
- The reply box sends `POST /conversations/{id}/messages`. Enter sends, and Shift+Enter adds a line. On WhatsApp the box takes a free reply only within 24 hours of the customer's last message; past that it says WhatsApp needs an approved template, which the desk does not send. An email reply goes under the customer's last subject, with `Re:` in front.
- **End conversation** calls `POST /conversations/{id}/close`, which opens the agent's wrap-up. A text's wrap-up is the same form as a call's.
- A task's or callback's line shows its description, priority and due date. **Resolve** sets the task's status to resolved (`PATCH /tasks/{id}`), which opens the same wrap-up. When the task names a customer, as a callback does, the line offers **Call**: `POST /calls` with the line's `conversation_id`, so the call joins the task's conversation and ends without closing it. HS refuses hangup, hold, transfer and recording controls on such a call, so its card shows mute and end, and end hangs up this browser's phone. Calling needs `agent_direct_dial`, as **Call** in the header does.
- HS names each ring in its SIP headers. The desk answers the agent's own dial-out and a supervisor's monitor leg at once, and shows the rest as offers to accept or reject.
- **Call** in the header dials out with `POST /calls` only while the organization's `agent_direct_dial` setting is on (`PATCH /settings`). While it is off, **Call** stays visible, and pressing it says that calling out is off and where an admin turns it on. A settings save renews the session at once, because the Plivo token carries `agent_direct_dial` from when it was minted. A dial shows as dialing, then ringing until the customer answers, then live. A dial that fails shows the reason.
- Transfer lists come from `GET /transfer-targets?channel=call`, in three tabs: agents who can take a call first, teams with how many agents are free, and saved contacts or a typed number. An agent who cannot take a call is dimmed and cannot be picked. A number transfer also needs `agent_direct_dial`.
- The desk offers **Pause rec** when `settings.recording` in `GET /me` says the call's direction records and `allow_pause` is on. An HS whose `/me` has no `settings.recording` gets no recording control. A pause made anywhere shows on the call.
- Wrap-up picks a disposition, then a sub-disposition when it has some, from lists.
- Beside the conversation, the desk shows the customer's details, the handoff `context` once the agent is on the conversation, and the customer's last five conversations the agent can see (`GET /conversations?number=&sort=last_activity_at`).

## Run it while you change it

`npm run dev` serves the app with hot reload and passes `/api`, `/plivo`, `/status` and `/openapi.json` to HS. Use the Node version in `ui/.nvmrc`.

```sh
cd ui
npm ci
HS_URL=http://127.0.0.1:6004 npm run dev
```

Before you open a pull request, run `npx tsc --noEmit`, `npm test` and `npm run build`, as CI does. Every API call in `ui/src` must use a literal `/api/v1/...` path, so that `tests/contract/test_ui_routes.py` can check it against the route table.

## Build your own UI

The app is a reference, not a framework. The screens live in `ui/src`, grouped by area (`setup/` and `desk/`), and each call they make is a literal path, so you can read off the calls your own UI needs. Start with `ui/src/desk/desk.tsx` for the session, the stream and the heartbeat, and `ui/src/desk/state.ts` for how events and conversation reads become what the agent sees.

## Settings

| Setting | Default | What it sets |
|---|---|---|
| `window.HS_URL` in `config.js` | `""` | The HS base URL. Empty means HS on the UI's own origin. `config.js` sits beside `index.html` in the build, so you change it without a rebuild. |
| `CORS_ALLOWED_ORIGINS` (HS) | empty | Must list the UI's origin when `config.js` names another origin. |
| `UI_PORT` (compose) | `6005` | The host port of the `ui` service. |
| `HS_UPSTREAM` (compose) | `hs:6004` | The HS address the `ui` service proxies to. |
| `HS_URL` (`npm run dev`) | `http://127.0.0.1:6004` | The HS address the dev server proxies to. |

## Limits

- Reports and search have no screen yet. On the desk, an agent cannot start a text, send a WhatsApp template, a file or an email's cc, transfer a text or a task, or mark a text waiting. The API serves all of these.
- A queue's strategy shows read-only, and its teams, skills and overflow have no screen. The API serves all of these.
- Search boxes filter the rows already loaded, ignoring spaces, `+`, `#`, dashes and parentheses. Load more rows to search further; `/conversations/search` searches closed conversations through the API.
- The desk transfers to agents, teams, saved contacts and numbers, not to a queue, because an agent token cannot list queues.
- The floor shows the waiting callers of each queue by name beside the total.
- While a call rings, the call card shows what the offer event carries (the caller and the number they called, or the conversation's own address when the event has none) and, for an inbound call, how long the caller has waited, read with `GET /conversations/{id}`. An agent cannot read a conversation's handoff `context` until it accepts, so the card shows none while it rings. Each offer has its own card, and My Conversations lists them all.
- Saved views, exports, extra conversation filters and the call session timeout have no screen: the UI is a reference kept to 5,000 lines of `ui/src` app code, tests excluded, not because HS lacks them. Your own UI uses `/views` (and `/views/{id}/conversations`), `POST /conversations/export` and `/conversations/export/content`, the `/conversations/search` filters, and `call_session_timeout_sec` in `/settings`.
- IVR menus, skill levels per agent and per queue, and the organization's settings beyond the screens above have no screen. Set them through the API.
- The header names the organization only when this browser registered it or re-applied its Plivo URLs, because an organization key cannot read the organization's name.
- Lists page through what the API returned so far. Where the API pages by cursor, the count reads `1-20 of 20+` until the last page loads.
