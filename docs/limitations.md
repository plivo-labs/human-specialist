# What it does not do

These are out of scope. Where a part is yours to build, the API, the event stream and webhooks are the way in.

- A production UI, a dialer, agent scripts or CRM sync. [The reference UI](ui.md) takes calls, texts, tasks and callbacks on its desk, signs people in with email and password, and covers basic setup. A production agent desk, any power or campaign dialer, and writes to your CRM are yours to build.
- Emergency calling (911/E911). Plivo does not offer it, so a softphone on this stack cannot be your only line.
- Answering-machine detection. An outbound call connects the agent to whatever answers, a person or a voicemail.
- Parallel dialing. An agent has one call slot: a second `POST /calls` while the agent is on a call answers 409 `agent_on_call`. A queue can ring several agents for one contact (`agents_at_once`), but one agent never dials several numbers at once; a dialer that does so is yours to build.
- Agents on a plain phone number. An agent takes calls on the Plivo Browser SDK or a SIP endpoint.
- A chat widget or socket, an email provider, or a media store. Your gateway opens a chat or email conversation at `POST /api/v1/queues/{id}/enqueue`, sends its later customer messages to `POST /api/v1/messages`, and agent replies on those channels go out as `message.sent` events for your gateway to deliver.
- Managing WhatsApp templates. Create, edit and delete them at Plivo; HS lists them and checks a message against them.
- Bots or AI. The service never calls a bot and stores no bot transcript. A bot hands a call or a contact over with `POST /queues/{id}/enqueue` and its `context`.
- Exporting recordings in bulk. List them with `GET /recordings` and fetch each signed `audio_url`.
- Long-lived file links in the content export. A Plivo-hosted file's link is the signed media URL the message API returns, valid for 5 minutes; read the message again for a fresh one.
- Transfers recorded before HS kept a record of each one. The content export reads such a transfer from the stints it started: a transfer to a queue, a menu, voicemail or a URL, or one no one took, has no row, and a transfer to a team reads as one to the agent who took it.
- Number buying, porting, bulk import, reputation and do-not-call scrubbing. Do these at Plivo or in your own systems.
- Moving data in from another contact-center product.
- Per-tenant spend, concurrency or call-rate caps. Plivo limits apply to the whole Plivo account.
- Tested loads at the top of the API's bounds. A queue's capacity up to 10,000, retries up to 10 and a wait up to 86,400 s are what the API accepts, not loads the test suite runs, and Plivo may end a call before a long wait does.
- Rate limiting of the API. Put a rate limiter at your ingress.
- Keeping a call alive through a browser refresh.
- Call controls on a call placed for a callback or a task. Hold, transfer, recording and hangup through the API answer 409 `call_not_live` for such a call; the reference desk ends it through the softphone.
- SMS and WhatsApp on live Plivo. The text path is tested against a simulated Plivo, not yet with live messages.
- Calls in the seconds after a Redis loss. Until the sweep rebuilds a tenant's hot state, up to about 15 seconds, a call that arrives answers 503 and Plivo ends it unless the number has a fallback answer URL.
- Sign-in limits per client behind a proxy you have not named. The limit of failed sign-ins per client address counts the address the service sees. Behind a proxy or load balancer that is the proxy's, so every client shares one limit until you set `FORWARDED_ALLOW_IPS` to the proxy's address, as [API](api.md#auth) describes.
- Calls across a session renewal on real telephony. A call still in progress when the desk renews its session, and the next call that rings after it, are tested only against a simulated softphone, not yet with real telephony.
- Per-tenant data purge. You can delete a recording; events expire after `EVENT_RETENTION_DAYS`.
- Transcription in languages other than English.
- Deleting call detail records, which Plivo keeps.
