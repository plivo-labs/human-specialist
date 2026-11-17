# Quick start

You will run the whole stack on your machine, register an organization in the reference UI, and take a call in your browser.

The service needs a public HTTPS URL that Plivo can reach, because Plivo sends every call and message to it. On your own machine, a `cloudflared` quick tunnel gives you that URL for as long as it runs. To run the service for real, or hosted with no tunnel, put it on a server with its own domain or on one of the platforms in [Deploy](deploy.md).

## What you need

- Docker with Compose v2.
- A Plivo account with one phone number on it, and your Plivo account's Auth ID and Auth Token.
- `cloudflared`, for the quick tunnel.
- A browser and a microphone.

> [!IMPORTANT]
> Know these two things before you start:
>
> - **Open the UI on `localhost`.** A quick tunnel does not carry server-sent events, the stream the desk runs on. Browsers also allow the microphone only on `localhost` or HTTPS. To open the UI from another machine, run the service on a server with its own domain.
> - **Keys need 32 bytes or more.** `docker/start.sh` generates `HS_OPERATOR_KEY` and `HS_SECRET_KEY` when they are empty. A shorter key you set yourself stops the start.

## 1. Open a tunnel

```sh
cloudflared tunnel --url http://localhost:6005
```

`cloudflared` opens a quick tunnel and prints its URL, such as `https://<random-words>.trycloudflare.com`. Leave it running.

## 2. Start the stack

```sh
git clone https://github.com/plivo-labs/human-specialist && cd human-specialist
docker/start.sh https://<random-words>.trycloudflare.com
```

`docker/start.sh` checks Docker, writes `docker/.env` with fresh keys, builds the image from your checkout, migrates the database, and starts the service, Postgres, Redis and the UI. The first run takes a few minutes. It ends like this:

```text
  ✓ database migrated, service healthy
  ✓ the service reaches Plivo
  ✓ https://<random-words>.trycloudflare.com answers
  ! a quick tunnel cannot carry the desk's live events, so open the UI on this machine

  Open      http://localhost:6005
  Plivo     https://<random-words>.trycloudflare.com   (calls and messages come back here)
  API       http://localhost:6004/api/v1
  Operator  grep HS_OPERATOR_KEY docker/.env | cut -d= -f2-   paste it under Register

  Logs  docker compose -f docker/compose.yaml --profile ui logs -f hs
  Stop  docker compose -f docker/compose.yaml --profile ui down
```

If a step fails, the script stops and prints why.

`docker/start.sh` publishes ports 6004 and 6005. To use others, run `docker/start.sh` once to create `docker/.env`, set `PORT` and `UI_PORT` there, and run it again. A second copy on the same machine also needs its own `COMPOSE_PROJECT_NAME`, or it takes over the first copy's containers and database.

## 3. Register your organization

1. Open http://localhost:6005 on the same machine.
2. Choose **Register**.
3. Enter your Plivo account's Auth ID and Auth Token, your email, a password of 8 to 128 characters, and the operator key, which is the text after `HS_OPERATOR_KEY=` in `docker/.env` (`grep HS_OPERATOR_KEY docker/.env | cut -d= -f2-` prints only the key). Press **Create organization**.
4. If the Plivo account is already registered here, the form offers **Make a new organization key**, which signs you in with it.

human-specialist checks the credentials with Plivo and creates a Plivo application that points at your public URL. A Plivo sub-account's Auth ID and Auth Token work the same way, and keep one organization's numbers and usage apart from another's. The same press:

- adds you as an administrator who signs in with that email and password,
- creates a queue named General with you in it,
- adds the wrap-up outcomes Resolved, Follow-up needed and Not resolved, when the organization has none an agent can pick.

Your desk opens at **My Inbox**, with your status and **Call** in the header of every page. A panel in the corner shows the organization's API key once, and the email you sign in with: copy the key into your password manager. Its **Add a phone number** opens the next step. From now on, you sign in with your email and password.

## 4. Add your phone number

1. Press **Add a phone number**, or open **Connect**, **Phone Numbers**. Add a number on your Plivo account and route it to General.
2. Under **Settings**, **Outbound Configurations**, let agents call out and pick that number as the caller ID.

## 5. Take your first call

1. Allow the microphone when the browser asks.
2. If the status menu in the header does not read **Available**, pick it.
3. Call your Plivo number from any phone. The call rings on the desk. Press **Accept**.
4. Press **Hold**, then **Unhold**, then **End call**.
5. Choose a disposition and press **Submit**.

That is one whole conversation, from ring to wrap-up. To call out, press **Call** in the header, type the number, and press **Call**.

## 6. Bring in your team

1. Under **Settings**, **Users**, add each agent with their email.
2. In the agent's sheet, press **Set password**, or **Generate password** for one that shows once. Both stay off until the agent has an email.
3. Under **Settings**, **Queues**, add them to General, so its calls ring them too.
4. Hand each agent the sign-in link with their email and password. Their desk renews itself for 12 hours, then asks them to sign in again.

Agents on other machines need the UI on your own domain, as [Host the UI](deploy.md#host-the-ui) describes.

## Keep it running

- **New tunnel URL.** A quick tunnel gets a new URL each time it starts. Run `docker/start.sh https://<new URL>`, then under **Connect**, **Plivo Account**, enter the operator key and press **Re-apply Plivo URLs**.
- **Upgrade.** Pull the new commits and run `docker/start.sh` again. It rebuilds the image from your checkout.
- **No UI.** `docker/start.sh --no-ui https://<public URL>` starts the stack without the UI. The public URL then has to reach port 6004.

## Set up a first call with the API instead

This path uses the API, `curl` and a SIP softphone, with no UI. You also need `jq`. Start the stack with `docker/start.sh` as above, then:

```sh
set -a; . docker/.env; set +a
HS=http://localhost:6004; J='Content-Type: application/json'

# 1. Register the tenant with the operator key. The first tenant key is returned once.
REG=$(curl -sS -X POST $HS/api/v1/tenants -H "Authorization: Bearer $HS_OPERATOR_KEY" -H "$J" \
  -d '{"name":"Demo","plivo_auth_id":"MA...","plivo_auth_token":"..."}')
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

# 6. Route a number on that Plivo account to the queue.
curl -s -X PUT $HS/api/v1/numbers/+14155550100 -H "$A" -H "$J" \
  -d '{"open_action":{"type":"queue","queue_id":"'$QUEUE'"}}'

# 7. Call +14155550100 from any phone. The softphone rings.
#    To dial out, POST /api/v1/calls with {"agent_id": ..., "to": ...}.
```

`PUT /numbers/{number}` points the number at the tenant's Plivo application, so its calls and messages reach the service. Registering the same Plivo account twice answers 409 `tenant_exists`.

To watch the organization live, set `KEY` to its API key and read the [event stream](events.md#stream):

```sh
curl -N http://localhost:6004/api/v1/events/stream -H "Authorization: Bearer $KEY"
```

Call your number, and the events arrive as the call moves (trimmed here):

```text
retry: 3000
: ping

id: 42
data: {"id":"42","type":"offer.created",…,"direction":"inbound",…,"data":{"to":"+14155550100","via":"queue","from":"+14155550123","channel":"call",…}}
```
