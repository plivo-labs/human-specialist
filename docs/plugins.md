# Plugins

A plugin adds your own behavior to the service without a fork: your single sign-on, an event sink, extra routes. It is a Python package registered under the entry-point group `hs.plugins`. At startup the service calls its `setup(hs)` once, and a plugin that raises stops startup. `hs` offers three hooks.

## The three hooks

- `hs.on_event(fn)` gets every committed event in `seq` order, at least once, with the webhook guarantees but no HTTP. `fn` takes the event envelope, a dict.
- `hs.add_resolver(fn)` turns a credential the service does not know into a `Principal`, for example from your SSO. `fn` takes the request and returns a `Principal` or `None`.
- `hs.app` is the FastAPI app: `hs.app.include_router(router)` adds routes.

A complete plugin, `examples/plugin/hs_toy/__init__.py`:

```python
import logging

from fastapi import APIRouter

log = logging.getLogger("hs_toy")
router = APIRouter()


@router.get("/toy/hello")
def hello() -> dict:
    return {"hello": "world"}


def on_event(envelope: dict) -> None:
    log.info("event %s %s", envelope["id"], envelope["type"])


def setup(hs) -> None:
    hs.app.include_router(router)
    hs.on_event(on_event)
```

Its `pyproject.toml` registers it:

```toml
[project.entry-points."hs.plugins"]
toy = "hs_toy:setup"
```

A resolver reads the credential from the request and maps it to a tenant and role:

```python
from hs.access import Principal

def sso(request):
    if request.headers.get("authorization") == "Bearer my-sso-token":
        return Principal(tenant_id="<tenant uuid>", kind="sso", agent_id="<agent id>", role="agent")
    return None  # not mine; the next resolver, then a 401

hs.add_resolver(sso)
```

A route that needs a credential declares it with `access.require`, which returns the caller's `Principal`:

```python
from fastapi import Depends
from hs import access

@router.get("/mine")
def mine(p: access.Principal = access.require("op", "agent")):
    return {"tenant": p.tenant_id}
```

A route that Plivo itself calls, under a tenant's callback path, verifies Plivo's signature with `Depends(hs.plivo.voice_signed)`, which answers with the tenant whose token signed the request.

## Behavior

A resolver's caller sees what an HS token of the principal's `role` sees, by that role's view level, in requests and on the event stream.

A hook is called one event at a time for the whole install, every tenant included, so it must return fast or hand its work off. The one overlap is a call still running when its replica loses the hook, described below. A resolver is asked again every 2 seconds for each open stream, so it must verify the credential locally or cache the answer. A hook's identity is its plugin, module and function name, and a renamed hook starts a new cursor at the present.

A failing hook is logged and retried. It never fails a request or delays the relay, and it holds no database connection while it runs. Each hook runs on one replica at a time, so a call that never returns holds that hook back on every replica. A call still running when its replica loses the hook, frozen or cut off from Postgres for over a minute, cannot be recalled, so it may end after another replica delivers later events. Each envelope's `id` is its `seq`, so a plugin that needs strict order drops any id at or below the highest it has applied.

### What a hook can rely on

- `on_event`: each committed event reaches the hook in `seq` order, at least once. A call that raises is retried in place, then parked and replayed on a backoff, while later events, webhooks and the stream carry on. The parked event arrives after the ones that followed it, and a replay whose claim lapsed is delivered once. Only a call still running as its replica loses the hook can overlap a later holder, so dedupe on the envelope `id`.
- `add_resolver`: asked only for a credential HS does not know, in order. A `Principal` accepts, `None` declines to the next resolver and then to a 401, and a resolver that raises or returns anything else rejects the caller. An accepted caller reads what an HS token of its `role` reads over HTTP, and the same scope on the stream.

  An open stream's caller is resolved again in turn, 16 callers at once on each replica, each with at most one resolution pending. A confirmation means the credential was valid when its resolution began: the resolver is the only authority on a plugin credential, so a revocation after that is caught by the stream's next resolution.

  A revoked credential ends its stream within one round, the replica's plugin streams times the resolver's latency divided by 16, plus one check pass of up to 4 seconds. At 145 streams a replica and 500 ms answers, under 200 concurrent calls on 2 replicas, revoked streams ended 2.6 to 9.6 seconds after the revocation. A resolver must answer well inside 2 seconds.

  A stream whose call stalls ends without `auth.expired` after about a minute, and its client resumes. Each stalled call holds one of the 16, so a resolver slow for every caller ends every stream on a plugin credential after about a minute.

  HS takes the `Principal` a resolver returns as the caller, its tenant, kind and role included, so mapping a credential to the right tenant and role is the resolver's job. HS checks an admin's own HS token again when a settings write takes its turn, after any demotion the write waited for, but it cannot check a resolver's caller again: a resolver whose roles change should refuse a demoted caller itself.
- `hs.app.include_router`: routes mount after HS's own, so a plugin route cannot shadow an HS path. A route is public unless it declares `access.require(...)`, which refuses a missing credential with 401.

Event retention waits for the cursor of every hook still running: those registered on the replica that runs the purge, and any other whose cursor moved or was held in the last 60 seconds, such as an old name a replica still runs during a rolling deploy. A hook you retire, by removing its plugin or renaming its function, keeps its row in `hs.hook_cursor`, and the row stops holding events back 60 seconds after the hook last ran. A name joins the plugin's entry point, the hook's module and its qualified name, so a hook defined inside `setup` has `setup.<locals>` in it.

## Put a plugin on the image

The runtime image holds the virtualenv at `/app/.venv` and no installer, so a plugin goes in through a derived image. Copy the installer in as root for one `RUN`, install the plugin into the image's virtualenv constrained to the packages the image already holds, so the plugin cannot move a dependency of the service, then go back to the image's user. A plugin that needs a different version of a package the service pins fails the build, which is the point. `examples/plugin/Dockerfile`:

```dockerfile
ARG HS_IMAGE=ghcr.io/plivo-labs/human-specialist:latest
FROM ${HS_IMAGE}
USER root
COPY . /plugin
RUN --mount=type=bind,from=ghcr.io/astral-sh/uv:0.12.15,source=/uv,target=/usr/local/bin/uv \
    uv pip freeze --python /app/.venv/bin/python > /tmp/frozen.txt \
    && uv pip install --python /app/.venv/bin/python --constraint /tmp/frozen.txt /plugin \
    && rm /tmp/frozen.txt
USER hs
```

The recipe pins the `uv` image it borrows the installer from. Move that pin to a current `uv` release whenever you move `HS_IMAGE` to a new version.

Build it from the plugin's directory, which holds the `pyproject.toml`:

```sh
docker build --build-arg HS_IMAGE=ghcr.io/plivo-labs/human-specialist:<version> -t my-hs .
```

Run `alembic upgrade head` from the derived image as the compose stack's `migrate` service does; the plugin's entry point loads on the service only. CI builds the toy plugin this way on every change and gets `200` from `/status` and from its route.

## What a plugin may rely on across versions

These names are stable inside a minor version. A change to one is listed in the [CHANGELOG](../CHANGELOG.md) under a breaking entry. Everything else in `hs.*` is internal and moves without notice.

- `Host.app`, `Host.on_event` and `Host.add_resolver`: the object `setup(hs)` receives.
- `hs.access.Principal` and `hs.access.require`.
- `hs.core.ApiError`: raise it for an error that renders as the service's error envelope.
- `hs.plivo.voice_signed`: the dependency that checks Plivo's signature on a callback.
- The event envelope: `id`, `type`, `created_at`, `tenant_id`, `conversation_id`, `agent_id`, `direction`, `context` and `data`, with the types in [Events and webhooks](events.md).
- The HTTP API under `/api/v1`.
