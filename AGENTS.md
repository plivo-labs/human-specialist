# A guide for coding assistants

This file is for coding assistants that work in this repository or answer questions about it. People can start with [README.md](README.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

In this product, an agent is a person who takes calls, texts and tasks at a desk. The code, the API and the docs use the word only in that sense.

## What this is

human-specialist is a self-hosted contact-center service for human agents on Plivo. It queues calls, SMS, WhatsApp, email, chat, callbacks and tasks, offers each one to agents, and runs the live conversation through wrap-up. It never calls a bot. A bot hands a contact over with `POST /api/v1/queues/{id}/enqueue`.

## Layout

```
src/hs/          the service, one module or package per domain
alembic/         database migrations, schema hs
tests/           unit, contract and acceptance tests
tests/harness/   the black-box World and the Plivo simulator
ui/              the optional reference UI, a React app
deploy/          files for hosted platforms
docker/          the Docker Compose stacks: self-hosted and test
docs/            the guides and reference pages
```

[Architecture](docs/architecture.md#modules) lists what each module owns and the one-way order of imports.

## Commands

```sh
uv sync                                            # Python 3.13 and the dev tools
docker compose -f docker/compose.test.yaml up -d   # Postgres and OpenSearch for the tests
make check                                         # the definition of green: lint, test, acceptance
make lint                                          # ruff and format; make fmt fixes them
make test                                          # unit, contract, simulator and migration tests
make acceptance                                    # a real service against the Plivo simulator
make snapshot                                      # rewrite the OpenAPI snapshot after an API change
```

The tests start their own Redis, so `redis-server` and `redis-cli` from Redis 7 or later must be on your `PATH`. For the UI, run `npx tsc --noEmit`, `npm test` and `npm run build` in `ui/`.

## Rules that matter most

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you change code. The sections people trip on:

- [Definition of done](CONTRIBUTING.md#definition-of-done): `make check` green, every public route in `tests/contract/routes.py`, and the snapshot updated.
- [Code rules](CONTRIBUTING.md#code-rules): the tenant comes only from the credential, every SQL value is bound, state changes emit events, and no new dependencies.
- [Tests](CONTRIBUTING.md#tests): `tests/acceptance/` is the spec. Changing one of its tests changes promised behavior.
- [Comments](CONTRIBUTING.md#comments) and [Commit messages](CONTRIBUTING.md#commit-messages).

## Where to look

| Question | Read |
|---|---|
| How do I run it on my machine? | [docs/quickstart.md](docs/quickstart.md) |
| How do I deploy it? | [docs/deploy.md](docs/deploy.md) |
| Which settings exist? | [docs/configuration.md](docs/configuration.md) |
| Which routes exist, and what auth do they take? | `/openapi.json` on a running service, its snapshot [tests/contract/openapi.json](tests/contract/openapi.json), [docs/api.md](docs/api.md) and [tests/contract/routes.py](tests/contract/routes.py) |
| Which events exist, and how do webhooks work? | [docs/events.md](docs/events.md) and [src/hs/events/catalog.py](src/hs/events/catalog.py) |
| How do I write a plugin? | [docs/plugins.md](docs/plugins.md) and [examples/plugin](examples/plugin) |
| How does the reference UI work, or how do I build my own? | [docs/ui.md](docs/ui.md) |
| How do the modules fit, and what does a word mean? | [docs/architecture.md](docs/architecture.md) and its [glossary](docs/architecture.md#glossary) |
| What is out of scope? | [docs/limitations.md](docs/limitations.md) |
| How much load does it hold? | [Capacity](docs/configuration.md#capacity) |
| What changed in a release? | [CHANGELOG.md](CHANGELOG.md) |
