# Contributing to human-specialist

## Setup

You need [uv](https://docs.astral.sh/uv/), Docker, and `redis-server` and `redis-cli` from Redis 7 or later on your `PATH` (`brew install redis`, or `apt-get install redis-server tzdata-legacy` on Ubuntu 24.04 or later): each acceptance test starts its own Redis.

```sh
uv sync                                            # .venv with Python 3.13 and the dev tools
docker compose -f docker/compose.test.yaml up -d   # Postgres and OpenSearch for the tests
uvx pre-commit install                             # lint, format, secret and commit-message hooks
```

Tests use the test stack's ports by default; point `HS_TEST_DATABASE_URL` and `HS_TEST_SEARCH_URL` elsewhere to use your own Postgres and OpenSearch. If 56432 or 59200 is taken, start the stack on other host ports with `HS_TEST_PG_PORT` and `HS_TEST_SEARCH_PORT`, and point those URLs at them. Without OpenSearch the search tests skip their OpenSearch variant. `HS_REQUIRE_STACK=1` (set in CI) makes that skip a failure. Databases are built from the Alembic migrations only.

## Stack and layout

Python 3.13 with uv, ruff and pytest; FastAPI, SQLAlchemy 1.4, Alembic, Pydantic 2. US English, no em dashes. human-specialist is open source and self-hosted: design for outside users, not for any one company's infrastructure.

```
src/hs/          the service: one module or package per domain
alembic/         database migrations, schema hs
tests/           unit, contract and acceptance tests
tests/harness/   the black-box World and the Plivo simulator
tests/migration/ the shared-database migration tests
tests/fuzz/      the seeded random walk
docker/          the image, the self-hosted stack and the test stack
ui/              the optional reference UI: a React app (Vite, TypeScript, Tailwind)
docs/            architecture and reference pages
```

Each domain in `src/hs/` owns its routes, logic and storage. [docs/architecture.md](docs/architecture.md#modules) lists what each module owns and the one-way order of imports, and its glossary explains the names the code uses (assignment, entry, round, room, ring).

## Checks

```sh
make check        # the definition of green: ruff, format, unit, contract, simulator, migration and acceptance tests
make test         # unit, contract, simulator and migration tests
make fuzz         # a seeded random walk over a live service, outside make check (HS_FUZZ_SEED=8,1 walks those seeds)
make snapshot     # rewrite the OpenAPI snapshot after an API change
make acceptance   # black-box tests: a real service process against the Plivo simulator
make fmt          # fix lint and format
make run          # the service with reload on port 6004
```

The Plivo simulator (`tests/harness/sim.py`) stands in for Plivo in the acceptance suite. It signs callbacks with Plivo's V3 algorithm, answers REST calls, and replays the callbacks a real call produces, so a full call runs with no carrier.

## Definition of done
1. `make check` exits 0.
2. Any acceptance test this change makes pass has its `xfail` marker removed. Acceptance tests are the spec: changing one changes promised behavior, so call it out in the pull request.
3. New non-trivial logic has the smallest test that would fail if it broke.
4. Every public route is listed in `tests/contract/routes.py` with its auth, and the OpenAPI snapshot is updated (`make snapshot`) and the diff reviewed.

## Code rules
- **Write less.** Before adding code: does it need to exist, does the codebase already have it, does the stdlib or an existing dependency do it? No speculative abstractions, no interface with one implementation, no config for a value that never changes.
- **No new dependencies.** Dependencies live in `pyproject.toml` and `uv.lock`; adding one needs a maintainer.
- **FastAPI-native.**
  - Auth is a dependency (`access.require(...)`), declared on the router or route so it runs before body parsing.
  - Bodies and responses are pydantic v2 models.
  - Errors raise `ApiError(status, code, message, details)` and render as `{"error": {...}, "request_id": ...}`.
  - Lists use `?limit=&cursor=` and return `{"data": [...], "next_cursor": ...}`.
- **Tenancy.** The tenant comes only from the credential. Every id from a path or body is resolved inside that tenant (404 for another tenant's id, 422 for a foreign id in a body). Never trust a tenant or org id sent by the caller.
- **State changes emit events** through `hs.events.emit` (catalog in `src/hs/events/catalog.py`). Timers and async work are `hs.jobs.schedule(...)` rows. There are no other outbound side channels.
- **SQL.**
  - Bind every value. SQL text may interpolate only module-level constants (gate: `tests/contract/test_sql_interpolation.py`).
  - Schema changes are Alembic revisions only, additive inside v1.
- **Secrets.**
  - Plivo tokens are encrypted at rest and never returned.
  - API keys are stored as SHA-256 hashes.
  - Raw recording URLs are never returned; return a signed `audio_url`.
  - Logs never contain tokens, keys or signatures.
- **Errors.**
  - Never swallow an exception silently: handle it, or let it propagate.
  - `except Exception` must log or re-raise. Use it only at a boundary: a loop (job runner, relay, SSE), a Plivo or search-index call whose failure the caller handles, or best-effort work (a metric, a cache write) that must not fail the request. Elsewhere catch the specific exception. Some older modules still catch broadly or silently; don't copy them, and narrow what you touch.
- **No print, no TODO/FIXME, no commented-out code.**

## Comments
- Default to no comment. Write one only for a *why* the code can't show (constraint, gotcha, contract), in one or two lines.
- Never cite tickets, PRs, reviews, customers or people.
- No narration ("Added X", "Now we…"), no restating the code. Delete comments your change made stale.

## Tests
- `tests/acceptance/` is the spec: black-box only, through `tests/harness` (real HS process, Plivo simulator, public HTTP API, events).
- `tests/unit/` and `tests/contract/` are for logic and API-shape checks.
- Tests are deterministic: no `sleep` over 0.2 s; wait on conditions with `world.wait(...)`.
- A test that needs a DB uses the migrated test database (Alembic only, never `create_all`).

## Branch and PR conventions

- Branch off `main`: `feat/<slug>`, `fix/<slug>`, `chore/<slug>`
- Keep PRs focused: one logical change per PR
- All checks must pass before merge
- Squash merge preferred; keep a clean linear history
- A new dependency needs a maintainer's agreement first

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/); the commit-message hook checks them:

```
feat(events): filter the event replay by agent
fix(voice): keep the caller on hold music during a transfer
docs: explain webhook retries
```

Types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`

Breaking changes: append `!` after the type/scope and add a `BREAKING CHANGE:` footer.

## Releases

Maintainers release from `main` with `uvx --from commitizen cz bump`. It sets the version in `pyproject.toml`, `uv.lock`, `src/hs/app.py` and the OpenAPI snapshot and tags `v<version>`. Write the version's `CHANGELOG.md` section by hand first, moving the entries from `## Unreleased`: the changelog is not generated from commits. Pushing the tag publishes the image `ghcr.io/plivo-labs/human-specialist` and a GitHub release with that version's changelog.

## License

Contributions are accepted under the project's [Apache-2.0 license](LICENSE).
