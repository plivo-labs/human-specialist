# Contributing to human-specialist

human-specialist is open source and self-hosted. People run it on their own infrastructure with their own Plivo account, so design every change for outside users, not for one company's setup.

## Set up

You need [uv](https://docs.astral.sh/uv/) and Docker.

```sh
uv sync                                            # .venv with Python 3.13 and the dev tools
docker compose -f docker/compose.test.yaml up -d   # Postgres and Redis for the tests
uvx pre-commit install                             # lint, format, secret and commit-message hooks
```

Tests use the test stack's ports by default. To use your own Postgres and Redis, point `HS_TEST_DATABASE_URL` and `HS_TEST_REDIS_URL` at them. If port 56432 or 57379 is taken, start the stack on other host ports with `HS_TEST_PG_PORT` and `HS_TEST_REDIS_PORT`, and point those two URLs at them. Test databases are built from the Alembic migrations only.

## Run the checks

```sh
make check        # the definition of green: ruff, format, unit, contract and acceptance tests
make test         # unit and contract tests only
make acceptance   # black-box tests: a real service process against the Plivo simulator
make snapshot     # rewrite the OpenAPI snapshot after an API change
make fmt          # fix lint and format
make run          # the service with reload on port 6004
```

The Plivo simulator (`tests/harness/sim.py`) stands in for Plivo in the acceptance suite. It signs callbacks with Plivo's V3 algorithm, answers REST calls, and replays the callbacks a real call produces, so a full call runs with no carrier.

## Find your way around

```
src/hs/    the service, one package per domain
alembic/   database migrations
tests/     unit, contract and acceptance tests; tests/harness holds the Plivo simulator
docker/    the image, the self-hosted stack and the test stack
docs/      architecture and reference pages
```

Each domain in `src/hs/` owns its router (`api.py`), logic and storage. [docs/architecture.md](docs/architecture.md#modules) lists what each package owns. Its glossary explains the older names the code keeps: aom, qnr run, chunk, and parent and child conversation.

## Finish a change

A change is done when all four hold:

1. `make check` exits 0.
2. Any acceptance test the change makes pass has its `xfail` marker removed. The acceptance tests are the spec, so a change to one changes promised behavior. Call it out in the pull request.
3. New non-trivial logic has the smallest test that fails if the logic breaks.
4. Every public route is listed in `tests/contract/routes.py` with its auth. After an API change, run `make snapshot` and review the OpenAPI diff.

## Code rules

- **Write less.** Before you add code, ask whether it needs to exist, whether the codebase already has it, and whether the standard library or an existing dependency does it. Add no speculative abstraction, no interface with one implementation, and no setting for a value that never changes.
- **No new dependencies without a maintainer.** Dependencies live in `pyproject.toml` and `uv.lock`.
- **Follow FastAPI.**
  - Auth is a dependency (`Depends(access.require(...))`) on the router or the route, so it runs before the body is parsed.
  - Request bodies and responses are pydantic v2 models.
  - Errors raise `ApiError(status, code, message, details)` and render as `{"error": {...}, "request_id": ...}`.
  - Lists take `?limit=&cursor=` and return `{"data": [...], "next_cursor": ...}`.
- **Tenancy.** The tenant comes only from the credential. Resolve every id from a path or a body inside that tenant: another tenant's id in a path is 404, and in a body it is 422. Never trust a tenant or organization id the caller sends.
- **State changes emit events** through `hs.events.emit`. The catalog is `src/hs/events/catalog.py`. Timers and async work are `hs.jobs.schedule(...)` rows. There are no other outbound side channels.
- **SQL.**
  - Bind every value. SQL text may interpolate only module-level constants. `tests/contract/test_sql_interpolation.py` enforces it.
  - Change the schema with Alembic revisions only, and only additively inside v1.
- **Secrets.**
  - Plivo tokens are encrypted at rest and never returned.
  - API keys are stored as SHA-256 hashes.
  - Raw recording URLs are never returned. Return a signed `audio_url` instead.
  - Logs never contain tokens, keys or signatures.
- **Errors.**
  - Never swallow an exception silently. Handle it, or let it propagate.
  - `except Exception` must log or re-raise. Use it only at a boundary: a loop (the job runner, a relay, SSE), a Plivo or Redis call whose failure the caller handles, or best-effort work (a metric, a cache write) that must not fail the request. Everywhere else, catch the specific exception. Some older modules still catch broadly. Don't copy them, and narrow what you touch.
- **No `print`, no TODO or FIXME, and no commented-out code.**

## Comments

- Write no comment by default. Add one only for a reason the code can't show, such as a constraint, a gotcha or a contract, in one or two lines.
- Never cite tickets, pull requests, reviews, customers or people.
- Don't narrate ("Added X") or restate the code. Delete the comments your change makes stale.

## Tests

- `tests/acceptance/` is the spec. It is black-box only: a real service process, the Plivo simulator, the public HTTP API and the events, all through `tests/harness`.
- `tests/unit/` and `tests/contract/` cover logic and API shape.
- Tests are deterministic. No `sleep` over 0.2 seconds. Wait on a condition with `world.wait(...)`.
- A test that needs a database uses the migrated test database: Alembic only, never `create_all`.

## Conventions

- Python 3.13 with uv, ruff and pytest. FastAPI, SQLAlchemy 1.4, Alembic and Pydantic 2.
- US English. No em dashes.

## Branches and pull requests

- Branch off `main`: `feat/<slug>`, `fix/<slug>` or `chore/<slug>`.
- Keep each pull request to one logical change.
- All checks pass before a merge.
- Squash merges are preferred, for a clean linear history.

## Commit messages

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/). The commit-message hook checks them:

```
feat(events): filter the event replay by agent
fix(voice): keep the caller on hold music during a transfer
docs: explain webhook retries
```

Types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`.

For a breaking change, add `!` after the type or scope, and add a `BREAKING CHANGE:` footer.

## Releases

Maintainers release from `main` with `uvx --from commitizen cz bump`. It sets the version in `pyproject.toml`, `uv.lock` and `docker/compose.yaml`, writes `CHANGELOG.md`, and tags `v<version>`. Pushing the tag publishes the image `ghcr.io/plivo-labs/human-specialist` and a GitHub release with that version's changelog.

## License

Contributions are accepted under the project's [Apache-2.0 license](LICENSE).
