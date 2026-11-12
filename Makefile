.PHONY: check lint test acceptance fuzz snapshot fmt run

RUN := uv run --locked

# Fail-fast, in this order, under make -j too.
check:
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) acceptance

lint:
	$(RUN) ruff check .
	$(RUN) ruff format --check .

test:
	$(RUN) python -m pytest tests/unit tests/contract tests/harness tests/migration -n auto --dist loadgroup

acceptance:
	$(RUN) python -m pytest tests/acceptance -n auto --dist loadgroup

# A seeded random walk over a live HS, outside check. HS_FUZZ_SEED=8,1 walks those seeds.
fuzz:
	$(RUN) python -m pytest tests/fuzz/walk.py

snapshot:
	$(RUN) python -m tests.contract.test_openapi_snapshot

fmt:
	$(RUN) ruff check --fix --exit-zero --silent .
	$(RUN) ruff format .

run:
	$(RUN) uvicorn hs.app:app --host 0.0.0.0 --port 6004 --reload
