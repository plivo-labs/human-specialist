.PHONY: check test acceptance fuzz snapshot fmt run

RUN := uv run --locked

# Fail-fast, in this order.
check:
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) python -m pytest tests/unit tests/contract tests/harness tests/migration -n auto --dist loadgroup
	@if [ -d tests/acceptance ]; then $(RUN) python -m pytest tests/acceptance; fi

test:
	$(RUN) python -m pytest tests/unit tests/contract tests/harness tests/migration -n auto --dist loadgroup

acceptance:
	$(RUN) python -m pytest tests/acceptance

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
