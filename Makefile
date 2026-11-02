.PHONY: check test acceptance snapshot fmt run

RUN := uv run --locked

# Fail-fast, in this order.
check:
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) python -m pytest tests/unit tests/contract -n auto --dist loadgroup
	@if [ -d tests/acceptance ]; then $(RUN) python -m pytest tests/acceptance; fi

test:
	$(RUN) python -m pytest tests/unit tests/contract -n auto --dist loadgroup

acceptance:
	$(RUN) python -m pytest tests/acceptance

snapshot:
	$(RUN) python -m tests.contract.test_openapi_snapshot

fmt:
	$(RUN) ruff check --fix --exit-zero --silent .
	$(RUN) ruff format .

run:
	$(RUN) uvicorn hs.app:app --host 0.0.0.0 --port 6004 --reload
