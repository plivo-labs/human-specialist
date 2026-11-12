FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.15 /uv /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY LICENSE README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim
# Debian 13 moved the legacy zone names (Asia/Calcutta, Europe/Kiev) out of tzdata; browsers still offer them.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata-legacy && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home-dir /app hs
WORKDIR /app
COPY --from=build /app/.venv ./.venv
COPY LICENSE alembic.ini ./
COPY alembic ./alembic
ENV PATH="/app/.venv/bin:$PATH"
USER hs
EXPOSE 6004
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:6004/status')"
CMD ["uvicorn", "hs.app:app", "--host", "0.0.0.0", "--port", "6004"]
