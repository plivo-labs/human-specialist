ARG HS_IMAGE=ghcr.io/plivo-labs/human-specialist:latest
FROM ${HS_IMAGE}
USER root
COPY . /plugin
RUN --mount=type=bind,from=ghcr.io/astral-sh/uv:0.12.15,source=/uv,target=/usr/local/bin/uv \
    uv pip freeze --python /app/.venv/bin/python > /tmp/frozen.txt \
    && uv pip install --python /app/.venv/bin/python --constraint /tmp/frozen.txt /plugin \
    && rm /tmp/frozen.txt
USER hs
