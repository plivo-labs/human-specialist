"""Test env and the migrated test database.

hs reads its settings once at import, so the env is set here, before anything imports hs; this module itself never
imports hs (the acceptance harness boots HS with its own env). Each run migrates one template database with
`alembic upgrade head`, and every xdist worker gets its own copy (CREATE DATABASE ... TEMPLATE).
"""

import os
import socket
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg2
import pytest
from psycopg2 import sql

REPO = Path(__file__).resolve().parents[1]
ADMIN_URL = os.environ.get("HS_TEST_DATABASE_URL", "postgresql://hs:hs@127.0.0.1:56432/hs")
# The controller picks the run id; xdist workers inherit it through the environment.
if "PYTEST_XDIST_WORKER" not in os.environ:
    os.environ["HS_TEST_RUN"] = uuid.uuid4().hex[:8]
TEMPLATE = f"hs_tpl_{os.environ['HS_TEST_RUN']}"
WORKER_DB = f"hs_t_{os.environ['HS_TEST_RUN']}_{os.environ.get('PYTEST_XDIST_WORKER', 'main')}"


def _url(database: str) -> str:
    return urlunsplit(urlsplit(ADMIN_URL)._replace(path=f"/{database}"))


os.environ["DATABASE_URL"] = _url(WORKER_DB)
with socket.socket() as _s:
    _s.bind(("127.0.0.1", 0))
    # The unit suite's redis-server takes this port (tests/unit/conftest.py); a World passes HS its own.
    os.environ["REDIS_URL"] = f"redis://127.0.0.1:{_s.getsockname()[1]}/0"
for name, value in {
    "PUBLIC_BASE_URL": "http://testserver",
    "HS_OPERATOR_KEY": "test-operator-key-of-at-least-32-bytes",
    "HS_SECRET_KEY": "test-secret-key-of-at-least-32-bytes",
    "METRICS_PORT": "0",
}.items():
    os.environ.setdefault(name, value)


def _admin(*statements) -> None:
    conn = psycopg2.connect(ADMIN_URL)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
    finally:
        conn.close()


def _drop(database: str) -> sql.Composable:
    return sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database))


def _create_worker_db() -> None:
    """Under a run-wide advisory lock: migrate the template once, then copy it for this worker."""
    lock = sql.SQL("SELECT pg_advisory_lock(hashtext({}))").format(sql.Literal(TEMPLATE))
    conn = psycopg2.connect(ADMIN_URL)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(lock)
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEMPLATE,))
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEMPLATE)))
                env = {**os.environ, "DATABASE_URL": _url(TEMPLATE)}
                migrate = [sys.executable, "-m", "alembic", "upgrade", "head"]
                done = subprocess.run(migrate, cwd=REPO, env=env, capture_output=True, text=True, check=False)
                if done.returncode:
                    cur.execute(_drop(TEMPLATE))
                    raise RuntimeError(f"alembic upgrade head failed:\n{done.stderr}")
            cur.execute(_drop(WORKER_DB))
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(sql.Identifier(WORKER_DB), sql.Identifier(TEMPLATE))
            )
    finally:
        conn.close()  # also releases the advisory lock


def pytest_unconfigure(config):
    if not hasattr(config, "workerinput"):
        _admin(_drop(TEMPLATE))


@pytest.fixture(scope="session")
def db_engine():
    """The engine on this worker's migrated database (the app's own engine: DATABASE_URL points here)."""
    _create_worker_db()
    from hs import core

    core.init()
    yield core.engine
    core.engine.dispose()
    _admin(_drop(WORKER_DB))
