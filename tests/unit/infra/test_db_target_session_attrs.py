"""Every connection lands on a writer: a host that resolves to a writer and standbys must never yield a standby,
which serves SELECTs and then fails the first write. pool_pre_ping cannot catch it, so the guard is a connect arg."""

import os

import psycopg2
import pytest

from hs.infra.db import CONNECT_ARGS


def test_connect_args_pin_read_write_and_bound_statements():
    assert CONNECT_ARGS["target_session_attrs"] == "read-write"
    assert CONNECT_ARGS["connect_timeout"] == 3
    assert "statement_timeout=5000" in CONNECT_ARGS["options"]
    assert "lock_timeout=3000" in CONNECT_ARGS["options"]


def test_libpq_enforces_the_attr(db_engine):
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, target_session_attrs="read-write")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            assert cur.fetchone()[0] is False
    finally:
        conn.close()
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(url, target_session_attrs="read-only")
