import json
import os
import subprocess
import sys
import uuid

import psycopg2
import pytest
from psycopg2 import sql
from sqlalchemy import create_engine, pool, text

from hs import config, tasks
from tests.conftest import REPO, _admin, _drop, _url

HS_TABLES = {
    "tenant",
    "credential",
    "idempotency",
    "config",
    "password",
    "presence",
    "conversation",
    "assignment",
    "entry",
    "task",
    "events",
    "webhooks",
    "jobs",
    "hook_cursor",
    "search",
    "status_history",
}


@pytest.fixture
def database():
    names = []

    def create() -> str:
        names.append(f"hs_mig_{uuid.uuid4().hex[:8]}")
        _admin(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(names[-1])))
        return _url(names[-1])

    yield create
    for name in names:
        _admin(_drop(name))


def alembic(url: str, *args: str) -> None:
    env = {**os.environ, "DATABASE_URL": url}
    done = subprocess.run([sys.executable, "-m", "alembic", *args], cwd=REPO, env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


# A toast table of an hs table lives in pg_toast.
OUTSIDE = (
    "SELECT relnamespace::regnamespace::text, relname FROM pg_class"
    " WHERE relnamespace::regnamespace::text NOT IN ('hs', 'pg_toast')"
    " UNION ALL SELECT pronamespace::regnamespace::text, proname FROM pg_proc WHERE pronamespace::regnamespace::text <> 'hs'"
    " UNION ALL SELECT nspname, '' FROM pg_namespace WHERE nspname <> 'hs' ORDER BY 1, 2"
)


def query(url: str, statement: str) -> list[tuple]:
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall()


def test_upgrade_head_builds_schema_hs_creates_nothing_outside_it_and_a_downgrade_empties_it(database):
    url = database()
    outside = query(url, OUTSIDE)
    alembic(url, "upgrade", "head")
    hs = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'hs'"
    assert {name for (name,) in query(url, hs)} == HS_TABLES | {"alembic_version"}
    assert query(url, OUTSIDE) == outside
    alembic(url, "downgrade", "base")
    assert query(url, hs) == [("alembic_version",)] and query(url, OUTSIDE) == outside
    alembic(url, "upgrade", "head")


def test_the_in_use_check_reads_ix_config_named(database):
    url = database()
    alembic(url, "upgrade", "head")
    tenant, box, menu, idle, number = (str(uuid.uuid4()) for _ in range(5))
    route = {"open_action": {"type": "menu", "menu_id": menu}, "closed_action": {"mailbox": {"queue_id": box}}}
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hs.tenant (id, name, plivo_auth_id, plivo_token_enc) VALUES (%s, 't', 'a', 'x')", [tenant]
        )
        for kind, id, body in (("queue", box, {}), ("menu", menu, {}), ("queue", idle, {}), ("number", number, route)):
            add = "INSERT INTO hs.config (id, tenant_id, kind, body) VALUES (%s, %s, %s, %s)"
            cur.execute(add, [id, tenant, kind, json.dumps(body)])
        # With a handful of rows the planner rightly takes the tenant index; this many make it read ix_config_named.
        cur.execute(
            "INSERT INTO hs.config (tenant_id, kind, body) SELECT %s, 'queue', '{}' FROM generate_series(1, 2000)",
            [tenant],
        )
    with create_engine(url, poolclass=pool.NullPool).connect() as db:
        db.execute(text("ANALYZE hs.config; SET search_path TO hs"))
        users = [db.execute(config._USED, {"tenant": tenant, "id": i}).all() for i in (box, menu, idle)]
        assert users == [[("number", uuid.UUID(number), None)]] * 2 + [[]]
        plan = "\n".join(db.execute(text("EXPLAIN " + config._USED.text), {"tenant": tenant, "id": idle}).scalars())
    assert "ix_config_named" in plan, plan


def test_a_conversations_tasks_are_listed_through_an_index_for_either_conversation(database):
    url, tenant = database(), str(uuid.uuid4())
    alembic(url, "upgrade", "head")
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        add = "INSERT INTO hs.tenant (id, name, plivo_auth_id, plivo_token_enc) VALUES (%s, 't', 'a', 'x')"
        cur.execute(add, [tenant])
        # Enough tasks that reading every one of the tenant's costs more than either index.
        cur.execute(
            "WITH c AS (INSERT INTO hs.conversation (tenant_id, channel, direction, state, customer)"
            " SELECT %s, 'task', 'outbound', 'open', '' FROM generate_series(1, 2000) RETURNING id, tenant_id)"
            " INSERT INTO hs.task (tenant_id, conversation_id, type, title, status, destination, origin_id)"
            " SELECT tenant_id, id, 'task', 'Follow up', 'todo', '{}', id FROM c",
            [tenant],
        )
    unset = dict.fromkeys(("id", "status", "routing", "agent", "after", "title", "type", "priority", "seen"))
    args = unset | {"t": tenant, "n": 51, "teams": [], "cid": str(uuid.uuid4())}
    with create_engine(url, poolclass=pool.NullPool).connect() as db:
        db.execute(text("ANALYZE hs.task; SET search_path TO hs"))
        plan = "\n".join(db.execute(text("EXPLAIN " + tasks._TASKS.text), args).scalars())
    assert "ix_task_conversation" in plan and "ix_task_origin" in plan, plan
