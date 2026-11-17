import contextlib
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from urllib.parse import urlsplit
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from tests.conftest import REPO, _admin, _drop, _url

pytestmark = pytest.mark.xdist_group("migration")
THEIRS = "SELECT version_num FROM public.alembic_version"
PUBLIC = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1"
VERSIONS = "SELECT table_schema FROM information_schema.tables WHERE table_name = 'alembic_version' ORDER BY 1"
WAITING = (
    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
    " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
)


THEIR_APP = (
    "CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY);"
    " INSERT INTO public.alembic_version VALUES ('theirs'); CREATE TABLE public.account (id int PRIMARY KEY)"
)


def alembic(url: str, *args: str) -> subprocess.CompletedProcess:
    env = os.environ | {"DATABASE_URL": url}
    cmd = [sys.executable, "-m", "alembic", *args]
    return subprocess.run(cmd, cwd=REPO, env=env, capture_output=True, text=True, check=False)


def rows(url: str, query: str) -> list[tuple]:
    with contextlib.closing(psycopg2.connect(url)) as db, db, db.cursor() as cur:
        cur.execute(query)
        return cur.fetchall() if cur.description else []


@contextlib.contextmanager
def shared(setup: str = THEIR_APP) -> Iterator[str]:
    name = f"hs_mig_{uuid4().hex}"
    _admin(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        rows(_url(name), setup)
        yield _url(name)
    finally:
        _admin(_drop(name))


@pytest.mark.parametrize("version_schema", [None, "hs_version"], ids=["default", "elsewhere"])
def test_an_upgrade_leaves_the_other_apps_revision_and_tables_alone(monkeypatch, version_schema):
    with shared() as url:
        before = rows(url, PUBLIC)
        if version_schema:
            monkeypatch.setenv("HS_VERSION_SCHEMA", version_schema)
        done = alembic(url, "upgrade", "head")
        assert done.returncode == 0, done.stderr
        assert rows(url, THEIRS) == [("theirs",)] and rows(url, PUBLIC) == before
        assert rows(url, VERSIONS) == [(version_schema or "hs",), ("public",)]
        assert rows(url, "SELECT count(*) FROM hs.tenant") == [(0,)]


def test_upgrades_that_start_while_the_migration_lock_is_held_wait_for_it_then_both_reach_head():
    with shared("SELECT 1") as url, contextlib.closing(psycopg2.connect(url)) as holder:
        holder.cursor().execute("SELECT pg_advisory_xact_lock(hashtext('hs.alembic'))")
        cmd, env = [sys.executable, "-m", "alembic", "upgrade", "head"], os.environ | {"DATABASE_URL": url}
        runs = []
        try:
            for _ in range(2):
                runs.append(subprocess.Popen(cmd, cwd=REPO, env=env, stderr=subprocess.PIPE, text=True))
            deadline = time.monotonic() + 30
            while rows(url, WAITING) != [(2,)]:
                assert time.monotonic() < deadline and all(run.poll() is None for run in runs), "both wait on the lock"
                time.sleep(0.1)
            holder.rollback()
            errors = [run.communicate(timeout=60)[1] for run in runs]
        finally:
            for run in runs:
                run.kill()
                run.wait(10)
        assert [run.returncode for run in runs] == [0, 0], errors
        head = alembic(url, "heads").stdout.split()[0]
        assert rows(url, "SELECT version_num FROM hs.alembic_version") == [(head,)]


def test_an_offline_upgrade_script_is_one_transaction():
    with shared("SELECT 1") as url:
        script = alembic(url, "upgrade", "head", "--sql")
        assert script.returncode == 0, script.stderr
        assert script.stdout.strip().startswith("BEGIN;") and script.stdout.strip().endswith("COMMIT;")


def test_a_schema_that_holds_tables_but_no_recorded_revision_is_refused_before_any_change():
    with shared("CREATE SCHEMA hs; CREATE TABLE hs.tenant (id int)") as url:
        done = alembic(url, "upgrade", "head")
        assert done.returncode != 0 and "holds tables but no recorded revision" in done.stderr, done.stderr
        assert rows(url, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'hs'") == [("tenant",)]


def test_a_role_without_create_on_the_database_upgrades_into_a_schema_hs_made_for_it():
    role = f"hs_mig_{uuid4().hex[:8]}"
    _admin(sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'hs'").format(sql.Identifier(role)))
    try:
        with shared(f"CREATE SCHEMA hs AUTHORIZATION {role}") as url:
            own = url.replace(urlsplit(url).netloc, f"{role}:hs@{urlsplit(url).netloc.rpartition('@')[2]}")
            for run in ("fresh", "again"):
                done = alembic(own, "upgrade", "head")
                assert done.returncode == 0, (run, done.stderr)
            assert rows(url, "SELECT count(*) FROM hs.alembic_version") == [(1,)]
    finally:
        _admin(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
