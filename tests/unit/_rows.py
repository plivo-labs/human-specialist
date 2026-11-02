"""Committed rows for tests that read through the app's own connections: each test seeds its own tenant."""

import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import column, insert, table

from hs.access import Principal


def add(conn, name: str, /, **cols) -> int:
    """Insert one row of `name`, the shared base columns filled in; returns its id."""
    now = datetime.now(UTC)
    cols = {"uuid": str(uuid.uuid4()), "created_at": now, "updated_at": now, "is_deleted": False, **cols}
    cols = {k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in cols.items()}
    t = table(name, *map(column, [*cols, "id"]))
    return conn.execute(insert(t).values(cols).returning(t.c.id)).scalar()


def uuid_of(conn, name: str, row_id: int) -> str:
    t = table(name, column("id"), column("uuid"))
    return str(conn.execute(t.select().with_only_columns(t.c.uuid).where(t.c.id == row_id)).scalar())


def tenant(conn) -> tuple[int, str]:
    org = add(
        conn, "organization", name="Acme", auth_id=f"SA{uuid.uuid4().hex[:18].upper()}", is_active=True, auth_token="",
        skip_contact_creation=True, skip_case_creation=True, allow_acw_post_transfer=False,
    )  # fmt: skip
    return org, uuid_of(conn, "organization", org)


def agent(conn, org: int, *teams: int, **cols) -> tuple[int, str]:
    aom = add(conn, "account_org_mapping", organization=org, sip_endpoint=f"sip:{uuid.uuid4().hex}@x", **cols)
    for team in teams:
        add(conn, "aom_team", organization=org, aom=aom, team=team)
    return aom, uuid_of(conn, "account_org_mapping", aom)


def principal(org: int, tenant_id: str, agent_id: str | None = None, role: str | None = None) -> Principal:
    """A tenant key with every scope, or an agent token when `agent_id` is given."""
    if agent_id is None:
        return Principal(tenant_id, org, "key", frozenset({"calls", "reports"}))
    return Principal(tenant_id, org, "agent", agent_id=agent_id, role=role)
