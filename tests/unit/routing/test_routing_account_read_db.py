"""routing_account_read against the test database: what the agent reads return, not how they are built."""

import uuid

import pytest
from sqlalchemy import text

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.admin.account import Account, AccountOrgMapping, Organization
from hs.routing import account_read as ar


@pytest.fixture()
def seeded(db, monkeypatch):
    monkeypatch.setattr(ar, "SessionLocal", lambda: db)
    monkeypatch.setattr("hs.admin.account.get_db_session", lambda: db)
    org = Organization(uuid=uuid.uuid4(), name="o", auth_id="AID-" + uuid.uuid4().hex[:8], auth_token="")
    org.is_deleted = False
    db.add(org)
    db.flush()

    def agent(sip, **account):
        acc = Account(uuid=uuid.uuid4(), email=f"{uuid.uuid4().hex[:8]}@example.com", is_deleted=False, **account)
        db.add(acc)
        db.flush()
        aom = AccountOrgMapping(uuid=uuid.uuid4(), organization=org.id, account=acc.id, sip_endpoint=sip)
        db.add(aom)
        db.flush()
        return aom

    return org, agent


@pytest.mark.parametrize(
    "account,name",
    [
        ({"first_name": "Ada", "last_name": "Lovelace", "name": "ada"}, "Ada Lovelace"),
        ({"first_name": "Ada", "name": "ada"}, "Ada"),
        ({"last_name": "Lovelace", "name": "ada"}, "Lovelace"),
        ({"name": "ada"}, "ada"),
        ({}, ""),
    ],
)
def test_the_local_account_names_the_agent(seeded, account, name):
    org, agent = seeded
    sip = "sip:" + uuid.uuid4().hex
    aom = agent(sip, **account)
    assert ar.get_agent_by_uuid(str(aom.uuid), org.auth_id).name == name
    assert ar.get_agent_by_uuid(str(aom.uuid), "another tenant") is None


@pytest.fixture()
def scrambled(db, monkeypatch):
    """Three agents stored in the heap as ids 3, 1, 2 (temp tables shadow the real ones for this transaction)."""
    monkeypatch.setattr(ar, "SessionLocal", lambda: db)
    for ddl in (
        "CREATE TEMP TABLE organization (id int, auth_id text)",
        "CREATE TEMP TABLE account (id int, email text, first_name text, last_name text, name text)",
        "CREATE TEMP TABLE account_org_mapping (id int, uuid uuid, sip_endpoint text, status text, account int,"
        " organization int)",
        "INSERT INTO organization VALUES (1, 'AID')",
        "INSERT INTO account VALUES (1, 'a@example.com', NULL, NULL, 'a')",
    ):
        db.execute(text(ddl))
    agents = {i: (str(uuid.uuid4()), f"sip:{i}") for i in (3, 1, 2)}
    for i, (u, sip) in agents.items():
        db.execute(
            text("INSERT INTO account_org_mapping VALUES (:i, :u, :s, 'active', 1, 1)"), {"i": i, "u": u, "s": sip}
        )
    return [agents[i] for i in (1, 2, 3)]


def test_agents_read_in_id_order_whatever_the_heap_order(scrambled):
    ids = [u for u, _ in scrambled]
    assert ar.get_agent_details_for_email_or_aom_id([], ids, "AID") == (ids, None)


def test_an_agent_leg_reads_the_name_and_email_and_the_org_reads_its_pair(seeded, monkeypatch):
    org, agent = seeded
    sip = "sip:" + uuid.uuid4().hex
    aom = agent(sip, name="ada")
    [row] = ar.get_account_rows_for_sip_endpoints([sip])
    assert row._asdict() == {"uuid": str(aom.uuid), "sip_endpoint": sip, "name": "ada", "email": row.email}
    users = AccountOrgMapping.get_user_details_by_aom_uuids([str(aom.uuid)], org.id)
    assert (users[str(aom.uuid)].name, users[str(aom.uuid)].email) == ("ada", row.email)
    assert AccountOrgMapping.get_user_details_by_aom_uuids([str(aom.uuid)], org.id + 1) == {}  # another tenant's
    # every token read goes through the tenant's own pair, by its organization uuid
    monkeypatch.setattr(ar, "tenant_plivo", lambda tenant: (org.auth_id, "tok") if tenant == str(org.uuid) else None)
    assert ar.get_org_for_auth_id(org.auth_id).auth_token == "tok"
    assert ar.get_organisation_auth_token(org.auth_id) == ("tok", None)


@pytest.fixture()
def routing_agents(db, monkeypatch):
    """The routing-agent read against temp tables carrying aom.status, which the mapped test schema lacks;
    the temp tables shadow the real ones for this transaction only and go with its rollback."""
    from sqlalchemy import text

    monkeypatch.setattr(ar, "SessionLocal", lambda: db)
    for ddl in (
        "CREATE TEMP TABLE organization (id int, auth_id text)",
        "CREATE TEMP TABLE account (id int, email text)",
        "CREATE TEMP TABLE account_org_mapping (uuid uuid, status text, account int, organization int, id serial)",
    ):
        db.execute(text(ddl))
    db.execute(text("INSERT INTO organization VALUES (1, 'AID'), (2, 'OTHER')"))
    db.execute(text("INSERT INTO account VALUES (1, 'ada@example.com'), (2, 'bob@example.com')"))
    rows = {"ada": (1, "active", 1), "ada-elsewhere": (2, "active", 1), "bob-inactive": (1, "inactive", 2)}
    ids = {}
    for key, (org, status, acc) in rows.items():
        ids[key] = str(uuid.uuid4())
        db.execute(
            text("INSERT INTO account_org_mapping VALUES (:u, :s, :a, :o)"),
            {"u": ids[key], "s": status, "a": acc, "o": org},
        )
    return ids


def test_routing_agents_listed_by_email_resolve_within_the_org(routing_agents):
    found, err = ar.get_agent_details_for_email_or_aom_id(
        ["ada@example.com", "bob@example.com", "nobody@example.com"], [], "AID"
    )
    assert (found, err) == ([routing_agents["ada"]], None)  # bob is inactive, ada-elsewhere another org


def test_routing_agents_by_uuid_and_email_together(routing_agents):
    found, _ = ar.get_agent_details_for_email_or_aom_id(["ada@example.com"], [routing_agents["ada-elsewhere"]], "AID")
    assert sorted(found) == sorted([routing_agents["ada"], routing_agents["ada-elsewhere"]])
    assert ar.get_agent_details_for_email_or_aom_id([], [routing_agents["bob-inactive"]], "AID") == ([], None)
    assert ar.get_agent_details_for_email_or_aom_id([], [], "AID") == ([], None)


def test_agent_selection_and_the_agent_read_stay_inside_the_tenant(db, monkeypatch):
    from hs.admin.account import OrgStatus
    from hs.routing import constants as rc
    from hs.routing import selection

    monkeypatch.setattr(selection, "SessionLocal", lambda: db)
    monkeypatch.setattr(ar, "SessionLocal", lambda: db)

    def tenant():
        org = Organization(uuid=uuid.uuid4(), name="t", auth_id="MA" + uuid.uuid4().hex[:8], is_deleted=False)
        db.add(org)
        db.flush()
        available = OrgStatus(uuid=uuid.uuid4(), organization=org.id, status="Available", availability=True)
        db.add(available)
        db.flush()
        sip = "sip:" + uuid.uuid4().hex
        aom = AccountOrgMapping(uuid=uuid.uuid4(), organization=org.id, sip_endpoint=sip, agent_status=available.id)
        db.add(aom)
        db.flush()
        return org, aom

    (home, mine), (_, theirs) = tenant(), tenant()
    both = [str(mine.uuid), str(theirs.uuid)]
    picked = selection.get_agents_for_contact(both, [], [], [], rc.SIMULTANEOUS_ROUTING, home.auth_id)
    assert picked == [mine.sip_endpoint]
    assert ar.get_agent_sip_by_uuid(str(theirs.uuid), home.auth_id) is None
    assert ar.get_agent_sip_by_uuid(str(mine.uuid), home.auth_id) == mine.sip_endpoint
