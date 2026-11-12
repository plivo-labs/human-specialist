import json
from concurrent.futures import ThreadPoolExecutor

import psycopg2

from tests.acceptance.conftest import connect, disposition, error, finish, ok, run_jobs_until, team

V = "/api/v1/views"
WAITING = "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()"
FILTER = {"channel": "call", "answered": True}


def agent(tenant, role="agent", **fields):
    return tenant.agent(role=role, **fields).login(available=False)


def make(client, name="Missed", **fields):
    return ok(client.post(V, json={"name": name, "filter": FILTER, "columns": ["number"], **fields}))


def names(client, **params) -> list[str]:
    return [v["name"] for v in ok(client.get(V, params=params))["data"]]


def test_a_view_keeps_its_filter_and_columns(tenant):
    mine = agent(tenant).api()
    view = make(mine, columns=["number", "agent_ids"])
    assert (view["type"], view["filter"]["channel"], view["columns"]) == ("private", "call", ["number", "agent_ids"])
    assert ok(mine.get(f"{V}/{view['id']}"))["filter"]["answered"] is True
    again = ok(mine.patch(f"{V}/{view['id']}", json={"name": "Answered", "columns": ["number"]}))
    assert (again["name"], again["columns"], again["filter"]["channel"]) == ("Answered", ["number"], "call")
    error(mine.post(V, json={"name": "Bad", "columns": ["nope"]}), 422)
    error(mine.post(V, json={"name": "Bad", "filter": {"channel": "fax"}}), 422)


def test_private_shared_and_public_views_reach_who_they_name(tenant):
    owner, other, outsider = agent(tenant, role="supervisor"), agent(tenant), agent(tenant)
    crew = team(tenant)
    member = agent(tenant, teams=[crew])
    make(owner.api(), "Private")
    make(owner.api(), "ToAgent", type="shared", agents=[other.id])
    make(owner.api(), "ToTeam", type="shared", teams=[crew])
    make(owner.api(), "All", type="public")
    assert sorted(names(owner.api())) == ["All", "Private", "ToAgent", "ToTeam"]
    assert sorted(names(other.api())) == ["All", "ToAgent"]
    assert sorted(names(member.api())) == ["All", "ToTeam"]
    assert names(outsider.api()) == ["All"]
    late = agent(tenant)
    assert names(late.api()) == ["All"]


def test_a_view_the_caller_may_not_see_answers_403(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    view = make(owner.api())
    error(other.api().get(f"{V}/{view['id']}"), 403)
    error(other.api().get(f"{V}/{'0' * 8}-0000-4000-8000-{'0' * 12}"), 404)
    error(other.api().patch(f"{V}/{view['id']}", json={"name": "x"}), 403)


def test_a_shared_view_names_someone_and_an_agent_makes_only_private_views(tenant):
    sup, plain = agent(tenant, role="supervisor"), agent(tenant)
    error(sup.api().post(V, json={"name": "Nobody", "type": "shared"}), 422)
    error(sup.api().post(V, json={"name": "Foreign", "type": "shared", "teams": [plain.id]}), 422, "foreign_id")
    for kind in ("public", "shared"):
        error(plain.api().post(V, json={"name": "No", "type": kind, "agents": [sup.id]}), 403)
    assert make(sup.api(), "Open", type="public")["type"] == "public"


def test_only_the_owner_edits_or_deletes_and_a_key_owns_the_views_it_makes(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant, role="supervisor")
    shared = make(owner.api(), "Shared", type="public")
    error(other.api().patch(f"{V}/{shared['id']}", json={"name": "Mine"}), 403)
    error(other.api().delete(f"{V}/{shared['id']}"), 403)
    keyed = make(tenant.api, "Keyed", type="public")
    assert keyed["owner_id"] is None and names(other.api()).count("Keyed") == 1
    error(other.api().patch(f"{V}/{keyed['id']}", json={"name": "Mine"}), 403)
    error(tenant.api.patch(f"{V}/{shared['id']}", json={"name": "Mine"}), 403)
    ok(tenant.api.patch(f"{V}/{keyed['id']}", json={"name": "Renamed"}))
    ok(owner.api().delete(f"{V}/{shared['id']}"))
    ok(tenant.api.delete(f"{V}/{keyed['id']}"))
    assert names(other.api()) == []


def test_a_view_leaves_the_others_when_it_stops_being_shared(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    view = make(owner.api(), "Wide", type="shared", agents=[other.id])
    assert names(other.api()) == ["Wide"]
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "private"}))
    assert names(other.api()) == []
    error(owner.api().patch(f"{V}/{view['id']}", json={"type": "shared"}), 422)


def ordered(client, ids, kind):
    return client.put(f"{V}/order", json={"ids": ids, "type": kind})


def test_favorites_and_order_are_per_agent(tenant):
    a, b = agent(tenant, role="supervisor"), agent(tenant)
    one, two, three = (make(a.api(), n, type="public") for n in ("One", "Two", "Three"))
    assert names(a.api()) == ["Three", "Two", "One"]
    ok(ordered(a.api(), [one["id"], two["id"], three["id"]], "public"))
    assert names(a.api()) == ["One", "Two", "Three"] and names(b.api()) == ["Three", "Two", "One"]
    assert ok(a.api().put(f"{V}/{two['id']}/favorite"))["favorite"] is True
    ok(a.api().put(f"{V}/{three['id']}/favorite"))
    assert names(a.api(), favorite=True) == ["Three", "Two"] and names(b.api(), favorite=True) == []
    ok(ordered(a.api(), [two["id"], three["id"]], "favorite"))
    assert names(a.api(), favorite=True) == ["Two", "Three"]
    ok(a.api().delete(f"{V}/{two['id']}/favorite"))
    assert names(a.api(), favorite=True) == ["Three"]
    error(ordered(b.api(), [make(a.api(), "Mine")["id"]], "private"), 422)
    error(tenant.api.put(f"{V}/order", json={"ids": [one["id"]], "type": "public"}), 403)


def test_ordering_one_type_leaves_the_order_of_another(tenant):
    a = agent(tenant, role="supervisor")
    p1, p2 = make(a.api(), "P1"), make(a.api(), "P2")
    u1, u2 = make(a.api(), "U1", type="public"), make(a.api(), "U2", type="public")
    assert names(a.api()) == ["P2", "P1", "U2", "U1"]
    ok(ordered(a.api(), [p1["id"], p2["id"]], "private"))
    ok(ordered(a.api(), [u1["id"], u2["id"]], "public"))
    assert names(a.api()) == ["P1", "P2", "U1", "U2"]
    error(ordered(a.api(), [p1["id"], u1["id"]], "private"), 422)


def test_a_repeated_favorite_is_refused_and_a_foreign_view_resolves(tenant):
    a, b = agent(tenant, role="supervisor"), agent(tenant)
    one, two = make(a.api(), "One", type="public"), make(a.api(), "Two", type="public")
    ok(a.api().put(f"{V}/{one['id']}/favorite"))
    ok(a.api().put(f"{V}/{two['id']}/favorite"))
    error(a.api().put(f"{V}/{one['id']}/favorite"), 400, "already_favorite")
    assert names(a.api(), favorite=True) == ["Two", "One"]
    ok(a.api().delete(f"{V}/{one['id']}/favorite"))
    error(a.api().delete(f"{V}/{one['id']}/favorite"), 400, "not_favorite")
    private = make(a.api(), "Mine")
    error(b.api().delete(f"{V}/{private['id']}/favorite"), 403)
    error(b.api().delete(f"{V}/{'0' * 8}-0000-4000-8000-{'0' * 12}/favorite"), 404)


def test_a_revoked_favorite_does_not_come_back(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    view = make(owner.api(), "Wide", type="public")
    ok(other.api().put(f"{V}/{view['id']}/favorite"))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "private"}))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "public"}))
    assert names(other.api(), favorite=True) == []
    kept = agent(tenant)
    shared = make(owner.api(), "Few", type="shared", agents=[other.id, kept.id])
    for who in (other, kept):
        ok(who.api().put(f"{V}/{shared['id']}/favorite"))
    ok(owner.api().patch(f"{V}/{shared['id']}", json={"agents": [kept.id]}))
    ok(owner.api().patch(f"{V}/{shared['id']}", json={"agents": [kept.id, other.id]}))
    assert names(other.api(), favorite=True) == [] and names(kept.api(), favorite=True) == ["Few"]


def test_a_view_pages_in_the_callers_order(tenant):
    a = agent(tenant)
    for n in ("A", "B", "C"):
        make(a.api(), n)
    first = ok(a.api().get(V, params={"limit": 2}))
    rest = ok(a.api().get(V, params={"limit": 2, "cursor": first["next_cursor"]}))
    assert [v["name"] for v in first["data"] + rest["data"]] == ["C", "B", "A"] and rest["next_cursor"] is None


def grouped(*atoms):
    return {"filter": {"groups": [{"atoms": list(atoms)}]}}


def test_a_filter_naming_another_tenants_ids_is_422(world, tenant):
    other = world.new_tenant("Other")
    theirs = {"team_id": team(other), "disposition_id": disposition(other)["id"], "agent_id": other.agent().id}
    mine = agent(tenant).api()
    own = make(mine, "Own")
    for field, value in theirs.items():
        error(mine.post(V, json={"name": "Top", "filter": {field: value}}), 422, "foreign_id")
        atom = {"field": field, "op": "is", "value": [value]}
        error(mine.post(V, json={"name": "Atom", "filter": grouped(atom)}), 422, "foreign_id")
        error(mine.patch(f"{V}/{own['id']}", json={"filter": grouped(atom)}), 422, "foreign_id")
    ok(mine.post(V, json={"name": "Mine", "filter": {"team_id": team(tenant)}}))


def test_a_view_returns_its_filter_without_deleted_agents_and_teams(tenant):
    keep, gone, crew = agent(tenant), agent(tenant), team(tenant)
    mine = agent(tenant).api()
    atom = {"field": "agent_id", "op": "is", "value": [keep.id, gone.id]}
    view = ok(mine.post(V, json={"name": "Crew", "filter": {"team_id": crew, **grouped(atom)}}))
    ok(tenant.api.delete(f"/api/v1/agents/{gone.id}"))
    ok(tenant.api.delete(f"/api/v1/teams/{crew}"))
    shown = ok(mine.get(f"{V}/{view['id']}"))["filter"]
    assert shown["filter"]["groups"][0]["atoms"][0]["value"] == [keep.id] and shown["team_id"] is None


def test_a_patch_with_a_null_is_422(tenant):
    mine = agent(tenant).api()
    view = make(mine)
    for body in ({"name": None}, {"filter": None}, {"columns": None}):
        error(mine.patch(f"{V}/{view['id']}", json=body), 422)


def test_going_private_clears_every_agents_favorite_and_order_entry_the_owners_too(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    older = make(owner.api(), "Older", type="public")
    view = make(owner.api(), "Wide", type="public")
    for who in (owner, other):
        ok(who.api().put(f"{V}/{view['id']}/favorite"))
        ok(ordered(who.api(), [older["id"], view["id"]], "public"))
    assert names(owner.api()) == ["Older", "Wide"]
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "private"}))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "public"}))
    for who in (owner, other):
        assert names(who.api(), favorite=True) == [] and names(who.api()) == ["Wide", "Older"]


def test_a_type_change_clears_the_order_and_keeps_the_favorites(tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    older = make(owner.api(), "Older", type="shared", agents=[other.id])
    view = make(owner.api(), "Wide", type="shared", agents=[other.id])
    ok(other.api().put(f"{V}/{view['id']}/favorite"))
    ok(ordered(other.api(), [older["id"], view["id"]], "shared"))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "public"}))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"type": "shared", "agents": [other.id]}))
    assert names(other.api(), favorite=True) == ["Wide"] and names(other.api()) == ["Wide", "Older"]


def test_an_update_of_a_shared_view_clears_the_entries_of_whoever_it_does_not_name_as_an_agent(tenant):
    owner, named = agent(tenant, role="supervisor"), agent(tenant)
    crew = team(tenant)
    member = agent(tenant, teams=[crew])
    view = make(owner.api(), "Few", type="shared", agents=[named.id], teams=[crew])
    for who in (owner, named, member):
        ok(who.api().put(f"{V}/{view['id']}/favorite"))
    ok(owner.api().patch(f"{V}/{view['id']}", json={"name": "Fewer"}))
    assert names(named.api(), favorite=True) == ["Fewer"]
    assert names(owner.api(), favorite=True) == [] and names(member.api(), favorite=True) == []


def test_a_write_waits_for_a_patch_in_flight_and_then_sees_what_it_did(world, tenant):
    owner, other = agent(tenant, role="supervisor"), agent(tenant)
    url = world._env["DATABASE_URL"]
    writes = ((lambda c, v: c.put(f"{V}/{v}/favorite"), 403), (lambda c, v: ordered(c, [v], "public"), 422))
    with psycopg2.connect(url) as patching, psycopg2.connect(url) as probing, ThreadPoolExecutor(1) as pool:
        probing.autocommit = True
        for write, refused in writes:
            view = make(owner.api(), "Wide", type="public")
            with patching.cursor() as patch, probing.cursor() as probe:
                patch.execute("SELECT id FROM hs.config WHERE id = %s FOR UPDATE", (view["id"],))
                late = pool.submit(write, other.api(), view["id"])
                world.wait(lambda: probe.execute(WAITING) or probe.fetchone()[0], what="the write waits for the patch")
                patch.execute(
                    "UPDATE hs.config SET body = jsonb_set(body, '{type}', '\"private\"') WHERE id = %s", (view["id"],)
                )
                patching.commit()
            error(late.result(), refused)
    assert names(other.api(), favorite=True) == []


def test_an_atom_on_a_reference_field_without_a_value_is_422(tenant):
    mine = agent(tenant).api()
    own = make(mine, "Own")
    for field in ("agent_id", "team_id", "queue_id", "disposition_id"):
        bare = grouped({"field": field, "op": "is"})
        error(mine.post(V, json={"name": "Bare", "filter": bare}), 422)
        error(mine.patch(f"{V}/{own['id']}", json={"filter": bare}), 422)


def test_a_view_runs_its_stored_filter_even_for_an_agent_the_returned_filter_no_longer_names(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    mine = agent(tenant, role="admin")
    kept, lost = finish(connect(tenant, a))["conversation_id"], finish(connect(tenant, b))["conversation_id"]
    atom = {"field": "agent_id", "op": "is", "value": [a.id, b.id]}
    view = ok(mine.api().post(V, json={"name": "Both", "type": "public", "filter": grouped(atom)}))
    run = f"{V}/{view['id']}/conversations"

    def found() -> set[str]:
        return {c["id"] for c in ok(mine.api().get(run))["data"]}

    run_jobs_until(world, lambda: found() == {kept, lost}, "both conversations indexed")
    ok(tenant.api.delete(f"/api/v1/agents/{b.id}"))
    assert found() == {kept, lost}
    assert ok(mine.api().get(f"{V}/{view['id']}"))["filter"]["filter"]["groups"][0]["atoms"][0]["value"] == [a.id]
    assert len(ok(mine.api().get(run, params={"limit": 1}))["data"]) == 1
    error(agent(tenant).api().get(f"{V}/{make(mine.api(), 'Mine')['id']}/conversations"), 403)


def test_a_shared_view_does_not_name_its_owner_among_its_agents(tenant):
    sup, other = agent(tenant, role="supervisor"), agent(tenant)
    error(
        sup.api().post(V, json={"name": "Me", "type": "shared", "agents": [sup.id, other.id]}), 422, "owner_in_shares"
    )
    view = make(sup.api(), "Mine", type="shared", agents=[other.id])
    error(sup.api().patch(f"{V}/{view['id']}", json={"agents": [sup.id]}), 422, "owner_in_shares")
    assert ok(sup.api().get(f"{V}/{view['id']}"))["agents"] == [other.id]


def test_a_view_stored_with_a_filter_key_the_search_lacks_still_runs_and_new_input_is_refused(world, tenant):
    mine = agent(tenant, role="admin")
    view = make(mine.api())
    with psycopg2.connect(world._env["DATABASE_URL"]) as db, db.cursor() as cur:
        cur.execute(
            "UPDATE hs.config SET body = jsonb_set(body, '{filter}', to_jsonb(%s::text)) WHERE id = %s",
            (json.dumps({**FILTER, "gone": 1}), view["id"]),
        )
    ok(mine.api().get(f"{V}/{view['id']}/conversations"))
    error(mine.api().post(V, json={"name": "New", "filter": {**FILTER, "gone": 1}}), 422)
