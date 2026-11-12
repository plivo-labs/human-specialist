"""Dispositions and sub-dispositions, note_required, locked system dispositions, and disabling."""

from tests.acceptance.conftest import connect, disposition, disposition_named, dispositions, error, ok, refused, wrap_up
from tests.acceptance.test_reports import report, settled

D = "/api/v1/dispositions"


def in_wrap_up(tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    return live


def test_a_wrap_up_records_disposition_sub_disposition_and_note(tenant):
    sale = disposition(tenant, "Sale")
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    rows = {d["id"]: d for d in dispositions(tenant.api)}
    assert rows[upsell["id"]]["parent_disposition_id"] == sale["id"] and rows[sale["id"]]["system"] is False
    live = in_wrap_up(tenant)
    ok(wrap_up(live.agent, live.id, sale["id"], sub_disposition_id=upsell["id"], note="call back Friday"))
    done = live.stream.wait_for("conversation.wrapped_up", conversation_id=live.id)
    assert (done["data"]["disposition_id"], done["data"]["note"]) == (sale["id"], "call back Friday")
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    assert closed["data"]["dispositions"][0]["sub_disposition_id"] == upsell["id"]


def test_a_wrap_up_needs_a_disposition_and_a_note_when_required(tenant):
    callback = disposition(tenant, "Callback", note_required=True)
    live = in_wrap_up(tenant)
    error(live.agent.api().post(f"/api/v1/conversations/{live.id}/wrap-up", json={"note": "no code"}), 422)
    error(wrap_up(live.agent, live.id, callback["id"]), 422)
    error(wrap_up(live.agent, live.id, callback["id"], note=""), 422)
    ok(wrap_up(live.agent, live.id, callback["id"], note="Tomorrow 10am"))


def test_system_dispositions_cannot_be_deleted_or_renamed(tenant):
    created = disposition(tenant, "No Answer", system=True)
    assert created["system"] is True
    for locked in (disposition_named(tenant, "Queue timeout"), created):
        refused(tenant.api.delete(f"{D}/{locked['id']}"))
        refused(tenant.api.patch(f"{D}/{locked['id']}", json={"name": "Renamed"}))
        assert ok(tenant.api.get(f"{D}/{locked['id']}"))["name"] == locked["name"]


def test_a_system_disposition_sent_back_unchanged_is_not_a_change(tenant):
    locked = disposition_named(tenant, "Queue timeout")
    same = {k: locked[k] for k in ("name", "parent_disposition_id", "note_required", "disabled")}
    ok(tenant.api.patch(f"{D}/{locked['id']}", json=same))
    refused(tenant.api.patch(f"{D}/{locked['id']}", json=same | {"disabled": not same["disabled"]}))


def test_agents_do_not_see_system_dispositions(tenant):
    disposition(tenant, "Sale")
    agent = tenant.agent().login(available=False)
    assert [d["name"] for d in dispositions(agent.api())] == ["Sale"]
    assert any(d["system"] for d in dispositions(tenant.api))


def test_a_regular_disposition_can_be_renamed_and_deleted(tenant):
    d = disposition(tenant, "Sael")
    ok(tenant.api.patch(f"{D}/{d['id']}", json={"name": "Sale"}))
    assert ok(tenant.api.get(f"{D}/{d['id']}"))["name"] == "Sale"
    ok(tenant.api.delete(f"{D}/{d['id']}"))
    error(tenant.api.get(f"{D}/{d['id']}"), 404)


def test_a_wrap_up_with_another_tenants_disposition_is_422(world, tenant):
    foreign = disposition(world.new_tenant("Other"), "Theirs")
    live = in_wrap_up(tenant)
    error(wrap_up(live.agent, live.id, foreign["id"]), 422)


def test_reorder_moves_the_listed_dispositions_into_the_places_they_held(world, tenant):
    a, b, c = (disposition(tenant, n) for n in ("A", "B", "C"))
    ok(tenant.api.post(f"{D}/reorder", json={"ids": [c["id"], a["id"]]}))
    assert [d["name"] for d in dispositions(tenant.api) if d["name"] in {"A", "B", "C"}] == ["C", "B", "A"]
    foreign = disposition(world.new_tenant("Other"), "Theirs")["id"]
    error(tenant.api.post(f"{D}/reorder", json={"ids": [foreign, b["id"]]}), 422)


def names(client) -> list[str]:
    return [d["name"] for d in dispositions(client)]


def test_a_disabled_disposition_and_its_sub_dispositions_leave_the_agents_list_and_refuse_a_wrap_up(tenant):
    sale = disposition(tenant, "Sale")
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    ok(tenant.api.patch(f"{D}/{sale['id']}", json={"disabled": True}))
    assert [ok(tenant.api.get(f"{D}/{d['id']}"))["disabled"] for d in (sale, upsell)] == [True, True]
    error(tenant.api.patch(f"{D}/{upsell['id']}", json={"disabled": False}), 409, "parent_disabled")
    error(tenant.api.patch(f"{D}/{disposition_named(tenant, 'Dropped')['id']}", json={"disabled": True}), 409)
    live = in_wrap_up(tenant)
    assert {"Sale", "Upsell"}.isdisjoint(names(live.agent.api()))
    error(wrap_up(live.agent, live.id, sale["id"]), 422, "disposition_disabled")
    ok(tenant.api.patch(f"{D}/{sale['id']}", json={"disabled": False}))
    assert {"Sale", "Upsell"} <= set(names(live.agent.api()))
    ok(wrap_up(live.agent, live.id, sale["id"], sub_disposition_id=upsell["id"]))


def test_an_unchanged_disabled_value_leaves_a_sub_disposition_disabled_on_its_own_alone(tenant):
    sale = disposition(tenant, "Sale")
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    ok(tenant.api.patch(f"{D}/{upsell['id']}", json={"disabled": True}))
    ok(tenant.api.patch(f"{D}/{sale['id']}", json={"disabled": False}))
    assert ok(tenant.api.get(f"{D}/{upsell['id']}"))["disabled"] is True


def test_a_sub_disposition_sits_one_level_under_a_top_level_disposition(tenant):
    sale, other, lone = (disposition(tenant, n) for n in ("Sale", "Other", "Lone"))
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    error(tenant.api.post(D, json={"name": "Deeper", "parent_disposition_id": upsell["id"]}), 422, "invalid_parent")

    def move(row: dict, parent: dict):
        return tenant.api.patch(f"{D}/{row['id']}", json={"parent_disposition_id": parent["id"]})

    for row, parent in ((lone, lone), (sale, upsell), (sale, other), (lone, upsell)):
        error(move(row, parent), 422, "invalid_parent")
    assert ok(move(upsell, other))["parent_disposition_id"] == other["id"]
    assert ok(move(lone, sale))["parent_disposition_id"] == sale["id"]


def test_a_deleted_disposition_leaves_lists_and_wrap_ups_and_keeps_its_conversations_in_reports(tenant):
    sale = disposition(tenant, "Sale")
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    live = in_wrap_up(tenant)
    ok(wrap_up(live.agent, live.id, sale["id"], sub_disposition_id=upsell["id"]))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    settled(tenant, "conversations", lambda rs: any(r["disposition_id"] == sale["id"] for r in rs), by="disposition")
    ok(tenant.api.delete(f"{D}/{sale['id']}"))
    for gone in (sale, upsell):
        error(tenant.api.get(f"{D}/{gone['id']}"), 404)
    assert {"Sale", "Upsell"}.isdisjoint(names(tenant.api))
    [row] = [r for r in report(tenant, "conversations", by="disposition") if r["disposition_id"] == sale["id"]]
    assert (row["disposition"], row["total"]) == ("Sale", 1)
    later = in_wrap_up(tenant)
    error(wrap_up(later.agent, later.id, sale["id"]), 422, "foreign_id")


def test_a_closed_conversation_names_its_disposition_and_sub_disposition_after_they_are_deleted(tenant):
    sale = disposition(tenant, "Sale")
    upsell = disposition(tenant, "Upsell", parent_disposition_id=sale["id"])
    live = in_wrap_up(tenant)
    ok(wrap_up(live.agent, live.id, sale["id"], sub_disposition_id=upsell["id"]))
    [told] = live.stream.wait_for("conversation.closed", conversation_id=live.id)["data"]["dispositions"]
    assert (told["disposition"], told["sub_disposition"]) == ("Sale", "Upsell")
    ok(tenant.api.delete(f"{D}/{sale['id']}"))
    [read] = ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))["dispositions"]
    assert (read["disposition_id"], read["disposition"], read["sub_disposition"]) == (sale["id"], "Sale", "Upsell")
