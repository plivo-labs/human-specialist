"""Dispositions and sub-dispositions, note_required, and locked system dispositions."""

from tests.acceptance.conftest import connect, disposition, disposition_named, dispositions, error, ok, refused, wrap_up

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
