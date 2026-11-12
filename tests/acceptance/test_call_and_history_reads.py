from datetime import datetime

from tests.acceptance.channels import accept, close, offered, take
from tests.acceptance.conftest import connect, disposition, error, lead, ok, team, wrap_up
from tests.acceptance.test_text_channels import CUSTOMER, live_text, text_transfer, whatsapp_in
from tests.acceptance.test_voice_calls import act, on_hold, to_agent, to_number


def on_it(client, cid: str) -> list[dict]:
    return ok(client.get(f"/api/v1/conversations/{cid}"))["assignments"]


def listed(agent, **params) -> list[dict]:
    return ok(agent.api().get("/api/v1/conversations", params={"state": "active", **params}))["data"]


def at(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def test_a_call_reads_its_agent_its_supervisors_mode_and_the_agents_wrap_up(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    live = connect(tenant, tenant.agent("Agent", teams=[t]).login())
    took = live.stream.wait_for("offer.accepted", conversation_id=live.id)["data"]["offer_id"]
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor", json={"mode": "listen"}))
    sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    shown = [(a["agent_id"], a["role"], a["via"], a["mode"], a["state"]) for a in on_it(sup.api(), live.id)]
    assert shown == [
        (live.agent.id, "primary", "queue", None, "active"),
        (sup.id, "monitor", "monitor", "listen", "active"),
    ]
    assert on_it(live.agent.api(), live.id)[0]["id"] == took
    ok(sup.api().delete(f"/api/v1/calls/{live.id}/monitor"))
    live.stream.wait_for("supervisor.left", conversation_id=live.id)
    live.caller.hangup()
    deadline = live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)["data"]["deadline_at"]
    [wrapping] = on_it(tenant.api, live.id)
    assert (wrapping["agent_id"], wrapping["state"], at(wrapping["wrap_up_until"])) == (
        live.agent.id,
        "wrap_up",
        at(deadline),
    )
    assert at(wrapping["started_at"]) < at(wrapping["ended_at"])
    assert [c["assignments"] for c in listed(live.agent, state="in_wrap_up")] == [[wrapping]]


def test_a_desk_removes_a_participant_and_switches_to_a_transfers_target_by_the_ids_it_reads(world, tenant):
    target = tenant.agent("Target").login()
    number, phone = lead(world)
    live = connect(tenant)

    def joined(**match) -> dict:
        return world.wait(
            lambda: next((a for a in on_it(live.agent.api(), live.id) if a.items() >= match.items()), None),
            what=f"an assignment with {match}",
        )

    ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))
    phone.wait_ring().wait_state("in_progress")
    added = joined(number=number)
    assert (added["agent_id"], added["role"], added["via"], added["transfer_id"]) == (
        None,
        "external",
        "participant",
        None,
    )
    ok(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{added['id']}"))
    live.stream.wait_for("call.participant_removed", conversation_id=live.id, participant_id=added["id"])
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    taking = joined(agent_id=target.id)
    assert (taking["via"], taking["mode"], taking["transfer_id"]) == ("transfer", "warm", tid)
    ok(act(live.agent.api(), live.id, "switch", {"participant_id": taking["transfer_id"]}))
    on_hold(tenant, live.caller)
    on_hold(tenant, leg, False)


def test_an_agent_lists_the_conversations_it_is_on_now_and_not_one_it_handed_on(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="completed")

    def on_now(agent) -> list[str]:
        return [c["id"] for c in listed(agent, participant_agent_id=agent.id)]

    assert on_now(first) == on_now(target) == [cid]
    ok(wrap_up(first, cid, disposition(tenant)["id"]))
    assert (on_now(first), on_now(target)) == ([], [cid])
    assert [c["id"] for c in listed(first, agent_id=first.id)] == [cid]


def test_a_whatsapp_template_or_location_reads_as_the_rows_last_message(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")

    def last() -> dict:
        return next(c for c in listed(agent) if c["id"] == cid)["last_message"]

    template = {"name": "order_update", "language": "en_US"}
    sent = ok(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"template": template}))
    assert (last()["id"], last()["template"], last()["whatsapp"]) == (sent["id"], template, None)
    where = {"Latitude": "37.7749", "Longitude": "-122.4194", "Name": "Head office", "Address": "1 Main St"}
    ok(whatsapp_in(tenant, number, "location", Location=where))
    stream.wait_for("message.received", conversation_id=cid, channel="whatsapp", text=None)
    assert (last()["template"], last()["whatsapp"]) == (None, {"type": "location", "location": where})


def test_a_conversation_is_found_by_the_attributes_a_caller_kept_on_it(world, tenant):
    cid, _, _ = live_text(world, tenant, tenant.agent().login())
    other, _, _ = live_text(world, tenant, tenant.agent("Other").login(), customer="+14155550902")
    ok(tenant.api.patch(f"/api/v1/conversations/{cid}", json={"attributes": {"case_id": "C-4521", "crm_id": "L:7"}}))
    ok(tenant.api.patch(f"/api/v1/conversations/{other}", json={"attributes": {"case_id": "C-9"}}))

    def found(*pairs: str):
        return tenant.api.get("/api/v1/conversations", params={"attribute": list(pairs)})

    assert [c["id"] for c in ok(found("case_id:C-4521"))["data"]] == [cid]
    assert [c["id"] for c in ok(found("case_id:C-4521", "crm_id:L:7"))["data"]] == [cid]
    assert ok(found("case_id:C-4521", "crm_id:L:8"))["data"] == []
    error(found("case_id"), 422)


def test_a_customers_history_reads_by_last_activity_with_its_counts(world, tenant):
    agent = tenant.agent().login()
    first, number, stream = live_text(world, tenant, agent)
    close(agent, first, disposition(tenant)["id"], stream)
    world.plivo.text(number, CUSTOMER, "Me again")
    second = take(
        stream, agent, conversation_id=stream.wait_for("message.received", text="Me again")["conversation_id"]
    )
    since = ok(tenant.api.get(f"/api/v1/conversations/{second}"))["last_activity_at"]
    ok(agent.api().post(f"/api/v1/conversations/{first}/notes", json={"text": "Called back about the refund"}))

    def history(**params) -> list[str]:
        found = ok(tenant.api.get("/api/v1/conversations", params={"number": CUSTOMER, "limit": 1, **params}))
        rest = found["next_cursor"] and history(cursor=found["next_cursor"], **params)
        return [c["id"] for c in found["data"]] + (rest or [])

    assert history() == [second, first]
    assert history(sort="last_activity_at") == [first, second]
    assert history(sort="last_activity_at", active_after=since) == [first]
    counts = ok(tenant.api.get("/api/v1/conversations/counts", params={"number": CUSTOMER}))
    assert counts == {"total": 2, "open": 1, "closed": 1}
    error(tenant.api.get("/api/v1/conversations", params={"sort": "last_activity_at"}), 422, "number_required")
    error(tenant.api.get("/api/v1/conversations/counts"), 422, "number_required")
