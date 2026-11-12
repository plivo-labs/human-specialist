"""The conversation reports a contact center console draws: each direction's answered, unanswered and failed split,
handle, wrap and transfer numbers, day and week buckets in the reader's zone, the latest disposition, CSAT, text
replies, callbacks raised from a conversation, a supervisor's tenant-wide numbers, and one row per conversation with
its agent and numbers, newest first."""

import math
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from tests.acceptance.channels import close, enqueue_text, reply, take, text_line
from tests.acceptance.conftest import (
    connect,
    disposition,
    error,
    finish,
    ok,
    replayed,
    run_jobs_until,
    seen,
    set_status,
    settings,
    status_changes,
    team,
    unavailable,
    wrap_up,
)
from tests.acceptance.test_closed_search import indexed, search
from tests.acceptance.test_in_call_controls import act, to_agent
from tests.acceptance.test_reports import outbound_closed, window


def conversations(client, **params) -> list[dict]:
    return ok(client.get("/api/v1/reports/conversations", params=window(**params)))["data"]


def one(rows: list[dict], **keys) -> dict:
    [row] = [r for r in rows if all(r[k] == v for k, v in keys.items())]
    return row


def unanswered(world, tenant) -> str:
    """An inbound call whose queue times out with no agent free."""
    number, _ = tenant.inbound_line(unavailable(tenant), wait_timeout_sec=1)
    stream, caller = tenant.stream(after=0), f"+1415555{random.randint(1000, 9999)}"
    world.plivo.call(number, from_=caller)
    cid = stream.wait_for("conversation.queued", **{"from": caller})["conversation_id"]
    run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "the queue timeout")
    return cid


def test_each_direction_splits_into_answered_unanswered_and_failed(world, tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    agent = tenant.agent().login()
    live = connect(tenant, agent)
    live.caller.hangup()
    run_jobs_until(world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "auto wrap-up")
    unanswered(world, tenant)
    for behaviour in ("answer", "busy", "fail"):
        outbound_closed(tenant, agent, behaviour)
    rows = conversations(tenant.api, by="direction")
    counts = ("total", "answered", "unanswered", "failed", "closed", "unassigned")
    assert [one(rows, direction="inbound")[k] for k in counts] == [2, 1, 1, 0, 2, 1]
    assert [one(rows, direction="outbound")[k] for k in counts] == [3, 1, 1, 1, 3, 0]
    [both] = conversations(tenant.api, direction="inbound")
    assert (both["total"], both["direction"], both["abandoned"]) == (2, None, 0)


def test_a_transferred_call_counts_its_transfer_and_its_handle_and_wrap_time(tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id)
    live.caller.hangup()
    run_jobs_until(tenant.world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "closed")
    [row] = conversations(tenant.api)
    assert (row["total"], row["transferred"], row["transfers"], row["short"]) == (1, 1, 1, 1)
    assert row["avg_handle_sec"] >= row["avg_wrap_sec"] >= 0


def test_buckets_follow_the_zone_and_a_week_starts_on_sunday(tenant):
    india = "Asia/Kolkata"
    finish(connect(tenant))
    today = datetime.now(ZoneInfo(india)).date()
    [day] = conversations(tenant.api, tz=india, bucket="day", by="channel")
    assert (day["channel"], day["total"]) == ("call", 1)
    assert datetime.fromisoformat(day["bucket"]).astimezone(ZoneInfo(india)).date() == today
    [week] = conversations(tenant.api, tz=india, bucket="week")
    start = datetime.fromisoformat(week["bucket"]).astimezone(ZoneInfo(india))
    assert (start.hour, start.weekday()) == (0, 6) and (today - start.date()).days < 7


def test_the_latest_disposition_is_the_agents_last_wrap_up_or_the_system_one(world, tenant):
    sale = disposition(tenant, "Sale")
    finish(connect(tenant), sale["id"])
    unanswered(world, tenant)
    rows = conversations(tenant.api, by="disposition")
    assert one(rows, disposition="Sale")["total"] == 1 and one(rows, disposition_id=sale["id"])["total"] == 1
    assert one(rows, disposition="Queue timeout")["total"] == 1


def test_csat_averages_the_numeric_answers_and_filters_by_score(tenant):
    for score in ("5", "3", None):
        live = connect(tenant)
        finish(live)
        ok(tenant.api.post(f"/api/v1/conversations/{live.id}/csat", json={"score": score}))
    [row] = conversations(tenant.api)
    assert (row["total"], row["avg_csat"]) == (3, 4.0)
    [five] = conversations(tenant.api, csat=["5", "No Response"])
    assert five["total"] == 2


def test_a_live_text_the_agent_answered_counts_replied_and_waiting(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, "+14155550731", "Hi")
    cid = take(stream, agent, **{"from": "+14155550731"})
    reply(agent, cid, "Hello, how can I help?")
    [row] = conversations(tenant.api, by="state")
    assert (row["state"], row["replied"], row["waiting"], row["live"]) == ("waiting", 1, 1, 0)
    assert row["avg_first_reply_sec"] >= 0
    world.plivo.text(number, "+14155550731", "My order is late")
    stream.wait_for("message.received", conversation_id=cid, text="My order is late")
    [row] = conversations(tenant.api, by="state")
    assert (row["state"], row["waiting"], row["live"]) == ("live", 0, 1)
    close(agent, cid, disposition(tenant)["id"], stream)
    [row] = conversations(tenant.api, channel="sms")
    assert (row["closed"], row["replied"]) == (1, 1)


def test_of_two_closed_texts_only_the_one_an_agent_answered_counts_a_reply(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    did = disposition(tenant)["id"]
    for customer, answer in (("+14155550711", "Hello"), ("+14155550712", None)):
        world.plivo.text(number, customer, "Hi")
        cid = take(stream, agent, **{"from": customer})
        if answer:
            reply(agent, cid, answer)
        close(agent, cid, did, stream)
    [row] = conversations(tenant.api, channel="sms")
    assert (row["closed"], row["replied"]) == (2, 1) and row["avg_first_reply_sec"] >= 0


def test_callbacks_raised_from_a_conversation_count_on_it(tenant):
    live = connect(tenant)
    callback = {"type": "callback", "title": "Call back", "customer": "+14155550741"}
    callback |= {"target": to_agent(live.agent), "conversation_id": live.id}
    ok(live.agent.api().post("/api/v1/tasks", json=callback))
    finish(live)
    calls = one(conversations(tenant.api, by="channel"), channel="call")
    assert (calls["total"], calls["callbacks"]) == (1, 1)


def test_a_supervisor_reads_report_numbers_for_the_whole_tenant(tenant):
    mine, theirs = team(tenant, "Mine"), team(tenant, "Theirs")
    sup = tenant.agent("Sup", role="supervisor", teams=[mine]).login(available=False)
    finish(connect(tenant, tenant.agent("Other", teams=[theirs]).login()))
    [row] = conversations(sup.api())
    assert row["total"] == 1
    assert tenant.agent("Plain").login().api().get("/api/v1/reports/conversations", params=window()).status_code == 403


def test_a_supervisor_reads_every_agents_report_numbers_across_the_tenant(tenant):
    mine, theirs = team(tenant, "Mine"), team(tenant, "Theirs")
    sup = tenant.agent("Sup", role="supervisor", teams=[mine]).login(available=False)
    other = tenant.agent("Other", teams=[theirs]).login()
    status_changes(tenant, other, "Away")
    client = sup.api()

    def agents(name: str, **params) -> set[str]:
        rows = ok(client.get(f"/api/v1/reports/{name}", params=window(**params)))["data"]
        return {r["agent_id"] for r in rows}

    assert other.id in agents("agent-activity") and other.id in agents("agent-status-log")


def test_availability_counts_available_and_away_time_who_became_available_and_logs_each_change(tenant):
    avery, blake = tenant.agent("Avery Stone"), tenant.agent("Blake")
    status_changes(tenant, avery, "Available", "Away")
    status_changes(tenant, blake, "Break")
    client = tenant.agent("Sup", role="supervisor", teams=[team(tenant, "Other")]).login(available=False).api()

    def read(name: str, **params) -> dict:
        return ok(client.get(f"/api/v1/reports/{name}", params=window(**params)))

    [row] = read("agent-activity", agent_name="avery")["data"]
    assert row["agent_id"] == avery.id and {"Available", "Away"} <= set(row["statuses"])
    assert row["available_sec"] == row["statuses"]["Available"] and row["away_sec"] >= row["statuses"]["Away"]
    [day] = read("available-agents", bucket="day", agent_name="a")["data"]
    assert day["agents"] == 1
    log = read("agent-status-log", agent_name="AVERY")["data"]
    assert [(e["status"], e["available"], e["agent_name"]) for e in log[:2]] == [
        ("Away", False, "Avery Stone"),
        ("Available", True, "Avery Stone"),
    ]
    assert log[1]["duration_sec"] >= 0 and log[0]["created_at"] >= log[1]["created_at"]


def test_an_agent_moved_off_a_deleted_status_logs_the_move_and_keeps_the_old_name(tenant):
    lunch = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Lunch", "availability": False}))
    agent = tenant.agent("Lee")
    ok(set_status(tenant, agent, "Lunch"))
    ok(tenant.api.delete(f"/api/v1/agent-statuses/{lunch['id']}"))
    log = ok(tenant.api.get("/api/v1/reports/agent-status-log", params=window(agent_id=agent.id)))["data"]
    assert [(e["status"], e["available"]) for e in log] == [("Break", False), ("Lunch", False)]


def rows(client, **params) -> dict:
    return ok(client.get("/api/v1/reports/conversations/rows", params=window(**params)))


def test_the_rows_list_each_conversation_newest_first_with_its_agent_and_numbers(world, tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("decline")
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    taken = stream.wait_for("offer.accepted", agent_id=second.id)["conversation_id"]
    caller.hangup()
    run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=taken), "closed")
    lost = unanswered(world, tenant)
    page = rows(tenant.api, limit=1)
    [newest] = page["data"]
    assert (newest["id"], newest["resolution"], newest["agent_name"]) == (lost, "unanswered", None)
    assert newest["disposition"] == "Queue timeout" and newest["unanswered_wait_sec"] >= 0
    [older] = rows(tenant.api, limit=1, cursor=page["next_cursor"])["data"]
    assert (older["id"], older["resolution"], older["agent_name"], older["missed_offers"]) == (
        taken,
        "answered",
        "Second",
        1,
    )
    assert (older["customer"], older["ended_by"], older["hold_sec"], older["disposition"]) == (
        caller.from_,
        "customer",
        0,
        "Auto-closed",
    )
    assert older["handle_sec"] >= older["wrap_sec"] >= 0 and older["response_sec"] >= 0


def test_each_caller_hold_counts_and_lasts_in_the_rows_and_search_with_a_hold_the_call_ended_in(world, tenant):
    live = connect(tenant)
    client = live.agent.api()

    def held(times: int) -> None:
        world.wait(lambda: len(seen(live.stream, "call.held", conversation_id=live.id)) == times, what=f"hold {times}")

    ok(act(client, live.id, "hold", {"on": True}))
    held(1)
    world.travel(30)
    ok(act(client, live.id, "hold", {"on": False}))
    live.stream.wait_for("call.resumed", conversation_id=live.id)
    ok(act(client, live.id, "hold", {"on": True}))
    held(2)
    world.travel(20)
    finish(live)
    row = one(rows(tenant.api)["data"], id=live.id)
    assert 50 <= row["hold_sec"] <= 55 and row["hold_count"] == 2
    indexed(tenant, [live.id])
    assert search(tenant, hold_sec={"gte": 50}) == [live.id]


def test_a_hold_and_a_resume_plivo_applied_without_answering_are_counted_on_their_retries(world, tenant):
    live = connect(tenant)
    for on, event in ((True, "call.held"), (False, "call.resumed")):
        world.plivo.fault("lose", lambda r, on=on: r.params.get("hold") is on, account=tenant.auth_id)
        error(act(live.agent.api(), live.id, "hold", {"on": on}), 503, "dependency_unavailable")
        run_jobs_until(world, lambda e=event: seen(live.stream, e, conversation_id=live.id), f"{event} on its retry")
        world.travel(30 if on else 20)
    finish(live)
    row = one(rows(tenant.api)["data"], id=live.id)
    assert row["hold_count"] == 1 and 30 <= row["hold_sec"] <= 34


def cold_transfer(tenant, live) -> None:
    target = tenant.agent("Target").login()
    billing = tenant.queue(name="Billing", agents=[target.id])
    moved = {"mode": "cold", "target": {"type": "queue", "queue_id": billing["id"]}}
    ok(act(live.agent.api(), live.id, "transfers", moved))
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)


def caller_hangs_up(tenant, live) -> None:
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)


@pytest.mark.parametrize("leave", [cold_transfer, caller_hangs_up], ids=["transfer", "hangup"])
@pytest.mark.parametrize("on", [True, False], ids=["hold", "resume"])
def test_a_hold_or_resume_plivo_confirms_after_the_caller_left_tells_and_counts_nothing(world, tenant, leave, on):
    live = connect(tenant)
    if not on:
        ok(act(live.agent.api(), live.id, "hold", {"on": True}))
        live.stream.wait_for("call.held", conversation_id=live.id)
        world.travel(30)
    stall = world.plivo.fault("stall", lambda r: r.params.get("hold") is on, account=tenant.auth_id)
    with ThreadPoolExecutor(1) as pool:
        holding = pool.submit(act, live.agent.api(), live.id, "hold", {"on": on})
        world.wait(lambda: stall.hits, what="the request stalled at Plivo")
        leave(tenant, live)
        stall.release()
        assert holding.result(15).status_code == 204
    world.travel(20)
    assert not replayed(tenant, "call.held" if on else "call.resumed", conversation_id=live.id)
    if leave is cold_transfer:
        caller_hangs_up(tenant, live)
    row = one(rows(tenant.api)["data"], id=live.id)
    if on:
        assert (row["hold_count"], row["hold_sec"]) == (0, 0)
    else:
        assert row["hold_count"] == 1 and 30 <= row["hold_sec"] <= 34


@pytest.mark.parametrize("surface", ["read", "list", "rows", "search"])
def test_a_deleted_queues_conversations_keep_its_name_on_every_history_read(tenant, surface):
    agent = tenant.agent().login()
    stream, billing = tenant.stream(after=0), tenant.queue(name="Billing", agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, billing, "chat", "visitor-q", "support-inbox", text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    close(agent, cid, disposition(tenant)["id"], stream)
    indexed(tenant, [cid])
    ok(tenant.api.delete(f"/api/v1/queues/{billing['id']}"))
    read = {
        "read": lambda: ok(tenant.api.get(f"/api/v1/conversations/{cid}")),
        "list": lambda: one(ok(tenant.api.get("/api/v1/conversations"))["data"], id=cid),
        "rows": lambda: one(rows(tenant.api)["data"], id=cid),
        "search": lambda: one(ok(tenant.api.post("/api/v1/conversations/search", json={}))["data"], id=cid),
    }[surface]()
    assert (read["queue_id"], read["queue_name"]) == (billing["id"], "Billing")


def test_a_conversation_indexed_after_its_queue_was_deleted_is_found_under_its_name(tenant):
    agent = tenant.agent().login()
    stream, billing = tenant.stream(after=0), tenant.queue(name="Billing", agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, billing, "chat", "visitor-q", "support-inbox", text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    ok(tenant.api.delete(f"/api/v1/queues/{billing['id']}"))
    close(agent, cid, disposition(tenant)["id"], stream)
    indexed(tenant, [cid])
    hit = one(ok(tenant.api.post("/api/v1/conversations/search", json={}))["data"], id=cid)
    assert (hit["queue_id"], hit["queue_name"]) == (billing["id"], "Billing")


def test_search_filters_on_a_calls_hold_count_and_returns_it(world, tenant):
    live = connect(tenant)
    for on in (True, False, True, False):
        ok(act(live.agent.api(), live.id, "hold", {"on": on}))
    world.wait(lambda: len(seen(live.stream, "call.resumed", conversation_id=live.id)) == 2, what="two holds")
    finish(live)
    indexed(tenant, [live.id])
    body = {"hold_count": {"gte": 2}, "fields": ["hold_count"]}
    [hit] = ok(tenant.api.post("/api/v1/conversations/search", json=body))["data"]
    assert (hit["id"], hit["fields"]) == (live.id, {"hold_count": 2})


@pytest.mark.parametrize("hangup", ["late", "lost"])
def test_a_caller_who_leaves_on_hold_stops_holding_when_its_exit_is_known(world, tenant, hangup):
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "hold", {"on": True}))
    live.stream.wait_for("call.held", conversation_id=live.id)
    world.travel(30)
    if hangup == "late":
        ends = world.plivo.fault("delay", lambda r: r.path.endswith("/hangup"), account=tenant.auth_id, times=math.inf)
        live.caller.hangup()
    else:
        ends = world.plivo.fault("drop", lambda r: True, account=tenant.auth_id, times=math.inf)
        live.caller.hangup()
        live.caller.wait_state("completed")
        ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hangup"))
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.travel(20)
    ends.release()
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    row = one(rows(tenant.api)["data"], id=live.id)
    assert row["hold_count"] == 1 and 30 <= row["hold_sec"] <= 34


def walked(client, path: str, **params) -> list[dict]:
    got = ok(client.get(f"/api/v1/reports/{path}", params=window(limit=1, **params)))
    out = got["data"]
    while got["next_cursor"]:
        got = ok(client.get(f"/api/v1/reports/{path}", params=window(limit=1, cursor=got["next_cursor"], **params)))
        out += got["data"]
    return out


def test_the_paged_reports_page_through_every_row_to_the_last(tenant):
    agent = unavailable(tenant)
    status_changes(tenant, agent, "Available", "Away", "Break")
    queue = tenant.queue(agents=[agent.id])
    for visitor in ("visitor-r1", "visitor-r2", "visitor-r3"):
        ok(enqueue_text(tenant.api, queue, "chat", visitor, "support-inbox", text="Hi"))
    every = rows(tenant.api, limit=200)["data"]
    assert len(every) == 3 and walked(tenant.api, "conversations/rows") == every
    log = ok(tenant.api.get("/api/v1/reports/agent-status-log", params=window(agent_id=agent.id, limit=200)))["data"]
    assert len(log) >= 3 and walked(tenant.api, "agent-status-log", agent_id=agent.id) == log


def test_a_team_supervisor_lists_the_rows_of_its_teams_and_their_outbound_calls(tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    mine, theirs = team(tenant, "Mine"), team(tenant, "Theirs")
    sup = tenant.agent("Sup", role="supervisor", teams=[mine]).login(available=False)
    mate = tenant.agent("Mate", teams=[mine]).login()
    dialed = outbound_closed(tenant, mate, "answer")
    live = connect(tenant, tenant.agent("Other", teams=[theirs]).login())
    live.caller.hangup()
    run_jobs_until(tenant.world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "closed")
    assert [r["id"] for r in rows(sup.api())["data"]] == [dialed]
    assert len(rows(tenant.api)["data"]) == 2
