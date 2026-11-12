"""Closed-conversation search as a contact center uses it: word and phrase matches over more fields, wrap-up notes and
disposition names, metric and keyword filters with their operators, grouped AND and OR filters, the time of day,
sorting, supervisors' view of unanswered work on a queue's route or an enqueue's own, hidden chats, the index sweep,
and the agent wrap time reports keep."""

import math
import time
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from tests.acceptance.channels import close, enqueue_text, messages, reply, take, text_line
from tests.acceptance.conftest import (
    connect,
    dial,
    disposition,
    error,
    fence,
    finish,
    hours,
    lead,
    noon_zone,
    ok,
    queue_action,
    run_jobs_until,
    status_changes,
    team,
    unavailable,
    voicemail_action,
    wrap_up,
)
from tests.acceptance.test_in_call_controls import act, to_agent
from tests.acceptance.test_reports import report
from tests.acceptance.test_search import closed_outbound
from tests.acceptance.test_timeline import timeline

INBOX = "support-inbox"


def search(tenant, **body) -> list[str]:
    found = ok(tenant.api.post("/api/v1/conversations/search", json=body))
    return [c["id"] for c in found["data"]]


def indexed(tenant, cids: list[str]) -> None:
    run_jobs_until(tenant.world, lambda: set(cids) <= set(search(tenant)), "the closed conversations indexed")


def closed_chat(tenant, agent, stream, visitor: str, *replies: str, text: str = "Hi", done: str | None = None) -> str:
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", visitor, INBOX, text=text))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    for line in replies:
        reply(agent, cid, line)
    close(agent, cid, done or disposition(tenant)["id"], stream)
    return cid


def atom(field: str, op: str, value=None) -> dict:
    return {"field": field, "op": op} | ({} if value is None else {"value": value})


def where(tenant, *atoms: dict, **body) -> list[str]:
    return search(tenant, filter={"groups": [{"atoms": list(atoms)}]}, **body)


def hold(world, seconds: float, what: str) -> None:
    """Let `seconds` of wall time pass: HS measures durations on its own clock, which no test can move."""
    since = time.monotonic()
    world.wait(lambda: time.monotonic() > since + seconds, timeout=seconds + 1, what=what)


def stamps(tenant, cid: str) -> dict[tuple[str, str | None], float]:
    """When each event type first shows on the conversation's timeline, in epoch seconds: by (type, agent id), and
    by (type, None) for any agent."""
    fence(tenant)
    out: dict = {}
    for e in timeline(tenant.api, cid):
        at = datetime.fromisoformat(e["created_at"]).timestamp()
        out.setdefault((e["type"], e["agent_id"]), at)
        out.setdefault((e["type"], None), at)
    return out


def durations(tenant, cid: str) -> dict[str, float]:
    """A closed chat's response, talk and wrap times in seconds, from the times HS reports: creation to the first
    reply, acceptance to wrap-up start, and wrap-up start to its end."""
    t = stamps(tenant, cid)
    created = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["created_at"]
    replied = next(m["created_at"] for m in messages(tenant.api, cid) if m["direction"] == "outbound")
    wrap = t["conversation.wrap_up_started", None]
    return {
        "response": (datetime.fromisoformat(replied) - datetime.fromisoformat(created)).total_seconds(),
        "talk": wrap - t["offer.accepted", None],
        "wrap": t["conversation.wrapped_up", None] - wrap,
    }


def midnight_zone() -> str:
    """An Etc/GMT zone where the local time is between 00:00 and 01:00 now."""
    offset = (12 - datetime.now(UTC).hour) % 24 - 12
    return f"Etc/GMT{-offset:+d}"


def test_one_word_matches_inside_a_word_and_several_words_match_as_a_phrase_ending_in_a_prefix(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = closed_chat(tenant, agent, stream, "visitor8", text="My parcel never arrived")
    indexed(tenant, [cid])
    assert search(tenant, text="ARCE") == [cid]
    assert search(tenant, text="visitor8") == [cid]
    assert search(tenant, text="PARCEL never arr") == [cid]
    assert search(tenant, text="parcel arrived") == []
    assert search(tenant, text="parc arriv") == []
    assert search(tenant, text="never parcel") == []


def test_a_word_in_a_wrap_up_note_or_a_disposition_name_is_searchable(world, tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant, "Escalation")["id"], note="Wants a refund"))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    indexed(tenant, [live.id])
    assert search(tenant, text="refund") == [live.id]
    assert search(tenant, text="escalation") == [live.id]


def test_closed_conversations_filter_by_counts_answered_and_result_and_sort_with_a_cursor(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    done = disposition(tenant)["id"]
    began = time.monotonic()
    short = closed_chat(tenant, agent, stream, "visitor-1", done=done)
    number, line = tenant.inbound_line(unavailable(tenant))
    caller = world.plivo.call(number)
    missed = stream.wait_for("conversation.queued", queue_id=line["id"])["conversation_id"]
    caller.hangup()
    stream.wait_for("conversation.closed", conversation_id=missed)
    span = time.monotonic() - began
    queue = tenant.queue(agents=[agent.id])
    long = ok(enqueue_text(tenant.api, queue, "chat", "visitor-2", INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=long)
    for text in ("Reply 0", "Reply 1"):
        reply(agent, long, text)
    hold(world, span + 2, "a long chat")
    close(agent, long, done, stream)
    indexed(tenant, [short, long, missed])
    # Whole seconds: the quick spans are at most int(span) + 1, the held ones at least int(span) + 2.
    most = int(span) + 1
    assert search(tenant, message_count={"gte": 2}) == [long]
    assert search(tenant, answered=False) == search(tenant, result="abandoned") == [missed]
    assert search(tenant, duration_sec={"gt": most}) == [long]
    assert set(search(tenant, duration_sec={"lte": most})) == {short, missed}
    assert search(tenant, channel="chat", sort="message_count", order="asc") == [short, long]

    def page(**params) -> dict:
        r = tenant.api.post("/api/v1/conversations/search", json={"sort": "message_count"}, params=params)
        return ok(r)

    first = page(limit=1)
    assert [c["id"] for c in first["data"]] == [long]
    assert [c["id"] for c in page(limit=1, cursor=first["next_cursor"])["data"]] == [short]


def test_a_supervisor_sees_a_call_no_agent_took_in_its_teams_queue(world, tenant):
    floor = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor]).login(available=False)
    outsider = tenant.agent("Outsider", role="supervisor", teams=[team(tenant, "Elsewhere")]).login(available=False)
    number, _ = tenant.inbound_line(teams=[floor])
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    caller.hangup()
    stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])
    assert ok(sup.api().get(f"/api/v1/conversations/{cid}"))["state"] == "closed"
    assert [c["id"] for c in ok(sup.api().get("/api/v1/conversations"))["data"]] == [cid]
    assert [c["id"] for c in ok(sup.api().post("/api/v1/conversations/search", json={}))["data"]] == [cid]
    error(outsider.api().get(f"/api/v1/conversations/{cid}"), 404)
    assert ok(outsider.api().post("/api/v1/conversations/search", json={}))["data"] == []


def test_a_supervisor_sees_a_contact_no_agent_took_on_its_teams_enqueue_route_and_not_the_queues(world, tenant):
    routed, queues = team(tenant, "Routed"), team(tenant, "Queue's")
    sup = tenant.agent("Sup", role="supervisor", teams=[routed]).login(available=False)
    outsider = tenant.agent("Outsider", role="supervisor", teams=[queues]).login(available=False)
    watch, other = (tenant.stream(token=a.stream_token, after=0) for a in (sup, outsider))
    q = tenant.queue(teams=[queues], text={"wait_timeout_sec": 60})
    route = {"teams": [routed]}
    cid = ok(enqueue_text(tenant.api, q, "chat", "visitor-90", INBOX, text="Hi", route=route))["conversation_id"]
    assert ok(sup.api().get(f"/api/v1/conversations/{cid}"))["id"] == cid
    watch.wait_for("conversation.queued", conversation_id=cid)
    error(outsider.api().get(f"/api/v1/conversations/{cid}"), 404)
    [own] = status_changes(tenant, outsider, "Away")
    other.wait_for("agent.status_changed", id=own["id"])
    assert not [e for e in other.events if e.get("conversation_id") == cid]
    world.advance(60)
    watch.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])
    for viewer, found in ((sup, [cid]), (outsider, [])):
        assert [c["id"] for c in ok(viewer.api().get("/api/v1/conversations"))["data"]] == found
        assert [c["id"] for c in ok(viewer.api().post("/api/v1/conversations/search", json={}))["data"]] == found


def test_a_transferred_calls_first_agent_wrap_ends_at_its_own_submit(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("answer")
    live = connect(tenant)
    done = disposition(tenant)["id"]
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    before = time.time()
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=cold))
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    started = time.time()
    hold(world, 2, "two seconds in wrap-up")
    submitting = time.time()
    ok(wrap_up(live.agent, live.id, done))
    submitted = time.time()
    hold(world, started - before + 4, "the target's turn, longer than the first agent's wrap could be")
    hanging = time.time()
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=target.id)
    ok(wrap_up(target, live.id, done))
    closing = time.time()
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    [row] = [r for r in report(tenant, "conversations/rows") if r["id"] == live.id]
    # Each end is stamped to the second; a first wrap that ran on into the target's turn exceeds the upper bound.
    assert submitting - started - 1 <= row["wrap_sec"] <= submitted - before + 1 + closing - hanging + 1


def test_filters_combine_in_groups_with_and_and_or(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    stream = tenant.stream(after=0)
    by_a = closed_chat(tenant, a, stream, "visitor-a")
    by_b = closed_chat(tenant, b, stream, "visitor-b")
    call = connect(tenant, a)
    call.caller.hangup()
    call.stream.wait_for("conversation.wrap_up_started", conversation_id=call.id)
    ok(wrap_up(a, call.id, disposition(tenant, "Called")["id"]))
    call.stream.wait_for("conversation.closed", conversation_id=call.id)
    indexed(tenant, [by_a, by_b, call.id])

    def grouped(*groups: dict, conditions: tuple = ()) -> set[str]:
        return set(search(tenant, filter={"groups": list(groups), "conditions": list(conditions)}))

    chat_or_call = {"atoms": [atom("channel", "is", "chat"), atom("channel", "is", "call")], "conditions": ["or"]}
    assert grouped(chat_or_call) == {by_a, by_b, call.id}
    assert grouped(chat_or_call, {"atoms": [atom("agent_id", "is", a.id)]}, conditions=("and",)) == {by_a, call.id}
    only_b = {"atoms": [atom("agent_id", "is", b.id)]}
    calls = {"atoms": [atom("channel", "is", "call")]}
    assert grouped(only_b, calls, conditions=("or",)) == {by_b, call.id}
    assert grouped({"atoms": [atom("channel", "is", [])]}) == set()
    assert grouped({"atoms": [atom("channel", "is_not", [])]}) == {by_a, by_b, call.id}


def test_an_agent_filter_takes_is_only(world, tenant):
    for op in ("is_not", "contains", "empty"):
        for body in (
            {"filter": {"groups": [{"atoms": [atom("agent_id", op, tenant.agent().id)]}]}},
            {
                "filter": {
                    "groups": [{"atoms": [atom("channel", "is", "call"), atom("agent_id", op)], "conditions": ["or"]}]
                }
            },
        ):
            error(tenant.api.post("/api/v1/conversations/search", json=body), 422, "validation_failed")


def test_a_time_of_day_filter_reads_the_viewers_zone_and_wraps_midnight(world, tenant):
    zone = midnight_zone()
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = closed_chat(tenant, agent, stream, "visitor-t")
    indexed(tenant, [cid])

    def at(window: str) -> list[str]:
        group = {"atoms": [atom("created_time", "between", window)]}
        return search(tenant, filter={"groups": [group]}, time_zone=zone)

    assert at("23:00:00,01:30:00") == [cid]
    assert at("02:00:00,22:00:00") == []


def test_a_date_filter_takes_relative_days_in_the_viewers_zone(world, tenant):
    zone = midnight_zone()
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = closed_chat(tenant, agent, stream, "visitor-d")
    indexed(tenant, [cid])

    def on(day: str) -> list[str]:
        group = {"atoms": [atom("created_date", "is", day)]}
        return search(tenant, filter={"groups": [group]}, time_zone=zone)

    assert on("today") == [cid]
    assert on("yesterday") == []
    assert on(datetime.now(ZoneInfo(zone)).date().isoformat()) == [cid]


def test_metric_filters_take_greater_and_less_than(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    done = disposition(tenant)["id"]
    began = time.monotonic()
    fast = closed_chat(tenant, agent, stream, "visitor-f", "At once", done=done)
    span = time.monotonic() - began
    queue = tenant.queue(agents=[agent.id])
    slow = ok(enqueue_text(tenant.api, queue, "chat", "visitor-s", INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=slow)
    hold(world, span + 3, "a slow first reply")
    reply(agent, slow, "Sorry for the wait")
    close(agent, slow, done, stream)
    lingering = ok(enqueue_text(tenant.api, queue, "chat", "visitor-l", INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=lingering)
    reply(agent, lingering, "At once")
    ok(agent.api().post(f"/api/v1/conversations/{lingering}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=lingering)
    hold(world, span + 3, "a long wrap-up")
    ok(wrap_up(agent, lingering, done))
    stream.wait_for("conversation.closed", conversation_id=lingering)
    indexed(tenant, [fast, slow, lingering])
    measured = {cid: durations(tenant, cid) for cid in (fast, slow, lingering)}

    def cut(metric: str, high: str, *low: str) -> int:
        """Whole seconds that `high` exceeds and no `low` does, rounded either way: the quick side's ceiling, which
        the held side must clear by a whole second."""
        edge = math.ceil(max(measured[c][metric] for c in low) + 0.01)
        assert measured[high][metric] - 0.01 >= edge + 1, f"inconclusive: {metric} sides overlap in {measured}"
        return edge

    response, talk, wrap = (
        cut("response", slow, fast, lingering),
        cut("talk", slow, fast, lingering),
        cut("wrap", lingering, fast, slow),
    )
    assert search(tenant, response_sec={"gt": response}) == [slow]
    assert set(search(tenant, response_sec={"lt": response + 1})) == {fast, lingering}
    assert search(tenant, talk_sec={"gt": talk}) == [slow]
    assert search(tenant, wrap_sec={"gt": wrap}) == [lingering]
    assert set(search(tenant, wrap_sec={"lt": wrap + 1})) == {fast, slow}
    assert search(tenant, talk_sec={"lt": talk + 1}, wrap_sec={"lt": wrap + 1}) == [fast]


def test_keyword_filters_take_is_not_contains_and_empty(world, tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant, "Sale")["id"], note="Asked for a Refund"))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    plain = connect(tenant)
    plain.caller.hangup()
    plain.stream.wait_for("conversation.wrap_up_started", conversation_id=plain.id)
    ok(wrap_up(plain.agent, plain.id, disposition(tenant, "Lost")["id"]))
    plain.stream.wait_for("conversation.closed", conversation_id=plain.id)
    indexed(tenant, [live.id, plain.id])

    def where(*atoms: dict) -> list[str]:
        return search(tenant, filter={"groups": [{"atoms": list(atoms)}]})

    assert where(atom("wrap_up_note", "contains", "refund")) == [live.id]
    assert where(atom("wrap_up_note", "empty")) == [plain.id]
    assert where(atom("wrap_up_note", "not_empty")) == [live.id]
    assert where(atom("disposition", "is", "sale")) == [live.id]
    assert where(atom("disposition", "is_not", "Sale")) == [plain.id]


def test_search_sorts_by_a_text_field(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    m, a, z = (closed_chat(tenant, agent, stream, f"visitor-{c}") for c in "maz")
    indexed(tenant, [m, a, z])
    assert search(tenant, sort="number", order="asc") == [a, m, z]
    assert search(tenant, sort="number", order="desc") == [z, m, a]


def test_an_outbound_call_the_customer_never_answers_has_no_talk_time(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, phone = lead(world, "no_answer")
    _, cid = dial(tenant, agent, to=number)
    phone.wait_ring().wait_state("completed")
    stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])
    assert search(tenant, talk_sec={"lte": 0}) == [cid]


def test_a_calls_talk_in_call_ended_is_the_talk_search_reports_whatever_zone_the_plivo_account_is_in(world, tenant):
    """The simulated account's zone is +05:30, which a Call's hangup times are in. The event ends the talk at
    Plivo's end of the call in whole seconds; search ends it when HS processes the hangup, so it reads up to 2 s
    more and never less."""
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    done = disposition(tenant)["id"]
    number, _ = tenant.inbound_line(agent)
    caller = world.plivo.call(number)
    inbound = stream.wait_for("conversation.queued")["conversation_id"]
    hold(world, 3, "the caller's wait in the queue")
    agent.set_status("Available")
    stream.wait_for("offer.accepted", conversation_id=inbound)
    hold(world, 2, "the inbound call's talk")
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=inbound)
    ok(wrap_up(agent, inbound, done))
    stream.wait_for("conversation.closed", conversation_id=inbound)
    to, phone = lead(world)
    _, outbound = dial(tenant, agent, to=to)
    stream.wait_for("call.answered", conversation_id=outbound)
    hold(world, 2, "the outbound call's talk")
    phone.legs[0].hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=outbound)
    ok(wrap_up(agent, outbound, done))
    stream.wait_for("conversation.closed", conversation_id=outbound)
    indexed(tenant, [inbound, outbound])
    hits = ok(tenant.api.post("/api/v1/conversations/search", json={"fields": ["talk_sec"]}))["data"]
    found = {h["id"]: h["fields"]["talk_sec"] for h in hits}
    for cid in (inbound, outbound):
        ended = stream.wait_for("call.ended", conversation_id=cid)["data"]
        talk = ended["talk_sec"]
        assert 1 <= talk <= ended["duration_sec"] and talk <= found[cid] <= talk + 2, (cid == inbound, ended, found)


def test_message_count_counts_messages_and_not_notes(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-n", INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    reply(agent, cid, "Hello")
    ok(agent.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "Gold customer"}))
    close(agent, cid, disposition(tenant)["id"], stream)
    indexed(tenant, [cid])
    assert search(tenant, message_count={"gte": 2, "lte": 2}) == [cid]


def test_calls_filter_by_who_hung_up(world, tenant):
    by_customer = connect(tenant)
    by_customer.caller.hangup()
    by_agent = connect(tenant)
    ok(by_agent.agent.api().post(f"/api/v1/calls/{by_agent.id}/hangup"))
    for live in (by_customer, by_agent):
        live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
        ok(wrap_up(live.agent, live.id, disposition(tenant, f"Done {live.id[:4]}")["id"]))
        live.stream.wait_for("conversation.closed", conversation_id=live.id)
    indexed(tenant, [by_customer.id, by_agent.id])
    assert search(tenant, ended_by="customer") == [by_customer.id]
    assert search(tenant, ended_by="agent") == [by_agent.id]


def test_a_chat_that_ended_with_no_message_is_not_searchable(world, tenant):
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[unavailable(tenant).id], text={"wait_timeout_sec": 60})
    silent = ok(enqueue_text(tenant.api, queue, "chat", "visitor-0", INBOX))["conversation_id"]
    spoke = ok(enqueue_text(tenant.api, queue, "chat", "visitor-1", INBOX, text="Hello?"))["conversation_id"]
    world.advance(61)
    for cid in (silent, spoke):
        stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [spoke])
    world.run_jobs()
    assert search(tenant) == [spoke]


def test_text_matches_the_handling_agents_their_teams_and_our_number_but_not_a_chat_address(world, tenant):
    t = team(tenant, "Escalations")
    agent = tenant.agent("Priyanka", teams=[t]).login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent, teams=[t])
    world.plivo.text(number, "+14155550961", "Hi")
    sms = take(stream, agent)
    close(agent, sms, disposition(tenant)["id"], stream)
    chat = closed_chat(tenant, agent, stream, "visitor-p")
    indexed(tenant, [sms, chat])
    assert set(search(tenant, text="priyanka")) == {sms, chat}
    assert set(search(tenant, text="escalations")) == {sms, chat}
    assert search(tenant, text=number.removeprefix("+")) == [sms]
    assert search(tenant, text=INBOX) == []


def test_a_call_is_filed_under_the_teams_its_handling_agent_was_in_when_it_closed(world, tenant):
    alpha, bravo, charlie = team(tenant, "Alpha"), team(tenant, "Bravo"), team(tenant, "Charlie")
    agent = tenant.agent("Ann", teams=[alpha, bravo]).login()
    live = connect(tenant, agent, teams=[alpha])
    cid = finish(live)["conversation_id"]
    outbound = closed_outbound(tenant, agent, lead(world)[0])
    indexed(tenant, [cid, outbound])
    assert set(search(tenant, team_id=alpha)) == set(search(tenant, text="alpha")) == {cid, outbound}
    assert set(search(tenant, team_id=bravo)) == set(search(tenant, text="bravo")) == {cid, outbound}
    ok(tenant.api.patch(f"/api/v1/agents/{agent.id}", json={"teams": [charlie]}))
    ok(tenant.api.patch(f"/api/v1/queues/{live.queue['id']}", json={"teams": [bravo]}))
    ok(tenant.api.post(f"/api/v1/conversations/{cid}/notes", json={"text": "Reviewed"}))
    run_jobs_until(world, lambda: search(tenant, text="reviewed") == [cid], "the note in the index")
    assert cid not in search(tenant, team_id=charlie) + search(tenant, text="charlie")
    assert cid in search(tenant, team_id=alpha) and cid in search(tenant, team_id=bravo)


def test_a_call_is_filed_under_the_teams_of_the_agents_who_owned_it_and_not_a_listeners_or_a_failed_consults(tenant):
    floor, desk, asked, watch = (team(tenant, name) for name in ("Floor", "Desk", "Asked", "Watch"))
    sup = tenant.agent("Sup", role="admin", teams=[watch]).login()
    consulted, taker = tenant.agent("Consulted", teams=[asked]).login(), tenant.agent("Taker", teams=[desk]).login()
    live = connect(tenant, tenant.agent("Ann", teams=[floor]).login())
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor", json={"mode": "listen"}))
    sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    ok(sup.api().delete(f"/api/v1/calls/{live.id}/monitor"))
    live.stream.wait_for("supervisor.left", conversation_id=live.id)
    warm = {"mode": "warm", "target": to_agent(consulted)}
    tid = ok(act(live.agent.api(), live.id, "transfers", warm))["transfer_id"]
    consulted.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=consulted.id)
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="cancelled")
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(taker)}))
    taker.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    live.caller.hangup()
    done = disposition(tenant)["id"]
    for agent in (live.agent, taker):
        live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=agent.id)
        ok(wrap_up(agent, live.id, done))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    indexed(tenant, [live.id])
    assert [search(tenant, team_id=t) for t in (floor, desk, asked, watch)] == [[live.id], [live.id], [], []]


def test_a_disposition_not_marked_answered_files_its_conversation_by_state(world, tenant):
    sale = disposition(tenant, "Sale", answered=True)
    wrong = disposition(tenant, "Wrong number", answered=False)
    assert ok(tenant.api.get(f"/api/v1/dispositions/{wrong['id']}"))["answered"] is False
    sold, missed = connect(tenant), connect(tenant)
    for live, d in ((sold, sale), (missed, wrong)):
        live.caller.hangup()
        live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
        ok(wrap_up(live.agent, live.id, d["id"]))
        live.stream.wait_for("conversation.closed", conversation_id=live.id)
    indexed(tenant, [sold.id, missed.id])
    assert search(tenant, filter={"groups": [{"atoms": [atom("state", "is", "wrong number")]}]}) == [missed.id]
    assert search(tenant, filter={"groups": [{"atoms": [atom("disposition", "is", "sale")]}]}) == [sold.id]


def test_a_call_no_agent_answered_files_its_system_disposition_as_its_state(world, tenant):
    number, line = tenant.inbound_line(unavailable(tenant))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    missed = stream.wait_for("conversation.queued", queue_id=line["id"])["conversation_id"]
    caller.hangup()
    stream.wait_for("conversation.closed", conversation_id=missed)
    sold = connect(tenant)
    finish(sold, disposition(tenant, "Sale")["id"])
    indexed(tenant, [missed, sold.id])
    assert where(tenant, atom("state", "is", "dropped")) == [missed]
    assert where(tenant, atom("state", "is", "customer answered")) == [sold.id]
    assert where(tenant, atom("disposition", "empty")) == [missed]


def test_a_voicemail_our_number_and_the_day_a_conversation_closed_filter_the_search(world, tenant):
    q = tenant.queue()
    closed = tenant.number()
    shut = hours(tenant, weekly=False)["id"]
    tenant.route_number(closed, queue_action(q), business_hours_id=shut, closed_action=voicemail_action(q))
    stream = tenant.stream(after=0)
    world.plivo.call(closed)
    left = stream.wait_for("voicemail.received", timeout=10)["conversation_id"]
    live = connect(tenant)
    finish(live)
    indexed(tenant, [left, live.id])
    assert search(tenant, voicemail=True) == where(tenant, atom("voicemail", "is", True)) == [left]
    assert search(tenant, voicemail=False) == [live.id]
    assert where(tenant, atom("our_number", "is", live.number)) == [live.id]
    zone = noon_zone()
    assert set(where(tenant, atom("closed_date", "is", "today"), time_zone=zone)) == {left, live.id}
    assert where(tenant, atom("closed_date", "before", "today"), time_zone=zone) == []


def test_the_latest_csat_answer_stands_and_a_survey_with_none_reads_no_response(world, tenant):
    live = connect(tenant)
    finish(live)
    indexed(tenant, [live.id])
    path = f"/api/v1/conversations/{live.id}/csat"
    error(live.agent.api().post(path, json={"score": "5"}), 403)
    ok(tenant.api.post(path, json={}))
    run_jobs_until(world, lambda: where(tenant, atom("csat", "is", "no response")) == [live.id], "no answer indexed")
    ok(tenant.api.post(path, json={"score": "4"}))
    ok(tenant.api.post(path, json={}))
    run_jobs_until(world, lambda: where(tenant, atom("csat", "is", "4")) == [live.id], "the answer indexed")
    assert where(tenant, atom("csat", "is", "no response")) == []

    def scores():
        return [e["data"]["score"] for e in timeline(tenant.api, live.id) if e["type"] == "conversation.csat_recorded"]

    world.wait(lambda: len(scores()) == 3, what="all three answers on the timeline")
    assert scores() == ["No Response", "4", "4"]


def test_a_conversation_whose_index_write_gave_up_is_indexed_by_the_sweep(world, tenant):
    live = connect(tenant)

    with world.search_down():
        live.caller.hangup()
        live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
        ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
        live.stream.wait_for("conversation.closed", conversation_id=live.id)
        world.run_jobs()
        # A pass runs every search_index job, not only this one, so its count cannot say when this row stopped.
        for _ in range(10):
            if world.parked("search_index", live.id):
                break
            world.run_jobs("search_index")
    assert world.parked("search_index", live.id)
    world.run_jobs("search_index")
    assert world.parked("search_index", live.id)
    assert search(tenant) == []
    world.run_jobs("search_reconcile")
    run_jobs_until(world, lambda: search(tenant) == [live.id], "the sweep to index the conversation")


def test_a_csat_answer_is_found_by_text(world, tenant):
    live = connect(tenant)
    finish(live)
    ok(tenant.api.post(f"/api/v1/conversations/{live.id}/csat", json={"score": "superb"}))
    run_jobs_until(world, lambda: search(tenant, text="superb") == [live.id], "the answer indexed as text")
    ok(tenant.api.post(f"/api/v1/conversations/{live.id}/csat", json={"score": "poor"}))
    run_jobs_until(world, lambda: where(tenant, atom("csat", "is", "poor")) == [live.id], "the later answer indexed")
    assert search(tenant, text="superb") == [live.id]


def test_a_sub_disposition_is_found_by_its_id_whatever_its_parent_is(world, tenant):
    wrong = disposition(tenant, "Wrong number", answered=False)
    child = disposition(tenant, "Dead line", parent_disposition_id=wrong["id"])
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, wrong["id"], sub_disposition_id=child["id"]))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    indexed(tenant, [live.id])
    assert where(tenant, atom("disposition_id", "is", child["id"])) == [live.id]
    assert where(tenant, atom("disposition_id", "is", wrong["id"])) == []


def test_a_disposition_that_is_the_state_is_not_found_by_its_id(world, tenant):
    wrong = disposition(tenant, "Wrong number", answered=False)
    live = connect(tenant)
    finish(live, wrong["id"])
    indexed(tenant, [live.id])
    assert where(tenant, atom("state", "is", "wrong number")) == [live.id]
    assert where(tenant, atom("disposition_id", "is", wrong["id"])) == []


def paged(tenant, **body) -> list[str]:
    out, params = [], {"limit": 1}
    while True:
        got = ok(tenant.api.post("/api/v1/conversations/search", json=body, params=params))
        out += [c["id"] for c in got["data"]]
        if not got["next_cursor"]:
            return out
        params["cursor"] = got["next_cursor"]


def test_a_search_and_a_view_return_the_fields_they_name_for_each_hit(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = closed_chat(tenant, agent, stream, "visitor-f", "Hello", done=disposition(tenant, "Resolved")["id"])
    indexed(tenant, [cid])
    queue = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["queue_id"]
    asked = ["disposition", "state", "message_count", "answered", "queue_id"]
    want = dict(zip(asked, ["Resolved", "Customer answered", 2, True, queue], strict=True))
    [hit] = ok(tenant.api.post("/api/v1/conversations/search", json={"fields": asked}))["data"]
    assert hit["fields"] == want
    view = ok(tenant.api.post("/api/v1/views", json={"name": "Closed", "columns": ["number", *asked]}))
    [shown] = ok(tenant.api.get(f"/api/v1/views/{view['id']}/conversations"))["data"]
    assert shown["fields"] == want


def test_search_sorts_and_pages_by_disposition_and_state_in_any_case(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    named = (disposition(tenant, "Bravo"), disposition(tenant, "alpha"), disposition(tenant, "Wrong", answered=False))
    b, a, w = (closed_chat(tenant, agent, stream, f"visitor-s{i}", done=d["id"]) for i, d in enumerate(named))
    indexed(tenant, [b, a, w])
    assert paged(tenant, sort="disposition", order="asc") == [w, a, b]
    assert paged(tenant, sort="disposition", order="desc") == [b, a, w]
    assert paged(tenant, sort="state", order="desc") == [w, a, b]


def test_search_sorts_and_pages_by_csat_in_any_case_with_a_tie_and_a_missing_answer(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    great, good, tie, none = (closed_chat(tenant, agent, stream, f"visitor-c{i}") for i in range(4))
    for cid, score in ((great, "Great"), (good, "good"), (tie, "good")):
        ok(tenant.api.post(f"/api/v1/conversations/{cid}/csat", json={"score": score}))

    def scored() -> bool:
        goods, greats = (set(where(tenant, atom("csat", "is", score))) for score in ("good", "great"))
        return goods == {good, tie} and greats == {great}

    run_jobs_until(world, scored, "the answers indexed")
    assert paged(tenant, sort="csat", order="asc") == [none, good, tie, great]
    assert paged(tenant, sort="csat", order="desc") == [great, tie, good, none]


def test_a_view_refuses_a_column_its_hits_cannot_carry(tenant):
    error(tenant.api.post("/api/v1/views", json={"name": "Texts", "columns": ["message"]}), 422)


def test_text_matches_the_state_a_conversation_is_filed_by(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = closed_chat(tenant, agent, stream, "visitor-st")
    indexed(tenant, [cid])
    assert search(tenant, text="customer answered") == [cid]


def test_filters_read_the_texts_of_the_messages_and_notes_and_the_kinds_of_item(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    refund = closed_chat(tenant, agent, stream, "visitor-r", "Your Refund is on its way")
    noted = closed_chat(tenant, agent, stream, "visitor-n")
    ok(agent.api().post(f"/api/v1/conversations/{noted}/notes", json={"text": "Called back"}))
    run_jobs_until(world, lambda: search(tenant, text="called") == [noted], "the note in the index")
    indexed(tenant, [refund])
    assert where(tenant, atom("message", "contains", "refund")) == [refund]
    assert where(tenant, atom("message", "does_not_contain", "refund")) == [noted]
    assert where(tenant, atom("message", "is", "your refund is on its way")) == [refund]
    assert where(tenant, atom("message", "is", "called back")) == [noted]
    assert where(tenant, atom("type", "is", "notes")) == [noted]
    assert set(where(tenant, atom("type", "is", "message"))) == {refund, noted}
    assert where(tenant, atom("type", "is_not", "notes")) == [refund]
