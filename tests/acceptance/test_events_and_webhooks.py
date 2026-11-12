"""The event outlets: SSE stream, signed webhooks with retries, replay; scoping and the envelope."""

import json
import logging
import time

import pytest

from tests.acceptance.conftest import (
    connect,
    data_frames,
    dial,
    error,
    finish,
    hangup_action,
    header,
    ok,
    queue_action,
    read_stream,
    run_jobs_until,
    seen,
    signatures,
    signed_with,
    sse_frames,
    status_changes,
    team,
    unavailable,
    webhook,
)


def bump(tenant, *names: str) -> list[dict]:
    """Emit one agent.status_changed per name on a fresh agent; return those events once replayable."""
    return status_changes(tenant, tenant.agent(), *names)


def delivered(sink, event_id) -> list:
    return [d for d in sink.deliveries if str(d.json["id"]) == str(event_id)]


def data_ids(lines: list[str]) -> list[str]:
    return [f["id"] for f in sse_frames(lines) if "data" in f]


def test_sse_frames_carry_an_id_no_event_field_and_a_retry_hint(tenant):
    [event] = bump(tenant, "Away")

    def mine(ls):
        return [f for f in sse_frames(ls) if f.get("id") == str(event["id"])]

    lines = read_stream(tenant.api, mine, params={"after": 0})
    frame = mine(lines)[0]
    assert "event" not in frame
    assert json.loads(frame["data"]) == event
    assert "retry:3000" in [line.replace(" ", "") for line in lines]


def test_sse_sends_a_ping_comment(tenant):
    lines = read_stream(tenant.api, lambda ls: any(line.startswith(":") and "ping" in line for line in ls))
    assert any(line.startswith(": ping") for line in lines)


def test_last_event_id_resumes_after_that_event_and_wins_over_after(tenant):
    events = bump(tenant, "Away", "Available", "Busy")
    lines = read_stream(tenant.api, data_frames(2), headers={"Last-Event-ID": str(events[0]["id"])})
    assert data_ids(lines)[:2] == [str(e["id"]) for e in events[1:]]
    both = read_stream(
        tenant.api, data_frames(1), params={"after": events[0]["id"]}, headers={"Last-Event-ID": str(events[1]["id"])}
    )
    assert data_ids(both)[0] == str(events[2]["id"])


def test_a_new_stream_opened_with_after_loses_nothing(tenant):
    agent = tenant.agent()
    first = tenant.stream(after=0)
    [last_seen] = status_changes(tenant, agent, "Away")
    first.wait_for("agent.status_changed", agent_id=agent.id)
    first.close()
    missed = status_changes(tenant, agent, "Available", "Busy")
    second = tenant.stream(after=int(last_seen["id"]))
    tenant.world.wait(lambda: len(seen(second, "agent.status_changed")) >= 2, what="replayed events")
    assert [e["id"] for e in seen(second, "agent.status_changed")] == [e["id"] for e in missed]


def test_the_stream_sends_auth_expired_then_closes(tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(token=agent.stream_token)
    agent.expire_tokens()
    stream.wait_for("auth.expired")
    stream.wait_closed()


def test_a_revoked_keys_stream_gets_auth_expired_and_closes(world, tenant):
    minted = ok(world.operator.post(f"/api/v1/tenants/{tenant.id}/api-keys", json={"scopes": ["events"]}))
    stream = tenant.stream(token=minted["api_key"])
    ok(world.operator.delete(f"/api/v1/tenants/{tenant.id}/api-keys/{minted['id']}"))
    stream.wait_for("auth.expired")
    stream.wait_closed()


def test_a_stream_never_carries_another_tenants_events(world, tenant):
    other = world.new_tenant("Other")
    theirs = other.stream(after=0)
    bump(tenant, "Away")
    [last] = bump(other, "Away")
    theirs.wait_for("agent.status_changed", id=last["id"])
    assert {e["tenant_id"] for e in theirs.events} == {other.id}


def test_replay_returns_events_after_the_cursor_in_order_and_pages(tenant):
    events = bump(tenant, "Away", "Available", "Busy")
    page = ok(tenant.api.get("/api/v1/events", params={"after": events[0]["id"]}))
    assert [e["id"] for e in page["data"]] == [e["id"] for e in events[1:]]
    first = ok(tenant.api.get("/api/v1/events", params={"after": 0, "limit": 2}))
    rest = ok(tenant.api.get("/api/v1/events", params={"after": first["next_cursor"], "limit": 2}))
    assert [e["id"] for e in first["data"] + rest["data"]] == [e["id"] for e in events]


def test_replay_filters_by_type(tenant):
    events = bump(tenant, "Away", "Busy")
    typed = ok(tenant.api.get("/api/v1/events", params={"after": 0, "type": "agent.status_changed"}))["data"]
    assert [e["id"] for e in typed] == [e["id"] for e in events]
    assert ok(tenant.api.get("/api/v1/events", params={"after": 0, "type": "call.ended"}))["data"] == []


def test_webhook_deliveries_are_signed_over_timestamp_and_body(world, tenant):
    sink = world.webhook_sink()
    hook = webhook(tenant, sink.url)
    assert hook["secret"] not in tenant.api.get(f"/api/v1/webhooks/{hook['id']}").text
    [event] = bump(tenant, "Away")
    d = sink.wait_for("agent.status_changed")
    assert signed_with(d, hook["secret"]) and d.json == event
    assert header(d, "X-HS-Event-Id") == str(event["id"]) and header(d, "Content-Type").startswith("application/json")
    assert abs(int(header(d, "X-HS-Timestamp")) - time.time()) < 60
    ok(tenant.api.post(f"/api/v1/webhooks/{hook['id']}/test"))
    assert signed_with(sink.wait_for("webhook.test"), hook["secret"])


@pytest.mark.parametrize("status", [500, 599])
def test_a_failed_delivery_is_retried_and_signed_again(world, tenant, status):
    sink = world.webhook_sink()
    sink.respond_with(status)
    hook = webhook(tenant, sink.url)
    [event] = bump(tenant, "Away")
    first = sink.wait_for("agent.status_changed")
    sink.respond_with(200)
    retry = run_jobs_until(world, lambda: delivered(sink, event["id"])[1:], "retry")[0]
    assert retry.body == first.body and signed_with(first, hook["secret"]) and signed_with(retry, hook["secret"])
    assert int(header(retry, "X-HS-Timestamp")) >= int(header(first, "X-HS-Timestamp"))


def test_a_410_disables_the_subscription(world, tenant):
    sink = world.webhook_sink()
    sink.respond_with(410)
    hook = webhook(tenant, sink.url)
    bump(tenant, "Away")
    sink.wait_for("agent.status_changed")
    world.wait(lambda: ok(tenant.api.get(f"/api/v1/webhooks/{hook['id']}"))["enabled"] is False, what="disabled")
    [later] = bump(tenant, "Busy")
    world.run_jobs()
    assert not delivered(sink, later["id"])


@pytest.mark.parametrize("status", [400, 600])
def test_a_delivery_answered_with_a_4xx_or_past_599_is_not_retried(world, tenant, status):
    sink = world.webhook_sink()
    sink.respond_with(status)
    hook = webhook(tenant, sink.url)
    [event] = bump(tenant, "Away")
    sink.wait_for("agent.status_changed")
    sink.respond_with(200)
    world.run_jobs()
    assert len(delivered(sink, event["id"])) == 1
    assert ok(tenant.api.get(f"/api/v1/webhooks/{hook['id']}"))["enabled"] is True


def test_rotating_the_secret_signs_with_both_secrets(world, tenant):
    sink = world.webhook_sink()
    hook = webhook(tenant, sink.url)
    new = ok(tenant.api.post(f"/api/v1/webhooks/{hook['id']}/rotate-secret"))["secret"]
    assert new != hook["secret"]
    bump(tenant, "Away")
    d = sink.wait_for("agent.status_changed")
    assert len(signatures(d)) == 2 and signed_with(d, hook["secret"]) and signed_with(d, new)


def test_a_subscription_gets_only_its_event_types(world, tenant):
    wanted, other = world.webhook_sink(), world.webhook_sink()
    webhook(tenant, wanted.url, event_types=["agent.status_changed"])
    webhook(tenant, other.url, event_types=["call.ended"])
    bump(tenant, "Away")
    wanted.wait_for("agent.status_changed")
    world.run_jobs()
    assert other.deliveries == []


def test_webhooks_neither_follow_nor_retry_redirects(world, tenant):
    target, sink = world.webhook_sink(), world.webhook_sink()
    sink.redirect_to(target.url)
    webhook(tenant, sink.url)
    [event] = bump(tenant, "Away")
    sink.wait_for("agent.status_changed")
    world.run_jobs()
    assert len(delivered(sink, event["id"])) == 1 and target.deliveries == []


def test_the_stream_token_never_reaches_the_logs(world, tenant, caplog):
    caplog.set_level(logging.INFO)
    agent = tenant.agent().login(available=False)

    def until_ping(ls):
        return any(line.startswith(":") for line in ls)

    read_stream(world.anon(), until_ping, params={"access_token": agent.stream_token})

    def hs_lines() -> list[str]:
        return [r.getMessage() for r in caplog.records if not r.name.startswith(("httpx", "httpcore"))]

    # The access line of a streamed response is written once the response has ended, after the client let go.
    world.wait(lambda: any("/events/stream" in line for line in hs_lines()), what="the stream's access line")
    assert not [line for line in hs_lines() if agent.stream_token in line]


def test_private_and_plain_http_webhook_urls_are_refused(world, tenant):
    urls = [
        "https://10.0.0.7/hook",
        "https://192.168.1.20/hook",
        "https://172.16.0.1/hook",
        "https://169.254.169.254/latest/meta-data",
        "https://[::1]/hook",
        "https://[fd00::1]/hook",
        "http://hooks.example.com/hook",
    ]
    assert [u for u in urls if tenant.api.post("/api/v1/webhooks", json={"url": u}).status_code != 422] == []
    hook = webhook(tenant, world.webhook_sink().url)
    error(tenant.api.patch(f"/api/v1/webhooks/{hook['id']}", json={"url": "https://10.1.2.3/x"}), 422)


def test_an_agent_stream_gets_its_own_calls_and_nothing_else(tenant):
    me, other = tenant.agent("Me").login(), tenant.agent("Other").login()
    desk = tenant.stream(token=me.stream_token, after=0)
    theirs = connect(tenant, other)
    mine = connect(tenant, me)
    mine.caller.hangup()
    for kind in ("offer.created", "offer.accepted", "call.ended", "conversation.wrap_up_started"):
        desk.wait_for(kind, conversation_id=mine.id)
    theirs.caller.hangup()
    theirs.leg.wait_state("completed")
    [last] = status_changes(tenant, me, "Busy")
    desk.wait_for("agent.status_changed", id=last["id"])
    assert not [e for e in desk.events if e.get("conversation_id") == theirs.id or e.get("agent_id") == other.id]


def test_a_supervisor_stream_gets_its_team_queue_events(world, tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    watch = tenant.stream(token=sup.stream_token, after=0)
    tenant.agent("Member", teams=[t]).login().set_status("Away")
    stream = tenant.stream(after=0)
    world.plivo.call(tenant.inbound_line(unavailable(tenant, "Outsider"))[0])
    elsewhere = stream.wait_for("conversation.queued")["conversation_id"]
    q = tenant.queue(name="Team", teams=[t], wait_timeout_sec=1, overflow_action=hangup_action())
    number = tenant.number()
    tenant.route_number(number, queue_action(q))
    world.plivo.call(number)
    cid = watch.wait_for("conversation.queued", queue_id=q["id"])["conversation_id"]
    watch.wait_for("conversation.overflowed", conversation_id=cid, timeout=10)
    assert not [e for e in watch.events if e.get("conversation_id") == elsewhere]


def test_a_silent_listen_reaches_its_supervisor_and_never_the_agent_it_listens_to(tenant):
    t = team(tenant, "Floor")
    lead, sup = (tenant.agent(name, role="supervisor", teams=[t]).login() for name in ("Lead", "Sup"))
    desk, watch = tenant.stream(token=lead.stream_token, after=0), tenant.stream(token=sup.stream_token, after=0)
    live = connect(tenant, lead)
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor", json={"mode": "listen"}))
    sup.phone.wait_ring().wait_state("in_progress")
    assert watch.wait_for("supervisor.joined", conversation_id=live.id)["data"]["visible"] is False
    ok(sup.api().delete(f"/api/v1/calls/{live.id}/monitor"))
    watch.wait_for("supervisor.left", conversation_id=live.id)
    finish(live)
    [last] = status_changes(tenant, lead, "Busy")
    replay = tenant.stream(token=lead.stream_token, after=0)
    for stream in (desk, replay):
        stream.wait_for("agent.status_changed", id=last["id"])
        assert not [e["type"] for e in stream.events if e["type"].startswith("supervisor.")]


def test_conversation_events_carry_the_envelope(tenant):
    live = connect(tenant)
    e = live.stream.wait_for("offer.accepted", conversation_id=live.id)
    assert {
        "id",
        "type",
        "created_at",
        "tenant_id",
        "conversation_id",
        "agent_id",
        "direction",
        "context",
        "data",
    } <= e.keys()
    assert (e["tenant_id"], e["agent_id"], e["direction"]) == (tenant.id, live.agent.id, "inbound")
    assert e["created_at"].endswith(("Z", "+00:00"))


def test_the_agent_who_answers_an_inbound_call_tells_call_answered_once_with_its_leg(tenant):
    live = connect(tenant)
    answered = live.stream.wait_for("call.answered", conversation_id=live.id)
    assert (answered["agent_id"], answered["data"]["call_uuid"]) == (live.agent.id, live.leg.call_uuid)
    target = tenant.agent("Target").login()
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(tenant.api.post(f"/api/v1/calls/{live.id}/transfers", json=cold))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    assert len(seen(live.stream, "call.answered", conversation_id=live.id)) == 1, "a call is answered once"


def test_the_context_of_a_dial_rides_on_every_event_of_the_call(tenant):
    ctx = {"lead_id": "L-1", "campaign": "spring"}
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, context=ctx)
    stream.wait_for("call.answered", conversation_id=cid)
    ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
    stream.wait_for("call.ended", conversation_id=cid)
    mine = [e for e in stream.events if e.get("conversation_id") == cid]
    assert all(e["context"] == ctx and e["direction"] == "outbound" for e in mine)


def test_every_event_of_a_call_reaches_both_sse_and_webhooks_in_id_order(world, tenant):
    sink = world.webhook_sink()
    webhook(tenant, sink.url)
    live = connect(tenant)
    finish(live)
    mine = [e for e in live.stream.events if e.get("conversation_id") == live.id]
    world.wait(
        lambda: {str(e["id"]) for e in mine} <= {str(d.json["id"]) for d in sink.deliveries}, what="all delivered"
    )
    assert {"offer.created", "offer.accepted", "call.ended", "conversation.closed"} <= {e["type"] for e in mine}
    ids = [int(e["id"]) for e in live.stream.events]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)


def test_a_webhook_can_be_paused_repointed_and_deleted(world, tenant):
    first, second = world.webhook_sink(), world.webhook_sink()
    hook = webhook(tenant, first.url)
    url = f"/api/v1/webhooks/{hook['id']}"
    assert [h["id"] for h in ok(tenant.api.get("/api/v1/webhooks"))["data"]] == [hook["id"]]
    assert ok(tenant.api.patch(url, json={"enabled": False}))["enabled"] is False
    [paused] = bump(tenant, "Away")
    world.run_jobs()
    assert not delivered(first, paused["id"])
    moved = ok(tenant.api.patch(url, json={"enabled": True, "url": second.url}))
    assert (moved["enabled"], moved["url"]) == (True, second.url)
    [later] = bump(tenant, "Busy")
    world.wait(lambda: delivered(second, later["id"]), what="delivery to the new url")
    ok(tenant.api.delete(url))
    [gone] = bump(tenant, "Away")
    world.run_jobs()
    assert not delivered(second, gone["id"]) and first.deliveries == []
    error(tenant.api.get(url), 404, "webhook_not_found")
    assert ok(tenant.api.get("/api/v1/webhooks"))["data"] == []
