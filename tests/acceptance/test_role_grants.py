"""Roles as a contact center sets them: each role's view level (the whole tenant, its teams or its own work) and
its download, monitor and barge grants, read by every list, search, report, recording, task and stream."""

import json

from tests.acceptance.conftest import (
    Live,
    connect,
    error,
    finish,
    hs_header,
    ok,
    read_stream,
    seen,
    settings,
    sse_frames,
    status_changes,
    team,
    unavailable,
)
from tests.acceptance.test_closed_search import indexed
from tests.acceptance.test_tasks import create

DOWNLOADS = dict.fromkeys(("download_list", "download_conversation", "download_recording"), True)
DEFAULTS = {
    "admin": {"view": "org", **DOWNLOADS, "monitor": True, "barge": True},
    "supervisor": {"view": "team", **DOWNLOADS, "monitor": True, "barge": True},
    "agent": {"view": "self", **DOWNLOADS, "monitor": False, "barge": False},
}


def work(tenant, agent) -> tuple[str, str, str]:
    """A recorded call `agent` took and closed, indexed for search, and a task aimed at `agent`: (conversation,
    recording, task)."""
    settings(tenant, recording={"inbound": True})
    live = connect(tenant, agent)
    finish(live)
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    indexed(tenant, [live.id])
    return live.id, rid, ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))["id"]


def sees(client) -> dict[str, set]:
    return {
        "conversations": {c["id"] for c in ok(client.get("/api/v1/conversations"))["data"]},
        "search": {c["id"] for c in ok(client.post("/api/v1/conversations/search", json={}))["data"]},
        "recordings": {r["id"] for r in ok(client.get("/api/v1/recordings"))["data"]},
        "tasks": {t["id"] for t in ok(client.get("/api/v1/tasks"))["data"]},
    }


def found(lists: dict[str, set], cid: str, rid: str, task: str) -> bool:
    c, r, t = lists["conversations"], lists["recordings"], lists["tasks"]
    return cid in c and cid in lists["search"] and rid in r and task in t


def floor(client) -> set[str]:
    return {a["agent_id"] for a in ok(client.get("/api/v1/floor"))["agents"]}


def test_each_role_holds_its_default_grants_until_an_admin_changes_them_and_the_desk_lists_its_own(tenant):
    assert ok(tenant.api.get("/api/v1/settings"))["permissions"] == DEFAULTS
    agent = tenant.agent().login(available=False)
    sup = tenant.agent("Sup", role="supervisor").login(available=False)
    assert ok(agent.api().get("/api/v1/me"))["grants"] == DEFAULTS["agent"]
    assert ok(sup.api().get("/api/v1/me"))["grants"] == DEFAULTS["supervisor"]
    settings(tenant, permissions={"agent": {"view": "team", "download_list": False}, "supervisor": {"barge": False}})
    assert ok(agent.api().get("/api/v1/me"))["grants"] == DEFAULTS["agent"] | {"view": "team", "download_list": False}
    assert ok(sup.api().get("/api/v1/me"))["grants"] == DEFAULTS["supervisor"] | {"barge": False}
    fixed = (
        {"admin": {"view": "team"}},
        {"admin": {"download_recording": False}},
        {"agent": {"monitor": True}},
        {"agent": {"barge": True}},
        {"supervisor": {"view": "peer"}},
    )
    patch = tenant.api.patch
    assert [f for f in fixed if patch("/api/v1/settings", json={"permissions": f}).status_code != 422] == []


def test_a_supervisor_set_to_self_loses_its_teams_work_in_every_list_and_set_to_org_sees_the_tenants(tenant):
    floor_team = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor_team]).login(available=False)
    mate = tenant.agent("Mate", teams=[floor_team]).login()
    outsider = tenant.agent("Outsider", teams=[team(tenant, "Elsewhere")]).login()
    cid, rid, task = work(tenant, mate)
    theirs = connect(tenant, outsider).id
    assert found(sees(sup.api()), cid, rid, task) and floor(sup.api()) == {sup.id, mate.id}
    settings(tenant, permissions={"supervisor": {"view": "self"}})
    assert not any(sees(sup.api()).values())
    error(sup.api().get(f"/api/v1/conversations/{cid}"), 404)
    assert floor(sup.api()) == {sup.id}
    settings(tenant, permissions={"supervisor": {"view": "org"}})
    assert {cid, theirs} <= sees(sup.api())["conversations"] and {mate.id, outsider.id} <= floor(sup.api())


def test_an_agent_set_to_team_sees_its_teammates_work_in_every_list(tenant):
    floor_team = team(tenant, "Floor")
    peer = tenant.agent("Peer", teams=[floor_team]).login(available=False)
    mate = tenant.agent("Mate", teams=[floor_team]).login()
    cid, rid, task = work(tenant, mate)
    assert not found(sees(peer.api()), cid, rid, task)
    settings(tenant, permissions={"agent": {"view": "team"}})
    assert found(sees(peer.api()), cid, rid, task)


def test_a_stream_narrowed_to_self_ends_and_one_widened_to_team_stays_open_and_takes_its_teammates(tenant):
    floor_team = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor_team]).login(available=False)
    peer = tenant.agent("Peer", teams=[floor_team]).login(available=False)
    mate = tenant.agent("Mate", teams=[floor_team]).login(available=False)
    watch, desk = (tenant.stream(token=a.stream_token) for a in (sup, peer))
    settings(tenant, permissions={"supervisor": {"view": "self"}, "agent": {"view": "team"}})
    watch.wait_closed()
    assert watch.events[-1]["type"] == "auth.expired"
    [change] = status_changes(tenant, mate, "Away")
    desk.wait_for("agent.status_changed", id=change["id"])


def test_a_resume_after_the_view_narrows_replays_only_what_the_new_scope_sees(tenant):
    floor_team = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor_team]).login(available=False)
    mate = tenant.agent("Mate", teams=[floor_team]).login(available=False)
    watch = tenant.stream(token=sup.stream_token)
    [theirs] = status_changes(tenant, mate, "Away")
    watch.wait_for("agent.status_changed", id=theirs["id"])
    settings(tenant, permissions={"supervisor": {"view": "self"}})
    watch.wait_closed()
    [own] = status_changes(tenant, sup, "Away")
    resume = {"Last-Event-ID": str(int(theirs["id"]) - 1)}
    lines = read_stream(sup.api(), lambda ls: own["id"] in [f.get("id") for f in sse_frames(ls)], headers=resume)
    replayed = [f["id"] for f in sse_frames(lines) if "data" in f]
    assert theirs["id"] not in replayed and replayed[-1] == own["id"]


def waiting(client) -> set[str]:
    return {q["queue_id"] for q in ok(client.get("/api/v1/floor"))["queues"]}


def test_the_floor_and_queue_stats_show_a_supervisor_only_the_queues_its_view_reaches(world, tenant):
    floor_team = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor_team]).login(available=False)
    stream = tenant.stream(after=0)
    number, mine = tenant.inbound_line(teams=[floor_team], wait_timeout_sec=600)
    other, theirs = tenant.inbound_line(unavailable(tenant), teams=[team(tenant, "Elsewhere")], wait_timeout_sec=600)
    for n in (number, other):
        world.plivo.call(n)
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 2, what="a caller waiting in each queue")
    assert waiting(sup.api()) == {mine["id"]}
    assert ok(sup.api().get(f"/api/v1/queues/{mine['id']}/stats"))["waiting"] == 1
    error(sup.api().get(f"/api/v1/queues/{theirs['id']}/stats"), 404, "queue_not_found")
    settings(tenant, permissions={"supervisor": {"view": "self"}})
    assert waiting(sup.api()) == set()
    error(sup.api().get(f"/api/v1/queues/{mine['id']}/stats"), 404, "queue_not_found")
    settings(tenant, permissions={"supervisor": {"view": "org"}})
    assert waiting(sup.api()) == {mine["id"], theirs["id"]}
    assert ok(sup.api().get(f"/api/v1/queues/{theirs['id']}/stats"))["waiting"] == 1


def test_a_supervisor_follows_its_teams_queue_call_only_until_an_agent_outside_the_team_takes_it(world, tenant):
    floor_team = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor_team]).login(available=False)
    outsider = tenant.agent("Outsider", teams=[team(tenant, "Elsewhere")]).login()
    watch, stream = tenant.stream(token=sup.stream_token, after=0), tenant.stream(after=0)
    number, q = tenant.inbound_line(outsider, teams=[floor_team])
    outsider.phone.on_ring("ignore")
    caller = world.plivo.call(number)
    leg = outsider.phone.wait_ring()
    cid = hs_header(leg, "HsConversation")
    watch.wait_for("offer.created", conversation_id=cid)
    leg.answer()
    stream.wait_for("offer.accepted", conversation_id=cid)
    finish(Live(caller, leg, outsider, q, number, stream, cid))
    [own] = status_changes(tenant, sup, "Away")
    watch.wait_for("agent.status_changed", id=own["id"])
    error(sup.api().get(f"/api/v1/conversations/{cid}"), 404)
    resume = {"Last-Event-ID": "0"}
    lines = read_stream(sup.api(), lambda ls: own["id"] in [f.get("id") for f in sse_frames(ls)], headers=resume)
    replayed = [json.loads(f["data"]) for f in sse_frames(lines) if "data" in f]
    taken = {"offer.accepted", "call.ended", "conversation.closed"}
    assert not [e for e in watch.events + replayed if e.get("conversation_id") == cid and e["type"] in taken]
