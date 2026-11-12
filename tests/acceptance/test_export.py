import csv
import io
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from tests.acceptance.channels import accept, decline, enqueue_text, messages, offered, reply, take
from tests.acceptance.conftest import connect, disposition, error, finish, ok, settings, team, wrap_up
from tests.acceptance.test_closed_search import indexed, search
from tests.acceptance.test_in_call_controls import act, to_agent
from tests.acceptance.test_text_transfer import CUSTOMER, live_sms, transfer

SUMMARY = ["conversation_number", "channel", "number", "agents", "agent_count", "teams", "our_address", "created_at"]
SUMMARY += ["direction", "conversation_state", "wrap_up_note", "disposition", "message_count", "duration_sec"]
SUMMARY += ["wrap_sec", "talk_sec", "answered", "types", "csat", "ended_by"]
CONTENT = ["conversation_number", "channel", "id", "from", "agent_name", "to", "meta", "type", "text", "number", "at"]
KOLKATA = ZoneInfo("Asia/Kolkata")


def export(client, form: str = "", **body) -> list[list[str]]:
    r = client.post(f"/api/v1/conversations/export{form}", json=body)
    assert r.is_success and r.headers["content-type"].startswith("text/csv"), f"{r.status_code} {r.text}"
    return list(csv.reader(io.StringIO(r.text)))


def content(client, **body) -> list[dict[str, str]]:
    head, *rows = export(client, "/content", **body)
    assert head == CONTENT
    return [dict(zip(head, r, strict=True)) for r in rows]


def items(client, **body) -> list[tuple[str, str, str]]:
    return [(r["type"], r["agent_name"], r["text"]) for r in content(client, **body)]


def transfers(client, *headers: str) -> list[tuple[str, ...]]:
    return [tuple(r[h] for h in headers) for r in content(client) if r["type"] == "transfer"]


def local(at: str) -> datetime:
    return datetime.fromisoformat(at).astimezone(KOLKATA)


def number_of(tenant, cid: str) -> str:
    return ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["conversation_number"]


def closed(tenant, stream, cid: str, *agents) -> None:
    for agent in agents:
        stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
        ok(wrap_up(agent, cid, disposition(tenant, f"By {agent.id[:6]}")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])


def live_chat(tenant, agent, visitor: str) -> tuple[str, object]:
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", visitor, "support-inbox", text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    return cid, stream


def test_the_summary_export_writes_every_field_of_the_search_document_with_its_time_in_the_time_zone(tenant):
    agent = tenant.agent("A").login()
    live = connect(tenant, agent)
    sold = finish(live, disposition(tenant, "Sale")["id"])["conversation_id"]
    indexed(tenant, [sold])
    [found] = ok(tenant.api.post("/api/v1/conversations/search", json={}))["data"]
    head, row = export(tenant.api, time_zone="Asia/Kolkata")
    assert head == SUMMARY
    got = dict(zip(head, row, strict=True))
    assert all(got.pop(f).isdigit() for f in ("duration_sec", "wrap_sec", "talk_sec")), row
    assert "signal" not in got.pop("types")
    assert got == {
        "conversation_number": found["conversation_number"],
        "channel": "call",
        "number": found["number"],
        "agents": "A",
        "agent_count": "1",
        "teams": "",
        "our_address": live.number,
        "created_at": local(found["created_at"]).isoformat(),
        "direction": "inbound",
        "conversation_state": "Customer answered",
        "wrap_up_note": "",
        "disposition": "Sale",
        "message_count": "0",
        "answered": "true",
        "csat": "",
        "ended_by": "customer",
    }


def test_the_summary_export_writes_the_fields_asked_in_their_order_with_the_conversation_number_last(tenant):
    sale = disposition(tenant, "Sale")
    agent = tenant.agent("A").login()
    sold = finish(connect(tenant, agent), sale["id"])["conversation_id"]
    other = finish(connect(tenant, agent))["conversation_id"]
    indexed(tenant, [sold, other])
    found = ok(tenant.api.post("/api/v1/conversations/search", json={}))["data"]
    head, *rows = export(tenant.api)
    assert [r[0] for r in rows] == [c["conversation_number"] for c in found] and len(rows) == 2
    columns = ["disposition", "agents", "answered", "direction"]
    head, *rows = export(tenant.api, disposition_id=sale["id"], columns=columns)
    assert head == [*columns, "conversation_number"]
    assert rows == [["Sale", "A", "true", "inbound", number_of(tenant, sold)]]
    error(tenant.api.post("/api/v1/conversations/export", json={"columns": ["Disposition"]}), 422)


def test_the_summary_export_counts_the_agents_apart_from_their_names(tenant):
    sold = finish(connect(tenant, tenant.agent("Doe, Jane").login()))["conversation_id"]
    indexed(tenant, [sold])
    assert export(tenant.api, columns=["agents", "agent_count"]) == [
        ["agents", "agent_count", "conversation_number"],
        ["Doe, Jane", "1", number_of(tenant, sold)],
    ]


def test_an_agent_exports_a_saved_view_by_sending_its_filter(tenant):
    sale = disposition(tenant, "Sale")
    agent = tenant.agent("A").login()
    sold = finish(connect(tenant, agent), sale["id"])["conversation_id"]
    theirs = finish(connect(tenant, tenant.agent("B").login()), sale["id"])["conversation_id"]
    indexed(tenant, [sold, theirs])
    view = ok(agent.api().post("/api/v1/views", json={"name": "Sales", "filter": {"disposition_id": sale["id"]}}))
    got = export(agent.api(), **view["filter"], columns=["disposition"])
    assert got == [["disposition", "conversation_number"], ["Sale", number_of(tenant, sold)]]


def test_the_summary_export_needs_download_list_and_the_content_export_download_conversation(tenant):
    sup = tenant.agent("Sup", role="supervisor").login(available=False)
    settings(tenant, permissions={"supervisor": {"download_list": False}})
    error(sup.api().post("/api/v1/conversations/export", json={}), 403, "download_not_allowed")
    assert export(sup.api(), "/content") == [CONTENT]
    settings(tenant, permissions={"supervisor": {"download_list": True, "download_conversation": False}})
    error(sup.api().post("/api/v1/conversations/export/content", json={}), 403, "download_not_allowed")
    assert export(sup.api()) == [SUMMARY]


def test_the_content_export_writes_each_item_by_field_name_in_time_order(tenant):
    agent = tenant.agent("A").login()
    cid, stream = live_chat(tenant, agent, "visitor-9")
    hi = messages(tenant.api, cid)[0]
    reply(agent, cid, "Hello", reply_to=hi["id"])
    ok(agent.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "VIP"}))
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, disposition(tenant, "Resolved")["id"], note="Sorted"))
    stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])
    rows = content(tenant.api, time_zone="Asia/Kolkata")
    assert [(r["type"], r["agent_name"], r["text"], r["meta"]) for r in rows] == [
        ("message", "", "Hi", ""),
        ("joined", "A", "", "queue"),
        ("message", "A", "Hello", hi["id"]),
        ("note", "A", "VIP", ""),
        ("disposition", "A", "Resolved", ""),
        ("wrap_up_note", "A", "Sorted", ""),
    ]
    number = number_of(tenant, cid)
    assert rows[0] == {
        "conversation_number": number,
        "channel": "chat",
        "id": hi["id"],
        "from": "visitor-9",
        "agent_name": "",
        "to": "support-inbox",
        "meta": "",
        "type": "message",
        "text": "Hi",
        "number": "visitor-9",
        "at": local(hi["created_at"]).isoformat(),
    }
    assert (rows[2]["from"], rows[2]["to"]) == ("support-inbox", "visitor-9")
    head, row = export(tenant.api, columns=["types", "wrap_up_note"])
    assert (head, row) == (["types", "wrap_up_note", "conversation_number"], ["message,notes", "Sorted", number])


def test_the_content_export_describes_each_file_of_a_message_or_note_by_its_link(tenant):
    agent = tenant.agent("A").login()
    cid, stream = live_chat(tenant, agent, "visitor-7")
    pdf, png, raw = "https://files.example.com/invoice.pdf", "https://files.example.com/a.png", "https://x.example/b"
    terms = {"url": "https://files.example.com/terms.pdf", "name": "terms.pdf", "size": 2048}
    terms["content_type"] = "application/pdf"
    reply(agent, cid, None, attachments=[{"url": pdf}])
    reply(agent, cid, "Both", attachments=[{"url": png}, {"url": raw}])
    reply(agent, cid, None, attachments=[terms])
    note = {"text": "Signed form", "attachments": [{"url": "https://files.example.com/form.pdf", "name": "form.pdf"}]}
    ok(agent.api().post(f"/api/v1/conversations/{cid}/notes", json=note))
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    closed(tenant, stream, cid, agent)
    shown = [[{"url": pdf}], [{"url": png}, {"url": raw}], [terms]]
    assert [m["attachments"] for m in messages(tenant.api, cid)[1:]] == shown
    files = [(t, json.loads(c)) for t, _, c in items(tenant.api) if t in ("message", "note") and c.startswith("[")]
    assert files == [
        ("message", [{"file_url": pdf}]),
        ("message", [{"file_url": png, "description": "Both"}, {"file_url": raw, "description": "Both"}]),
        ("message", [{"file_url": terms["url"]}]),
        ("note", [{"file_url": "https://files.example.com/form.pdf", "description": "Signed form"}]),
    ]


def test_a_team_transfer_that_rang_two_agents_exports_one_row_naming_the_team_and_who_took_it(world, tenant):
    escalations = team(tenant, "Escalations")
    first = tenant.agent("First").login()
    one, two = (tenant.agent(name, teams=[escalations]).login() for name in ("One", "Two"))
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "team", "team_id": escalations}))
    offered(stream, one, conversation_id=cid)
    ok(accept(two, offered(stream, two, conversation_id=cid)))
    ok(two.api().post(f"/api/v1/conversations/{cid}/close"))
    closed(tenant, stream, cid, first, two)
    shown = transfers(tenant.api, "agent_name", "from", "to", "meta", "text")
    assert shown == [("First", "First", "Two", "team", "Escalations")]


def test_a_queue_transfer_exports_one_row_naming_the_queue(world, tenant):
    first, taker = tenant.agent("First").login(), tenant.agent("Taker").login()
    billing = tenant.queue(name="Billing", agents=[taker.id])
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))
    ok(accept(taker, offered(stream, taker, conversation_id=cid)))
    ok(taker.api().post(f"/api/v1/conversations/{cid}/close"))
    closed(tenant, stream, cid, first, taker)
    shown = transfers(tenant.api, "agent_name", "to", "meta", "text")
    assert shown == [("First", "", "queue", "Billing")]


def test_a_deleted_queue_leaves_lists_and_reads_and_frees_its_name_while_its_history_keeps_it(world, tenant):
    first, taker = tenant.agent("First").login(), tenant.agent("Taker").login()
    billing = tenant.queue(name="Billing", agents=[taker.id])
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))
    ok(accept(taker, offered(stream, taker, conversation_id=cid)))
    ok(taker.api().post(f"/api/v1/conversations/{cid}/close"))
    closed(tenant, stream, cid, first, taker)
    ok(tenant.api.delete(f"/api/v1/queues/{billing['id']}"))
    error(tenant.api.get(f"/api/v1/queues/{billing['id']}"), 404, "queue_not_found")
    assert billing["id"] not in [q["id"] for q in ok(tenant.api.get("/api/v1/queues", params={"limit": 200}))["data"]]
    assert tenant.queue(name="Billing")["id"] != billing["id"]
    to_gone = {"open_action": {"type": "queue", "queue_id": billing["id"]}}
    error(tenant.api.put(f"/api/v1/numbers/{tenant.number()}", json=to_gone), 422, "foreign_id")
    assert transfers(tenant.api, "meta", "text") == [("queue", "Billing")]
    assert search(tenant, queue_id=billing["id"]) == [cid]


def test_the_content_export_drops_rows_by_unseen_agents_but_keeps_transfers_notes_and_rows_with_no_agent(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream, number = live_sms(world, tenant, first)
    reply(first, cid, "Let me get a colleague")
    ok(first.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "Wants billing"}))
    ok(transfer(first, cid, {"type": "agent", "agent_id": target.id}))
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=first.id)
    ok(wrap_up(first, cid, disposition(tenant, "Passed on")["id"]))
    reply(target, cid, "Billing here")
    ok(target.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=target.id)
    ok(wrap_up(target, cid, disposition(tenant, "Billed")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    indexed(tenant, [cid])
    mine = [
        ("message", "", "Hi"),
        ("note", "First", "Wants billing"),
        ("joined", "Target", ""),
        ("transfer", "First", "Target"),
        ("message", "Target", "Billing here"),
        ("disposition", "Target", "Billed"),
    ]
    assert items(target.api()) == mine
    hidden = [
        ("joined", "First", ""),
        ("message", "First", "Let me get a colleague"),
        ("disposition", "First", "Passed on"),
    ]
    assert sorted(items(tenant.api)) == sorted(mine + hidden)
    assert (CUSTOMER, number) in {(r["from"], r["to"]) for r in content(tenant.api)}


def test_a_declined_text_transfer_exports_no_transfer_row(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "agent", "agent_id": target.id}))
    ok(decline(target, offered(stream, target, conversation_id=cid)))
    ok(first.api().post(f"/api/v1/conversations/{cid}/close"))
    closed(tenant, stream, cid, first)
    assert transfers(tenant.api, "type") == []


def test_a_call_transfer_whose_ring_plivo_refuses_exports_no_transfer_row(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    world.plivo.fault(
        "reject", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id, status=400
    )
    error(
        act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}), 422, "plivo_refused"
    )
    finish(live)
    indexed(tenant, [live.id])
    assert transfers(tenant.api, "type") == []


def test_a_call_transferred_to_a_queue_exports_one_row_naming_the_queue(tenant):
    first, taker = tenant.agent("First").login(), tenant.agent("Taker").login()
    billing = tenant.queue(name="Billing", agents=[taker.id])
    live = connect(tenant, first)
    taker.phone.on_ring("answer")
    target = {"type": "queue", "queue_id": billing["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": target}))
    taker.phone.wait_ring()
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=taker.id)
    live.caller.hangup()
    closed(tenant, live.stream, live.id, live.agent, taker)
    shown = transfers(tenant.api, "agent_name", "meta", "text")
    assert shown == [("First", "queue", "Billing")]
