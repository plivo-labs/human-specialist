"""Search over closed human conversations: POST /conversations/search reads the search index, which takes a
conversation when it closes and again when it changes after closing. test_search_index.py runs these against
OpenSearch; here HS searches Postgres."""

from tests.acceptance.channels import close, enqueue_text, take
from tests.acceptance.conftest import connect, dial, disposition, error, finish, lead, ok, run_jobs_until, team, wrap_up


def search(tenant, **filters) -> list[str]:
    return [c["id"] for c in ok(tenant.api.post("/api/v1/conversations/search", json=filters))["data"]]


def indexed(tenant, cids: list[str]) -> None:
    run_jobs_until(tenant.world, lambda: set(cids) <= set(search(tenant)), "the closed conversations indexed")


def closed_outbound(tenant, agent, number: str) -> str:
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, agent, to=number)
    stream.wait_for("call.answered", conversation_id=cid)
    ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(agent, cid, disposition(tenant, "Called")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    return cid


def test_closed_conversations_are_found_by_disposition_and_team(world, tenant):
    red = team(tenant, "Red")
    sale, lost = disposition(tenant, "Sale"), disposition(tenant, "Lost")
    a, b = tenant.agent("A", teams=[red]).login(), tenant.agent("B").login()
    sold = finish(connect(tenant, a, teams=[red]), sale["id"])["conversation_id"]
    other = finish(connect(tenant, b), lost["id"])["conversation_id"]
    indexed(tenant, [sold, other])
    assert search(tenant, disposition_id=sale["id"]) == [sold]
    assert search(tenant, team_id=red) == [sold]


def test_closed_conversations_are_found_by_agent_queue_direction_number_and_dates(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    live = connect(tenant, a)
    inbound = finish(live)["conversation_id"]
    number, _ = lead(world)
    outbound = closed_outbound(tenant, b, number)
    indexed(tenant, [inbound, outbound])
    assert search(tenant, agent_id=a.id) == [inbound]
    assert search(tenant, queue_id=live.queue["id"]) == [inbound]
    assert search(tenant, direction="outbound") == [outbound]
    assert search(tenant, number=number) == [outbound]
    assert search(tenant, created_before="2000-01-01T00:00:00Z") == []
    assert set(search(tenant, created_after="2000-01-01T00:00:00Z")) == {inbound, outbound}


def test_a_closed_chat_is_found_by_a_word_from_its_messages_and_by_its_channel(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    body = {"text": "My parcel never arrived"}
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-7", "support-inbox", **body))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    close(agent, cid, disposition(tenant)["id"], stream)
    call = finish(connect(tenant, agent))["conversation_id"]
    indexed(tenant, [cid, call])
    assert search(tenant, text="parcel") == [cid]
    assert search(tenant, channel="chat") == [cid]


def test_text_matches_a_word_in_any_case_and_results_come_newest_first(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    done = disposition(tenant)["id"]

    def closed_chat(visitor: str, text: str) -> str:
        cid = ok(enqueue_text(tenant.api, queue, "chat", visitor, "support-inbox", text=text))["conversation_id"]
        take(stream, agent, conversation_id=cid)
        close(agent, cid, done, stream)
        return cid

    oldest = closed_chat("visitor-1", "Parcel arrived")
    partial = closed_chat("visitor-2", "Where is my parcel")
    newest = closed_chat("visitor-3", "The PARCEL finally ARRIVED today, but the box inside it was crushed and wet")
    indexed(tenant, [oldest, partial, newest])
    assert search(tenant, text="pArCeL") == [newest, partial, oldest]


def test_a_note_added_after_close_is_searchable(world, tenant):
    closed = finish(connect(tenant))["conversation_id"]
    indexed(tenant, [closed])
    assert search(tenant, text="legal") == []
    ok(tenant.api.post(f"/api/v1/conversations/{closed}/notes", json={"text": "Escalated to legal"}))
    run_jobs_until(world, lambda: search(tenant, text="legal") == [closed], "the note in the index")


def test_a_tenant_with_nothing_indexed_finds_nothing(tenant):
    assert search(tenant) == []
    assert search(tenant, text="parcel", channel="chat") == []


def test_an_unknown_filter_is_422(tenant):
    error(tenant.api.post("/api/v1/conversations/search", json={"colour": "red"}), 422, "validation_failed")
