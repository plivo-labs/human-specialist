"""Search on both sinks (Postgres, and the OpenSearch index at the harness's SEARCH_URL when it answers): every Q11 filter,
paging newest first, visibility, an index write that arrives after a newer one, a missing index and an index that
does not answer."""

import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest
from sqlalchemy import delete, func, insert, null, select, update

from hs import access, core, jobs, search
from hs import conversations as cv
from hs.core import ApiError, Paging, T
from tests.harness import SEARCH_URL, require_search, slow
from tests.unit import tenants


@pytest.fixture(params=["postgres", "opensearch"])
def sink(request, monkeypatch):
    index = request.param == "opensearch"
    if index:
        require_search()
        monkeypatch.setattr(search, "HTTP", httpx.Client(timeout=30 * slow(), trust_env=False))
    monkeypatch.setattr(search, "URL", SEARCH_URL if index else "")
    yield request.param
    while MADE:
        gone = f"{SEARCH_URL}/{search._index(MADE.pop())}"
        if index:
            httpx.delete(gone, params={"ignore_unavailable": "true"}, timeout=5 * slow()).raise_for_status()


MADE: list[uuid.UUID] = []  # tenants whose index the sink deletes


class Tenant(tenants.Tenant):
    def __init__(self):
        super().__init__()
        MADE.append(self.id)

    def closed(self, agent, **kw) -> uuid.UUID:
        """A closed conversation whose index job has run."""
        cid = super().closed(agent, **kw)
        self.index(cid)
        return cid

    def index(self, cid) -> None:
        jobs.run_now("search_index", str(cid), wait=True)
        self.refresh()

    def refresh(self) -> None:
        """A search index shows a write to searches once it refreshes."""
        if search.URL:
            httpx.post(f"{search.URL}/{search._index(self.id)}/_refresh", timeout=5 * slow())

    def find(self, p=None, limit: int = 50, cursor: str | None = None, **filters) -> list:
        found = search.search_conversations(search.SearchBody(**filters), p or self.key, Paging(limit, cursor))
        return [uuid.UUID(c["id"]) for c in found["data"]]

    def stint(self, cid, agent, **fields) -> None:
        row = {"tenant_id": self.id, "conversation_id": cid, "agent_id": agent, "role": "secondary", "via": "invite"}
        row |= {"channel": "call", "state": "ended", "started_at": func.now(), "ended_at": func.now()}
        with core.tx() as db:
            db.execute(insert(T.assignment).values(row | fields))
            cv.touched(db, cv.lock(db, cid))
        self.index(cid)


def test_a_conversation_is_found_by_its_handling_agents_team_and_not_its_queues(sink):
    t = Tenant()
    with core.tx() as db:
        red, blue = t.doc(db, "team", name="Red"), t.doc(db, "team", name="Blue")
        teams = T.config.c.body.concat({"teams": [str(blue)]})
        db.execute(T.config.update().where(T.config.c.id == t.queue).values(body=teams))
    cid = t.closed(t.agent(red))
    assert t.find(team_id=red) == [cid]
    assert t.find(team_id=blue) == []


def test_a_backfill_files_a_conversation_under_its_primary_agents_teams_alone(sink):
    t = Tenant()
    with core.tx() as db:
        red, blue, green = (t.doc(db, "team", name=n) for n in ("Red", "Blue", "Green"))
    cid = t.closed(t.agent(red))
    t.stint(cid, t.agent(blue, role="supervisor"), role="monitor", via="monitor")
    t.stint(cid, t.agent(green), via="transfer")
    with core.tx() as db:
        db.execute(update(T.conversation).where(T.conversation.c.id == cid).values(team_ids=null()))
        cv.touched(db, cv.lock(db, cid))
    t.index(cid)
    assert [t.find(team_id=x) for x in (red, blue, green)] == [[cid], [], []]


def test_each_filter_finds_exactly_its_conversations(sink):
    t = Tenant()
    with core.tx() as db:
        red = t.doc(db, "team", name="Red")
    a, b = t.agent(red), t.agent(red)
    with core.tx() as db:
        teams = T.config.c.body.concat({"teams": [str(red)]})
        db.execute(T.config.update().where(T.config.c.id == t.queue).values(body=teams))
    chat = t.closed(a, channel="chat", words=["My parcel never ARRIVED"], context={"lead": "L-1"}, ours="inbox")
    with core.tx() as db:
        conv = cv.lock(db, chat)
        cv.entry(db, conv, "message", {"direction": "outbound", "text": "Sorry", "email": {"subject": "Courier"}})
    t.index(chat)
    call = t.closed(b, queued=False, direction="outbound", customer="+15550009", context={"lead": {"id": 7}})
    with core.tx() as db:
        cv.entry(db, cv.lock(db, call), "note", {"text": "Escalated to legal"})
    t.index(call)
    assert t.find() == [call, chat]
    assert t.find(agent_id=a) == t.find(queue_id=t.queue) == t.find(channel="chat") == [chat]
    assert t.find(team_id=red) == [call, chat]
    assert t.find(direction="outbound") == t.find(number="15550009") == t.find(number="+15550009") == [call]
    assert t.find(text="parcel NEVER arr") == t.find(text="courier") == t.find(text="RCE") == [chat]
    assert t.find(text="LEGAL") == [call] and t.find(text="parcel legal") == []
    assert t.find(disposition_id=t.sale) == [call, chat] and t.find(disposition_id=t.lost) == []
    assert (
        t.find(created_before="2000-01-01T00:00:00Z") == [] and len(t.find(created_after="2000-01-01T00:00:00Z")) == 2
    )


def test_a_disposition_finds_the_final_one_with_no_sub_disposition_and_a_sub_disposition_its_own(sink):
    t = Tenant()
    agent = t.agent()
    plain, sub = t.closed(agent), t.closed(agent, sub=t.refund)
    t.stint(plain, t.agent(), disposition_id=t.lost, offered_at=func.now() - timedelta(hours=1))
    assert t.find(disposition_id=t.sale) == [plain] and t.find(disposition_id=t.refund) == [sub]
    assert t.find(disposition_id=t.lost) == []


def test_results_page_newest_first(sink):
    t = Tenant()
    agent = t.agent()
    first, second, third = (t.closed(agent) for _ in range(3))
    page = search.search_conversations(search.SearchBody(), t.key, Paging(2, None))
    assert [uuid.UUID(c["id"]) for c in page["data"]] == [third, second]
    after = core.paging(2, page["next_cursor"]).after
    assert t.find(limit=2, cursor=after) == [first]


def test_created_at_sorts_and_pages_by_the_creation_time_then_the_number(sink):
    t = Tenant()
    agent = t.agent()
    first, second = t.closed(agent), t.closed(agent)
    with core.tx() as db:
        C = T.conversation
        db.execute(update(C).where(C.c.id == second).values(opened_at=C.c.opened_at - timedelta(hours=1)))
        cv.touched(db, cv.lock(db, second))
    t.index(second)
    assert t.find() == [first, second] and t.find(sort="created_at", order="asc") == [second, first]
    page = search.search_conversations(search.SearchBody(sort="created_at", order="asc"), t.key, Paging(1, None))
    assert [uuid.UUID(c["id"]) for c in page["data"]] == [second]
    after = core.paging(1, page["next_cursor"]).after
    assert t.find(limit=1, cursor=after, sort="created_at", order="asc") == [first]


def test_a_supervisor_finds_what_its_team_handled_and_an_agent_only_its_own(sink):
    t = Tenant()
    red = uuid.uuid4()
    mine, other = t.agent(red), t.agent()
    sup = t.agent(red, role="supervisor")
    handled, elsewhere = t.closed(mine), t.closed(other)
    as_sup = access.Principal(tenant_id=str(t.id), kind="agent", agent_id=str(sup), role="supervisor")
    as_other = access.Principal(tenant_id=str(t.id), kind="agent", agent_id=str(other), role="agent")
    assert t.find(as_sup) == [handled] and t.find(as_other) == [elsewhere]


def test_a_viewer_filtering_for_an_agent_it_may_not_see_finds_nothing_even_where_both_took_part(sink):
    t = Tenant()
    a, b = t.agent(), t.agent()
    both = t.closed(a)
    t.stint(both, b)
    as_a = access.Principal(tenant_id=str(t.id), kind="agent", agent_id=str(a), role="agent")
    atom = {"groups": [{"atoms": [{"field": "agent_id", "op": "is", "value": [str(b)]}]}]}
    assert t.find(as_a, agent_id=a) == t.find(agent_id=b) == [both]
    assert t.find(as_a, agent_id=b) == t.find(as_a, filter=atom) == []


def test_an_older_index_write_arriving_last_changes_nothing(sink):
    t = Tenant()
    cid = t.closed(t.agent())
    with core.read() as db:
        old = db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one()
        stale = search.document(db, old)
    with core.tx() as db:
        cv.entry(db, cv.lock(db, cid), "note", {"text": "Escalated to legal"})
    t.index(cid)
    search.write(old, stale)
    t.refresh()
    assert t.find(text="legal") == [cid]


def test_a_backfill_write_at_the_same_version_lands_and_an_older_one_does_not(sink):
    t = Tenant()
    cid = t.closed(t.agent())
    with core.read() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one()
        doc = search.document(db, conv)
    search.write(conv, doc | {"words": [*doc["words"], "backfilled"]})
    older = SimpleNamespace(id=conv.id, tenant_id=conv.tenant_id, version=conv.version - 1)
    search.write(older, doc | {"words": ["stale"]})
    t.refresh()
    assert t.find(text="backfilled") == [cid] and t.find(text="stale") == []


def test_the_backfill_queues_a_batch_once_the_last_one_ran_and_stops_after_a_short_one(monkeypatch):
    t = Tenant()
    cid = t.closed(t.agent())
    J, before = T.jobs, uuid.UUID(int=cid.int - 1)
    searches = J.c.kind.in_(("search_index", "search_backfill"))

    def backfill(after) -> dict:
        search._backfill(SimpleNamespace(payload={"after": str(after)}, rev=0))
        with core.read() as db:
            return dict(db.execute(select(J.c.key, J.c.payload).where(searches)).all())

    with core.tx() as db:
        db.execute(delete(J).where(searches))
    monkeypatch.setattr(search, "BATCH", 1)
    index = {str(cid): {"tenant_id": str(t.id), "conversation_id": str(cid)}}
    assert backfill(before) == index | {"search_backfill": {"after": str(cid)}}
    assert backfill(before) == index | {"search_backfill": {"after": str(before)}}
    with core.tx() as db:
        db.execute(delete(J).where(searches))
    assert backfill(uuid.UUID(int=2**128 - 1)) == {}


@pytest.mark.parametrize("last", [False, True], ids=["mid-run", "last-batch"])
def test_a_backfill_reset_wins_over_a_run_in_flight(monkeypatch, last):
    t = Tenant()
    t.closed(t.agent())
    J, reset = T.jobs, {"after": str(uuid.UUID(int=0))}
    searches = J.c.kind.in_(("search_index", "search_backfill"))
    with core.tx() as db:
        db.execute(delete(J).where(searches))
        jobs.schedule(
            db, "search_backfill", {"after": str(uuid.UUID(int=2**128 - 1 if last else 0))}, key="search_backfill"
        )
    monkeypatch.setattr(search, "BATCH", 1)
    run = jobs._claim(J.c.kind == "search_backfill")
    with core.tx() as db:
        jobs.schedule(db, "search_backfill", reset, key="search_backfill")
    jobs.work(run)
    with core.read() as db:
        assert db.execute(select(J.c.payload).where(J.c.kind == "search_backfill")).scalars().all() == [reset]


def held(answer) -> httpx.Client:
    def mget(r: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"docs": [d | answer(d["_id"]) for d in json.loads(r.content)["docs"]]})

    return httpx.Client(transport=httpx.MockTransport(mget))


def queues_write(monkeypatch, cid, since, answer: dict) -> bool:
    monkeypatch.setattr(search, "URL", "http://search.test")
    monkeypatch.setattr(search, "HTTP", held(lambda i: answer if i == str(cid) else {"found": True}))
    search._reconcile(SimpleNamespace(payload={"since": since.isoformat()}))
    J = T.jobs
    pending = (J.c.kind == "search_index", J.c.key == str(cid), J.c.locked_until.is_distinct_from(jobs.INFINITY))
    with core.read() as db:
        return db.execute(select(J.c.id).where(*pending)).first() is not None


def close_ago_and_cutoff(cid, ago: timedelta):
    C = T.conversation
    with core.tx() as db:
        aged = update(C).where(C.c.id == cid).values(closed_at=func.now() - ago)
        return db.execute(aged.returning(C.c.closed_at)).scalar() - timedelta(seconds=1)


def test_a_sweep_page_keeps_the_cutoff_its_pass_started_with(monkeypatch):
    t = Tenant()
    monkeypatch.setattr(search, "URL", "")
    cid = t.closed(t.agent())
    since = close_ago_and_cutoff(cid, timedelta(days=7, hours=1))
    assert queues_write(monkeypatch, cid, since, {"found": False})


def test_a_sweep_page_writes_again_a_conversation_whose_index_write_parked(monkeypatch):
    t = Tenant()
    monkeypatch.setattr(search, "URL", "")
    cid = t.closed(t.agent())
    since, J = close_ago_and_cutoff(cid, timedelta(hours=1)), T.jobs
    with core.tx() as db:
        jobs.schedule(db, "search_index", {"tenant_id": str(t.id), "conversation_id": str(cid)}, key=str(cid))
        db.execute(update(J).where(J.c.kind == "search_index", J.c.key == str(cid)).values(locked_until=jobs.INFINITY))
    assert queues_write(monkeypatch, cid, since, {"found": False})


def test_a_pass_whose_first_page_fails_keeps_its_cutoff_for_the_retry(monkeypatch):
    t = Tenant()
    monkeypatch.setattr(search, "URL", "")
    t.closed(t.agent())
    monkeypatch.setattr(search, "URL", "http://search.test")
    monkeypatch.setattr(search, "HTTP", held(lambda i: {"error": {"type": "no_shard_available_action_exception"}}))
    J = T.jobs
    with core.tx() as db:
        db.execute(delete(J).where(J.c.kind == "search_reconcile"))
    with pytest.raises(ApiError):
        search._reconcile(SimpleNamespace(payload={}))
    with core.read() as db:
        assert list(db.execute(select(J.c.payload).where(J.c.kind == "search_reconcile")).scalar()) == ["since"]


def test_a_sweep_counts_a_document_lost_only_when_the_index_answers_it_is_absent(monkeypatch):
    t = Tenant()
    monkeypatch.setattr(search, "URL", "")
    cid = t.closed(t.agent())
    since, J = close_ago_and_cutoff(cid, timedelta(days=7) - timedelta(hours=1)), T.jobs

    def lost(answer: dict) -> bool:
        with core.tx() as db:
            db.execute(delete(J).where(J.c.kind == "search_index", J.c.key == str(cid)))
        return queues_write(monkeypatch, cid, since, answer)

    assert lost({"found": False}) and lost({"error": {"type": "index_not_found_exception"}})
    with pytest.raises(ApiError):
        lost({"error": {"type": "no_shard_available_action_exception"}})
    assert not queues_write(monkeypatch, cid, since, {"found": True})


def test_a_sweep_runs_parked_index_writes_again_as_it_starts_and_never_past_the_throttle(monkeypatch):
    J = T.jobs
    index = J.c.kind == "search_index"
    with core.tx() as db:
        db.execute(delete(J).where(J.c.kind.in_(("search_index", "search_reconcile"))))
        for key in (str(uuid.uuid4()) for _ in range(4)):
            jobs.schedule(db, "search_index", {"conversation_id": key}, key=key)
        db.execute(update(J).where(J.c.id.in_(select(J.c.id).where(index).limit(3))).values(locked_until=jobs.INFINITY))
    monkeypatch.setattr(search, "URL", "http://search.test")
    monkeypatch.setattr(search, "HTTP", held(lambda i: {"found": True}))

    def sweep(payload: dict) -> tuple:
        search._reconcile(SimpleNamespace(payload=payload))
        parked = select(func.count()).where(index, J.c.locked_until == jobs.INFINITY)
        with core.read() as db:
            then = db.execute(select(J.c.payload).where(J.c.kind == "search_reconcile")).scalar()
            return search._waiting(db), db.execute(parked).scalar(), then

    monkeypatch.setattr(search, "BATCH", 2)
    waiting, parked, paused = sweep({})
    assert (waiting, parked) == (2, 2) and list(paused) == ["since"]
    monkeypatch.setattr(search, "BATCH", 3)
    assert sweep(paused)[:2] == (2, 2)


def test_a_missing_index_finds_nothing_and_the_next_write_rebuilds_it(sink):
    t = Tenant()
    assert t.find() == []
    cid = t.closed(t.agent(), words=["parcel"])
    if search.URL:
        httpx.delete(f"{search.URL}/{search._index(t.id)}", timeout=5 * slow())
    else:
        with core.read() as db:
            db.execute(delete(T.search).where(T.search.c.conversation_id == cid))
    assert t.find() == []
    with core.tx() as db:
        cv.touched(db, cv.lock(db, cid))
    t.index(cid)
    assert t.find(text="parcel") == [cid]


def test_an_index_that_does_not_answer_is_503_and_the_write_waits_to_retry(monkeypatch):
    t = Tenant()
    monkeypatch.setattr(search, "URL", "")
    cid = t.closed(t.agent())
    monkeypatch.setattr(search, "URL", "http://127.0.0.1:9")
    with core.tx() as db:
        cv.touched(db, cv.lock(db, cid))
    jobs.run_now("search_index", str(cid), wait=True)
    with pytest.raises(ApiError) as refused:
        t.find()
    assert (refused.value.status, refused.value.code) == (503, "search_unavailable")
    J = T.jobs
    with core.read() as db:
        job = db.execute(select(J).where(J.c.kind == "search_index", J.c.key == str(cid))).one()
    assert job.attempts == 1 and job.last_error and job.locked_until is None


def test_an_index_answer_that_timed_out_failed_a_shard_is_malformed_or_names_another_index_is_503(monkeypatch):
    t = Tenant()
    cid = str(uuid.uuid4())
    hit = {"_index": search._index(t.id), "_id": cid, "_source": {"id": cid}, "sort": [1, 1]}
    whole = {"timed_out": False, "_shards": {"total": 1, "failed": 0}, "hits": {"total": {"value": 1}, "hits": [hit]}}
    answer = {}
    monkeypatch.setattr(search, "URL", "http://search.test")
    monkeypatch.setattr(search, "HTTP", httpx.Client(transport=httpx.MockTransport(lambda _: answer["now"])))
    answer["now"] = httpx.Response(200, json=whole)
    assert t.find() == [uuid.UUID(cid)]
    foreign = {"hits": {"hits": [hit | {"_index": search._index(uuid.uuid4())}]}}
    bad = ({"timed_out": True}, {"_shards": {"total": 1, "failed": 1}}, {"hits": {"hits": {}}}, foreign)
    for answer["now"] in [*(httpx.Response(200, json=whole | b) for b in bad), httpx.Response(200, text="<html>")]:
        with pytest.raises(ApiError) as refused:
            t.find()
        assert (refused.value.status, refused.value.code) == (503, "search_unavailable"), answer["now"].text


def test_a_filter_atom_negates_a_number_or_a_word_and_text_finds_a_handlers_skill(sink):
    t = Tenant()
    a, b = t.agent(), t.agent()
    with core.tx() as db:
        skills = {"skills": [{"skill_id": str(t.doc(db, "skill", name="Spanish")), "level": 3}]}
        db.execute(T.config.update().where(T.config.c.id == b).values(body=T.config.c.body.concat(skills)))
    two, one = t.closed(a, words=["Hi", "Still there?"]), t.closed(b, words=["Hi"], disposition=t.lost)

    def where(field: str, op: str, value) -> list:
        return t.find(filter={"groups": [{"atoms": [{"field": field, "op": op, "value": value}]}]})

    assert where("message_count", "is_not", 2) == [one] and where("message_count", "between", "2,5") == [two]
    assert where("disposition", "does_not_contain", "SALE") == [one]
    assert t.find(text="spanish") == [one]


def test_each_period_runs_from_its_first_day_to_the_next_ones_in_the_viewers_zone():
    zone = ZoneInfo("Pacific/Kiritimati")
    today, tomorrow = search._span("today", zone)
    assert search._span("yesterday", zone) == (today - timedelta(days=1), today) and tomorrow - today == timedelta(1)
    week, _ = search._span("this_week", zone)
    assert week.weekday() == 0 and search._span("last_week", zone) == (week - timedelta(days=7), week)
    for unit, months in (("month", 1), ("quarter", 3), ("year", 12)):
        start, end = search._span(f"this_{unit}", zone)
        assert start <= today < end and start.day == 1 and (start.month - 1) % months == 0
        assert search._span(f"last_{unit}", zone)[1] == start
    assert search._span("2026-03-08", ZoneInfo("America/New_York"))[1].hour == 0


def test_who_ended_a_call_is_found_after_event_retention_and_after_a_backfill(sink):
    t = Tenant()
    cid = t.closed(t.agent())
    hangup = {"hangup_cause": None, "hangup_cause_code": None, "duration_sec": None, "ended_by": "customer"}
    with core.tx() as db:
        conv = cv.lock(db, cid)
        cv.call_ended(db, conv, hangup, None, None)
        cv.touched(db, conv)
    t.index(cid)
    assert t.find(ended_by="customer") == [cid]
    with core.tx() as db:
        db.execute(delete(T.events).where(T.events.c.conversation_id == str(cid)))
        cv.touched(db, cv.lock(db, cid))
    t.index(cid)
    assert t.find(ended_by="customer") == [cid]
    with core.tx() as db:
        jobs.schedule(db, "search_index", {"tenant_id": str(t.id), "conversation_id": str(cid)}, key=str(cid))
    t.index(cid)
    assert t.find(ended_by="customer") == [cid]


def test_a_phrase_spans_two_texts_on_postgres_only(sink):
    t = Tenant()
    cid = t.closed(t.agent(), words=["Where is my parcel", "Never arrived"])
    assert t.find(text="my parcel") == t.find(text="never arr") == [cid]
    assert t.find(text="parcel never") == ([cid] if sink == "postgres" else [])


def test_one_word_with_punctuation_matches_inside_a_text_of_any_length(sink):
    t = Tenant()
    long = "word " * 1700 + "Your parcel zx-4417 shipped"
    cid = t.closed(t.agent(), words=[long, "Thanks"])
    assert len(long) > 8191
    assert t.find(text="zx-4417") == t.find(text="x-44") == t.find(text="shipped") == [cid]


def test_a_row_the_backfill_has_not_reached_matches_one_word_whole_on_postgres_and_inside_a_word_on_an_index(sink):
    t = Tenant()
    cid = t.closed(t.agent(), words=["My parcel never arrived"])
    with core.read() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one()
        doc = search.document(db, conv)
        if not search.URL:
            db.execute(update(T.search).where(T.search.c.conversation_id == cid).values(content=None))
    if search.URL:
        search.write(conv, {k: doc[k] for k in search.SUMMARY} | {"words": " ".join(doc["words"])})
    t.refresh()
    assert t.find(text="parcel") == [cid]
    assert t.find(text="arce") == ([] if sink == "postgres" else [cid])


def test_a_message_or_note_of_any_allowed_length_matches_whole_in_any_case(sink):
    t = Tenant()
    cid = t.closed(t.agent())
    notes = [("parcel " * 1500)[:n] for n in (search.RAW, search.RAW + 1, 10_000)]
    with core.tx() as db:
        for note in notes:
            cv.entry(db, cv.lock(db, cid), "note", {"text": note})
    t.index(cid)
    for note in notes:
        whole = {"field": "message", "op": "is", "value": note.upper()}
        assert t.find(filter={"groups": [{"atoms": [whole]}]}) == [cid]


def test_each_whole_message_operator_reads_python_lower_case_on_both_backends(sink):
    t = Tenant()
    agent = t.agent()
    uni, long, blank, none = (t.closed(agent) for _ in range(4))
    texts = {uni: "Straße Ünïcode café", long: ("Parcel " * 1500)[:10_000], blank: ""}
    with core.tx() as db:
        for cid, note in texts.items():
            cv.entry(db, cv.lock(db, cid), "note", {"text": note})
    for cid in (uni, long, blank, none):
        t.index(cid)

    def found(op: str, value: str | None = None) -> set:
        atom = {"field": "message", "op": op} | ({"value": value} if value is not None else {})
        return set(t.find(filter={"groups": [{"atoms": [atom]}]}))

    assert found("is", "STRAßE ÜNÏCODE CAFÉ") == {uni}
    assert found("is", texts[long].upper()) == {long}
    assert found("is_not", "straße ünïcode café") == {long, blank, none}
    assert found("empty") == {blank, none}
    assert found("not_empty") == {uni, long}


def test_a_conversations_wrap_time_sums_each_agents_own_wrap_up():
    t = tenants.Tenant()
    first, second = t.agent(), t.agent()
    cid = t.closed(first)
    at = func.now() - timedelta(minutes=10)
    stamps = {"started_at": at + timedelta(seconds=5), "ended_at": at + timedelta(seconds=20)}
    stamps |= {"wrap_up_until": at + timedelta(seconds=60), "submitted_at": at + timedelta(seconds=30)}
    late = {"role": "secondary", "via": "participant", "started_at": at + timedelta(seconds=40)}
    late |= {"ended_at": at + timedelta(seconds=90), "wrap_up_until": at + timedelta(seconds=150)}
    with core.tx() as db:
        db.execute(
            update(T.conversation)
            .where(T.conversation.c.id == cid)
            .values(opened_at=at, closed_at=at + timedelta(seconds=100))
        )
        db.execute(update(T.assignment).where(T.assignment.c.conversation_id == cid).values(**stamps))
        db.execute(
            insert(T.assignment).values(
                **late, tenant_id=t.id, conversation_id=cid, agent_id=second, channel="call", state="done"
            )
        )
        wrap = db.execute(search.FACTS, {"c": cid}).one().wrap_sec
    assert wrap == 20


@pytest.mark.parametrize("sink", ["opensearch"], indirect=True)
def test_two_first_writes_racing_an_older_replicas_template_both_land_and_sort_by_text(sink, monkeypatch):
    template = "/_index_template/hs-conversations-fields"
    unsorted = {"fields": {"text": {"type": "text"}}}
    props = {k: v | unsorted if k in search.TEXTS else v for k, v in search.MAPPING["properties"].items()}
    older = {"index_patterns": [search._index("*")], "priority": 1}
    older |= {"template": {"mappings": search.MAPPING | {"properties": props}}}
    request = search._request

    def raced(method: str, path: str, *a, **kw) -> httpx.Response:
        r = request(method, path, *a, **kw)
        if path == template:
            request("PUT", template, json=older)
        return r

    t = Tenant()
    agent = t.agent()
    late, early = (tenants.Tenant.closed(t, agent, customer=n) for n in ("+15550009", "+15550001"))
    start = threading.Barrier(2)

    def write(cid) -> None:
        start.wait()
        jobs.run_now("search_index", str(cid), wait=True)

    monkeypatch.setattr(search, "_request", raced)
    try:
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(write, (late, early)))
    finally:
        request("PUT", template, json=older | {"template": {"mappings": search.MAPPING}})
    t.refresh()
    assert t.find(sort="number", order="asc") == [early, late]
