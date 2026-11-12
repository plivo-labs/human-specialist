import uuid

from sqlalchemy import func, insert, null, select, update

from hs import conversations as cv
from hs import core, export, search
from hs.core import T
from tests.unit.tenants import Tenant


def test_an_export_reads_the_search_a_page_at_a_time_and_stops_at_the_cap(monkeypatch):
    asked = []

    def endless(p, body, paged, fields=()):
        asked.append(paged.limit)
        return {"data": [{"id": str(len(asked))}] * paged.limit, "next_cursor": "MQ=="}

    monkeypatch.setattr(search, "run", endless)
    monkeypatch.setattr(export, "CAP", 450)
    assert sum(map(len, export._pages(None, export.ExportIn()))) == 450 and asked == [200, 200, 50]


def test_the_content_reads_items_a_batch_at_a_time_in_the_search_order(monkeypatch):
    t = Tenant()
    ids = [str(t.closed(t.agent(), words=("Hi", "There", "Again"))) for _ in range(2)][::-1]
    [whole] = export._items(ids)
    monkeypatch.setattr(export, "BATCH", 2)
    batches = list(export._items(ids))
    assert max(map(len, batches)) == 2 and [r.id for b in batches for r in b] == [r.id for r in whole]
    assert len(whole) > 6 and list(dict.fromkeys(r.conversation_id for r in whole)) == ids


def test_a_transfer_entry_is_one_row_naming_its_target_and_the_agent_who_took_it_and_rings_without_an_entry_have_none():
    t = Tenant()
    first, took, missed = t.agent(), t.agent(), t.agent()
    cid = t.closed(first)
    logged, older, G = uuid.uuid4(), uuid.uuid4(), T.config
    with core.tx() as db:
        for agent, name in ((first, "First"), (took, "Took"), (missed, "Missed")):
            db.execute(update(G).where(G.c.id == agent).values(body=G.c.body.concat({"name": name})))
        conv, team = cv.lock(db, cid), t.doc(db, "team", name="Escalations")
        targets = ({"type": "team", "team_id": str(team)}, {"type": "queue", "queue_id": str(t.queue)})
        for tid, target in zip((logged, uuid.uuid4()), targets, strict=True):
            body = {"transfer_id": str(tid), "mode": "cold", "target": target}
            cv.entry(db, conv, "transfer", body, agent_id=first)
        rung = {"tenant_id": t.id, "conversation_id": cid, "role": "secondary", "via": "transfer", "channel": "call"}
        for tid in (logged, older):
            for agent, started in ((took, func.now()), (missed, None)):
                row = rung | {"agent_id": agent, "state": "ended", "transfer_id": tid, "started_at": started}
                db.execute(insert(T.assignment).values(**row))
    [rows] = export._items([str(cid)])
    assert sorted((r.agent_name or "", r.to or "", r.text, r.meta) for r in rows if r.type == "transfer") == [
        ("First", "", "Q", "queue"),
        ("First", "Took", "Escalations", "team"),
    ]


def _closed_with_a_failed_message():
    t = Tenant()
    with core.tx() as db:
        team = t.doc(db, "team", name="Billing")
    agent = t.agent(team)
    cid = t.closed(agent)
    with core.tx() as db:
        db.execute(update(T.assignment).where(T.assignment.c.conversation_id == cid).values(note="Sorted"))
        cv.entry(
            db, cv.lock(db, cid), "message", {"direction": "outbound", "text": "Lost"}, agent_id=agent, status="failed"
        )
    with core.read() as db:
        facts = db.execute(export.FACTS, {"ids": [str(cid)]}).one()
        [items] = export._items([str(cid)])
        doc = search.document(db, db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one())
    return facts, items, doc


def test_the_summary_leaves_out_a_failed_message_and_counts_a_disposition_note():
    assert _closed_with_a_failed_message()[2]["types"] == ["notes"]


def test_the_content_leaves_out_a_failed_message():
    assert "Lost" not in [r.text for r in _closed_with_a_failed_message()[1]]


def test_the_team_cell_lists_the_teams_of_the_agents_who_handled_it():
    assert _closed_with_a_failed_message()[0].teams == ["Billing"]


def test_the_team_cell_keeps_the_teams_of_the_close_and_reads_todays_only_for_a_conversation_closed_without_them():
    t = Tenant()
    with core.tx() as db:
        billing, sales = t.doc(db, "team", name="Billing"), t.doc(db, "team", name="Sales")
    agent = t.agent(billing)
    cid = t.closed(agent)
    G, C = T.config, T.conversation

    def teams() -> list[str]:
        with core.read() as db:
            return db.execute(export.FACTS, {"ids": [str(cid)]}).one().teams

    with core.tx() as db:
        db.execute(update(G).where(G.c.id == agent).values(body=G.c.body.concat({"teams": [str(sales)]})))
    assert teams() == ["Billing"], "the agent moved to Sales after the close"
    with core.tx() as db:
        db.execute(update(C).where(C.c.id == cid).values(team_ids=null()))
    assert teams() == ["Sales"]


def test_a_customer_message_names_the_agent_of_the_stint_it_came_in():
    t = Tenant()
    cid = t.closed(t.agent(), words=("Hi",))
    [rows] = export._items([str(cid)])
    assert [(r.type, r.agent_name) for r in rows if r.type == "message"] == [("message", "A")]
