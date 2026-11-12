from datetime import UTC, datetime, timedelta

import pytest
from redis import RedisError
from sqlalchemy import update

from hs import conversations as cv
from hs import core, routing
from hs.core import T
from tests.unit.test_routing import Floor, age, sweep


@pytest.fixture
def floor(monkeypatch) -> Floor:
    return Floor(monkeypatch)


def wrote(cid, agent=None, at=None) -> None:
    with core.tx() as db:
        body = {"direction": "outbound" if agent else "inbound", "text": "Hi"}
        cv.entry(db, cv.lock(db, cid), "message", body, agent_id=agent, at=at)


def unread(floor, cid) -> int:
    with core.read() as db:
        return cv.view(db, floor.conv(cid))["unread_count"]


def lost(floor) -> None:
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))


def test_a_message_after_an_agents_read_counts_on_top_of_the_0_the_read_set(floor):
    agent = floor.agent()
    cid = floor.open("sms").id
    wrote(cid, agent)
    wrote(cid)
    lost(floor)
    cv.inbox.mark(floor.conv(cid), read=True)
    wrote(cid)
    sweep()
    assert unread(floor, cid) == 1


def test_a_message_no_agent_saw_creates_no_unread_count_and_reads_its_count_from_postgres(floor):
    cid = floor.open("sms").id
    wrote(cid)
    assert not core.redis.exists(routing.hot.UNREAD % (floor.tenant, cid))
    assert unread(floor, cid) == 1


def test_a_read_while_redis_does_not_answer_counts_from_postgres(floor, monkeypatch):
    cid = floor.open("sms").id
    wrote(cid)

    def down(*_):
        raise RedisError("down")

    monkeypatch.setattr(routing.hot, "unread_counts", down)
    assert unread(floor, cid) == 1


def test_a_message_written_after_an_agents_reply_counts_after_a_redis_loss_whatever_time_it_carries(floor):
    agent = floor.agent()
    cid = floor.open("sms").id
    wrote(cid, agent)
    wrote(cid, at=datetime.now(UTC) - timedelta(minutes=5))
    assert unread(floor, cid) == 1, "counted as it arrived"
    lost(floor)
    assert unread(floor, cid) == 1, "counted from Postgres"


def test_after_a_redis_loss_messages_written_before_entries_recorded_it_count_in_the_order_of_their_time(floor):
    agent = floor.agent()
    cid = floor.open("sms").id
    wrote(cid, agent)
    wrote(cid)
    with core.tx() as db:
        db.execute(update(T.entry).where(T.entry.c.conversation_id == cid).values(written_at=None))
    lost(floor)
    assert unread(floor, cid) == 1


def test_an_open_text_keeps_its_unread_count_through_a_quiet_day(floor):
    cid = floor.open("sms").id
    wrote(cid)
    cv.inbox.mark(floor.conv(cid), read=True)
    wrote(cid)
    age(26 * 3600)
    assert unread(floor, cid) == 1
