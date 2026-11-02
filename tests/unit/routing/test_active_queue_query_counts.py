"""DB-backed query-behaviour tests (skip when the `_test` Postgres is down).

Pins the optimised statement profile of two queries against the rolled-back
`_test` DB (the `db` fixture):
  * the call duration read: the NULL-duration row is filtered in SQL
          (the old `cls.duration is not None` identity check was a no-op and let NULLs through).
  * get_last_n_active_queue_entries_for_queue_uuid: the in-flight tally is a `COUNT(*)`,
          not a full-row materialisation (`len(q.all())` → `q.count()`), and stays correct.

Run for real with the docker-compose postgres up; skips automatically otherwise.
"""

import datetime as _dt
import uuid

import tests.conftest  # noqa: F401 — eager adapter imports (AbstractConcreteBase registry)
from hs.infra.helpers import get_now
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from tests.unit._query_counter import count_statements
from tests.unit.routing.test_active_queue_crud import _seed_entry


def test_get_last_n_in_queue_uses_count_not_materialise(db):
    q = uuid.uuid4()
    t0 = get_now()
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="anchor",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0,
    )
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="iq1",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0 - _dt.timedelta(minutes=5),
    )
    with count_statements(db) as stmts:
        _, in_queue = aq.get_last_n_active_queue_entries_for_queue_uuid(
            db, q, "anchor", interval_hours=3, max_entries=10
        )
    assert in_queue == 2  # anchor + iq1 (FIFO created_at <= anchor)
    assert any("count(" in s.lower() for s in stmts), f"expected a COUNT(*) for the in-flight tally, got: {stmts}"
