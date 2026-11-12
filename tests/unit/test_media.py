import uuid
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import select

from hs import core
from hs.core import T
from hs.voice import media, mpc
from tests.unit.tenants import Tenant


def test_a_room_recording_waits_its_whole_grace_from_when_it_is_written():
    t, rid = Tenant(), uuid.uuid4().hex
    conv = t.open()
    form = {"EventName": "MPCRecordingCompleted", "RecordingUUID": rid, "RecordingDuration": "5", "MPCUUID": "room"}
    media.recording(conv.id, tenant=SimpleNamespace(id=t.id), form=form)
    E, J = T.entry, T.jobs
    with core.read() as db:
        kept = db.execute(select(E.c.id, E.c.written_at).where(E.c.provider_id == rid)).one()
        due = db.execute(select(J.c.run_at).where(J.c.kind == "transcript_settle", J.c.key == str(kept.id))).scalar()
    assert due >= kept.written_at + timedelta(seconds=mpc.CALLBACK_GRACE_SEC)
