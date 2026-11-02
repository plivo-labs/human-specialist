"""Voice persistence: call-leg semantics (role gate, (leg,role) dedupe-201, internal-participant aom
resolve, the parent-keyed-by-CHILD-uuid auto-create), the per-leg recording variant (dedupe 422 / 200)
vs the full one (recording_uuid dedupe 422 / 201 / chunk-or-direct-row), the callback event gates
(duration<1s, track-type 'all', the main-call parse-first failure, the '/Recording/' format sniff),
the stream recording↔transcription redis rendezvous, the transcript create/read math (segment
start_ts, the parent-relative stream timing, the MPC-gate duplicate-error reuse, the start_ts window
filter, speaker resolution), the chunk-sink enrichment (Recording from chunk), and the
recording/supervisor-left publish branches.
"""

import datetime

import pytest

import hs.conversations.child_update as ccu
import hs.conversations.management as convmgmt
import hs.conversations.store as da_conv
import hs.voice.call_legs as cm
import hs.voice.call_store as da_call
import hs.voice.recording as vrec
import hs.voice.transcripts as tm
import tests.conftest  # noqa: F401
from hs.infra.models import UseCaseResponse
from hs.voice.call_models import CallLegInsertModel, CallRecordingRequestModel

CT = "11111111-2222-3333-4444-555555555555"


class Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _fake_callleg(monkeypatch, legs=None, create_raises=None):
    """Patch cm.CallLeg: `legs` maps (uuid, role-or-None) -> Row; creates append + get an id."""
    legs = dict(legs or {})
    created, updated = [], []

    class FCL:
        end_time = Row(name="end_time")
        start_time = Row(name="start_time")
        participant = Row(name="participant")

        @staticmethod
        def get_by_call_uuid(uuid_, role=None, **_kw):
            return legs.get((uuid_, role))

        @staticmethod
        def get_any_by_call_uuid(uuid_):
            return next((row for (candidate_uuid, _role), row in legs.items() if candidate_uuid == uuid_), None)

        @staticmethod
        def outside_call(_leg_uuid, _call_uuid):
            return False

        @staticmethod
        def create_call_leg(row):
            if create_raises:
                raise create_raises
            row.id = 100 + len(created)
            created.append(row)
            legs[(row.call_uuid, getattr(row, "role", None))] = row
            return row

        @staticmethod
        def update_call_leg(uuid_, d):
            updated.append((uuid_, dict(d)))
            for (candidate_uuid, _role), row in legs.items():
                if candidate_uuid == uuid_:
                    for key, value in d.items():
                        setattr(row, key, value)

        @staticmethod
        def get_by_id(i):
            return legs.get(i)

    monkeypatch.setattr(cm, "CallLeg", FCL)
    return created, updated, legs


def _quiet_plumbing(monkeypatch):
    import hs.voice.hangup as vh
    import hs.voice.monitor as vm

    monkeypatch.setattr(vm, "_seed_context", lambda meta: None)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)


# ============================================================================ call legs
def test_create_call_leg_role_required(monkeypatch):
    _fake_callleg(monkeypatch)
    resp = cm.create_call_leg(
        CallLegInsertModel(call_uuid="par", call_leg_uuid="leg", start_time="2026-06-13 10:00:00")
    )
    assert (resp.status_code, resp.error) == (400, "Role is required")


def test_create_call_leg_dedupe_201(monkeypatch):
    existing = Row(id=7, start_time="2026-06-13 10:00:00", participant="77")
    _fake_callleg(monkeypatch, legs={("leg", "agent"): existing})
    resp = cm.create_call_leg(
        CallLegInsertModel(
            call_uuid="par",
            call_leg_uuid="leg",
            role="agent",
            member_address="sip:b@x",
            start_time="2026-06-13 10:00:00",
        )
    )
    assert (resp.status_code, resp.msg) == (201, "Call leg already exists")
    assert resp.data is existing


def test_create_call_leg_internal_member_address_gates(monkeypatch):
    _fake_callleg(monkeypatch)
    base = {"call_uuid": "par", "call_leg_uuid": "leg", "role": "agent", "start_time": "2026-06-13 10:00:00"}
    resp = cm.create_call_leg(CallLegInsertModel(**base))
    assert (resp.status_code, resp.error) == (400, "Member address is not a valid SIP endpoint")
    monkeypatch.setattr(cm, "AccountOrgMapping", Row(get_by_sip_endpoint=staticmethod(lambda sip: None)))
    resp = cm.create_call_leg(CallLegInsertModel(member_address="sip:ben@x", **base))
    assert (resp.status_code, resp.error) == (400, "Account org mapping not found for SIP endpoint")


def test_create_call_leg_participant_resolution(monkeypatch):
    # sip: internal -> participant = aom.id; non-sip internal (a phone) skips the resolve.
    created, _u, _legs = _fake_callleg(monkeypatch, legs={("par", None): Row(id=50)})
    monkeypatch.setattr(cm, "AccountOrgMapping", Row(get_by_sip_endpoint=staticmethod(lambda sip: Row(id=77))))
    resp = cm.create_call_leg(
        CallLegInsertModel(
            call_uuid="par",
            call_leg_uuid="leg",
            role="agent",
            member_address="sip:ben@x",
            start_time="2026-06-13 10:00:00",
        )
    )
    assert (resp.status_code, resp.msg) == (201, "Call leg created successfully")
    assert str(created[-1].participant) == "77" and created[-1].parent_call_leg == 50
    cm.create_call_leg(
        CallLegInsertModel(
            call_uuid="par",
            call_leg_uuid="leg2",
            role="customer",
            member_address="+15551234",
            start_time="2026-06-13 10:00:00",
        )
    )
    assert created[-1].participant == "+15551234"


def test_create_call_leg_parent_autocreate_uses_canonical_parent_uuid(monkeypatch):
    # Participant callbacks may arrive first. The fallback must create the
    # canonical MPC parent by call_uuid and link the participant to it.
    created, _u, _l = _fake_callleg(monkeypatch)
    resp = cm.create_call_leg(
        CallLegInsertModel(
            call_uuid="par",
            call_leg_uuid="leg",
            role="customer",
            member_address="+15551234",
            start_time="2026-06-13 10:00:00",
        )
    )
    assert resp.status_code == 201 and len(created) == 2
    parent, child = created
    assert parent.call_uuid == "par"
    assert child.call_uuid == "leg" and child.parent_call_leg == parent.id


def test_authoritative_mpc_start_repairs_participant_first_parent_only_earlier(monkeypatch):
    existing = Row(id=9, start_time="2026-07-23 10:05:00")
    _created, updated, _legs = _fake_callleg(monkeypatch, legs={("parent", None): existing})

    resp = cm.create_parent_call_leg(
        CallLegInsertModel(call_uuid="parent", call_leg_uuid="parent", start_time="2026-07-23 10:00:00")
    )

    assert resp.status_code == 201
    assert updated == [("parent", {"start_time": "2026-07-23 10:00:00"})]
    assert existing.start_time == "2026-07-23 10:00:00"

    # A duplicate with a later timestamp cannot move the canonical start forward.
    updated.clear()
    cm.create_parent_call_leg(
        CallLegInsertModel(call_uuid="parent", call_leg_uuid="parent", start_time="2026-07-23 10:10:00")
    )
    assert updated == []
    assert existing.start_time == "2026-07-23 10:00:00"


def test_authoritative_mpc_start_repairs_concurrent_fallback_insert_winner(monkeypatch):
    existing = Row(id=9, start_time="2026-07-23 10:05:00")
    _created, updated, _legs = _fake_callleg(
        monkeypatch, legs={("parent", None): existing}, create_raises=RuntimeError("duplicate key value (23505)")
    )
    lookups = {"count": 0}

    def lookup(_uuid, role=None, **_kwargs):
        lookups["count"] += 1
        return None if lookups["count"] == 1 else existing

    monkeypatch.setattr(cm.CallLeg, "get_by_call_uuid", staticmethod(lookup))

    resp = cm.create_parent_call_leg(
        CallLegInsertModel(call_uuid="parent", call_leg_uuid="parent", start_time="2026-07-23 10:00:00")
    )

    assert (resp.status_code, resp.error) == (201, None)
    assert updated == [("parent", {"start_time": "2026-07-23 10:00:00"})]
    assert existing.start_time == "2026-07-23 10:00:00"


def test_terminal_first_agent_without_member_address_persists_and_late_join_enriches(monkeypatch):
    created, updated, _legs = _fake_callleg(
        monkeypatch, legs={("parent", None): Row(id=50, start_time="2026-07-23 10:00:00")}
    )

    terminal = CallLegInsertModel(
        call_uuid="parent",
        call_leg_uuid="agent-leg",
        role="agent",
        member_address=None,
        start_time="2026-07-23 10:05:00",
        end_time="2026-07-23 10:06:00",
    )
    monkeypatch.setattr(
        cm,
        "_set_terminal_end_time_once",
        lambda call_uuid, end_time: (
            updated.append((call_uuid, {"end_time": end_time}))
            or UseCaseResponse(status_code=200, msg=cm.MSG_UPDATE_CALL_LEG_SUCCESS)
        ),
    )
    resp = cm.finalize_call_leg(terminal)

    assert (resp.status_code, resp.error) == (200, None)
    assert len(created) == 1
    assert created[0].call_uuid == "agent-leg"
    assert created[0].participant is None
    assert updated[-1] == ("agent-leg", {"end_time": "2026-07-23 10:06:00"})

    monkeypatch.setattr(cm, "AccountOrgMapping", Row(get_by_sip_endpoint=staticmethod(lambda _sip: Row(id=77))))
    resp = cm.create_call_leg(
        CallLegInsertModel(
            call_uuid="parent",
            call_leg_uuid="agent-leg",
            role="agent",
            member_address="sip:agent@x",
            start_time="2026-07-23 10:04:00",
        )
    )

    assert (resp.status_code, resp.error) == (201, None)
    assert updated[-1] == ("agent-leg", {"start_time": "2026-07-23 10:04:00", "participant": 77})


def test_create_parent_call_leg_dedupe_and_race(monkeypatch):
    existing = Row(id=9)
    _fake_callleg(monkeypatch, legs={("leg", None): existing})
    resp = cm.create_parent_call_leg(CallLegInsertModel(call_uuid="par", call_leg_uuid="leg", start_time="t"))
    assert (resp.status_code, resp.msg) == (201, "Parent call leg already exists")
    # duplicate-key race: create raises 23505 -> re-fetch wins the same 201.
    _fake_callleg(monkeypatch, create_raises=RuntimeError("duplicate key value (23505)"))
    seen = {"n": 0}

    def _get(uuid_, role=None, **_kw):
        seen["n"] += 1
        return existing if seen["n"] > 1 else None  # miss, then the race re-fetch hits

    monkeypatch.setattr(cm.CallLeg, "get_by_call_uuid", staticmethod(_get))
    resp = cm.create_parent_call_leg(CallLegInsertModel(call_uuid="par", call_leg_uuid="leg", start_time="t"))
    assert (resp.status_code, resp.msg, resp.data) == (201, "Parent call leg already exists", existing)
    _fake_callleg(monkeypatch, create_raises=RuntimeError("boom"))
    resp = cm.create_parent_call_leg(CallLegInsertModel(call_uuid="par", call_leg_uuid="leg", start_time="t"))
    assert (resp.status_code, resp.msg, resp.error) == (400, "Failed to create call leg", "boom")


def test_da_create_call_leg_savepoint_isolates_duplicate_flush(monkeypatch):
    """The data-adapter INSERT is savepoint-isolated: a duplicate-key flush rolls back only the
    nested block and re-raises, leaving the request session USABLE (not aborted) so callers can
    recover by re-fetching. Regression for the MpcParticipantJoin call-leg race that poisoned the
    whole request session ('This Session's transaction has been rolled back')."""
    from contextlib import contextmanager

    from sqlalchemy.exc import IntegrityError

    class FakeSession:
        def __init__(self, flush_raises=None):
            self.flush_raises = flush_raises
            self.events = []
            self.depth = 0
            self.aborted = False  # models SQLAlchemy's post-failed-flush aborted state

        @contextmanager
        def begin_nested(self):
            self.events.append("savepoint")
            self.depth += 1
            try:
                yield
            except Exception:
                self.events.append("rollback")  # savepoint rollback keeps the session usable
                self.depth -= 1
                raise
            else:
                self.events.append("release")
                self.depth -= 1

        def add(self, _obj):
            self.events.append("add")

        def flush(self):
            self.events.append("flush")
            if self.flush_raises:
                if self.depth == 0:  # a bare flush (no savepoint) would poison the session
                    self.aborted = True
                raise self.flush_raises

    class _Leg:
        def _CallLeg__to_model(self):  # name-mangled target of call_leg.__to_model()
            return "MODEL_OK"

    # happy path: add + flush run INSIDE the savepoint, then release
    ok = FakeSession()
    monkeypatch.setattr(da_call, "get_db_session", lambda: ok)
    assert da_call.CallLeg.create_call_leg(_Leg()) == "MODEL_OK"
    assert ok.events == ["savepoint", "add", "flush", "release"]

    # duplicate-key path: savepoint rolls back, exception propagates, session NOT aborted
    dup = FakeSession(flush_raises=IntegrityError("INSERT", {}, Exception("duplicate key value (23505)")))
    monkeypatch.setattr(da_call, "get_db_session", lambda: dup)
    with pytest.raises(IntegrityError):
        da_call.CallLeg.create_call_leg(_Leg())
    assert dup.events == ["savepoint", "add", "flush", "rollback"]
    assert dup.aborted is False  # the point: session survives for the caller's re-fetch


@pytest.mark.parametrize("initial_empty", [None, ""])
@pytest.mark.parametrize("replay_end_time", ["2026-07-23 09:59:59", "2026-07-23 10:00:01"])
def test_terminal_end_time_is_first_writer_wins_under_replay(monkeypatch, initial_empty, replay_end_time):
    state = {"end_time": initial_empty, "writes": [], "flushes": 0}

    class FakeDB:
        def flush(self):
            state["flushes"] += 1

    class Query:
        def filter(self, *_conditions):
            return self

        def update(self, values, synchronize_session=False):
            assert synchronize_session is False
            if state["end_time"] not in (None, ""):
                return 0
            state["end_time"] = values["end_time"]
            state["writes"].append(values["end_time"])
            return 1

    monkeypatch.setattr(cm, "get_db_session", lambda: FakeDB())
    monkeypatch.setattr(cm.CallLeg, "select_query", classmethod(lambda cls, _db: Query()))
    monkeypatch.setattr(
        cm.CallLeg,
        "get_any_by_call_uuid",
        classmethod(lambda cls, call_uuid: Row(call_uuid=call_uuid, end_time=state["end_time"])),
    )

    first = "2026-07-23 10:00:00"
    first_response = cm._set_terminal_end_time_once("terminal-leg", first)
    replay_response = cm._set_terminal_end_time_once("terminal-leg", replay_end_time)

    assert (first_response.status_code, replay_response.status_code) == (200, 200)
    assert state["end_time"] == first
    assert state["writes"] == [first]


# ============================================================================ recording management
def _fake_recording(monkeypatch, by_uuid=None, by_leg=None):
    # Patch the REAL class's methods (build_db_model constructs Recording(**...) and reads
    # its Column descriptors, so the class itself must stay).
    created = []
    R = da_conv.Recording
    monkeypatch.setattr(R, "get_by_recording_uuid", classmethod(lambda cls, u, **_kw: by_uuid))
    monkeypatch.setattr(R, "get_by_call_leg_id", classmethod(lambda cls, i: by_leg))
    monkeypatch.setattr(R, "create_recording", classmethod(lambda cls, row: created.append(row) or row))
    monkeypatch.setattr(
        R,
        "create_from_chunk",
        classmethod(lambda cls, chunk, call_leg_id=None: created.append((chunk, call_leg_id)) or Row(id=9)),
    )
    return created


def _req_model(**over):
    base = {
        "call_uuid": "par",
        "call_leg_uuid": "leg",
        "recording_url": "http://r/x.mp3",
        "recording_duration": "30",
        "recording_uuid": "rec-1",
        "recording_format": "mp3",
        "recording_start_time": "2026-06-13 10:00:00",
        "recording_end_time": "2026-06-13 10:00:30",
        "recording_type": "mpc",
    }
    base.update(over)
    return CallRecordingRequestModel(**base)


def test_per_leg_recording_variant(monkeypatch):
    resp = convmgmt.create_recording(_req_model(call_uuid="", call_leg_uuid=""))
    assert (resp.status_code, resp.error) == (400, "Call UUID or Call Leg UUID is required")
    seen = {}

    class FCL:
        @staticmethod
        def get_by_call_uuid(u, participant_leg=False, **_kw):
            seen["flag"] = participant_leg
            return

    monkeypatch.setattr(da_call, "CallLeg", FCL)
    resp = convmgmt.create_recording(_req_model(recording_type="participant"))
    assert (resp.status_code, resp.error) == (422, "Call Leg not found")
    assert seen["flag"] is True  # the participant child-leg lookup
    monkeypatch.setattr(FCL, "get_by_call_uuid", staticmethod(lambda u, **_kw: Row(id=4)))
    _fake_recording(monkeypatch, by_leg=Row(id=8))  # per-call-LEG dedupe (not per uuid)
    resp = convmgmt.create_recording(_req_model())
    assert (resp.status_code, resp.error) == (422, "Recording already exists")
    created = _fake_recording(monkeypatch)
    resp = convmgmt.create_recording(_req_model())
    assert (resp.status_code, resp.msg) == (200, "Recording Created Successfully")
    assert created[0].call_leg_id == 4 and created[0].recording_uuid == "rec-1"


# ============================================================================ px superset
def _chunk_sink(monkeypatch, error=None):
    chunks = []

    def fake(non_qnr_chunk=None, **_kw):
        chunks.append(non_qnr_chunk)
        return UseCaseResponse(status_code=400 if error else 201, error=error)

    monkeypatch.setattr(ccu, "add_chunk_to_non_qnr_child", fake)
    return chunks


def _px_env(monkeypatch, leg=Row(id=4), by_uuid=None, foreign=False):
    _quiet_plumbing(monkeypatch)
    fake = Row(get_by_call_uuid=staticmethod(lambda u, **_kw: leg), outside_call=staticmethod(lambda *_: foreign))
    monkeypatch.setattr(da_call, "CallLeg", fake)
    return _fake_recording(monkeypatch, by_uuid=by_uuid)


def test_px_create_recording_gates(monkeypatch):
    _quiet_plumbing(monkeypatch)
    status, msg, err = vrec.px_create_recording({})
    assert (status, err) == (400, "call_leg_uuid or call_uuid is required")
    _px_env(monkeypatch, leg=None)
    status, msg, err = vrec.px_create_recording({"call_uuid": "par", "call_leg_uuid": "leg"})
    assert (status, msg, err) == (404, "Call leg not found", "call leg not found")
    _px_env(monkeypatch, foreign=True)
    status, msg, err = vrec.px_create_recording({"call_uuid": "par", "call_leg_uuid": "leg"})
    assert (status, err) == (400, "call leg belongs to another call")
    _px_env(monkeypatch, by_uuid=Row(id=8))  # recording_uuid dedupe (not per leg)
    status, msg, err = vrec.px_create_recording({"call_uuid": "par", "call_leg_uuid": "leg", "recording_uuid": "rec-1"})
    assert (status, msg) == (409, "Recording with same UUID already exists")


def test_px_create_recording_chunk_path(monkeypatch):
    created = _px_env(monkeypatch)
    chunks = _chunk_sink(monkeypatch)
    status, msg, err = vrec.px_create_recording(
        {
            "call_uuid": "par",
            "call_leg_uuid": "leg",
            "recording_uuid": "rec-1",
            "recording_url": "u",
            "recording_type": "main",
            "create_chunk": True,
        }
    )
    assert (status, msg, err) == (201, "Recording created successfully", None)
    assert created[0].call_leg_id == 4  # the row is written FIRST, w/ the FK
    chunk = chunks[0]
    assert (chunk.parent_contact_uuid, chunk.type, chunk.is_main_recording) == (
        "par",
        "recording",
        True,
    )  # ai flag ONLY for type=='main'
    assert chunk.recording == created[0].id  # the px FK preset on the chunk
    assert chunk.attributes[0].recording_uuid == "rec-1"  # pydantic-coerced attrs


def test_px_create_recording_chunk_failure_and_direct_row(monkeypatch):
    import hs.infra.context as ctxm

    flagged = []
    monkeypatch.setattr(ctxm, "set_db_session_rollback", lambda: flagged.append(True))
    _px_env(monkeypatch)
    _chunk_sink(monkeypatch, error="sink down")
    status, msg, err = vrec.px_create_recording(
        {"call_uuid": "par", "call_leg_uuid": "leg", "recording_type": "mpc", "create_chunk": True}
    )
    assert (status, msg, err) == (400, "Failed to create chunk", "failed to create chunk")
    assert flagged  # px rolls the recording row back too
    created = _px_env(monkeypatch)
    status, _m, err = vrec.px_create_recording(
        {"call_uuid": "par", "call_leg_uuid": "p-leg", "recording_uuid": "rec-2", "recording_type": "participant"}
    )  # no create_chunk -> DIRECT row
    assert status == 201 and err is None
    assert created[0].call_leg_id == 4 and created[0].recording_uuid == "rec-2"


# ============================================================================ recording callback events
def _capture_px(monkeypatch, ret=(201, "ok", None)):
    reqs = []
    monkeypatch.setattr(vrec, "px_create_recording", lambda req: reqs.append(req) or ret)
    return reqs


def test_mpc_recording_event_gates_and_mapping(monkeypatch):
    reqs = _capture_px(monkeypatch)
    assert vrec.handle_mpc_recording_event(CT, {"RecordingDuration": "0"}) == ("", None)
    assert reqs == []  # duration<1s ignored
    vrec.handle_mpc_recording_event(CT, {"RecordingDuration": "oops", "RecordingUUID": "r1"})
    assert reqs[0]["call_leg_uuid"] == CT  # a parse error SKIPS the gate
    assert (reqs[0]["recording_type"], reqs[0]["create_chunk"]) == ("mpc", True)
    # EVERY px failure (404/409/400…) surfaces as the integration boundary string.
    _capture_px(monkeypatch, ret=(409, "Recording with same UUID already exists", "dup"))
    _s, err = vrec.handle_mpc_recording_event(CT, {"RecordingDuration": "30"})
    assert err == "error in creating recording"


def test_participant_recording_event(monkeypatch):
    reqs = _capture_px(monkeypatch)
    assert vrec.handle_participant_recording_event(CT, {"RecordTrackType": "all", "RecordingDuration": "30"}) == (
        "",
        None,
    )
    assert reqs == []  # track-type 'all' ignored
    _s, err = vrec.handle_participant_recording_event(CT, {"RecordingDuration": "30", "ParticipantCallUUID": "p-leg"})
    assert err is None
    assert (reqs[0]["call_leg_uuid"], reqs[0]["recording_type"]) == ("p-leg", "participant")
    assert "create_chunk" not in reqs[0]  # participant writes the row directly


def test_main_recording_event_parses_the_start_time_first(monkeypatch):
    reqs = _capture_px(monkeypatch)
    _s, err = vrec.handle_main_call_recording_event(CT, {"RecordingStartMs": "x", "RecordingDuration": "0"})
    assert err == 'not an integer: "x"'
    assert reqs == []  # parse fails BEFORE the duration gate
    _s, err = vrec.handle_main_call_recording_event(
        CT, {"RecordingStartMs": "1760000000000", "RecordingEndMs": "1760000030000", "RecordingDuration": "0"}
    )
    assert err is None and reqs == []  # then duration<1s ignores
    vrec.handle_main_call_recording_event(
        CT,
        {
            "RecordingStartMs": "1760000000000",
            "RecordingEndMs": "1760000030000",
            "RecordingDuration": "30",
            "RecordingID": "r9",
            "RecordUrl": "http://x/Recording/abc.wav",
        },
    )
    req = reqs[0]
    assert (req["recording_format"], req["recording_type"], req["create_chunk"]) == (
        "wav",
        "main",
        True,
    )  # '/Recording/' ext sniff
    assert req["recording_start_time"] == "2025-10-09 08:53:20"  # gmtime of the ms epoch
    vrec.handle_main_call_recording_event(
        CT,
        {
            "RecordingStartMs": "1760000000000",
            "RecordingEndMs": "1760000030000",
            "RecordingDuration": "30",
            "RecordUrl": "http://x/other/abc.wav",
        },
    )
    assert reqs[1]["recording_format"] == "mp3"  # non-/Recording/ url -> mp3


def test_transcription_event_integration_string(monkeypatch):
    _quiet_plumbing(monkeypatch)
    monkeypatch.setattr(
        tm, "create_transcripts", lambda req, **kw: UseCaseResponse(status_code=404, error="No transcript provided")
    )
    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"RecordingID": "r1", "CallUUID": "leg"})
    assert err is None  # the retry is a scheduled transcript_attach job
    monkeypatch.setattr(tm, "create_transcripts", lambda req, **kw: UseCaseResponse(status_code=201, msg="ok"))
    assert vrec.handle_transcription_event(CT, "AUTH1", {}) == ("", vrec.ERR_TRANSCRIPTION_INTEGRATION)


def test_transcription_event_reads_plivos_snake_case_keys(monkeypatch):
    # Plivo's transcription callback posts snake_case (recording_id / call_uuid); reading
    # PascalCase made every lookup empty => "No recording provided".
    captured = {}

    def _capture(req, **kw):
        captured["recording_uuid"] = req.recording_uuid
        captured["call_leg_uuid"] = req.call_leg_uuid
        return UseCaseResponse(status_code=201, msg="ok")

    monkeypatch.setattr(tm, "create_transcripts", _capture)
    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-9", "call_uuid": "leg-9"})
    assert err is None
    assert captured == {"recording_uuid": "rec-9", "call_leg_uuid": "leg-9"}
    # PascalCase fallback stays accepted
    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"RecordingID": "rec-8", "CallUUID": "leg-8"})
    assert err is None
    assert captured == {"recording_uuid": "rec-8", "call_leg_uuid": "leg-8"}


# ============================================================================ stream rendezvous
class FakeRedis:
    def __init__(self, store=None):
        self.store = dict(store or {})
        self.sets = []

    def get(self, k):
        return self.store.get(k)

    def delete(self, k):
        self.store.pop(k, None)

    def set(self, k, v, ex=None):
        self.sets.append((k, v, ex))
        self.store[k] = v


class FakeCache:
    def __init__(self, redis):
        self.client = redis

    def get(self, k):
        return self.client.get(k)

    def get_instance(self):
        return self


def _tm_env(monkeypatch, org=Row(auth_id="A", uuid="t-1"), recording=None, leg=None, fetch=None):
    monkeypatch.setattr(tm, "Organization", Row(get_org_by_auth_id=staticmethod(lambda a: org)))
    monkeypatch.setattr("hs.routing.account_read.tenant_plivo", lambda tenant_id: ("A", "T"))
    monkeypatch.setattr(tm, "Recording", Row(get_by_recording_uuid=staticmethod(lambda u, **_kw: recording)))
    monkeypatch.setattr(tm, "get_db_session", lambda: Row(execute=lambda *a: Row(first=lambda: (1,))))
    monkeypatch.setattr(
        tm,
        "CallLeg",
        Row(
            get_by_call_uuid=staticmethod(lambda u, **_kw: leg),
            get_by_id=staticmethod(lambda i: None),
            get_all_call_legs_by_parent_call_leg=staticmethod(lambda i: []),
        ),
    )
    if fetch is not None:
        monkeypatch.setattr(tm, "_fetch_plivo_transcription", lambda a, t, r: fetch() if callable(fetch) else fetch)
    segs = []
    monkeypatch.setattr(
        tm,
        "Transcription",
        Row(
            acquire_ingestion_lock=staticmethod(lambda recording_uuid: None),
            get_or_create_transcript=staticmethod(lambda i: Row(id=5)),
            get_participant_bundle=staticmethod(lambda ids: ({}, {})),
        ),
    )
    monkeypatch.setattr(
        tm,
        "TranscriptionSegment",
        Row(
            create_missing_transcript_segments=staticmethod(
                lambda transcription_id, rows: (segs.extend(rows), len(rows))[1]
            )
        ),
    )
    return segs


def _sdk(dct):
    return tm._SDKResponseObject(dct)


def test_create_transcripts_gate_taxonomy(monkeypatch):
    _tm_env(monkeypatch, org=None)
    req = tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="r1", call_leg_uuid="leg")
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (404, "Organization not found")
    _tm_env(monkeypatch)
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (404, "No recording provided")
    # The MPC-level gate reuses the DUPLICATE-transcript error string (misleading, but the real behaviour).
    _tm_env(monkeypatch, recording=Row(call_leg=Row(parent_call_leg=None)))
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (422, "Transcript already exists")


def test_create_transcripts_fetch_taxonomy(monkeypatch):
    req = tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="r1", call_leg_uuid="leg")
    rec = Row(call_leg=Row(parent_call_leg=3), recording_start_time="2026-06-13 10:00:00")

    def _raise_nf():
        raise tm._PlivoNotFound("404")

    _tm_env(monkeypatch, recording=rec, fetch=_raise_nf)
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (404, "No transcript provided")
    _tm_env(monkeypatch, recording=rec, fetch=_sdk({"transcription": []}))
    resp = tm.create_transcripts(req)
    # Fetched-but-empty is terminal (final provider result), split from the
    # retryable not-ready 404 above so the job retry never runs again for it.
    assert (resp.status_code, resp.error) == (404, "Provider transcription has no segments")

    def _raise_500():
        raise RuntimeError("plivo transcription GET 500")

    _tm_env(monkeypatch, recording=rec, fetch=_raise_500)
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (500, "No transcript provided")
    _tm_env(
        monkeypatch,
        recording=rec,
        fetch=_sdk({"transcription": [_sdk({"start": 1.5, "end": 2.5, "text": "hi"})]}),
        leg=None,
    )  # raw segments parse; then leg-miss 422
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (422, "Call leg not found")


def test_an_empty_final_transcript_still_counts_toward_readiness(monkeypatch):
    req = tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="r1", call_leg_uuid="leg")
    rec = Row(call_leg=Row(parent_call_leg=3), recording_start_time="2026-06-13 10:00:00")
    _tm_env(monkeypatch, recording=rec, leg=Row(id=11), fetch=_sdk({"transcription": []}))
    created = []
    monkeypatch.setattr(tm.Transcription, "get_or_create_transcript", lambda leg_id: created.append(leg_id))
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.error) == (404, "Provider transcription has no segments")
    assert created == [11]  # the leg's (empty) transcript exists, so get_transcriptions counts it as fetched


def test_create_transcripts_success_segment_math(monkeypatch):
    req = tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="r1", call_leg_uuid="leg")
    rec = Row(call_leg=Row(parent_call_leg=3), recording_start_time="2026-06-13 10:00:00")
    segs = _tm_env(
        monkeypatch,
        recording=rec,
        leg=Row(id=11),
        fetch=_sdk({"transcription": [_sdk({"start": 1.5, "end": 2.5, "text": "hi"})]}),
    )
    persistence_order = []
    monkeypatch.setattr(
        tm.Transcription,
        "get_or_create_transcript",
        lambda call_leg_id: persistence_order.append("resolve-transcript") or Row(id=5),
    )
    monkeypatch.setattr(
        tm.Transcription, "acquire_ingestion_lock", lambda recording_uuid: persistence_order.append("lock-recording")
    )
    resp = tm.create_transcripts(req)
    assert (resp.status_code, resp.msg) == (201, "Transcript created successfully")
    # get_or_create can roll back its insert race, so the xact lock must follow it.
    assert persistence_order == ["resolve-transcript", "lock-recording"]
    seg = segs[0]
    want = (datetime.datetime(2026, 6, 13, 10, 0, 0) + datetime.timedelta(seconds=1.5)).timestamp()
    assert (seg.transcription_id, seg.start_time, seg.end_time, seg.segment) == (5, 1.5, 2.5, "hi")
    assert seg.start_ts == want  # recording-start + segment offset


def test_get_transcriptions_reads(monkeypatch):
    resp = tm.get_transcriptions("")
    assert (resp.status_code, resp.error) == (400, "No recording provided")
    _tm_env(monkeypatch, recording=Row(call_leg=Row(parent_call_leg=7)))
    resp = tm.get_transcriptions("r1")
    assert (resp.status_code, resp.error) == (400, "Invalid recording id")  # participant-level
    # Speaker resolution + the start_ts window filter + all_fetched.
    start = datetime.datetime(2026, 6, 13, 10, 0, 0)
    rec = Row(
        call_leg=Row(parent_call_leg=None),
        call_leg_id=1,
        recording_start_time="2026-06-13 10:00:00",
        recording_duration="60",
    )
    _tm_env(monkeypatch, recording=rec)
    monkeypatch.setattr(
        tm,
        "CallLeg",
        Row(
            get_by_id=staticmethod(lambda i: Row(id=1)),
            get_all_call_legs_by_parent_call_leg=staticmethod(
                lambda i: [
                    Row(id=2, role="customer", call_uuid="par-ct", participant="+1555"),
                    Row(id=3, role="agent", participant="77"),
                    Row(id=4, role="agent", participant="88"),
                ]
            ),
        ),
    )
    monkeypatch.setattr(
        tm, "ConversationParent", Row(get_by_contact_uuid=staticmethod(lambda u: Row(contact_name="Bob")))
    )
    monkeypatch.setattr(
        tm,
        "AccountOrgMapping",
        Row(
            get_by_id=staticmethod(lambda i: Row(uuid="u-77") if i == "77" else None),
            get_agent_name_by_aom_uuid=staticmethod(lambda u: "Agent Two"),
        ),
    )

    def _transcript_for(leg_id):
        if leg_id == 4:
            return None  # missing -> all_fetched False
        return Row(
            transcription_segments=[
                Row(start_ts=start.timestamp() + 5, start_time=5.0, segment=f"hi{leg_id}"),
                Row(start_ts=start.timestamp() + 999, start_time=999.0, segment="out"),
            ]
        )

    bundle = staticmethod(lambda ids: ({}, {str(i): _transcript_for(i) for i in ids}))
    monkeypatch.setattr(tm.Transcription, "get_participant_bundle", bundle)
    resp = tm.get_transcriptions("r1")
    assert resp.status_code == 202 and resp.data["all_fetched"] is False
    rows = resp.data["transcriptions"]
    assert [r.speaker for r in rows] == ["Bob", "Agent Two"]  # window-filtered, sorted
    assert all(r.start_time == 5 for r in rows)


# ============================================================================ chunk sink
def test_chunk_sink_recording_enrichment(monkeypatch):
    conv = Row(contact_uuid="par-ct", generation_type="non_qnr", parent_conversation=Row(conversation_number="555"))
    monkeypatch.setattr(convmgmt, "Conversation", Row(get_by_id=staticmethod(lambda i: conv)))
    created_chunks = []
    monkeypatch.setattr(
        convmgmt, "ConversationChunk", Row(create_chunk=staticmethod(lambda c: created_chunks.append(c)))
    )
    monkeypatch.setattr(da_call, "CallLeg", Row(get_by_call_uuid=staticmethod(lambda u, **_kw: Row(id=44))))
    rec_calls = _fake_recording(monkeypatch)
    chunk = Row(conversation=1, type="recording", attributes=[{"recording_uuid": "r1"}], recording=None)
    convmgmt.create_chunk_with_related_data(chunk)
    assert rec_calls[0][1] == 44  # parent leg by the conv contact uuid
    assert chunk.recording == 9 and created_chunks == [chunk]
    # an FK-preset (px-path) recording chunk SKIPS the create_from_chunk enrichment
    chunk3 = Row(conversation=1, type="recording", attributes=[{"recording_uuid": "r2"}], recording=77)
    convmgmt.create_chunk_with_related_data(chunk3)
    assert chunk3.recording == 77 and len(rec_calls) == 1  # untouched, no second row


# ============================================================================ event branches
def test_recording_chunk_event_branch(monkeypatch, emitted):
    assert ccu._recording_chunk_event(Row(is_main_recording=True), None, None) is None
    assert ccu._recording_chunk_event(Row(is_main_recording=False), Row(recording=None), None) is None
    assert emitted == []
    recording = Row(uuid="rec-1", recording_duration="42")
    monkeypatch.setattr(ccu.Recording, "get_by_id", classmethod(lambda cls, i: recording if i == 5 else None))
    child = Row(parent_conversation=Row(organization=7, uuid="00000000-0000-4000-a000-000000000011"))
    ccu._recording_chunk_event(Row(is_main_recording=False), Row(recording=5), child)
    assert [(t, d) for t, d, _ in emitted] == [("recording.ready", {"recording_id": "rec-1", "duration_sec": 42})]


@pytest.mark.parametrize(("chunk_type", "description"), [("recording", ""), ("signal", "Incoming_call_ended")])
def test_no_answer_voice_chunks_persist_without_agent_event(monkeypatch, chunk_type, description):
    """No answered agent means there is no agent channel to notify.
    The flow-owned chunk is still durable and the callback must succeed; returning a
    400 after commit would cause provider retries.
    """
    parent = Row(id=11, organization=7, parent_contact_uuid="parent-contact")
    non_qnr_child = Row(id=12, parent=11, parent_conversation=parent, channel="call", contact_uuid="flow-child")
    db_chunk = Row(id=13, recording=None)
    inserted = []
    commits = []
    activity_updates = []

    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_non_qnr_child_by_contact_uuid",
        classmethod(lambda cls, *args, **kwargs: non_qnr_child),
    )
    monkeypatch.setattr(ccu.Conversation, "get_child_by_conv_parent", classmethod(lambda cls, *args, **kwargs: None))
    monkeypatch.setattr(ccu.Conversation, "get_all_active_conversations_for_parent", classmethod(lambda cls, p: []))
    monkeypatch.setattr(ccu, "fetch_org_by_id_and_set_context", lambda org_id: (None, Row(id=org_id)))
    monkeypatch.setattr(convmgmt, "create_chunk_with_related_data", lambda chunk: inserted.append(chunk))
    monkeypatch.setattr(ccu, "get_db_session", lambda: Row(commit=lambda: commits.append(True)))
    monkeypatch.setattr(
        ccu.ConversationParent,
        "update_last_activity_at",
        classmethod(lambda cls, conversation_id, now=None: activity_updates.append(conversation_id)),
    )

    request_chunk = Row(
        type=chunk_type,
        description=description,
        channel="call",
        parent_contact_uuid="parent-contact",
        is_inbound_chunk=False,
        is_main_recording=False,
        disposition="",
        chunk_for_hangup_node=False,
        remove_cache=False,
        create_chunk=True,
        skip_for_active_child=False,
        build_db_model=lambda conversation: db_chunk,
    )

    response = ccu.add_chunk_to_non_qnr_child(request_chunk)

    assert (response.status_code, response.error) == (201, None)
    assert response.data is db_chunk
    assert inserted == [db_chunk]
    assert commits == [True]
    assert activity_updates == [11]
