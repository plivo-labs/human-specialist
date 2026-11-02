from hs.access import Principal
from hs.voice import outbound

FORM = {"AnswerTime": "t0", "EndTime": "t1", "HangupCauseCode": "4000", "HangupSource": "Caller"}
P = Principal("t", "o", "plivo")


def test_an_answered_agent_hangup_is_one_leave_under_one_claim_key(monkeypatch):
    dial = {"call_uuid": "c", "run_id": "r", "agent_sip": "sip:a@x"}
    monkeypatch.setattr(outbound, "dialed", lambda cid, tenant_id: dial)
    first, again = (outbound.hangup(P, FORM, "d") for _ in range(2))
    call, run, left = first
    assert (call, run) == ("c", "r") and first == again
    assert (left["EventName"], left["To"], left["HangupTime"]) == ("Hangup", "sip:a@x", "t1")


def test_an_unanswered_agent_hangup_is_not_a_leave(monkeypatch):
    failed = []
    monkeypatch.setattr(outbound, "dialed", lambda cid, tenant_id: {"agent_sip": "s"})
    monkeypatch.setattr(outbound, "_fail", lambda *a: failed.append(a))
    assert outbound.hangup(P, FORM, "d") is None and failed
