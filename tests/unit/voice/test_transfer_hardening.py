"""Transfer-path hardening:
* _resolve_destination_name: agent names come from the account enrichment — a never-filled
  default left every agent-transfer signal reading "Transfer initiated to " and the `if name:`
  gate silently skipped the transfer-successful chunk.
* reverse routing: a phone-number member (external / dial-by-number transfer leg) must
  short-circuit WITHOUT the RRLOCK and WITHOUT the "org auth_id not found" alert.
* _member_address_matches: MPC reports member_address in whichever form the leg was dialed
  with (bare phone / +phone / sip: URI / schemeless SIP); byte equality silently skipped
  warm-transfer hangups.
"""

import pytest

from hs.routing import constants as rc
from hs.voice import hangup as vh
from hs.voice import transfer as ct

# ---- destination-name enrichment -----------------------------------------------------------


class _Req:
    def __init__(self, transfer_type=rc.TRANSFER_TYPE_AGENT, identifier="aom-1", member=""):
        self.transfer_type = transfer_type
        self.identifier = identifier
        self.member = member


def test_agent_destination_name_resolves_within_the_tenant(monkeypatch):
    monkeypatch.setattr(
        vh,
        "_get_agent_name_by_uuid",
        lambda uuid, auth_id: ("Agent One", None) if auth_id == "A1" else (None, "no rows"),
    )
    assert ct._resolve_destination_name(_Req(), "A1") == "Agent One"


def test_agent_destination_name_error_degrades_to_empty(monkeypatch):
    monkeypatch.setattr(vh, "_get_agent_name_by_uuid", lambda uuid, auth_id: (None, "not found"))
    assert ct._resolve_destination_name(_Req(), "A1") == ""


def test_team_and_external_destination_names_unchanged(monkeypatch):
    monkeypatch.setattr(ct.ar, "get_team_name", lambda ident: "Support")
    assert ct._resolve_destination_name(_Req(transfer_type=rc.TRANSFER_TYPE_TEAM), "A1") == "Support"
    req = _Req(transfer_type=rc.TRANSFER_TYPE_EXTERNAL, member="+15550001111")
    assert ct._resolve_destination_name(req, "A1") == "+15550001111"


# ---- reverse-routing phone-member guard ----------------------------------------------------


class _GuardRR:
    """Fails the test if reverse routing proceeds past the guard."""

    def acquire_reverse_routing_lock_bool(self, *a):
        raise AssertionError("RRLOCK must not be taken for a phone-number member")


@pytest.mark.parametrize("member", ["15550005678", "+15550005678"])
def test_reverse_routing_short_circuits_phone_members(member, monkeypatch):
    monkeypatch.setattr(vh, "RoutingRedis", lambda _v=_GuardRR(): _v)
    res, err = vh.handle_reverse_routing_and_agent_capacity(member, "call", "conv-1", db=object())
    # the same dead-end tuple an org-by-sip miss produces -> callers behave identically
    assert (res, err) == ("failed to run reverse routing", "not found")


def test_reverse_routing_still_runs_for_sip_members(monkeypatch):
    class _LockFalseRR:
        def acquire_reverse_routing_lock_bool(self, *a):
            return False

    monkeypatch.setattr(vh, "RoutingRedis", lambda _v=_LockFalseRR(): _v)
    res, err = vh.handle_reverse_routing_and_agent_capacity("sip:ben@x.sip.plivo.com", "call", "conv-1", db=object())
    assert (res, err) == ("handled reverse routing and agent capacity", "failed to acquire reverse routing lock")


# ---- tolerant member-address matching ------------------------------------------------------


@pytest.mark.parametrize(
    "participant,requested,should_match",
    [
        ("15550001234", ("+15550001234",), True),  # + stripped
        ("+15550001234", ("15550001234",), True),  # reverse direction
        ("agent1@phone.plivo.com", ("sip:agent1@phone.plivo.com",), True),  # schemeless MPC form
        ("sip:agent1@phone.plivo.com", ("sip:agent1@phone.plivo.com",), True),
        ("15550001234", ("sip:agent1@phone.plivo.com", "15550001234"), True),  # any form in set
        ("15559999999", ("15550001234",), False),  # different numbers never match
        ("", ("15550001234",), False),
    ],
)
def test_member_address_matches(participant, requested, should_match):
    assert ct._member_address_matches(participant, requested) is should_match


def test_hangup_call_for_mpc_member_matches_cross_format(monkeypatch):
    hangups = []
    participants = [
        {"member_address": "15550001234", "call_uuid": "leg-cust", "exit_cause": ""},
        {"member_address": "agent1@phone.plivo.com", "call_uuid": "leg-agent", "exit_cause": ""},
    ]
    import hs.voice.agent_leg as val

    monkeypatch.setattr(val, "_get_participants", lambda *a: (participants, None))

    class _Client:
        def __init__(self, *a):
            pass

        def call_delete(self, call_uuid):
            hangups.append(call_uuid)
            return "", 204, None

    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", _Client)
    err = ct.hangup_call_for_mpc_member("mpc_q1", "sip:agent1@phone.plivo.com", "AUTH", "TOKEN")
    assert err is None
    assert hangups == ["leg-agent"]
