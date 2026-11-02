"""The agent leg: the add_participant wire form (declaration order, omitempty rules, the always-present
record_participant_track / delay_dial / create_mpc_with_single_participant=false), the SIP header
families (Incoming%20call[%20transfer] + AliasName/team_name/FromAgent + auto-accept),
encode_sip_header_value (%2E + the %-safe 64-trim), the extension split, handle_restricted_number,
the IA/CONTACTRETRY/MPCCALLSKEY-or-MTCK bookkeeping, the ParticipantJoin role dispatch (the
primary chain, secondary agent/participant, supervisor), the CallAnswered double-parse, and the
routing-event row shapes.
"""

import json
import uuid

import pytest

import hs.voice.agent_leg as val
from hs.plivo import voice as pv
from hs.voice import constants as vc

_ACCOUNT_DETAILS = val.get_account_details_for_sip_endpoints  # the real one; agent_leg_env fakes it
_SET_AGENT = val.set_agent_for_contact_and_update_conversation  # the real one; primary_join_env fakes it


# ============================================================================ helpers
def test_encode_sip_header_value_leaves_only_alphanumerics_and_escapes():
    assert val.encode_sip_header_value("Team One") == "Team%20One"
    assert val.encode_sip_header_value("a/b") == "a%2Fb"
    assert val.encode_sip_header_value("a.b-c_d") == "a%2Eb%2Dc%5Fd"
    assert val.encode_sip_header_value("a&b=cé") == "a%26b%3Dc%C3%A9"
    # the 64-trim never cuts mid-%xx: a name of 21×'.' encodes to 63 chars (21×%2E) — add one
    # more char to cross 64 and verify the dangling '%'/'%x' is dropped, not split
    encoded = val.encode_sip_header_value("." * 22)  # 66 chars pre-trim
    assert len(encoded) <= 64 and not encoded.endswith("%") and not encoded.endswith("%2")
    assert encoded == "%2E" * 21  # 63 — the %-safe cut


def test_decode_sip_header_value_roundtrip_and_error():
    assert val.decode_sip_header_value("Team%20One") == ("Team One", None)
    assert val.decode_sip_header_value("a%2Eb") == ("a.b", None)
    decoded, err = val.decode_sip_header_value("bad%zz")
    assert decoded == "" and err is not None


def test_contact_detail_lookup_retries_once_then_recovers(monkeypatch):
    calls = []
    sleeps = []
    names = [None, "Alice"]
    monkeypatch.setattr(val, "_get_contact_name_once", lambda contact_uuid: calls.append(contact_uuid) or names.pop(0))
    monkeypatch.setattr(val.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert val.get_contact_details_for_contact_uuid("contact-1") == {"name": "Alice"}
    assert calls == ["contact-1", "contact-1"]
    assert sleeps == [0.05]


def test_contact_detail_lookup_exhaustion_is_bounded_safe_fallback(monkeypatch):
    calls = []
    monkeypatch.setattr(val, "_get_contact_name_once", lambda contact_uuid: calls.append(contact_uuid))
    monkeypatch.setattr(val.time, "sleep", lambda _seconds: None)

    assert val.get_contact_details_for_contact_uuid("contact-1") is None
    assert calls == ["contact-1", "contact-1"]


def test_extract_phone_number_and_extension():
    # no digits-pattern match → passthrough
    assert val.extract_phone_number_and_extension_if_any("sip:agent@x.sip.plivo.com", False) == (
        "sip:agent@x.sip.plivo.com",
        "",
    )
    assert val.extract_phone_number_and_extension_if_any("+14155551234", False) == ("+14155551234", "")
    assert val.extract_phone_number_and_extension_if_any("+14155551234#789", False) == ("+14155551234", "789")
    assert val.extract_phone_number_and_extension_if_any("+14155551234#789", True) == ("14155551234", "789")
    # ≥3 '#' parts → NEITHER switch case hits: the ORIGINAL string, no extension
    assert val.extract_phone_number_and_extension_if_any("123#45#6", False) == ("123#45#6", "")


def test_handle_restricted_number_branches():
    # sip→sip: dummy From + the lowercase 'restricted' constant as the name
    assert val.handle_restricted_number("sip:a@x", "sip:b@x", "Alice", "c1", "t") == ("+19090909090", "restricted")
    # sip caller → phone number: Plivo refuses a SIP From, so ring from the dialed number
    assert val.handle_restricted_number("sip:a@x", "+1650", "Alice", "c1", "t") == ("+1650", "Alice")
    # ...unless its user part is a phone number: that is the caller
    assert val.handle_restricted_number("sip:+14155550123@x", "+1650", "Al", "c1", "t") == ("+14155550123", "Al")
    # no 'restricted' substring → passthrough
    assert val.handle_restricted_number("+1415", "+1650", "Alice", "c1", "t") == ("+1415", "Alice")
    # 'Restricted' (any case) in From → From=To + 'No Caller ID'
    assert val.handle_restricted_number("Restricted", "+1650", "Alice", "c1", "t") == ("+1650", "No Caller ID")


# ============================================================================ the wire form
def _dialed_params(env, agents, *transfer):
    val.add_agents_to_mpc(CP, agents, *(transfer or ("", False, "", "")))
    return env["add_calls"][-1][2]


def test_add_participant_params_order_and_omitempty_record_false(agent_leg_env):
    p = _dialed_params(agent_leg_env, ["sip:ben@x", "sip:cara@x"])
    # declaration ORDER — dict insertion order is the wire order
    assert list(p.keys()) == [
        "role",
        "from",
        "to",
        "caller_name",
        "call_status_callback_url",
        "call_status_callback_method",
        "sip_headers",
        "ring_timeout",
        "delay_dial",
        "recording_callback_url",
        "recording_callback_method",
        "status_callback_url",
        "status_callback_method",
        "status_callback_events",
        "create_mpc_with_single_participant",
        "transcription_url",
        "record_participant_track",
    ]
    assert p["ring_timeout"] == "30"  # a STRING
    assert p["delay_dial"] == 0  # SDK-forced int, ALWAYS on the wire
    assert p["create_mpc_with_single_participant"] is False  # set *bool → present
    assert p["record_participant_track"] is False  # NO omitempty → present even when false
    assert "record" not in p and "transcript" not in p  # false bools omitted
    assert p["status_callback_events"] == "mpc-state-changes,participant-state-changes,add-participant-api-events"


def test_add_participant_params_record_true_and_send_digits(agent_leg_env, monkeypatch):
    monkeypatch.setattr(
        val.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound: ("tok", True, None)
    )
    p = _dialed_params(agent_leg_env, ["+14085551234#789"], "sip:ben@x", True, "external", "")
    keys = list(p.keys())
    # record slots between status_callback_method and status_callback_events; transcript
    # between transcription_url and record_participant_track; send_digits after create_mpc
    assert keys.index("record") == keys.index("status_callback_method") + 1
    assert keys.index("send_digits") == keys.index("create_mpc_with_single_participant") + 1
    assert keys.index("transcript") == keys.index("transcription_url") + 1
    assert p["record"] is True and p["transcript"] is True
    assert p["record_participant_track"] is True


def test_compact_json_wire_bytes(agent_leg_env):
    """The exact json.Encoder bytes: compact, insertion order, HTML-escaped '<', trailing \\n."""
    p = _dialed_params(agent_leg_env, ["sip:ben@x", "sip:cara@x"])
    raw = pv.compact_json(p)
    assert raw.endswith(b"\n") and b" " not in raw.split(b'"caller_name"')[0]
    assert b'"to":"sip:ben@x\\u003csip:cara@x"' in raw  # the '<' join HTML-escaped
    assert b'"ring_timeout":"30"' in raw
    assert b'"delay_dial":0' in raw
    assert b'"create_mpc_with_single_participant":false' in raw
    assert b'"record_participant_track":false' in raw
    # parses back to the same object (escapes are valid JSON)
    assert json.loads(raw.decode()) == p


# ============================================================================ add_agents_to_mpc
class FakeRR:
    def __init__(self, qnr_cfg_raw=None, reservation=(30, None)):
        self.qnr_cfg_raw = qnr_cfg_raw or json.dumps(
            {"queue": "q-1", "sticky_routing_type": "No sticky routing", "auto_accept_call_config": {}}
        )
        self.reservation = reservation
        self.rrlock_deletes = []
        self.retry_incrs = []
        self.invited = []
        self.retry_result = 3
        self.mapping_sets = []
        self.mapping_get = None
        self.tip = None
        self.mtbpak = []
        self.recompute = []
        self.attempt = (2, None)
        self.contact_params = None
        self.transfer_mpc_deleted = []
        self.offers, self.made = {}, set()

    def set_offer(self, run_uuid, agents, offer_id, ttl_seconds):
        self.offers.update({(run_uuid, agent): offer_id for agent in agents})
        self.made |= {(run_uuid, agent, offer_id) for agent in agents}

    def get_offer(self, run_uuid, agent):
        return self.offers.get((run_uuid, agent), "")

    def is_offer_of(self, run_uuid, agent, offer_id):
        return (run_uuid, agent, offer_id) in self.made

    def get_queue_and_route_config(self, flow_run_uuid, run_uuid):
        return self.qnr_cfg_raw

    def get_reservation_timeout(self, auth_id, channel):
        return self.reservation

    def delete_reverse_routing_lock(self, agent, conversation_id, channel):
        self.rrlock_deletes.append((agent, conversation_id, channel))

    def set_retry_val(self, flow_run_uuid, run_uuid):
        self.retry_incrs.append((flow_run_uuid, run_uuid))
        return self.retry_result

    def set_invited_agents(self, flow_run_uuid, run_uuid, agents, ttl_seconds, attempt_count):
        self.invited.append((flow_run_uuid, run_uuid, tuple(agents), ttl_seconds, attempt_count))

    def set_contact_to_agent_mapping(self, contact_uuid, mapping_json, ttl_seconds):
        self.mapping_sets.append((contact_uuid, mapping_json, ttl_seconds))
        return

    def get_contact_to_agent_mapping(self, contact_uuid):
        return self.mapping_get

    def get_contact_parameters(self, call_uuid, qnr_run_uuid):
        return self.contact_params

    def get_transfer_in_progress_info(self, contact_uuid):
        return self.tip

    def set_to_be_primary_agent(self, qnr_run_uuid, mapping_json, ttl_seconds, nx=False):
        self.mtbpak.append((qnr_run_uuid, mapping_json, ttl_seconds))
        return True

    def delete_transfer_in_progress(self, contact_uuid):
        return None

    def set_recompute_flag_for_queue_position_calculation(self, node_uuid, queue_uuid):
        self.recompute.append((node_uuid, queue_uuid))

    def get_invited_agent_attempt(self, flow_run_uuid, run_uuid, agent):
        return self.attempt


CP = {
    "conversation_number": "1234567890",
    "flow_uuid": "ph-u",
    "flow_run_uuid": "ph-run",
    "queue_uuid": "q-1",
    "contact_channel": "call",
    "from_number": "+14155551234",
    "contact_uuid": "ct-1",
    "auth_id": "MA_AUTH",
    "auth_token": "tok",
    "queue_and_route_run_uuid": "qnr-1",
    "to": "+16505551234",
    "flow_name": "voice flow",
    "node_uuid": "node-1",
}


@pytest.fixture
def agent_leg_env(monkeypatch):
    """add_agents_to_mpc with every cross-module dep faked; returns the capture dict."""
    cap = {"add_calls": [], "spawned": [], "number_calls": [], "team_calls": [], "rr": FakeRR(), "offers": []}
    monkeypatch.setattr(val, "RoutingRedis", lambda: cap["rr"])
    monkeypatch.setattr(val, "_emit_offers", lambda cp, agents, *offer: cap["offers"].append((tuple(agents), *offer)))
    monkeypatch.setattr(
        val.ar, "get_org_for_auth_id", lambda auth: val.ar.OrgForAuthId(7, "plivo", "tok", auth, "org-u")
    )
    monkeypatch.setattr(val, "_conversation_id", lambda number: "c-1")
    monkeypatch.setattr(val.uuid, "uuid4", lambda: "o-1")
    monkeypatch.setattr(
        val.ar,
        "get_team_names_for_sips_in_queue",
        lambda agents, qnr: cap["team_calls"].append((tuple(agents), qnr)) or ["Team One", ""][: len(agents)],
    )
    monkeypatch.setattr(val.ar, "get_team_name", lambda team_uuid: "Support Pod")
    monkeypatch.setattr(
        val.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound: ("tok", False, None)
    )
    monkeypatch.setattr(
        val,
        "get_account_details_for_sip_endpoints",
        lambda sips: ([{"uuid": "aom-1", "sip_endpoint": sips[0], "name": "Ben Agent", "email": ""}], None),
    )
    monkeypatch.setattr(
        val.ar, "get_account_rows_for_sip_endpoints", lambda sips: [val.ar.AomRowBySip("aom-1", sips[0], "", "")]
    )
    monkeypatch.setattr(val, "get_contact_details_for_contact_uuid", lambda c: {"name": "Agent One"})
    import hs.plivo.numbers as nm

    monkeypatch.setattr(
        nm, "get_plivo_number", lambda auth, tok, num: cap["number_calls"].append(num) or {"alias": "Main Line"}
    )
    monkeypatch.setattr(
        val,
        "add_participants_to_mpc_utility",
        lambda auth, tok, call, mpc, params: (
            cap["add_calls"].append((auth, mpc, params))
            or ({"api_id": "x", "calls": [{"call_uuid": "leg-1"}, {"call_uuid": "leg-2"}]}, None)
        ),
    )
    monkeypatch.setattr(val, "_spawn", lambda fn, *a: cap["spawned"].append((fn.__name__, a)))
    return cap


def test_an_outbound_hand_off_reads_the_outbound_recording_setting(agent_leg_env, monkeypatch):
    asked = []
    monkeypatch.setattr(
        val.ar,
        "get_organisation_auth_token_and_record_value",
        lambda auth, inbound: asked.append(inbound) or ("t", 0, None),
    )
    agent_leg_env["rr"] = FakeRR()
    val.add_agents_to_mpc(CP | {"inbound": False}, ["sip:ben@x"], "", False, "", "")
    assert asked == [False]


def test_an_offer_that_cannot_be_stored_rings_no_one(agent_leg_env, monkeypatch):
    agent_leg_env["rr"] = FakeRR()
    monkeypatch.setattr(agent_leg_env["rr"], "set_offer", lambda *a: "error while storing the offer")
    with pytest.raises(val.AddAgentsToMPCError, match="storing the offer"):
        val.add_agents_to_mpc(CP, ["sip:ben@x"], "", False, "", "")
    assert agent_leg_env["add_calls"] == []


def test_add_agents_happy_inbound_two_agents(agent_leg_env):
    rr = FakeRR()
    agent_leg_env["rr"] = rr
    val.add_agents_to_mpc(CP, ["sip:ben@x", "sip:cara@x"], "", False, "", "")
    assert agent_leg_env["number_calls"] == ["+16505551234"]
    # RRLOCK released per agent BEFORE the invite
    assert rr.rrlock_deletes == [("sip:ben@x", "1234567890", "call"), ("sip:cara@x", "1234567890", "call")]
    auth, mpc, params = agent_leg_env["add_calls"][0]
    assert (auth, mpc) == ("MA_AUTH", "mpc_qnr-1")
    # contact name: sync fetch (flag false) won → truncated name as CallerName
    assert params["caller_name"] == "Agent One"
    # headers: agent0 has Alias+Team, agent1 alias-only; '<'-joined; alias encoded (space→%20)
    assert params["sip_headers"] == (
        "header1=Incoming%20call,AliasName=Main%20Line,TeamName=Team%20One,HsKind=offer,HsOffer=o%2D1,HsConversation=c%2D1"
        "<header1=Incoming%20call,AliasName=Main%20Line,HsKind=offer,HsOffer=o%2D1,HsConversation=c%2D1"
    )
    assert params["to"] == "sip:ben@x<sip:cara@x"
    assert params["ring_timeout"] == "30"  # reservation timeout hit
    # CONTACTRETRY INCR + IA write (5 MINUTES → 300s; the INCR result as the attempt)
    assert rr.retry_incrs == [("ph-run", "qnr-1")]
    assert rr.invited == [("ph-run", "qnr-1", ("sip:ben@x", "sip:cara@x"), 300, 3)]
    # one offer for the attempt, stored for both legs, announced with the ring timeout
    [(agents, offer_id, ring, auto_accept)] = agent_leg_env["offers"]
    assert (agents, ring, auto_accept) == (("sip:ben@x", "sip:cara@x"), 30, False)
    assert rr.offers == {("qnr-1", "sip:ben@x"): offer_id, ("qnr-1", "sip:cara@x"): offer_id}


def test_an_offer_is_announced_to_each_offered_agent(monkeypatch):
    from types import SimpleNamespace

    import hs.conversations.store as store

    parent, sent = SimpleNamespace(uuid="p-1"), []
    monkeypatch.setattr(store.ConversationParent, "get_by_conversation_number", classmethod(lambda cls, n: parent))
    monkeypatch.setattr(store, "emit", lambda *a: sent.append(a))
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: {"sip:ben@x": "aom-ben"}.get(sip))
    val._emit_offers(CP, ["sip:ben@x", "+14155550000"], "offer-1", 30, True)
    data = {"offer_id": "offer-1", "queue_id": CP.get("queue_uuid") or None, "from": CP["from_number"]}
    assert sent == [("offer.created", parent, {**data, "ring_timeout_sec": 30, "auto_accept": True}, "aom-ben")]


def test_add_agents_sip_destination_skips_phone_number_metadata(agent_leg_env):
    rr = FakeRR()
    sip_destination = "sip:00000000000000001@app.plivo.com"

    agent_leg_env["rr"] = rr
    val.add_agents_to_mpc({**CP, "to": sip_destination}, ["sip:ben@x"], "", False, "", "")

    assert agent_leg_env["number_calls"] == []
    _auth, _mpc, params = agent_leg_env["add_calls"][0]
    assert "AliasName=" not in params["sip_headers"]


def test_add_agents_mpc_calls_key_vs_transfer(agent_leg_env, monkeypatch):
    stored = {"plain": [], "transfer": []}

    class FakeVR:
        def set_mpc_call_uuids(self, flow, qnr, uuids, ttl):
            stored["plain"].append((flow, qnr, tuple(uuids), ttl))

        def set_transfer_mpc_call_uuids(self, qnr, uuids, ttl):
            stored["transfer"].append((qnr, tuple(uuids), ttl))

    monkeypatch.setattr(val, "VoiceRedis", FakeVR)
    rr = FakeRR(reservation=(0, "error fetching routing settings"))  # a read error → the 45 default
    agent_leg_env["rr"] = rr
    val.add_agents_to_mpc(CP, ["sip:ben@x"], "", False, "", "")
    assert stored["plain"] == [("ph-run", "qnr-1", ("leg-1", "leg-2"), 45)]
    # transfer → MTCK; NO retry INCR (attempt stays 1); the invite conversation_event fires
    rr2 = FakeRR()
    agent_leg_env["rr"] = rr2
    val.add_agents_to_mpc(CP, ["sip:cara@x"], "sip:ben@x", True, "agent", "")
    assert stored["transfer"] == [("qnr-1", ("leg-1", "leg-2"), 30)]
    assert rr2.retry_incrs == [] and rr2.invited[0][4] == 1


def test_add_agents_transfer_headers_team(agent_leg_env):
    rr = FakeRR()
    agent_leg_env["rr"] = rr
    val.add_agents_to_mpc(CP, ["sip:cara@x"], "sip:ben@x", True, "team", "team-7")
    _auth, _mpc, params = agent_leg_env["add_calls"][0]
    # transfer header family: Incoming%20call%20transfer + conversation number + FromAgent
    # (the CURRENT primary's name) + AliasName + team_name (team transfer)
    assert params["sip_headers"] == (
        "header1=Incoming%20call%20transfer,header2=1234567890,FromAgent=Ben%20Agent,"
        "AliasName=Main%20Line,TeamName=Support%20Pod,HsKind=transfer,HsOffer=o%2D1,HsConversation=c%2D1"
    )


def test_add_agents_transfer_without_primary_account_row_dials_without_from_agent(agent_leg_env, monkeypatch):
    monkeypatch.setattr(val, "get_account_details_for_sip_endpoints", lambda sips: ([], None))
    val.add_agents_to_mpc(CP, ["sip:cara@x"], "sip:ben@x", True, "agent", "")
    _auth, _mpc, params = agent_leg_env["add_calls"][0]
    assert params["sip_headers"] == (
        "header1=Incoming%20call%20transfer,header2=1234567890,AliasName=Main%20Line,HsKind=transfer,HsOffer=o%2D1,HsConversation=c%2D1"
    )


def test_add_agents_external_transfer_from_override_and_extension(agent_leg_env):
    rr = FakeRR()
    agent_leg_env["rr"] = rr
    val.add_agents_to_mpc(CP, ["+14085551234#55"], "sip:ben@x", True, "external", "")
    _auth, _mpc, params = agent_leg_env["add_calls"][0]
    assert params["from"] == "+16505551234"  # external → org number, not the customer
    assert params["to"] == "+14085551234"  # extension split off
    assert params["send_digits"] == "55"


def test_add_agents_failure_raises_and_counts(agent_leg_env, monkeypatch):
    monkeypatch.setattr(val, "add_participants_to_mpc_utility", lambda *a: (None, "non 2xx response (400)"))
    counted = []
    monkeypatch.setattr(val, "increase_counter", lambda m, args: counted.append((m, args)))
    rr = FakeRR()
    agent_leg_env["rr"] = rr
    with pytest.raises(val.AddAgentsToMPCError):
        val.add_agents_to_mpc(CP, ["sip:ben@x"], "", False, "", "")
    assert counted and counted[0][1] == {"output_state": "add_agent"}
    assert rr.invited == []  # bookkeeping never reached


@pytest.mark.parametrize(("status", "expected"), [(503, "non 2xx response (503)"), (400, "non 2xx response (400)")])
def test_add_participant_provider_mutation_is_never_callback_retried(monkeypatch, status, expected):
    class Client:
        def __init__(self, _auth_id, _auth_token):
            pass

        def mpc_add_participant(self, _mpc_id, _params, raw_body=None):
            return None, status, f"non 2xx response ({status})"

    monkeypatch.setattr(val.pv, "PlivoVoiceClient", Client)

    _body, error = val.add_participants_to_mpc_utility(
        "auth", "token", "contact-1", "mpc_qnr-1", {"caller_name": "Customer"}
    )

    assert error == expected


def test_a_long_sip_caller_name_is_truncated_not_refused(monkeypatch):
    sent = {}

    class Client:
        def __init__(self, _auth_id, _auth_token):
            pass

        def mpc_add_participant(self, _mpc_id, params, raw_body=None):
            sent.update(params=params, raw_body=raw_body)
            return {"calls": [{"call_uuid": "leg-1"}]}, 201, None

    monkeypatch.setattr(val.pv, "PlivoVoiceClient", Client)
    caller = "sip:test-caller-00000000000000000000000@phone.plivo.com"  # 55 characters, over the 50 the header allows

    body, error = val.add_participants_to_mpc_utility(
        "auth", "token", "contact-1", "mpc_qnr-1", {"caller_name": caller}
    )

    assert (body, error) == ({"calls": [{"call_uuid": "leg-1"}]}, None)
    assert sent["params"]["caller_name"] == caller[:50]
    assert caller[:50].encode() in sent["raw_body"] and caller.encode() not in sent["raw_body"]


@pytest.mark.parametrize(
    ("status", "expected"),
    [(503, "retryable voice callback: provider voice API unavailable"), (404, "non 2xx response (404)")],
)
def test_participant_read_provider_failure_is_classified_at_source(monkeypatch, status, expected):
    class Client:
        def __init__(self, _auth_id, _auth_token):
            pass

        def mpc_list_participants(self, _mpc_id):
            return None, status, f"non 2xx response ({status})"

    monkeypatch.setattr(val.pv, "PlivoVoiceClient", Client)

    assert val._get_participants("mpc_qnr-1", "auth", "token") == (None, expected)


def test_participant_read_missing_2xx_body_is_retryable_protocol_failure(monkeypatch):
    class Client:
        def __init__(self, _auth_id, _auth_token):
            pass

        def mpc_list_participants(self, _mpc_id):
            return None, 200, None

    monkeypatch.setattr(val.pv, "PlivoVoiceClient", Client)

    assert val._get_participants("mpc_qnr-1", "auth", "token") == (
        None,
        ("retryable voice callback: provider participants response unavailable"),
    )


def test_add_agents_caller_name_is_the_customer_not_the_agent_account(agent_leg_env, monkeypatch):
    monkeypatch.setattr(val, "get_account_details_for_sip_endpoints", _ACCOUNT_DETAILS)
    monkeypatch.setattr(
        val.ar, "get_account_rows_for_sip_endpoints", lambda sips: [val.ar.AomRowBySip("aom-1", sips[0], "Ben", "b@x")]
    )
    assert _ACCOUNT_DETAILS(["sip:ben@x"]) == (
        [{"uuid": "aom-1", "sip_endpoint": "sip:ben@x", "name": "Ben", "email": "b@x"}],
        None,
    )
    val.add_agents_to_mpc(CP, ["sip:ben@x"], "", False, "", "")
    assert agent_leg_env["add_calls"][0][2]["caller_name"] == "Agent One"


# ============================================================================ ParticipantJoin
def test_participant_join_customer_and_unknown_roles(monkeypatch):
    pushed = []
    legs = []
    import hs.voice.callbacks as vcb

    monkeypatch.setattr(vcb, "push_voice_event_metric", lambda c, q, k, m: pushed.append((k, m)))
    monkeypatch.setattr(val, "_spawn", lambda fn, *a: legs.append((fn.__name__, a)))
    msg, err = val.handle_participant_join("ct-1", "qnr-1", {"participant_role": "Customer"}, None)
    assert (msg, err) == ("Participant joined to MPC", None)
    assert pushed == [("MPCInitialized", "CustomerJoinDuration")]
    assert legs and legs[0][0] == "_create_call_leg_seam"  # the record-only background call
    # unknown role → still success + the leg seam
    legs.clear()
    msg, err = val.handle_participant_join("ct-1", "qnr-1", {"participant_role": "weird"}, None)
    assert (msg, err) == ("Participant joined to MPC", None) and legs


def test_participant_join_supervisor_routes_to_monitor(monkeypatch):
    # The supervisor branch delegates to voice_monitor.handle_supervisor_join_event
    import hs.voice.monitor as vmon

    seen = []
    monkeypatch.setattr(
        vmon,
        "handle_supervisor_join_event",
        lambda ct, qnr, member, mpc, voice_redis=None, routing_redis=None: seen.append((ct, qnr, member, mpc)) or None,
    )
    _msg, err = val.handle_participant_join(
        "ct-1",
        "qnr-1",
        {"participant_role": "supervisor", "member_address": "sip:sup@x", "mpc_name": "mpc_qnr-1"},
        None,
    )
    assert err is None and seen == [("ct-1", "qnr-1", "sip:sup@x", "mpc_qnr-1")]


def test_only_the_first_leg_the_transfer_dialed_becomes_the_successor(monkeypatch):
    class SlotRR:
        slot = None

        def set_to_be_primary_agent(self, qnr, mapping_json, ttl, nx=False):
            if nx and self.slot is not None:
                return False
            self.slot = json.loads(mapping_json)
            return True

        def get_to_be_primary_agent(self, qnr):
            return self.slot

        def get_offer(self, run, agent):
            return ""

    rr = SlotRR()
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(val, "get_account_details_for_sip_endpoints", lambda sips: ([{"name": "N", "uuid": "u"}], None))
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: f"aom-{sip}")
    monkeypatch.setattr("hs.conversations.transfer.add_secondary", lambda **kw: None)
    monkeypatch.setattr(val, "_spawn", lambda *a: None)
    warm = {"transfer_type": "team", "transfer_mode": "warm"}

    def join(sip: str, kind: str) -> None:
        params = {"member_address": sip, "participant_call_uuid": f"leg-{sip}", "hs_kind": kind}
        assert val.handle_secondary_agent_join("c-1", "q-1", {"conversation_number": "1"}, params, None, warm) is None

    join("sip:late-invite@x", "offer")
    assert rr.slot is None, "a leg the transfer did not dial took the successor slot"
    join("sip:first@x", "transfer")
    join("sip:second@x", "transfer")
    assert rr.slot["AgentSipEndpoint"] == "sip:first@x"


class FakeVRJoin:
    def __init__(self):
        self.toggles = []
        self.cap = []

    def toggle_active_conference(self, qnr, val_, ttl):
        self.toggles.append((qnr, val_, ttl))
        return

    def set_current_participant(self, contact_uuid, member):
        self.cap.append((contact_uuid, member))
        return


def _join_params(**over):
    p = {
        "participant_role": "Agent",
        "member_address": "sip:ben@x",
        "participant_call_uuid": "leg-1",
        "add_participant_header": "",
        "add_participant_alias_name": "",
        "digits_sent": "",
        "participant_join_time": "2026-06-11 10:00:00",
        "participant_exit_time": "",
    }
    p.update(over)
    return p


@pytest.fixture
def primary_join_env(monkeypatch):
    cap = {"set_agent": [], "routing_events": [], "stop_prompt": [], "metric": [], "aqe_deletes": [], "events": []}
    cap["rr"], cap["vr"] = FakeRR(), FakeVRJoin()
    cap["rr"].contact_params = dict(CP)
    monkeypatch.setattr(val, "RoutingRedis", lambda: cap["rr"])
    monkeypatch.setattr(val, "VoiceRedis", lambda: cap["vr"])
    import hs.voice.callbacks as vcb

    monkeypatch.setattr(vcb, "push_voice_event_metric", lambda c, q, k, m: cap["metric"].append(k))
    monkeypatch.setattr(
        val,
        "set_agent_for_contact_and_update_conversation",
        lambda c, q, cp, m, pcu: cap["set_agent"].append((m, pcu)) and None,
    )
    monkeypatch.setattr(val, "stop_wait_prompt_for_customer", lambda cp: cap["stop_prompt"].append(True))
    monkeypatch.setattr(
        val,
        "_spawn",
        lambda fn, *a: (
            cap["routing_events"].append((fn.__name__, a))
            if fn.__name__ == "post_conversation_routing_events"
            else cap["events"].append(fn.__name__)
        ),
    )

    class Entry:
        status = "QUEUED"

    monkeypatch.setattr(val.aq, "get_current_active_queue_entry_by_contact_uuid", lambda db, c, run: Entry())
    monkeypatch.setattr(
        val.aq, "delete_active_queue_entry", lambda db, c, q, status: cap["aqe_deletes"].append((c, q, status))
    )
    return cap


def test_primary_agent_join_happy_chain(primary_join_env):
    rr, vr = primary_join_env["rr"], primary_join_env["vr"]
    err = val.handle_agent_join("ct-1", "qnr-1", _join_params(), None)
    assert err is None
    assert vr.toggles == [("qnr-1", "false", 0)]  # conference OFF first
    assert primary_join_env["set_agent"] == [("sip:ben@x", "leg-1")]
    assert primary_join_env["aqe_deletes"] == [("ct-1", "qnr-1", "ACCEPTED")]
    assert rr.recompute == [("node-1", "q-1")]
    assert primary_join_env["stop_prompt"] == [True]
    # the Answered-keyed join metric
    assert primary_join_env["metric"] == ["Answered:sip:ben@x"]


def test_primary_join_set_agent_failure_marks_queue_failed(primary_join_env, monkeypatch):
    """Set-agent failure → end_mpc_calls; on its SUCCESS the join returns the set-agent error (the callback must
    not answer 200 for a join that failed) and the active queue entry is marked FAILED so the contact does not
    linger QUEUED. On end_mpc_calls FAILURE the teardown error is surfaced and NO cleanup runs."""
    import hs.voice.callbacks as vcb

    monkeypatch.setattr(val, "set_agent_for_contact_and_update_conversation", lambda *a: "boom")
    ended = []
    with monkeypatch.context() as patch:
        patch.setattr(vcb, "_end_mpc_calls_seam", lambda c, q: ended.append((c, q)))
        err = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {}, None)
    assert err == "boom"
    assert ended == [("ct-1", "qnr-1")]
    assert primary_join_env["aqe_deletes"] == [("ct-1", "qnr-1", "FAILED")]  # queue entry cleaned up (not leaked)
    # and when end_mpc_calls FAILS, its error is returned — driven through the REAL default seam
    # (voice_callbacks._end_mpc_calls_seam → voice_controls.end_mpc_calls) — and NO extra cleanup runs
    import hs.voice.controls as vctl

    monkeypatch.setattr(vctl, "end_mpc_calls", lambda c, q: ("", "end failed"))
    err2 = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {}, None)
    assert err2 == "end failed"
    assert primary_join_env["aqe_deletes"] == [("ct-1", "qnr-1", "FAILED")]  # unchanged: end-fail → no cleanup


def test_a_replayed_primary_join_after_the_accept_committed_keeps_the_call(primary_join_env, monkeypatch):
    """A cache blip after the durable accept replays the join. The child is already answered by this agent, so the
    replay finishes the join instead of failing it and tearing the live call down."""
    from types import SimpleNamespace

    import hs.conversations.child_update as ccu
    import hs.routing.invitation as invitation
    import hs.voice.callbacks as vcb

    aom = SimpleNamespace(id="7", uuid="aom-1", organization=1)
    answered = SimpleNamespace(
        primary_aom=7,
        generation_type="qnr_generated",
        state="agent_answered",
        end_at=None,
        channel="call",
        parent_conversation=SimpleNamespace(parent_end_at=None),
    )
    monkeypatch.setattr(val, "set_agent_for_contact_and_update_conversation", _SET_AGENT)
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-1")
    monkeypatch.setattr(invitation, "_ensure_request_context", lambda: None)
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda sip: aom)
    unanswered = classmethod(lambda cls, contact_uuid, channel=None, active_queue_id=0: None)
    monkeypatch.setattr(ccu.Conversation, "get_active_unanswered_qnr_child_by_contact_uuid", unanswered)
    monkeypatch.setattr(
        ccu.Conversation, "get_conversation_for_child_contact_uuid", classmethod(lambda cls, c, aom_id=None: answered)
    )
    torn_down = []
    monkeypatch.setattr(vcb, "_end_mpc_calls_seam", lambda c, q: torn_down.append((c, q)))

    err = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {}, None)

    assert err is None and torn_down == []
    assert primary_join_env["aqe_deletes"] == [("ct-1", "qnr-1", "ACCEPTED")]


def test_primary_join_cleanup_delete_failure_is_swallowed(primary_join_env, monkeypatch):
    """Best-effort FAILED-marking: if delete_active_queue_entry itself raises, the error is swallowed and
    handle_primary_agent_join still returns the set-agent error — the cleanup never masks it."""
    monkeypatch.setattr(val, "set_agent_for_contact_and_update_conversation", lambda *a: "boom")

    def _raise(*a, **k):
        raise RuntimeError("aqe down")

    import hs.voice.callbacks as vcb

    monkeypatch.setattr(val.aq, "delete_active_queue_entry", _raise)
    monkeypatch.setattr(vcb, "_end_mpc_calls_seam", lambda c, q: None)
    err = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {}, None)
    assert err == "boom"
    assert primary_join_env["aqe_deletes"] == []  # the raising delete recorded nothing


def test_primary_join_accepted_delete_failure_is_retryable(primary_join_env, monkeypatch):
    monkeypatch.setattr(
        val.aq,
        "delete_active_queue_entry",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("writer unavailable")),
    )
    error = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {}, None)

    assert error == ("retryable voice callback: queue status persistence unavailable")


def test_secondary_participant_cache_write_failure_is_retryable(monkeypatch):
    import hs.conversations.transfer as conversation_transfer

    monkeypatch.setattr(conversation_transfer, "add_secondary", lambda **_mapping: None)

    class VoiceRedis:
        def set_current_participant(self, _call_uuid, _member):
            return "redis timeout"

    monkeypatch.setattr(val, "VoiceRedis", VoiceRedis)
    error = val.handle_secondary_participant_join(
        "ct-1", dict(CP), _join_params(add_participant_header=vc.HEADER_PARTICIPANT_EXISTS_VALUE)
    )

    assert error == ("retryable voice callback: participant cache persistence unavailable")


def test_primary_join_reverse_routing_and_flow_transfer(primary_join_env):
    class Entry:
        status = "IN_REVERSE_ROUTING"

    val.aq.get_current_active_queue_entry_by_contact_uuid = lambda db, c, run: Entry()
    err = val.handle_primary_agent_join("ct-1", "qnr-1", dict(CP), _join_params(), {"transfer_type": "flow"}, None)
    assert err is None


def test_set_agent_mapping_bytes_sticky_and_child_accept_payload(monkeypatch):
    caps = {"sticky": [], "child_accept": [], "events": []}
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-1")
    monkeypatch.setattr(
        val, "_update_child_conversation_voice_accept", lambda c, sip: caps["child_accept"].append((c, sip)) and None
    )
    import hs.routing.sticky as rs

    monkeypatch.setattr(rs, "is_agent_in_routing_assignment", lambda cfg, agent: (True, None))
    monkeypatch.setattr(
        rs,
        "set_inbound_sticky_mapping",
        lambda agent, contact, cfg, routing_redis=None: caps["sticky"].append((agent, contact, cfg.queue)) and None,
    )
    rr = FakeRR(qnr_cfg_raw=json.dumps({"queue": "q-1", "sticky_routing_type": "Loosely bind"}))
    rr.offers[("qnr-1", "sip:ben@x")] = "offer-1"
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    import hs.conversations.store as cstore

    monkeypatch.setattr(cstore, "emit_by_number", lambda *a: caps["events"].append((a, len(rr.mapping_sets))))
    err = val.set_agent_for_contact_and_update_conversation("ct-1", "qnr-1", dict(CP), "sip:ben@x", "leg-1")
    assert err is None
    contact_uuid, mapping_json, ttl = rr.mapping_sets[0]
    assert (contact_uuid, ttl) == ("ct-1", 2 * 3600)
    # the field NAMES in declaration order, compact
    assert mapping_json == (
        '{"AgentSipEndpoint":"sip:ben@x","FromNumber":"+14155551234",'
        '"QnrRunUuid":"qnr-1","AgentUUID":"aom-1","AgentCallUuid":"leg-1",'
        '"PreviousPrimaryAgent":"","FlowRunUUID":"","ToNumber":"+16505551234",'
        '"IsInbound":true}'
    )
    assert caps["sticky"] == [("aom-1", "+14155551234", "q-1")]
    assert caps["child_accept"] == [("ct-1", "sip:ben@x")]
    # the run's offer to this agent is announced accepted, after the mapping is stored
    assert caps["events"] == [
        (("offer.accepted", CP.get("conversation_number", ""), {"offer_id": "offer-1"}, "aom-1"), 1)
    ]


def test_an_accept_after_the_offer_was_withdrawn_reports_only_the_withdrawal(monkeypatch):
    import hs.conversations.store as cstore
    import hs.voice.hangup as vh

    sent = []
    monkeypatch.setattr(cstore, "emit_by_number", lambda *a: sent.append(a[0]))
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-1")
    monkeypatch.setattr(val, "_update_child_conversation_voice_accept", lambda c, sip: None)
    rr = FakeRR()
    rr.offers[("qnr-1", "sip:ben@x")], rr.contact_params = str(uuid.uuid4()), dict(CP)
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    vh.emit_offer_outcome("ct-1", "qnr-1", "sip:ben@x", "")  # the caller left: the offer is withdrawn
    assert val.set_agent_for_contact_and_update_conversation("ct-1", "qnr-1", dict(CP), "sip:ben@x", "leg-1") is None
    assert sent == ["offer.withdrawn"]


def test_a_late_hangup_of_an_older_leg_reports_its_own_offer_and_the_newer_accept_still_emits(monkeypatch):
    import hs.conversations.store as cstore
    import hs.voice.dispatch as dispatch
    import hs.voice.hangup as vh

    sent = []
    monkeypatch.setattr(cstore, "emit_by_number", lambda *a: sent.append((a[0], a[2]["offer_id"])))
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-1")
    monkeypatch.setattr(val, "_update_child_conversation_voice_accept", lambda c, sip: None)
    older, newer = str(uuid.uuid4()), str(uuid.uuid4())
    rr = FakeRR()
    rr.contact_params = dict(CP)
    for offer in (older, newer):  # the run has offered ben again
        rr.set_offer("qnr-1", ["sip:ben@x"], offer, 60)
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    body = {"EventName": "Hangup", "To": "sip:ben@x", "X-PH-HsOffer": val.encode_sip_header_value(older)}
    leg_offer = dispatch._callback_params(body, dispatch._CS_CALLBACK_FIELDS)["offer_id"]
    vh.emit_offer_outcome("ct-1", "qnr-1", "sip:ben@x", "3020", leg_offer)  # the older leg's late decline
    assert val.set_agent_for_contact_and_update_conversation("ct-1", "qnr-1", dict(CP), "sip:ben@x", "leg-2") is None
    assert sent == [("offer.rejected", older), ("offer.accepted", newer)]


def test_secondary_agent_join_race_and_cold(monkeypatch):
    caps = {"chunks": [], "secondary": [], "hangups": [], "events": []}
    import hs.conversations.transfer as cxfer

    monkeypatch.setattr(cxfer, "add_secondary", lambda **mapping: caps["secondary"].append(mapping) and None)
    monkeypatch.setattr(
        val,
        "get_account_details_for_sip_endpoints",
        lambda sips: ([{"uuid": "aom-2", "sip_endpoint": sips[0], "name": "Cara Agent", "email": ""}], None),
    )
    monkeypatch.setattr(val.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-2")
    monkeypatch.setattr(
        val, "hangup_call_for_mpc_member", lambda mpc, member, a, t: caps["hangups"].append((mpc, member)) and None
    )
    monkeypatch.setattr(
        val,
        "_spawn",
        lambda fn, *a: caps["chunks"].append(a) if fn.__name__ == "signal_chunk" else caps["events"].append(a),
    )
    from hs.routing.invitation_models import AgentAndContactMapping

    prev = AgentAndContactMapping(agent_sip_endpoint="sip:ben@x")
    rr = FakeRR()
    rr.offers[("qnr-1", "sip:cara@x")] = "offer-2"
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    accepted = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: accepted.append(a))
    # (a) the parallel-answer RACE (no TIP): warm-shaped add_secondary, attempt from IA
    err = val.handle_secondary_agent_join(
        "ct-1", "qnr-1", dict(CP), _join_params(member_address="sip:cara@x"), prev, {}
    )
    assert err is None
    assert accepted == [("offer.accepted", "1234567890", {"offer_id": "offer-2"}, "aom-2")]
    assert caps["secondary"] == [
        {"conversation_number": "1234567890", "secondary_agent_state": "transferred", "agent_sip_or_uuid": "sip:cara@x"}
    ]
    assert caps["chunks"][0][2] == "TRANSFER_ACCEPTED"
    assert caps["chunks"][0][4] == "agent" and "Cara Agent" in caps["chunks"][0][5]
    assert caps["hangups"] == [] and rr.mtbpak == []
    # (b) COLD transfer: MTBPAK promotion + primary hangup, NO add_secondary
    caps["secondary"].clear()
    err = val.handle_secondary_agent_join(
        "ct-1",
        "qnr-1",
        dict(CP),
        _join_params(member_address="sip:cara@x", hs_kind="transfer"),
        prev,
        {"transfer_type": "agent", "transfer_mode": "cold"},
    )
    assert err is None and caps["secondary"] == []
    qnr, mtb_json, ttl = rr.mtbpak[0]
    assert (qnr, ttl) == ("qnr-1", 25 * 3600)
    assert '"AgentSipEndpoint":"sip:cara@x"' in mtb_json and '"QnrRunUuid":"qnr-1"' in mtb_json
    assert caps["hangups"] == [("mpc_qnr-1", "sip:ben@x")]


def test_secondary_participant_join_alias_and_cap(monkeypatch):
    caps = {"secondary": []}
    import hs.conversations.transfer as cxfer

    monkeypatch.setattr(cxfer, "add_secondary", lambda **mapping: caps["secondary"].append(mapping) and None)
    vr = FakeVRJoin()
    monkeypatch.setattr(val, "VoiceRedis", lambda: vr)
    err = val.handle_secondary_participant_join(
        "ct-1",
        dict(CP),
        _join_params(
            member_address="+1408555", add_participant_header="true", add_participant_alias_name="Dr%2E%20Who"
        ),
    )
    assert err is None
    assert caps["secondary"] == [
        {
            "conversation_number": "1234567890",
            "participant_identifier": "+1408555",
            "secondary_agent_state": "add_participant_success",
            "type": "add_participant",
            "participant_alias": "Dr. Who",
        }
    ]
    assert vr.cap == [("ct-1", "+1408555")]


def test_agent_join_secondary_dispatch_via_mapping_and_header(monkeypatch):
    """CONTACT_AGENT_MAPPING present → secondary; the X-PH header picks participant vs agent."""
    calls = []
    monkeypatch.setattr(val, "handle_secondary_participant_join", lambda c, cp, p: calls.append("participant") and None)
    monkeypatch.setattr(val, "handle_secondary_agent_join", lambda c, q, cp, p, m, t: calls.append("agent") and None)
    import hs.voice.callbacks as vcb

    monkeypatch.setattr(vcb, "push_voice_event_metric", lambda *a, **k: None)
    rr = FakeRR()
    rr.contact_params = dict(CP)
    rr.mapping_get = (
        '{"AgentSipEndpoint":"sip:ben@x","FromNumber":"","QnrRunUuid":"qnr-1",'
        '"AgentUUID":"aom-1","AgentCallUuid":"","PreviousPrimaryAgent":"",'
        '"FlowRunUUID":"","ToNumber":"","IsInbound":true}'
    )
    monkeypatch.setattr(val, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(val, "VoiceRedis", FakeVRJoin)
    assert val.handle_agent_join("ct-1", "qnr-1", _join_params(add_participant_header="true"), None) is None
    assert val.handle_agent_join("ct-1", "qnr-1", _join_params(), None) is None
    assert calls == ["participant", "agent"]


# ============================================================================ CallAnswered
def test_parse_answer_time_fraction_and_failures():
    ns = val.parse_answer_time("2026-06-11 10:00:00.123456")
    assert ns is not None and ns % 1_000_000_000 == 123456000
    ns2 = val.parse_answer_time("2026-06-11 10:00:00")
    assert ns2 is not None and ns2 % 1_000_000_000 == 0
    assert val.parse_answer_time("garbage") is None
    assert val.parse_answer_time("2026-06-11 10:00:00.12ab") is None


def test_parse_answer_time_iso_offsets_nanoseconds_and_time_string():
    utc = val.parse_answer_time("2026-06-11T04:30:00.123456789Z")
    assert utc is not None and utc % 1_000_000_000 == 123456789
    assert val.parse_answer_time("2026-06-11 10:00:00.123456789+05:30") == utc
    assert val.parse_answer_time("2026-06-11 10:00:00.123456789+0530") == utc
    assert val.parse_answer_time("2026-06-11 04:30:00.123456789 +0000 UTC") == utc
    assert val.parse_answer_time("2026-06-11 04:30:00.123456789 UTC") == utc
    assert val.parse_answer_time("2026-06-11 04:30:00.1+24:00") is None


def test_handle_call_answered_stamps_with_answer_ns(monkeypatch):
    stamps = []

    class VR:
        def set_mpc_event_with_dependency(self, c, q, event_name, sequence_number="", dependent_event="", timestamp=""):
            stamps.append((event_name, timestamp))
            return

    monkeypatch.setattr(val, "VoiceRedis", VR)
    val.handle_call_answered("ct-1", "qnr-1", {"answer_time": "2026-06-11 10:00:00.500000", "to": "sip:ben@x"})
    assert stamps and stamps[0][0] == "Answered:sip:ben@x"
    assert stamps[0][1].endswith("500000000")
    # Malformed provider timing is deterministic. The call still connected, so
    # acknowledge it without a retry storm and omit only the timing metric.
    stamps.clear()
    assert val.handle_call_answered("ct-1", "qnr-1", {"answer_time": "nope", "to": "x"}) is None
    assert stamps == []


def test_handle_call_answered_duplicate_is_success_but_real_cache_error_retries(monkeypatch):
    class VR:
        def __init__(self, result):
            self.result = result

        def set_mpc_event_with_dependency(self, *args, **kwargs):
            return self.result

    params = {"answer_time": "2026-06-11 10:00:00.123456", "to": "sip:ben@x"}
    monkeypatch.setattr(val, "VoiceRedis", lambda: VR(vc.VALUE_EXISTS_ERROR))
    assert val.handle_call_answered("ct-1", "qnr-1", params) is None
    monkeypatch.setattr(val, "VoiceRedis", lambda: VR("redis unavailable"))
    assert val.handle_call_answered("ct-1", "qnr-1", params) == (
        "retryable voice callback: answer timestamp cache persistence unavailable"
    )


# ============================================================================ the ACW gate
def test_acw_fetcher_happy_skip_semantics(monkeypatch):
    """GetAgentsOnActiveCallAndInACW(skip=true): flag from routing_settings, ACW via the
    in-process filter, and the SKIP initializers — active_in_call/registered = ALL agents
    (which makes occupied Call agents always 'on a call' downstream; faithful)."""
    import hs.routing.acw as racw

    monkeypatch.setattr(racw.ar, "get_allow_routing_in_acw_flag", lambda a: (True, None))
    monkeypatch.setattr(racw, "_filter_agents_in_acw_inprocess", lambda agents: ["sip:b@x"])
    acw = racw.get_agents_on_active_call_and_in_acw("A", ["sip:a@x", "sip:b@x"])
    assert acw.active_agents_in_call == ("sip:a@x", "sip:b@x")
    assert acw.agents_with_registered_sip == ("sip:a@x", "sip:b@x")
    assert acw.agents_in_acw == ("sip:b@x",) and acw.allow_routing_in_acw is True


def test_acw_fetcher_flag_and_filter_errors_abort(monkeypatch):
    import hs.routing.acw as racw

    monkeypatch.setattr(racw.ar, "get_allow_routing_in_acw_flag", lambda a: (None, "not found"))
    with pytest.raises(racw.AcwFetchError):
        racw.get_agents_on_active_call_and_in_acw("A", ["sip:a@x"])
    monkeypatch.setattr(racw.ar, "get_allow_routing_in_acw_flag", lambda a: (True, None))

    class Resp:
        error = "Error while filtering agents in ACW"
        data = None

    import hs.conversations.management as cm

    monkeypatch.setattr(cm, "filter_active_agents_in_acw", lambda agents, channel: Resp())
    with pytest.raises(racw.AcwFetchError) as ei:
        racw.get_agents_on_active_call_and_in_acw("A", ["sip:a@x"])
    # the fixed lowercase error string rides the abort
    assert str(ei.value) == "error while filtering agents in acw"


def test_acw_inprocess_filter_passes_call_channel(monkeypatch):
    """The ACW filter always asks about the call channel."""
    import hs.routing.acw as racw

    seen = {}

    class Resp:
        error = None
        data = ["sip:a@x"]

    import hs.conversations.management as cm

    monkeypatch.setattr(
        cm,
        "filter_active_agents_in_acw",
        lambda agents, channel: seen.update({"agents": agents, "channel": channel}) or Resp(),
    )
    out = racw._filter_agents_in_acw_inprocess(["sip:a@x", "sip:b@x"])
    assert out == ["sip:a@x"]
    assert seen == {"agents": ["sip:a@x", "sip:b@x"], "channel": "call"}
