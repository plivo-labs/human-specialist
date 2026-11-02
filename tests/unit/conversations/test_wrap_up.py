"""Conversation wrap-up / resolve: the voice delegation, the resolve dispatch, and null-tolerant params."""

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.conversations import wrap_up as wu
from hs.conversations.wrap_up_models import ResolveConversationParams, WrapUpConversationParams


def test_voice_delegates_to_seam(monkeypatch):
    seen = {}
    monkeypatch.setattr("hs.voice.hangup.voice_handle_wrap_up", lambda p: seen.update(hit=True))
    p = WrapUpConversationParams(channel="call")
    assert wu.handle_wrap_up_voice(p) is None
    assert seen.get("hit") is True


# --- resolve_conversation dispatch (the background body) ---
def test_resolve_dispatch_call_delegates_voice(monkeypatch):
    seen = {}
    monkeypatch.setattr("hs.voice.hangup.voice_handle_resolve", lambda p: seen.update(voice=True))
    wu.resolve_conversation(ResolveConversationParams(channel="call"))
    assert seen == {"voice": True}  # voice delegate only


def test_resolve_dispatch_never_raises():
    # fire-and-forget: the background body swallows.
    wu.resolve_conversation(ResolveConversationParams(channel="chat"))


# ======================================================================================
# Regression — LenientModel must drop nulls like _LenientModel: a call hangup builds
# ResolveConversationParams with sender_id=None, and wrap-up bodies carry null fields.
# LenientModel coerces null->default.
# ======================================================================================
def test_lenient_resolve_params_coerce_explicit_none():
    # The resolve-params construction child_update does, with sender_id=None.
    p = ResolveConversationParams(
        agent_sip_endpoint="sip:a@x",
        channel="call",
        contact_uuid="c1",
        agent_uuid="a1",
        sender_id=None,
        contact_identifier=None,
    )
    assert p.sender_id == "" and p.contact_identifier == ""  # null -> field default
    assert p.agent_sip_endpoint == "sip:a@x" and p.channel == "call"  # non-null preserved


def test_lenient_wrap_up_params_drop_nulls():
    # A wrap-up body that serializes unset fields as null.
    p = WrapUpConversationParams.model_validate(
        {
            "channel": "task",
            "sender_id": None,
            "agent_uuid": None,
            "release_agent_capacity": None,
            "parent_contact_uuid": None,
        }
    )
    assert p.channel == "task"  # real value kept
    assert p.sender_id == "" and p.agent_uuid == "" and p.parent_contact_uuid == ""
    assert p.release_agent_capacity is False  # null bool -> default
