"""Fakes for the invite flow's collaborators, by the role each plays (the module attribute it replaces)."""

from hs.routing import invitation as ri
from hs.voice import intake as voice_intake

_SEAMS = {
    "update_unanswered_conversation": (ri, "_update_unanswered_inprocess"),
    "handle_reverse_routing": (ri, "_handle_reverse_routing_inprocess"),
    "handle_incoming_call": (voice_intake, "handle_incoming_call"),
}


def patch_invite_seams(monkeypatch, **fakes):
    for role, fake in fakes.items():
        monkeypatch.setattr(*_SEAMS[role], fake)
