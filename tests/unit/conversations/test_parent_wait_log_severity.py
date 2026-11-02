from types import SimpleNamespace

from hs.infra.models import UseCaseResponse


def test_expected_parent_wait_is_info_while_child_close_is_persisted(monkeypatch):
    from hs.conversations import child_update as ccu

    writes = []
    infos = []
    errors = []
    monkeypatch.setattr(
        ccu.ConversationParent, "get_by_conversation_number", classmethod(lambda cls, *_a, **_kw: SimpleNamespace(id=9))
    )
    monkeypatch.setattr(
        ccu.Conversation,
        "update_conversation",
        classmethod(lambda cls, child_id, update, *_a, **_kw: writes.append((child_id, dict(update)))),
    )
    monkeypatch.setattr(
        ccu,
        "update_parent_conversation_by_conversation_number",
        lambda **_kw: UseCaseResponse(status_code=422, error=(ccu.MSG_PARENT_CONVERSATION_CLOSURE_WAIT_FOR_OTHERS)),
    )
    monkeypatch.setattr(ccu.logger, "info", lambda *args, **kwargs: infos.append(args[0] if args else ""))
    monkeypatch.setattr(ccu.logger, "error", lambda *args, **kwargs: errors.append(args[0] if args else ""))

    class _Conversation:
        id = 17
        parent = 9
        primary_aom = None
        parent_conversation = SimpleNamespace(conversation_number="323-17", parent_channel="call", organization=323)

        def model_copy(self, update):
            return self

    result = ccu.close_conversation(_Conversation())

    assert result.id == 17
    assert len(writes) == 1
    assert writes[0][1]["status"] == "closed"
    assert any("parent remains open" in message for message in infos)
    assert errors == []
