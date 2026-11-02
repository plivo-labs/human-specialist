"""Conversation and chunk write helpers:
  * Conversation.create_conversation        — bare add+flush (NO commit; caller commits)
  * ConversationChunk.create_chunk/create_chunks — bare add(+_all)+flush (NO commit)
  * create_chunk_with_related_data — resolve conversation -> INSERT chunk,
    with the unviewed-count cache skip for signal chunks.

Mock-based and offline.
"""

from unittest.mock import MagicMock, patch

# --- data_adapter write methods (no-commit contract) --------------------------


def test_create_conversation_adds_flushes_no_commit():
    from hs.conversations.store import Conversation

    fake_db, conv = MagicMock(), MagicMock()
    with patch("hs.conversations.store.get_db_session", return_value=fake_db):
        result = Conversation.create_conversation(conv)
    fake_db.add.assert_called_once_with(conv)
    fake_db.flush.assert_called_once()
    fake_db.commit.assert_not_called()  # caller / PostConversationStatusChange commits
    assert result == conv._Conversation__to_model.return_value  # name-mangled __to_model


def test_create_chunk_adds_flushes_no_commit():
    from hs.conversations.store import ConversationChunk

    fake_db, chunk = MagicMock(), MagicMock()
    with patch("hs.conversations.store.get_db_session", return_value=fake_db):
        ConversationChunk.create_chunk(chunk)
    fake_db.add.assert_called_once_with(chunk)
    fake_db.flush.assert_called_once()
    fake_db.commit.assert_not_called()


# --- create_chunk_with_related_data (usecase) ---------------------------------


def _chunk(conversation=5, type_="signal"):
    c = MagicMock()
    c.conversation = conversation
    c.type = type_
    return c


def _patches():
    return (patch("hs.conversations.management.Conversation"), patch("hs.conversations.management.ConversationChunk"))


def test_chunk_with_related_data_swallows_errors():
    from hs.conversations import management as cm

    chunk = _chunk()
    pConv, pChunk = _patches()
    with pConv, pChunk as Chunk:
        Chunk.create_chunk.side_effect = RuntimeError("boom")
        # swallowed (logged) — must NOT raise.
        cm.create_chunk_with_related_data(chunk)
