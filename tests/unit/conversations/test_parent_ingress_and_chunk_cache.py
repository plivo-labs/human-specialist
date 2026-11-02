"""Parent ingress and the async chunk cache (offline pins).

* the async conversation routing model: from/to aliases; the 'from number' message on the TO validator;
  build_for_cache (int epoch → isoformat(' '), absent → now, media type → 'file', by_alias dict);
  build_from_cache round-trip; build_db_model from_user/to_user mapping incl. the sent_chunk email
  flip and the agent-uuid override; Config extra='forbid'.
* the async chunk cache: key `chunks:{from}:{to}:{channel}`; rpush JSON; message_id dedupe;
  remove = key delete; add_and_get_count.
* _create_parent_with_unique_number: bounded ≤5 regenerate-on-unique-violation; foreign
  IntegrityErrors re-raise untouched; exhaustion re-raises the last error.
* create_parent_conversation guard branches: outbound-without-agent 400; inbound-with-agent 400;
  unknown org 422; async-channel duplicate-active 409.
"""

import uuid as uuid_mod
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import IntegrityError

import hs.conversations.management as cm
import tests.conftest  # noqa: F401

AOM_UUID = str(uuid_mod.uuid4())


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import context_aom_uuid, context_api_id, context_req_meta

    t = [
        context_req_meta.set({"api_id": "parent-ingress"}),
        context_api_id.set("parent-ingress-unit"),
        context_aom_uuid.set(AOM_UUID),
    ]
    yield
    from hs.infra.context import context_aom_uuid, context_api_id, context_req_meta

    for var, tok in zip((context_req_meta, context_api_id, context_aom_uuid), t):
        var.reset(tok)


class _FakeListCache:
    """rpush/lrange/delete fake with the Cache list semantics (strings in, strings out)."""

    def __init__(self):
        self.store = {}

    def rpush(self, key, values):
        self.store.setdefault(key, []).append(values)
        return len(self.store[key])

    def lrange(self, key, start=0, end=-1):
        items = self.store.get(key, [])
        end = len(items) if end == -1 else end + 1
        return items[start:end]

    def delete(self, key):
        self.store.pop(key, None)

    def expire(self, key, expiry_in_seconds):
        return True

    def set(self, *a, **kw):
        return True


@pytest.fixture()
def fake_cache(monkeypatch):
    fc = _FakeListCache()
    monkeypatch.setattr(cm.Cache, "get_instance", classmethod(lambda cls: fc))
    return fc


# ============================================================== unique retry
class _FakeNested:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _integrity(msg):
    return IntegrityError("INSERT …", {}, Exception(msg))


def test_retry_regenerates_on_unique_violation(monkeypatch):
    import hs.infra.context as ctx

    monkeypatch.setattr(ctx, "get_db_session", lambda: SimpleNamespace(begin_nested=lambda: _FakeNested()))
    from hs.conversations.store import ConversationParent

    calls = {"create": 0, "regen": 0}

    def _create(entity):
        calls["create"] += 1
        if calls["create"] < 3:
            raise _integrity(
                "duplicate key value violates unique constraint "
                '"uq_conversation_parent_conversation_number" '
                "DETAIL: Key (conversation_number)=(123) already exists."
            )
        return SimpleNamespace(conversation_number="999")

    monkeypatch.setattr(ConversationParent, "create_conversation", staticmethod(_create))
    conv = SimpleNamespace(build_db_model=lambda **kw: calls.__setitem__("regen", calls["regen"] + 1) or object())
    out = cm._create_parent_with_unique_number(object(), conv, None, None, "+1")
    assert out.conversation_number == "999"
    assert calls == {"create": 3, "regen": 2}  # two collisions → two regenerations


def test_foreign_integrity_error_reraises(monkeypatch):
    import hs.infra.context as ctx

    monkeypatch.setattr(ctx, "get_db_session", lambda: SimpleNamespace(begin_nested=lambda: _FakeNested()))
    from hs.conversations.store import ConversationParent

    monkeypatch.setattr(
        ConversationParent,
        "create_conversation",
        staticmethod(
            lambda e: (_ for _ in ()).throw(_integrity('null value in column "organization" violates not-null'))
        ),
    )
    with pytest.raises(IntegrityError):
        cm._create_parent_with_unique_number(
            object(), SimpleNamespace(build_db_model=lambda **kw: object()), None, None, "+1"
        )


def test_exhaustion_raises_last_error(monkeypatch):
    import hs.infra.context as ctx

    monkeypatch.setattr(ctx, "get_db_session", lambda: SimpleNamespace(begin_nested=lambda: _FakeNested()))
    from hs.conversations.store import ConversationParent

    calls = {"n": 0}

    def _always_collide(entity):
        calls["n"] += 1
        raise _integrity("Key (conversation_number)=(123) already exists.")

    monkeypatch.setattr(ConversationParent, "create_conversation", staticmethod(_always_collide))
    with pytest.raises(IntegrityError):
        cm._create_parent_with_unique_number(
            object(), SimpleNamespace(build_db_model=lambda **kw: object()), None, None, "+1"
        )
    assert calls["n"] == 5  # bounded


# ============================================================== usecase guard branches
def _parent_conv(**over):
    from hs.conversations.models import ConvParentInsertModel

    base = {
        "phone_or_email": "+15550109999",
        "parent_channel": "message",
        "parent_contact_uuid": "parent-ingress-c1",
        "org_auth_id": "AUTHX12345",
        "direction_inbound": True,
        "flow_run_uuid": "parent-ingress-run",
    }
    base.update(over)
    return ConvParentInsertModel(**base)
