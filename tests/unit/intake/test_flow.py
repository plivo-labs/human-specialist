"""The inbound path's own logic: context limits, the queue row -> engine config, one answer per call, and a
callback acting only on its signer's call."""

import json
import uuid

import pytest
import redis

from hs.access import Principal
from hs.errors import ApiError, status_of
from hs.infra.redis import Cache
from hs.intake import api, flow
from hs.routing import constants as rc
from hs.setup.api import QueueIn


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (json.dumps({"blob": "x" * 2100}), "context_too_large"),
        ("[1, 2]", "context_invalid"),
        ("{not json", "context_invalid"),
        (json.dumps({"a": "\0"}), "context_invalid"),
    ],
)
def test_context_is_a_small_json_object_without_nul(raw, code):
    with pytest.raises(ApiError) as e:
        flow.context_of(raw)
    assert (e.value.status, e.value.code) == (422, code)


def test_context_passes_through():
    assert flow.context_of('{"lead_id": "L-42"}') == {"lead_id": "L-42"} and flow.context_of(None) is None


CALL = {"call_uuid": "c1", "auth_id": "SA1", "conversation_number": "123", "run_id": "r1", "from": "+1", "to": "+2"}


def test_a_queue_row_becomes_the_engine_config():
    team, agent = str(uuid.uuid4()), str(uuid.uuid4())
    q = QueueIn(name="Q", teams=[team], agents=[agent], sticky={"mode": "strict"}, wait_timeout_sec=9)
    q = q.model_dump(mode="json") | {"dtmf_actions": {"9": {"type": "hangup"}}}
    params = flow.contact_params(CALL, "q1", q, [])
    route, queue = params.queue_and_route_config, params.queue_config
    assert (route.queue, route.routing_teams, route.routing_agents, route.retry_limit) == ("q1", [team], [agent], 3)
    assert (route.sticky_routing_type, route.sticky_routing_expiry) == (rc.STICKY_STRICTLY_BIND, True)
    assert (queue.queue_call_timeout, queue.queue_capacity, queue.queue_call_allowed_inputs) == (9, 50, ["9"])


def test_named_agents_are_the_only_ones_offered():
    q = QueueIn(name="Q", teams=[str(uuid.uuid4())], agents=[str(uuid.uuid4())]).model_dump(mode="json")
    route = flow.contact_params(CALL, "q1", q, ["owner"]).queue_and_route_config
    assert (route.routing_teams, route.routing_agents) == ([], ["owner"])


def test_a_repeated_key_gets_the_first_answer():
    key, calls = f"ANS:test:{uuid.uuid4()}", []
    assert flow.once(key, lambda: calls.append(1) or "<Response/>") == "<Response/>"
    assert flow.once(key, lambda: calls.append(1) or "<Other/>") == "<Response/>" and calls == [1]


def test_a_signed_request_only_loads_its_own_tenants_call(monkeypatch):
    p = type("P", (), {"tenant_id": "t-b"})()
    monkeypatch.setattr(flow, "load", lambda call_uuid: CALL)
    monkeypatch.setattr(flow, "tenant_plivo", lambda tenant_id: ("SA1" if tenant_id == "t-a" else "SA2", "token"))
    assert flow.load_for(type("P", (), {"tenant_id": "t-a"})(), "c1", {"CallUUID": "c1"}) == CALL
    assert flow.load_for(p, "c1", {"CallUUID": "c1"}) == {}
    assert flow.load_for(type("P", (), {"tenant_id": "t-a"})(), "c1", {"CallUUID": "other"}) == {}


def test_an_answer_retried_across_the_upgrade_gets_the_previous_releases_reply(monkeypatch):
    call_uuid, owner, other = str(uuid.uuid4()), Principal("t-a", 1, "plivo"), Principal("t-b", 2, "plivo")
    flow.redis().set(f"ANS:{call_uuid}", "<Response>queued</Response>", ex=60)
    monkeypatch.setattr(flow, "load_for", lambda p, c: CALL if p is owner else {})
    monkeypatch.setattr(api, "_answer", lambda p, form: "<Response><Hangup/></Response>")
    form = {"CallUUID": call_uuid, "From": "14155550111", "To": "14155550123"}
    assert api.answer(owner, form, None).body == b"<Response>queued</Response>"
    assert api.answer(other, form, None).body == b"<Response><Hangup/></Response>"


class _Down:
    def hget(self, *args):
        raise redis.ConnectionError("down")


def test_an_engine_callback_is_a_503_while_the_store_is_down_and_a_404_for_a_call_it_lacks(monkeypatch):
    p = Principal("t-a", 1, "plivo")
    monkeypatch.setattr(api, "tenant_plivo", lambda tenant_id: ("SA1", "token"))
    with pytest.raises(ApiError) as missing:
        api._own(p, str(uuid.uuid4()), "r1")
    monkeypatch.setattr(Cache.get_instance(), "_client", _Down())
    with pytest.raises(Exception) as down:
        api._own(p, str(uuid.uuid4()), "r1")
    assert (missing.value.status, status_of(down.value)) == (404, 503)
