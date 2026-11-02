"""Channel-transfer DATA LAYER + models (offline-deterministic).

Pins the transfer Redis surface on a fake Cache injected into RoutingRedis:
  * TIP get/derive — is_transfer_in_progress, the warm/cold checks and the external check that reads TYPE, not MODE (a quirk);
  * MTBPAK — set_to_be_primary_agent / get_to_be_primary_agent / delete_to_be_primary_agent keyed by qnrRunUUID;
  * TransferInfo.to_json `,omitempty` (a flow transfer writes only transfer_type);
  * the IncomingTransferReq / AgentDropParams bind shapes.
"""

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.voice.transfer_models import AgentDropParams, IncomingTransferReq, TransferInfo


class FakeCache:
    def __init__(self, strings=None, eval_returns=None, eval_raises=False):
        self.strings = dict(strings or {})
        self.expires = {}
        self.eval_calls = []
        self._eval_returns = eval_returns or {}
        self._eval_raises = eval_raises

    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.strings[key] = value
        if expiry_in_seconds:
            self.expires[key] = expiry_in_seconds
        return True

    def delete(self, key):
        existed = key in self.strings
        self.strings.pop(key, None)
        return 1 if existed else 0

    def eval_script(self, src, keys=None, args=None):
        self.eval_calls.append((src, keys, args))
        if self._eval_raises:
            raise RuntimeError("NOSCRIPT / redis down")
        return self._eval_returns.get(src, "0")


def _rr(**kw):
    return RoutingRedis(cache=FakeCache(**kw))


# --- models -------------------------------------------------------------------------------------
def test_transfer_info_to_json_omitempty():
    assert (
        TransferInfo(transfer_type="agent", transfer_mode="warm").to_json()
        == '{"transfer_type":"agent","transfer_mode":"warm"}'
    )
    assert (
        TransferInfo(transfer_type="team", transfer_mode="cold").to_json()
        == '{"transfer_type":"team","transfer_mode":"cold"}'
    )
    # flow transfer: transfer_mode EMPTY → the key is OMITTED
    assert TransferInfo(transfer_type="flow").to_json() == '{"transfer_type":"flow"}'
    assert TransferInfo().to_json() == "{}"


def test_bind_models():
    r = IncomingTransferReq.model_validate(
        {
            "contact_uuid": "c1",
            "transfer_type": "agent",
            "identifier": "sip:t",
            "agent_sip": "sip:a",
            "member": "m",
            "extra": "ignored",
        }
    )
    assert (r.contact_uuid, r.transfer_type, r.identifier, r.agent_sip, r.member) == (
        "c1",
        "agent",
        "sip:t",
        "sip:a",
        "m",
    )
    d = AgentDropParams.model_validate({"leave_type": "leave_primary", "contact_uuid": "c1", "channel": "chat"})
    assert (d.leave_type, d.contact_uuid, d.channel) == ("leave_primary", "c1", "chat")


def test_mtbpak_key_builder():
    assert rc.mpc_to_be_primary_agent_key("qnr-1") == "MTBPAK:qnr-1"


# --- TIP get + Is* derivations ------------------------------------------------------------------
def test_get_transfer_in_progress_info():
    key = rc.transfer_in_progress_key("c1")
    rr = _rr(strings={key: '{"transfer_type":"agent","transfer_mode":"warm"}'})
    assert rr.get_transfer_in_progress_info("c1") == {"transfer_type": "agent", "transfer_mode": "warm"}
    assert _rr().get_transfer_in_progress_info("c1") is None  # miss
    assert _rr(strings={key: "garbage"}).get_transfer_in_progress_info("c1") is None  # parse-fail


def test_is_transfer_in_progress():
    key = rc.transfer_in_progress_key("c1")
    assert _rr(strings={key: '{"transfer_type":"agent","transfer_mode":"warm"}'}).is_transfer_in_progress("c1") == (
        True,
        None,
    )
    assert _rr().is_transfer_in_progress("c1") == (False, "data not found in cache")
    # present row but EMPTY transfer_type → still 'data not found in cache'
    assert _rr(strings={key: '{"transfer_mode":"warm"}'}).is_transfer_in_progress("c1") == (
        False,
        "data not found in cache",
    )


# --- MTBPAK set / get / delete ------------------------------------------------------------------
def test_mtbpak_set_get_delete():
    rr = _rr()
    mapping = AgentAndContactMapping(
        agent_sip_endpoint="sip:p", from_number="+1", qnr_run_uuid="Q", agent_uuid="ag-uuid"
    )
    assert rr.set_to_be_primary_agent("Q", mapping.to_json(), rc.TRANSFER_KEY_TTL_SECONDS) is True
    key = rc.mpc_to_be_primary_agent_key("Q")
    assert rr._cache.expires[key] == rc.TRANSFER_KEY_TTL_SECONDS
    got = rr.get_to_be_primary_agent("Q")
    assert got["AgentSipEndpoint"] == "sip:p" and got["AgentUUID"] == "ag-uuid"  # capitalized keys
    rr.delete_to_be_primary_agent("Q")
    assert rr.get_to_be_primary_agent("Q") is None
    # absent delete is a no-op (no error)
    assert rr.delete_to_be_primary_agent("missing") is None
