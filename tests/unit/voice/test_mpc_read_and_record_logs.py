"""The MPC reads behind the supervisor gate and the bounded single-attempt read, and the rule that no
record carrying a token is interpolated into a log line. Offline; ids and tokens are obviously fake."""

import json as json_mod
import re
from pathlib import Path

import httpx
import pytest

import hs.infra.http_session as hsess
import hs.voice.monitor as vm
import tests.conftest  # noqa: F401  (registry completeness — see conftest)
from hs.plivo import voice as pv

AUTH_ID = "MATESTAUTHID00000001"
TOKEN = "tok-ma-fake"


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import context_api_id, context_req_meta

    tokens = [context_req_meta.set({"api_id": "mpc-read"}), context_api_id.set("mpc-read-unit")]
    yield
    for var, tok in zip((context_req_meta, context_api_id), tokens, strict=False):
        var.reset(tok)


# ============================================================== the one bounded MPC read
class _FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.content = body.encode()
        self.headers = {}
        self._body = body

    def json(self):
        return json_mod.loads(self._body)


class TestBoundedMPCRead:
    """A bounded read is one attempt with the pair and a 5xx is an error, not retried; the supervisor
    gate reads through the retrying session, so a transient fault does not refuse a supervisor join."""

    def test_a_5xx_is_one_attempt_on_the_no_retry_session_and_refuses_the_gate(self, monkeypatch):
        calls = {"single": [], "read": []}

        def _single(method, url, **kwargs):
            calls["single"].append((method, url, kwargs))
            return _FakeResponse(502, '{"error":"upstream"}')

        def _read(method, url, **kwargs):
            calls["read"].append((method, url, kwargs))
            raise AssertionError("the bounded read must not go through the retrying session")

        monkeypatch.setattr(hsess.WRITES, "request", _single)
        monkeypatch.setattr(hsess.READS, "request", _read)

        body, status, err = pv.get_mpc("mpc_qnr-1", AUTH_ID, TOKEN)
        assert (status, err) == (502, "plivo unavailable: non 2xx response (502)")
        assert body == {"error": "upstream"}
        assert len(calls["single"]) == 1 and calls["read"] == []
        method, url, kwargs = calls["single"][0]
        assert method == "GET"
        assert url.endswith(f"/Account/{AUTH_ID}/MultiPartyCall/name_mpc_qnr-1/")
        assert kwargs["auth"] == (AUTH_ID, TOKEN)
        assert kwargs["timeout"] == httpx.Timeout(4, connect=1.5)

    def test_the_supervisor_gate_reads_through_the_retrying_session(self, monkeypatch):
        monkeypatch.setattr("hs.infra.http_session.time.sleep", lambda s: None)
        calls = {"single": [], "read": []}

        def _single(method, url, **kwargs):
            calls["single"].append((method, url, kwargs))
            raise AssertionError("the supervisor gate must ride out transient faults")

        def _read(method, url, **kwargs):
            calls["read"].append((method, url, kwargs))
            return _FakeResponse(502, '{"error":"upstream"}')

        monkeypatch.setattr(hsess.WRITES, "request", _single)
        monkeypatch.setattr(hsess.READS, "request", _read)

        assert vm.check_if_mpc_is_active("mpc_qnr-1", AUTH_ID, TOKEN) == "plivo unavailable: non 2xx response (502)"
        assert calls["single"] == [] and len(calls["read"]) == 3  # the first try and two retries
        assert calls["read"][0][2]["auth"] == (AUTH_ID, TOKEN)


class TestCheckIfMPCIsActive:
    """The supervisor gate reads the MPC with the pair, riding out transient faults, and is
    refused unless the read is a clean 200 of an MPC that has not ended."""

    def _gate(self, monkeypatch, answer):
        monkeypatch.setattr(vm.pv, "get_mpc", lambda mpc_name, auth_id, auth_token, **kwargs: answer)
        return vm.check_if_mpc_is_active("mpc_qnr-1", AUTH_ID, TOKEN)

    def test_a_live_mpc_the_pair_owns_passes(self, monkeypatch):
        assert self._gate(monkeypatch, ({"end_time": None}, 200, None)) is None

    def test_an_ended_mpc_is_refused_before_any_leg_is_dialled(self, monkeypatch):
        assert self._gate(monkeypatch, ({"end_time": "2026-09-09 10:00:00"}, 200, None)) == "mpc is ended"

    def test_an_mpc_the_pair_cannot_read_is_refused(self, monkeypatch):
        assert self._gate(monkeypatch, (None, 404, "non 2xx response (404)")) == "non 2xx response (404)"

    def test_the_gate_uses_the_retrying_read_and_refuses_a_fault_that_outlasts_it(self, monkeypatch):
        reads = []

        def read(mpc_name, auth_id, auth_token, **kwargs):
            reads.append((mpc_name, auth_id, auth_token, kwargs))
            return None, 0, "transport error"

        monkeypatch.setattr(vm.pv, "get_mpc", read)
        assert vm.check_if_mpc_is_active("mpc_qnr-1", AUTH_ID, TOKEN) == "transport error"
        assert reads == [("mpc_qnr-1", AUTH_ID, TOKEN, {"single_attempt": False})]

    @pytest.mark.parametrize(
        "auth_id,auth_token",
        [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
        ids=["both", "no-token", "no-id"],
    )
    def test_missing_credentials_never_reach_plivo(self, monkeypatch, auth_id, auth_token):
        monkeypatch.setattr(vm.pv, "get_mpc", lambda *a: pytest.fail("read attempted without credentials"))
        assert vm.check_if_mpc_is_active("mpc_qnr-1", auth_id, auth_token) == "missing credentials"


# ===================================================== records with tokens never reach a log
_REPO_ROOT = Path(__file__).resolve().parents[3]
_RECORD_HANDLING_SOURCES = (
    "src/hs/routing/capacity.py",
    "src/hs/intake/enqueue.py",
    "src/hs/voice/callbacks.py",
    "src/hs/voice/monitor.py",
    "src/hs/voice/hangup.py",
    "src/hs/voice/participant.py",
    "src/hs/voice/controls.py",
    "src/hs/voice/intake.py",
    "src/hs/voice/agent_leg.py",
    "src/hs/voice/transfer.py",
    "src/hs/routing/redis_store.py",
)
# The dict-shaped records these sources handle: the contact record (tenant token), the call
# claims (auth_token) and their serialised forms.
_WHOLE_RECORD = re.compile(
    r"\{(existing|record|stored|updated|contact_data|flow_run_data|flow_data|claim|cp|contact_params|"
    r"contact_info|params_json|mapping_json|data)\}"
)


class TestRecordsNeverReachLogLines:
    """HS logs keys and absence, never a record: a log line that interpolates one of the record
    variables whole fails here instead of leaking the tenant token to the logs."""

    def test_no_log_line_interpolates_a_whole_record(self):
        offenders = []
        for rel in _RECORD_HANDLING_SOURCES:
            lines = (_REPO_ROOT / rel).read_text().splitlines()
            for index, line in enumerate(lines):
                if not _WHOLE_RECORD.search(line):
                    continue
                window = "\n".join(lines[max(0, index - 3) : index + 1])
                if "logger." in window or "msg=" in window:
                    offenders.append(f"{rel}:{index + 1}: {line.strip()}")
        assert offenders == [], "\n".join(offenders)
