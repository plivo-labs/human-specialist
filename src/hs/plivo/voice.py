import json
from contextlib import suppress
from urllib.parse import urlencode
from uuid import UUID

import httpx

from hs.infra.http_session import WRITES, read
from hs.infra.log import logger
from hs.infra.settings import settings
from hs.voice import constants as vc

_TIMEOUT_SECONDS = 10
# A leg's own recording is single-track (recordParticipantTrack); Plivo's default `all` names another one.
_ONE_TRACK = {"record_track_type": "participant"}
_PROVIDER_REQUEST_ID_BODY_FIELDS = ("api_id", "request_id", "request_uuid")
_PROVIDER_REQUEST_ID_HEADER_FIELDS = ("X-Plivo-Request-UUID", "X-Plivo-Request-ID", "X-Request-ID")
_PROVIDER_ERROR_CODE_FIELDS = ("error_code", "code")


def _canonical_provider_request_id(value) -> str | None:
    """Return a correlation-only UUID, never an arbitrary provider string."""
    candidate = value.strip().lower() if isinstance(value, str) else ""
    with suppress(ValueError):
        if str(UUID(candidate)) == candidate:
            return candidate
    return None


def _safe_provider_error_code(value) -> str | None:
    """Accept code-shaped values while rejecting phone/error-message-shaped text."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    candidate = str(value).strip()
    # Provider error codes are short; a longer digit string may be a phone number and must not be logged.
    return candidate if candidate.isdigit() and 3 <= len(candidate) <= 6 else None


def _provider_error_diagnostics(parsed, headers) -> str:
    """Build bounded diagnostics from an explicit allowlist of structured fields."""
    body = parsed if isinstance(parsed, dict) else {}
    header = getattr(headers, "get", lambda _field: None)
    ids = [*map(body.get, _PROVIDER_REQUEST_ID_BODY_FIELDS), *map(header, _PROVIDER_REQUEST_ID_HEADER_FIELDS)]
    request_id = next(filter(None, map(_canonical_provider_request_id, ids)), None)
    code = next(filter(None, map(_safe_provider_error_code, map(body.get, _PROVIDER_ERROR_CODE_FIELDS))), None)
    pairs = (("provider_request_id", request_id), ("provider_error_code", code))
    return "".join(f" {name}={value}" for name, value in pairs if value)


def make_mpc_id(friendly_name: str) -> str:
    return f"name_{friendly_name}"


def worst_error(errors: list[str]) -> str | None:
    """The one error of several Plivo requests to report: an outage outranks a refusal, so the caller retries."""
    return next((e for e in errors if vc.PLIVO_UNAVAILABLE in e), errors[0] if errors else None)


def compact_json(obj) -> bytes:
    s = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    s = s.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    return (s + "\n").encode("utf-8")


def _callback_url(name: str, auth_id: str, **query) -> str:
    """/plivo/{tenant}/{name}: an engine callback, signed by Plivo, with its retry policy in the fragment."""
    from hs.routing import account_read as ar

    tenant_id = ar.get_org_for_auth_id(auth_id).org_uuid
    return f"{settings.public_base_url}/plivo/{tenant_id}/{name}?{urlencode(query)}{vc.CALLBACK_CONFIG}"


def mpc_call_status_callback_url(auth_id: str, contact_uuid: str, qnr_run_uuid: str) -> str:
    return _callback_url("call-status", auth_id, call_uuid=contact_uuid, qnr_run_uuid=qnr_run_uuid)


def mpc_status_callback_url(auth_id: str, contact_uuid: str, qnr_run_uuid: str) -> str:
    return _callback_url("mpc-status", auth_id, call_uuid=contact_uuid, qnr_run_uuid=qnr_run_uuid)


def mpc_recording_callback_url(auth_id: str, contact_uuid: str, qnr_run_uuid: str) -> str:
    return _callback_url("recording", auth_id, call_uuid=contact_uuid, qnr_run_uuid=qnr_run_uuid)


def mpc_transcription_callback_url(auth_id: str, contact_uuid: str) -> str:
    return _callback_url("transcription", auth_id, call_uuid=contact_uuid)


class PlivoVoiceClient:
    def __init__(self, auth_id: str, auth_token: str):
        self.auth_id = auth_id
        self.auth_token = auth_token

    def _request(
        self,
        method,
        path,
        body=None,
        params=None,
        raw_body=None,
        *,
        quiet_not_found=False,
        single_attempt=False,
        timeout=None,
    ) -> tuple[dict | None, int, str | None]:
        """(parsed body, status, error); a transport failure is status 0. single_attempt: a read whose
        caller must not wait through connection retries."""
        send = read if method in ("GET", "HEAD") and not single_attempt else WRITES.request
        payload = {"content": raw_body} if raw_body is not None else {"json": body}
        try:
            resp = send(
                method,
                f"{settings.plivo_api_url}/v1/Account/{self.auth_id}/{path}",
                **payload,
                params=params,
                auth=(self.auth_id, self.auth_token),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=timeout or _TIMEOUT_SECONDS,
                follow_redirects=False,
            )
        except Exception as e:
            logger.error(f"plivo_voice {method} {path} transport error: {e}")
            return None, 0, f"{vc.PLIVO_UNAVAILABLE}: {e}"
        parsed = None
        if resp.content:
            with suppress(ValueError):
                parsed = resp.json()
        if resp.status_code < 300:
            return parsed, resp.status_code, None
        quiet = quiet_not_found and resp.status_code == 404
        # A 409 is Plivo refusing the call's current state; the caller decides whether that is an error.
        (logger.info if quiet else logger.warning if resp.status_code == 409 else logger.error)(
            f"plivo_voice {method} {path} -> {resp.status_code} response_body_present={bool(resp.content)}"
            + _provider_error_diagnostics(parsed, getattr(resp, "headers", None))
            + (" (caller supplied exact terminal proof)" if quiet else "")
        )
        return (
            parsed,
            resp.status_code,
            f"{vc.PLIVO_UNAVAILABLE}: non 2xx response ({resp.status_code})"
            if resp.status_code >= 500
            else f"non 2xx response ({resp.status_code})",
        )

    def mpc_add_participant(self, mpc_id: str, body: dict, raw_body: bytes | None = None):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Participant/", body=body, raw_body=raw_body)

    def mpc_list_participants(self, mpc_id: str, **kw):
        return self._request("GET", f"MultiPartyCall/{mpc_id}/Participant/", **kw)

    def mpc_update_participant(self, mpc_id: str, participant_id: str, body: dict):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Participant/{participant_id}/", body=body)

    # **kw: _request's quiet_not_found / single_attempt / timeout
    def mpc_get(self, mpc_id: str, **kw):
        return self._request("GET", f"MultiPartyCall/{mpc_id}/", **kw)

    def mpc_stop(self, mpc_id: str, **kw):
        return self._request("DELETE", f"MultiPartyCall/{mpc_id}/", **kw)

    # member_id may be "all"
    def mpc_start_speak(self, mpc_id: str, member_id: str, body: dict, **kw):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Member/{member_id}/Speak/", body=body, **kw)

    def mpc_stop_speak(self, mpc_id: str, member_id: str):
        return self._request("DELETE", f"MultiPartyCall/{mpc_id}/Member/{member_id}/Speak/")

    def mpc_start_play_audio(self, mpc_id: str, member_id: str, body: dict, **kw):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Member/{member_id}/Play/", body=body, **kw)

    def mpc_stop_play_audio(self, mpc_id: str, member_id: str):
        return self._request("DELETE", f"MultiPartyCall/{mpc_id}/Member/{member_id}/Play/")

    def mpc_start_recording(self, mpc_id: str, body: dict):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Record/", body=body)

    def mpc_pause_recording(self, mpc_id: str):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Record/Pause/")

    def mpc_resume_recording(self, mpc_id: str):
        return self._request("POST", f"MultiPartyCall/{mpc_id}/Record/Resume/")

    def mpc_start_participant_recording(self, mpc_id: str, participant_id: str, body: dict):
        path = f"MultiPartyCall/{mpc_id}/Participant/{participant_id}/Record/"
        return self._request("POST", path, body=body | _ONE_TRACK)

    def mpc_pause_participant_recording(self, mpc_id: str, participant_id: str):
        path = f"MultiPartyCall/{mpc_id}/Participant/{participant_id}/Record/Pause/"
        return self._request("POST", path, body=_ONE_TRACK)

    def mpc_resume_participant_recording(self, mpc_id: str, participant_id: str):
        path = f"MultiPartyCall/{mpc_id}/Participant/{participant_id}/Record/Resume/"
        return self._request("POST", path, body=_ONE_TRACK)

    def call_update(self, call_uuid: str, body: dict):
        return self._request("POST", f"Call/{call_uuid}/", body=body)

    def call_delete(self, call_uuid: str, **kw):
        return self._request("DELETE", f"Call/{call_uuid}/", **kw)

    def call_stop_recording(self, call_uuid: str):
        return self._request("DELETE", f"Call/{call_uuid}/Record/")

    def request_cancel(self, request_uuid: str):
        return self._request("DELETE", f"Request/{request_uuid}/")


def get_mpc(
    mpc_name: str, auth_id: str, auth_token: str, *, single_attempt: bool = True
) -> tuple[dict | None, int, str | None]:
    """A bounded read of an MPC by name with a pair. A subaccount reading an MPC it does not own
    gets 404 (a main account, though, can read its subaccounts' MPCs). The owner check runs inside
    a callback and makes a single attempt; the supervisor gate passes single_attempt=False to ride
    out a transient connection fault or 502/503/504."""
    return PlivoVoiceClient(auth_id, auth_token).mpc_get(
        make_mpc_id(friendly_name=mpc_name),
        quiet_not_found=True,
        single_attempt=single_attempt,
        timeout=httpx.Timeout(4, connect=1.5),
    )
