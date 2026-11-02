import time
from urllib.parse import urlencode
from xml.sax.saxutils import escape as xml_escape

from hs import jobs
from hs.infra.counters import MetricName, increase_counter
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing.redis_store import RoutingRedis
from hs.voice import constants as vc
from hs.voice.redis_store import VoiceRedis

_ATTR_ENTITIES = {'"': "&#34;", "'": "&#39;", "\t": "&#x9;", "\n": "&#xA;", "\r": "&#xD;"}


def _attr_escape(value: str) -> str:
    """An attribute value with quotes and whitespace escaped as numeric character references."""
    return xml_escape(value, _ATTR_ENTITIES)


def queue_exit_url(auth_id: str, contact_uuid: str, qnr_run_uuid: str, **outcome) -> str:
    """The one continuation for a caller leaving a queue; `outcome` is its result (and a digit for a DTMF exit),
    absent when Plivo itself ends the caller's stay."""
    tenant_id = ar.get_org_for_auth_id(auth_id).org_uuid
    query = urlencode({"call_uuid": contact_uuid, "qnr_run_uuid": qnr_run_uuid, **outcome})
    return f"{pv.settings.public_base_url}/plivo/{tenant_id}/actions/queue-exit?{query}"


def hold_music(agent: str = "", customer: str = "") -> str:
    """The room's hold music, HOLD_MUSIC_URL where the queue sets none: Plivo's own is silence."""
    default = pv.settings.hold_music_url
    tracks = (("AgentHoldMusic", agent or default), ("CustomerHoldMusic", customer or default))
    return "".join(f"<{tag}><Play>{xml_escape(url)}</Play></{tag}>" for tag, url in tracks if url)


def _spawn(target, *args) -> None:
    # Detached spawn (hs/infra/bg_spawn.py): keeps log context, but the worker gets its OWN DB
    # session -- sharing the request Session across threads raced its COMMIT against the
    # worker's SAVEPOINT (psycopg2 NoActiveSqlTransaction).
    from hs.infra.bg_spawn import spawn_detached

    spawn_detached(target, *args, log_context="voice_intake")


def _stop_call_recording(call_uuid: str, auth_id: str, auth_token: str) -> None:
    if not auth_id or not auth_token:
        logger.error(f"_stop_call_recording: error initialising plivo client call_uuid={call_uuid}")
        return
    _body, _status, err = pv.PlivoVoiceClient(auth_id, auth_token).call_stop_recording(call_uuid)
    if err is not None:
        logger.error(f"_stop_call_recording: error while stopping recording call_uuid={call_uuid}: {err}")
    elif VoiceRedis().stop_recording_for_call(call_uuid) is None:
        logger.info(f"_stop_call_recording: call recording stopped successfully call_uuid={call_uuid}")


def initiate_mpc(params) -> str:
    initiate_ts = str(time.time_ns())
    _token, record_value, err = ar.get_organisation_auth_token_and_record_value(params.auth_id, params.inbound)
    if err is not None:
        record_value = False
        logger.error(
            f"initiate_mpc: Error occurred while fetching record value from org table "
            f"calluuid={params.contact_uuid}: {err}"
        )
    contact, qnr, queue = params.contact_uuid, params.queue_and_route_run_uuid, params.queue_config
    exit_url = queue_exit_url(params.auth_id, contact, qnr)
    rec, esc = ("true" if record_value else "false"), _attr_escape
    recording = esc(pv.mpc_recording_callback_url(params.auth_id, contact, qnr))
    status = esc(pv.mpc_status_callback_url(params.auth_id, contact, qnr))
    transcription = esc(pv.mpc_transcription_callback_url(params.auth_id, contact))
    hold = hold_music(queue.agent_hold_music.url, queue.customer_hold_music.url)
    response_xml = (
        f'<Response><MultiPartyCall role="{vc.MPC_ROLE_CUSTOMER}" waitTime="{queue.queue_call_timeout}" record="{rec}"'
        f' recordingCallbackUrl="{recording}" statusCallbackEvents="{vc.MPC_STATUS_CALLBACK_EVENTS}"'
        f' statusCallbackUrl="{status}" startMpcOnEnter="false" endMpcOnExit="true"'
        f' onExitActionUrl="{esc(exit_url)}" transcript="{rec}" transcriptionUrl="{transcription}"'
        f' recordParticipantTrack="{rec}">mpc_{qnr}{hold}</MultiPartyCall></Response>'
    )
    if VoiceRedis().set_mpc_callback_event(contact, qnr, vc.MPC_INITIALISE_EVENT, initiate_ts) is not None:
        logger.error(f"initiate_mpc: unable to set initiate mpc time in cache CallUUID={contact}")
        increase_counter(MetricName.MPC_FAILURE, {"output_state": "add_customer"})
    missed = {"callUUID": contact, "qnrRunUUID": qnr, "eventName": vc.MPC_INITIALISED_CALLBACK_EVENT}
    jobs.schedule("missed_mpc_event", missed, delay=10)
    logger.info(f"MPC XML response response={response_xml}")
    return response_xml


def handle_incoming_call(params) -> str:
    response_xml = initiate_mpc(params)
    rr = RoutingRedis()
    contact_params = rr.get_contact_parameters(params.contact_uuid, params.queue_and_route_run_uuid)
    _spawn(_stop_call_recording, params.contact_uuid, *ar.contact_pair(contact_params))
    return response_xml
