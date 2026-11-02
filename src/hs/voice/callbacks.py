import collections
import contextvars
import re
import time
from dataclasses import dataclass

from hs.infra.counters import MetricName, increase_counter
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice.call_models import CallLegInsertModel
from hs.voice.intake import queue_exit_url
from hs.voice.phase_metrics import time_phase, timed_phase
from hs.voice.redis_store import VoiceRedis

_RETRYABLE_QUEUE_PERSISTENCE_PREFIX = "retryable_queue_persistence:"
_AFTER_COMMIT_EFFECTS_KEY = "hs_voice_after_commit_effects"
_UNSAFE_CALLBACK_EFFECTS_KEY = "hs_voice_unsafe_callback_effects"
_CALLBACK_EFFECT_SESSION = contextvars.ContextVar("hs_voice_callback_effect_session", default=None)
_RETRYABLE_VOICE_AGENT_SELECTION_ERRORS = frozenset(
    {"error fetching agents for contact", "error while filtering agents in acw", "error filtering agents by capacity"}
)
_PROMPT_STATE = {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0}
_WAIT_STRATEGIES = {
    vc.MIN_ESTIMATED_WAIT_TIME_VALUE: "min",
    vc.MAX_ESTIMATED_WAIT_TIME_VALUE: "max",
    vc.ESTIMATED_WAIT_TIME_VALUE: "mean",
}


def is_retryable_queue_persistence_error(error: str | None) -> bool:
    return bool(error and error.startswith(_RETRYABLE_QUEUE_PERSISTENCE_PREFIX))


def is_retryable(error: str | None) -> bool:
    """A transient dependency failure: the provider retries the callback, nothing is failed or handed back."""
    return is_retryable_queue_persistence_error(error) or voice_hangup.is_retryable_voice_callback_error(error)


def _effect_info(db) -> dict | None:
    """Session.info of the callback's session (the bound one when db is None): per request, so a
    rollback or close can never leak staged effects or markers into another request."""
    info = getattr(db if db is not None else _CALLBACK_EFFECT_SESSION.get(), "info", None)
    return info if isinstance(info, dict) else None


def stage_after_commit_effect(db, callback, *args, **kwargs) -> str | None:
    """Stage a voice side effect; the controller runs it only after the commit succeeds."""
    info = _effect_info(db)
    if info is None:
        return "database session cannot stage after-commit effects"
    info.setdefault(_AFTER_COMMIT_EFFECTS_KEY, []).append((callback, args, kwargs))
    return None


def stage_or_run(db, callback, *args, spawn=None) -> str | None:
    """Stage callback(*args) to run after the commit. With no session to stage on (the direct / injected
    unit seam; request sessions always have one) it runs now, through spawn when given."""
    if isinstance(getattr(db, "info", None), dict):
        return stage_after_commit_effect(db, callback, *args)
    (spawn or (lambda cb, *a: cb(*a)))(callback, *args)
    return None


def mark_unsafe_callback_effect(db, effect: str) -> None:
    """Record an attempted external mutation with no idempotency key (Plivo prompt / add-participant,
    events, ...): replaying this callback is then unsafe, so the controller keeps its claim."""
    info = _effect_info(db)
    if info is not None:
        info.setdefault(_UNSAFE_CALLBACK_EFFECTS_KEY, []).append(str(effect or "external mutation"))


def unsafe_callback_effects(db) -> tuple[str, ...]:
    return tuple((_effect_info(db) or {}).get(_UNSAFE_CALLBACK_EFFECTS_KEY) or ())


def has_unsafe_callback_effect(db) -> bool:
    return bool(unsafe_callback_effects(db))


def bind_callback_effect_session(db):
    return _CALLBACK_EFFECT_SESSION.set(db)


def reset_callback_effect_session(token) -> None:
    _CALLBACK_EFFECT_SESSION.reset(token)


def discard_after_commit_effects(db) -> None:
    (_effect_info(db) or {}).pop(_AFTER_COMMIT_EFFECTS_KEY, None)


def drain_after_commit_effects(db) -> None:
    """Run and remove staged effects exactly once after the commit. Keeps going past a failed one so it
    can't drop the rest; the controller keeps the claim and sees the aggregate failure."""
    failures = 0
    for callback, args, kwargs in (_effect_info(db) or {}).pop(_AFTER_COMMIT_EFFECTS_KEY, ()):
        try:
            callback(*args, **kwargs)
        except Exception:
            failures += 1
            logger.error("Voice callback after-commit effect failed", exc_info=True)
    if failures:
        raise RuntimeError(f"{failures} voice callback after-commit effect(s) failed")


@dataclass
class EventDetails:
    event_name: str
    dependent_event: str = ""
    sequence_number: str = ""
    timestamp: str = ""


def _create_call_leg_seam(
    call_uuid: str, call_leg_uuid: str, member_address: str, start_time: str, end_time: str, role: str, is_mpc: bool
):
    from hs.voice import call_legs as call_management
    from hs.voice.monitor import _seed_context

    _seed_context({})
    create = call_management.create_parent_call_leg if is_mpc else call_management.create_call_leg
    try:
        leg = {"call_uuid": call_uuid, "call_leg_uuid": call_leg_uuid, "member_address": member_address, "role": role}
        error = create(CallLegInsertModel(**leg, start_time=start_time, end_time=end_time or None)).error
    except Exception as e:
        error = e
    if error:
        logger.error(f"_create_call_leg_seam: failed to create call leg call_uuid={call_uuid}: {error}")
    voice_hangup._commit_session()


def _end_mpc_calls_seam(call_uuid: str, qnr_run_uuid: str):
    from hs.voice.controls import end_mpc_calls

    _res, err = end_mpc_calls(call_uuid, qnr_run_uuid)
    if err is not None:
        raise RuntimeError(err)


def _spawn(target, *args) -> None:
    # Detached spawn (hs/infra/bg_spawn.py): keeps log context, but the worker gets its OWN DB
    # session -- sharing the request Session across threads raced its COMMIT against the
    # worker's SAVEPOINT (psycopg2 NoActiveSqlTransaction).
    from hs.infra.bg_spawn import spawn_detached

    spawn_detached(target, *args, log_context="voice_callbacks")


def update_voice_event_time_in_cache(call_uuid: str, qnr_run_uuid: str, event_details: EventDetails) -> str | None:
    """Take the event's ordering / dedupe claim; None when taken, else why not (duplicate, superseded,
    Redis unavailable). The timestamp is the claim token, so a failed handler can release only its own."""
    d = event_details
    d.timestamp = d.timestamp or str(time.time_ns())
    if d.dependent_event:
        claim = {"sequence_number": d.sequence_number, "dependent_event": d.dependent_event, "timestamp": d.timestamp}
        err = VoiceRedis().set_mpc_event_with_dependency(call_uuid, qnr_run_uuid, d.event_name, **claim)
    else:
        err = VoiceRedis().set_mpc_callback_event(call_uuid, qnr_run_uuid, d.event_name, d.timestamp)
    if err in (vc.VALUE_EXISTS_ERROR, vc.LATER_EVENT_FOUND_ERROR):
        logger.info(
            "update_voice_event_time_in_cache: duplicate/out-of-order callback ignored "
            f"CallUUID={call_uuid} event={d.event_name}: {err}"
        )
    elif err is not None:
        logger.error(
            "update_voice_event_time_in_cache: unable to persist callback ordering gate "
            f"CallUUID={call_uuid} event={d.event_name}: {err}"
        )
    return err


def release_voice_event_claim(call_uuid: str, qnr_run_uuid: str, event_details: EventDetails) -> str | None:
    if not event_details.timestamp:
        return "event claim token missing"
    event = event_details.event_name
    err = VoiceRedis().release_mpc_callback_event(call_uuid, qnr_run_uuid, event, event_details.timestamp)
    if err is not None:
        logger.error(f"voice callback claim release failed CallUUID={call_uuid} event={event}: {err}")
    return err


def build_event_details(event_name: str, member_address: str, sequence_number: str) -> EventDetails:
    if event_name in vc.PARTICIPANT_SPECIFIC_EVENTS:
        return EventDetails(
            event_name=f"{event_name}:{member_address}:{sequence_number}",
            dependent_event=f"{vc.MPC_PARTICIPANT_EXIT_EVENT}:{member_address}",
            sequence_number=sequence_number,
        )
    return EventDetails(event_name=event_name)


def push_voice_event_metric(call_uuid: str, qnr_run_uuid: str, cache_event_name: str, metric_name: str) -> None:
    now_ns = time.time_ns()
    ts, _err = VoiceRedis().get_mpc_callback_event(call_uuid, qnr_run_uuid, cache_event_name)
    try:
        duration_seconds = (now_ns - int(ts)) / 1e9
    except (TypeError, ValueError):
        logger.error(
            f"push_voice_event_metric: unable to get cached timestamp CallUUID={call_uuid} "
            f"cacheEventName={cache_event_name}"
        )
        return
    logger.info(f"push_voice_event_metric: metric={metric_name} duration={duration_seconds} CallUUID={call_uuid}")


def _ssml_get_voice_and_content(text: str) -> tuple[str, str, str | None]:
    text = text.replace("Speak", "speak").replace("Voice", "voice").replace('"', "'")
    m = re.match(r"^\s*<speak(\s[^>]*)?>(.*)</speak>\s*$", text, flags=re.DOTALL)
    if not m:
        return "Polly.Joey", text, "Not able to parse XML"
    vm = re.search(r"voice\s*=\s*'([^']*)'", m.group(1) or "")
    voice = vm.group(1) if vm and vm.group(1) else "Polly.Joey"
    return voice, m.group(2).replace("\n", "").replace("\t", ""), None


def has_exact_mpc_end_proof(call_uuid: str, qnr_run_uuid: str, voice_redis: VoiceRedis | None, on_error=None) -> bool:
    """Prove MPCEnd for this exact parent call and QNR run; a failed lookup proves nothing (on_error sees it)."""
    if not call_uuid or not qnr_run_uuid or voice_redis is None:
        return False
    try:
        ended_at, error = voice_redis.get_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_EVENT)
    except Exception as exc:
        if on_error:
            on_error(exc)
        return False
    return error is None and bool(ended_at)


def _play_prompt(prompt, cp: dict, member_id: str, call_uuid: str, db) -> tuple[str, str | None]:
    """Speak (with the wait-time / queue-position placeholders filled) or play one queue prompt to the customer."""
    qnr_run = cp.get("queue_and_route_run_uuid", "")
    speak = prompt.prompt_type == "speak"
    if speak:
        text = check_and_update_play_text(db, qnr_run, cp.get("queue_uuid", ""), cp.get("node_uuid", ""), prompt.text)
    auth_id, auth_token = ar.contact_pair(cp)
    if not auth_id or not auth_token:
        return "", "error in creating plivo client"
    if speak:
        label, api, method = "PlayText", "member speak", "mpc_start_speak"
        ok = "Successfully played wait text to customer"
        body = {"text": text, "voice": prompt.voice, "language": prompt.language, "mix": False}
        if (prompt.prompt_speak_type or "").lower() == "polly":
            voice, text, err = _ssml_get_voice_and_content(text)
            if err is not None:
                logger.error(f"_play_prompt: could not parse the SSML prompt mpc=mpc_{qnr_run}")
                return "", err
            body.update(text=text, voice=voice, type="ssml")
    else:
        label, api, method = "PlayAudio", "play audio", "mpc_start_play_audio"
        ok = "Successfully played wait audio to the customer"
        body = {"url": prompt.url}
    terminal_proven = has_exact_mpc_end_proof(call_uuid, qnr_run, VoiceRedis())
    # The transport deliberately does not retry this mutation. Mark it before dispatch because a timeout/5xx
    # cannot prove whether Plivo applied it.
    mark_unsafe_callback_effect(db, method.replace("mpc_", "plivo_", 1))
    start = getattr(pv.PlivoVoiceClient(auth_id, auth_token), method)
    mpc_id = pv.make_mpc_id(friendly_name=f"mpc_{qnr_run}")
    _res, status, err = start(mpc_id, member_id, body, quiet_not_found=terminal_proven)
    if err is None:
        return ok, None
    if status == 404 and terminal_proven:
        logger.info(f"{label}: MPC already ended before prompt call_uuid={call_uuid} qnr_run_uuid={qnr_run}")
        return "MPC already ended", None
    logger.error(f"{label}: error response for {api} api: {err}")
    return "", voice_hangup.provider_voice_callback_error(err, status, retry_safe=False)


def calculate_estimated_wait_time_and_queue_position(
    db, queue_uuid: str, strategy: str, qnr_run_uuid: str
) -> tuple[int, int, int, str | None]:
    entries, in_queue_count = aq.get_last_n_active_queue_entries_for_queue_uuid(db, queue_uuid, qnr_run_uuid)
    queue_position = in_queue_count or 1
    if not entries:
        logger.info(f"No active queue entries found for last 30 mins queue_uuid={queue_uuid}")
        return 0, 0, queue_position, None
    durations = [(e.deleted_at - e.created_at).total_seconds() for e in entries if e.deleted_at is not None]
    if strategy in ("min", "max"):
        result = (min if strategy == "min" else max)(durations, default=0.0)
    elif strategy != "mean":
        return 0, 0, 0, "Estimated wait time is not supported for this strategy"
    elif not durations:
        return 0, 0, 0, "Cannot calculate average of mean and mode of an empty list"
    else:
        # the average of the mean and the mode: the first most frequent value, the maximum when none repeats
        value, count = collections.Counter(durations).most_common(1)[0]
        result = (sum(durations) / len(durations) + (value if count > 1 else max(durations))) / 2
    total_seconds = int(result) * queue_position
    return total_seconds // 60, total_seconds % 60, queue_position, None


def check_and_update_play_text(db, qnr_run_uuid: str, queue_uuid: str, node_uuid: str, text: str) -> str:
    """Fill the wait-time / queue-position placeholders. The estimate is recomputed every time; the cached
    copy it rewrites is never read back."""
    if not any(p in text for p in (*vc.WAIT_TIME_PATTERNS, vc.QUEUE_POSITION_VALUE)):
        return text
    placeholder = next((p for p in vc.WAIT_TIME_PATTERNS if p in text), vc.ESTIMATED_WAIT_TIME_VALUE)
    minutes, seconds, position, err = calculate_estimated_wait_time_and_queue_position(
        db, queue_uuid, _WAIT_STRATEGIES[placeholder], qnr_run_uuid
    )
    if err is not None:
        logger.error(f"Error while fetching estimated wait time for queue queue_uuid={queue_uuid}: {err}")
    estimate = {
        "estimated_wait_time_minutes": minutes,
        "estimated_wait_time_seconds": seconds,
        "queue_position": position,
    }
    RoutingRedis().set_queue_position_and_wait_time(
        node_uuid, {**estimate, "recompute": False}, vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
    )
    if minutes <= 0 and seconds <= 0:
        minutes, seconds = 1, 0
    if minutes and seconds:
        text = text.replace(placeholder, f"{minutes} minutes and {seconds} seconds")
    else:
        text = text.replace(placeholder, f"{minutes} minutes" if minutes else f"{seconds} seconds")
    if vc.QUEUE_POSITION_VALUE in text and position != 0:
        text = text.replace(vc.QUEUE_POSITION_VALUE, str(position))
    return text


def send_to_queue_exit(call_uuid: str, cp: dict, result: str, digit: str = "") -> tuple[str, str | None]:
    """Move the caller to the queue-exit continuation with this result; the leg's owner signs."""
    outcome = {"result": result, **({"digit": digit} if digit else {})}
    url = queue_exit_url(cp.get("auth_id", ""), call_uuid, cp.get("queue_and_route_run_uuid", ""), **outcome)
    increase_counter(MetricName.QNR_EXECUTION_RESULT, {"state": result})
    auth_id, auth_token = ar.contact_pair(cp)
    if not auth_id or not auth_token:
        return "", "error in creating plivo client"
    res, _status, err = pv.PlivoVoiceClient(auth_id, auth_token).call_update(
        call_uuid, {"legs": "aleg", "aleg_url": url}
    )
    if err is not None:
        logger.error(f"failed to send the call to the queue exit calluuid={call_uuid}: {err}")
        return "", err
    message = (res or {}).get("message", "")
    logger.info(f"Customer call sent to the queue exit {message} calluuid={call_uuid}")
    return message, None


def _publish_queued_after_commit(cp: dict) -> None:
    RoutingRedis().set_recompute_flag_for_queue_position_calculation(cp.get("node_uuid", ""), cp.get("queue_uuid", ""))


def preflight_route_and_add_to_mpc(
    call_uuid: str,
    qnr_run_uuid: str,
    is_transfer: bool,
    transfer_type: str,
    transfer_destination_id: str,
    member: str,
    db,
    primary_sip: str = "",
) -> tuple[dict | None, str | None]:
    """Resolve routing and durable queue prerequisites before provider effects. The call's own agent
    (`primary_sip`) is never offered it."""
    from hs.routing import get_agents as routing_get_agents

    rr = RoutingRedis()
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        logger.error(f"preflight_route_and_add_to_mpc: fail to get contact params contactUUid={call_uuid}")
        return None, "contact params not found"
    if member != "":
        return {"contact_params": dict(cp), "agents": [member], "terminal_noop": False}, None
    with time_phase("agent_selection"):
        agents, gerr = routing_get_agents.get_agents(
            call_uuid,
            rc.CHANNEL_CALL,
            cp.get("auth_id", ""),
            cp.get("conversation_number", ""),
            cp.get("flow_run_uuid", ""),
            cp.get("queue_and_route_run_uuid", ""),
            transfer_to=transfer_destination_id if is_transfer and transfer_type != rc.TRANSFER_TYPE_FLOW else None,
            transfer_type=transfer_type,
            contact_identifier=cp.get("from_number", ""),
            contact_params=cp,
            skip_agents=(primary_sip,) if primary_sip else (),
        )
    if gerr is not None:
        if gerr == "known_error: retry limit reached":
            logger.info(
                f"preflight_route_and_add_to_mpc: agent routing retries exhausted; returning assignment_failed "
                f"contactUUid={call_uuid}"
            )
        else:
            logger.error(f"preflight_route_and_add_to_mpc: Could not fetch agents contactUUid={call_uuid}: {gerr}")
        if gerr in _RETRYABLE_VOICE_AGENT_SELECTION_ERRORS:
            return None, voice_hangup.retryable_voice_callback_error("agent selection dependency unavailable")
        return None, gerr
    if agents:
        return {"contact_params": dict(cp), "agents": list(agents), "terminal_noop": False}, None
    effects = []
    result, error = {"contact_params": dict(cp), "agents": [], "terminal_noop": False}, None
    if is_transfer and transfer_type != rc.TRANSFER_TYPE_FLOW:
        logger.error(f"no available agents found contactUUID={call_uuid}")
        result, error = None, "no available agents found"
    else:
        queue_error = aq.update_queue_status(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_QUEUED, strict=True)
        if aq.is_no_live_queue_status(queue_error):
            logger.info(
                f"preflight_route_and_add_to_mpc: queue entry is no longer live; ignoring late callback contactUUID={call_uuid}"
            )
            result["terminal_noop"] = True
        elif queue_error is not None:
            logger.error(
                f"preflight_route_and_add_to_mpc: refusing to publish QUEUED before durable status update "
                f"contactUUID={call_uuid}: {queue_error}"
            )
            return None, f"{_RETRYABLE_QUEUE_PERSISTENCE_PREFIX} {queue_error}"
        else:
            effects.insert(0, (_publish_queued_after_commit, (dict(cp),)))
    for callback, args in effects:
        stage_error = stage_after_commit_effect(db, callback, *args)
        if stage_error is not None:
            return None, f"{_RETRYABLE_QUEUE_PERSISTENCE_PREFIX} {stage_error}"
    return result, error


def route_and_add_to_mpc(
    call_uuid: str,
    qnr_run_uuid: str,
    is_transfer: bool,
    transfer_type: str,
    transfer_destination_id: str,
    member: str,
    primary_sip: str,
    db,
    route_preflight: dict | None = None,
) -> tuple[str, str | None]:
    from hs.voice.agent_leg import add_agents_to_mpc

    if route_preflight is None:
        route = (call_uuid, qnr_run_uuid, is_transfer, transfer_type, transfer_destination_id, member, db)
        route_preflight, err = preflight_route_and_add_to_mpc(*route, primary_sip)
        if err is not None:
            soft = err == "no available agents found" or is_retryable(err)
            return ("" if soft else rc.QNR_STATE_ASSIGNMENT_FAILED), err
    agents = list(route_preflight.get("agents") or ())
    if route_preflight.get("terminal_noop"):
        return "", None
    if not agents:
        # No eligible agent is a normal queued outcome: the active-queue row and the NODE_UPDATED
        # event keep the call for reverse routing.
        logger.info(f"route_and_add_to_mpc: no agent currently eligible; call remains queued contact_uuid={call_uuid}")
        return "", None
    cp = route_preflight["contact_params"]
    try:
        with time_phase("add_agents_to_mpc"):
            # add-participant is non-idempotent with an ambiguous timeout/5xx outcome: any later
            # handler or commit failure must keep this callback's exact claim.
            mark_unsafe_callback_effect(db, "plivo_add_participant")
            add_agents_to_mpc(cp, agents, primary_sip, is_transfer, transfer_type, transfer_destination_id)
    except Exception as e:
        logger.error(
            f"route_and_add_to_mpc: Could not add agents to MPC, reducing the agent capacity "
            f"contactUUid={call_uuid} agents={agents}: {e}"
        )
        if not voice_hangup.is_retryable_voice_callback_error(str(e)):
            rr = RoutingRedis()
            for agent in agents:
                try:
                    rr.decrease_agents_capacity(
                        cp.get("auth_id", ""), agent, rc.CHANNEL_CALL, cp.get("conversation_number", "")
                    )
                except Exception as ce:
                    logger.error(f"route_and_add_to_mpc: Could not decrease agent capacity agent={agent}: {ce}")
        return rc.QNR_STATE_ASSIGNMENT_FAILED, str(e)
    return "", None


def _play_initial_queue_prompt(queue_config, cp: dict, member_id: str, call_uuid: str, db) -> None:
    if not queue_config.prompt:
        logger.info(f"_play_initial_queue_prompt: empty prompt list; skipping customer prompt call_uuid={call_uuid}")
        return
    _msg, prompt_error = _play_prompt(queue_config.prompt[0], cp, member_id, call_uuid, db)
    if prompt_error is not None:
        # Best effort once routing prerequisites succeeded: an ambiguous provider error is not replayed.
        logger.error(f"_play_initial_queue_prompt: initial prompt failed call_uuid={call_uuid}: {prompt_error}")
    if VoiceRedis().set_prompt_parameters(call_uuid, dict(_PROMPT_STATE)) is not None:
        logger.error(f"Could not set prompt params to cache queue_run_uuid={cp.get('queue_and_route_run_uuid', '')}")


def _handle_initial_routing_failure(
    call_uuid: str, qnr_run_uuid: str, cp: dict, flow_action: str, routing_error: str, db
) -> tuple[str, str | None]:
    """Persist and hand off a permanent initial-routing failure exactly once."""
    # The conversation updates, the flow hand-back and the conversation events do not share
    # the callback SQL transaction or a common idempotency key. Once this branch
    # starts, retain the exact callback claim even if the outer commit fails.
    mark_unsafe_callback_effect(db, "initial_routing_failure_handoff")
    try:
        active_queue_id = aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_FAILED)
    except Exception as qe:
        logger.error(f"Could not update queue status call_uuid={call_uuid}: {qe}")
        active_queue_id = 0
    _update_unanswered(call_uuid, rc.UNANSWERED_ASSIGNMENT_FAILED, active_queue_id)
    _msg, transfer_error = send_to_queue_exit(call_uuid, cp, flow_action)
    if transfer_error is not None:
        logger.error(
            f"_handle_initial_routing_failure: failed to transfer the call calluuid={call_uuid}: {transfer_error}"
        )
    return "", routing_error


@timed_phase("initialized_callback")
def handle_initialised_callback(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> tuple[str, str | None]:
    from hs.voice import outbound

    if outbound.dialed(call_uuid):
        return "outbound MPC: no queue to route", None
    rr, vr = RoutingRedis(), VoiceRedis()
    mpc_start_time = params.get("mpc_start_time", "") or params.get("mpc_creation_time", "")
    val, err = vr.get_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_EVENT)
    if err is None and val != "":
        logger.error(f"handle_initialised_callback: MPC is already ended CallUUID={call_uuid}")
        return "", "mpc is already ended"
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        logger.error("error in fetching call parameters from cache")
        return "", "contact params not found"
    queue_config, qerr = rr.get_queue_config(cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", ""))
    if qerr is not None:
        logger.error(f"queue config not found flowRunUUID={cp.get('flow_run_uuid', '')}")
        return "", qerr
    if queue_config.prompt and queue_config.prompt[0].prompt_type not in ("speak", "play"):
        # Validate configuration before capacity selection/reservation.
        return _handle_initial_routing_failure(
            call_uuid, qnr_run_uuid, cp, rc.QNR_STATE_ASSIGNMENT_FAILED, "invalid prompt type", db
        )
    if vr.toggle_active_conference(qnr_run_uuid, "true", queue_config.queue_call_timeout) is not None:
        logger.error(f"handle_initialised_callback: error in setting active conference call_uuid={call_uuid}")
    try:
        transfer_type = (rr.get_transfer_in_progress_info(call_uuid) or {}).get("transfer_type", "")
    except Exception:
        transfer_type = ""
    is_transfer = transfer_type != ""
    route_preflight, preflight_error = preflight_route_and_add_to_mpc(
        call_uuid, qnr_run_uuid, is_transfer, transfer_type, "", "", db
    )
    if preflight_error is not None:
        # All replayable selection/queue prerequisites run before prompt/add
        # mutations, so an ACW/DB retry cannot duplicate customer audio.
        if is_retryable(preflight_error):
            return "", preflight_error
        preflight_action = "" if preflight_error == "no available agents found" else rc.QNR_STATE_ASSIGNMENT_FAILED
        return _handle_initial_routing_failure(call_uuid, qnr_run_uuid, cp, preflight_action, preflight_error, db)
    if route_preflight.get("terminal_noop"):
        return "Initialised MPC", None
    _spawn(_create_call_leg_seam, "", call_uuid, "", mpc_start_time, "", "", True)
    member_id = params.get("member_id", "")
    if route_preflight.get("agents"):
        _play_initial_queue_prompt(queue_config, cp, member_id, call_uuid, db)
    else:
        # QUEUED must commit before any customer prompt mutation. A writer
        # failure can then release/retry the callback without duplicate audio
        # or a missing durable queue row.
        prompt = (queue_config, dict(cp), member_id, call_uuid, db)
        stage_error = stage_after_commit_effect(db, _play_initial_queue_prompt, *prompt)
        if stage_error is not None:
            return "", f"{_RETRYABLE_QUEUE_PERSISTENCE_PREFIX} {stage_error}"
    route = (call_uuid, qnr_run_uuid, is_transfer, transfer_type, "", "", "", db)
    res, rerr = route_and_add_to_mpc(*route, route_preflight=route_preflight)
    if is_retryable(rerr):
        # The callback must be retried: failing the live queue row, publishing assignment_failed or
        # transferring the customer away waits until the transient dependency recovers.
        return "", rerr
    if rerr is not None:
        return _handle_initial_routing_failure(call_uuid, qnr_run_uuid, cp, res, rerr, db)
    return "Initialised MPC", None


def _update_unanswered(contact_uuid: str, reason: str, active_queue_id) -> None:
    from hs.routing.invitation import _update_unanswered_inprocess

    _update_unanswered_inprocess(contact_uuid, reason, active_queue_id=active_queue_id)


def handle_prompt_stopped(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> tuple[str, str | None]:
    """A hold prompt finished: play the next one. The main prompts play in order (each repeated `loops`
    times), then the loop prompts cycle until an agent joins."""
    member_id = params.get("member_id", "")
    if member_id == "all":
        return "", None
    rr, vr = RoutingRedis(), VoiceRedis()
    is_active, err = vr.is_conference_active(qnr_run_uuid)
    if err is not None:
        logger.error(f"error in fetching IsConferenceActive from cache: {err}")
        return "", voice_hangup.retryable_voice_callback_error("conference state cache unavailable")
    if not is_active:
        return "", None
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        return "", "contact params not found"
    queue_config, qerr = rr.get_queue_config(cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", ""))
    if qerr is not None:
        return "", qerr
    state, perr = vr.get_prompt_parameters(call_uuid)
    if perr is not None:
        return "", "prompt parameters not found"
    prompts = queue_config.prompt
    if not prompts:
        logger.info("Length of prompt 0")
        return "", None
    loop_prompts = [p for p in prompts if p.loop_with_others]
    number, complete, loop, repeat = ({**_PROMPT_STATE, **state}[k] for k in _PROMPT_STATE)
    to_play = None
    if complete and loop_prompts:
        if loop > len(loop_prompts) - 1:
            return "", "invalid loop count"
        if loop_prompts[loop].loops > 0 and repeat < loop_prompts[loop].loops:
            nxt, repeat = loop, repeat + 1
        else:
            nxt, repeat = (loop + 1 if loop < len(loop_prompts) - 1 else 0), 0
        to_play, number, loop = loop_prompts[nxt], 0, nxt
    elif not complete:
        if number > len(prompts) - 1:
            return "", "invalid loop count"
        if prompts[number].loops > 0 and repeat < prompts[number].loops:
            to_play, repeat = prompts[number], repeat + 1
        elif number < len(prompts) - 1:
            to_play, number, repeat = prompts[number + 1], number + 1, 0
        elif loop_prompts:
            to_play, number, complete, repeat = loop_prompts[0], number + 1, True, 0
        else:
            return "", "completed playing all prompts, no loop prompts"
        loop = 0
    if to_play is None or to_play.prompt_type not in ("speak", "play"):
        return "", "invalid prompt type"
    _msg, serr = _play_prompt(to_play, cp, member_id, call_uuid, db)
    if serr is not None:
        return "", serr
    if vr.set_prompt_parameters(call_uuid, dict(zip(_PROMPT_STATE, (number, complete, loop, repeat)))) is not None:
        logger.error("Could not set prompt params to cache")
        return "", "could not set prompt params to cache"
    return "OK", None


def handle_dial_input(call_uuid: str, qnr_run_uuid: str, callback_params: dict, db) -> tuple[str, str | None]:
    if callback_params.get("participant_role", "") != vc.MPC_ROLE_CUSTOMER:
        return "input for agent received", None
    rr, vr = RoutingRedis(), VoiceRedis()
    is_active, err = vr.is_conference_active(qnr_run_uuid)
    if err is not None:
        logger.error(f"error in fetching IsConferenceActive from cache: {err}")
        return "", voice_hangup.retryable_voice_callback_error("conference state cache unavailable")
    if not is_active:
        return "input for agent received", None
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        logger.error("error in fetching call parameters from cache")
        return "", "contact params not found"
    queue_config, qerr = rr.get_queue_config(cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", ""))
    if qerr is not None:
        logger.error(f"queue config not found flowRunUUID={cp.get('flow_run_uuid', '')}")
        return "", qerr
    digit = callback_params.get("digit_input", "")
    logger.info(f"Digit entered={digit} Contact uuid={call_uuid}")
    if digit not in (queue_config.queue_call_allowed_inputs or []):
        return "Entered invalid digit", None
    try:
        active_queue_id = aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_EXIT)
    except Exception as qe:
        logger.error(f"handle_dial_input: Could not update queue status call UUID={call_uuid}: {qe}")
        return "", voice_hangup.retryable_voice_callback_error("unable to update queue status")
    mark_unsafe_callback_effect(db, "digit_input_external_effects")
    # Only after the durable row changed: a transient DB failure must not hide behind an inactive-conference no-op.
    vr.toggle_active_conference(qnr_run_uuid, "false", 0)
    logger.info(f"Updating the unanswered conversation call_uuid={call_uuid}")
    _update_unanswered(call_uuid, rc.UNANSWERED_QUEUE_EXIT, active_queue_id)
    contact_uuid = cp.get("contact_uuid", "")
    res, terr = send_to_queue_exit(contact_uuid, cp, rc.QNR_STATE_QUEUE_EXIT, digit)
    if terr is not None:
        logger.error(f"call transfer failed calluuid={contact_uuid}: {terr}")
        return "", terr
    client = pv.PlivoVoiceClient(*ar.contact_pair(cp))
    for call_id in vr.get_mpc_call_uuids(cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", "")):
        _b, _s, derr = client.call_delete(call_id)
        if derr is not None:
            logger.error(f"deleting mpc calls failed call id={call_id}: {derr}")
    logger.info(f"call transfer successful {res} calluuid={contact_uuid}")
    return res, None


def handle_wait_timeout(contact_uuid: str, qnr_run_uuid: str, db) -> tuple[str, str | None]:
    VoiceRedis().toggle_active_conference(qnr_run_uuid, "false", 0)
    cp = RoutingRedis().get_contact_parameters(contact_uuid, qnr_run_uuid)
    if cp is None:
        logger.error(f"Could not fetch contact params Call Uuid={contact_uuid}")
        return "", "contact params not found"
    logger.info(f"Customer wait timeout ended, so deleting active queue entry Contact uuid={contact_uuid}")
    active_queue_id = 0
    try:
        active_queue_id = aq.delete_active_queue_entry(db, contact_uuid, qnr_run_uuid, rc.CONTACT_STATE_TIMEDOUT)
    except Exception as e:
        logger.error(f"unable to update queue status Call Uuid={contact_uuid}: {e}")
    _update_unanswered(contact_uuid, rc.UNANSWERED_QUEUE_TIMEOUT, active_queue_id)
    return "Successfully handled wait timeout event", None


def handle_mpc_start(call_uuid: str, qnr_run_uuid: str) -> tuple[str, str | None]:
    vr = VoiceRedis()
    try:
        if vr.toggle_active_conference(qnr_run_uuid, "false", 0) is not None:
            logger.error(f"handle_mpc_start: error in resetting active conference call_uuid={call_uuid}")
        val, err = vr.get_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_EVENT)
        if err is None and val != "":
            logger.error(f"handle_mpc_start: MPC is already ended CallUUID={call_uuid}")
            _end_mpc_calls_seam(call_uuid, qnr_run_uuid)
    except Exception as e:
        logger.error(f"handle_mpc_start: failed call UUID={call_uuid}: {e}")
    return "Successfully updated conversation", None


def handle_missed_mpc_event(missed_req: dict, db, claimed_event_out: list | None = None) -> str | None:
    event_name, call_uuid, qnr_run_uuid = (missed_req.get(k, "") for k in ("eventName", "callUUID", "qnrRunUUID"))
    if event_name != vc.MPC_INITIALISED_CALLBACK_EVENT:
        logger.info(f"Ignoring a missed MPC event timeout for event={event_name} callUUID={call_uuid}")
        return None
    customer, err = _get_customer_details_from_mpc(call_uuid, qnr_run_uuid)
    if err == "mpc already absent":
        logger.info(
            f"missed MPC event timer arrived after exact terminal teardown; ignoring call_uuid={call_uuid} "
            f"qnr_run_uuid={qnr_run_uuid}"
        )
        return None
    if err is not None:
        logger.error(f"could not fetch customer member from mpc callUUID={call_uuid}: {err}")
        return err
    details = EventDetails(event_name=vc.MPC_INITIALISED_CALLBACK_EVENT, dependent_event=vc.MPC_END_EVENT)
    lua_err = update_voice_event_time_in_cache(call_uuid, qnr_run_uuid, details)
    if lua_err is not None:
        duplicate = lua_err in (vc.VALUE_EXISTS_ERROR, vc.LATER_EVENT_FOUND_ERROR)
        (logger.info if duplicate else logger.error)(
            "could not claim missed MPC event because it was already processed, terminal, or Redis failed "
            f"callUUID={call_uuid}: {lua_err}"
        )
        return None if duplicate else lua_err
    if claimed_event_out is not None:
        claimed_event_out.append(details)
    # The customer's member id is the one field of the callback the initialised handler reads (its prompt).
    callback_params = {"member_id": str(customer.get("member_id", ""))}
    _msg, herr = handle_initialised_callback(call_uuid, qnr_run_uuid, callback_params, db)
    if herr is not None:
        logger.error(f"Error while mimicking the MPCInitialized Event after timeout callUUID={call_uuid}: {herr}")
        if claimed_event_out is None:
            release_voice_event_claim(call_uuid, qnr_run_uuid, details)
        return herr
    logger.info(f"MPC Initialized event successfully mimicked after timeout callUUID={call_uuid}")
    return None


def _get_customer_details_from_mpc(call_uuid: str, qnr_run_uuid: str) -> tuple[dict | None, str | None]:
    rr = RoutingRedis()
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        return None, "error in fetching call parameters from cache"
    client = pv.PlivoVoiceClient(*ar.contact_pair(cp))
    terminal_proven = has_exact_mpc_end_proof(call_uuid, qnr_run_uuid, VoiceRedis())
    mpc_id = pv.make_mpc_id(friendly_name=f"mpc_{qnr_run_uuid}")
    body, status, err = client.mpc_list_participants(mpc_id, quiet_not_found=terminal_proven)
    if err is not None:
        if status == 404 and terminal_proven:
            return None, "mpc already absent"
        return None, voice_hangup.provider_voice_callback_error(err, status, retry_safe=True)
    for participant in (body or {}).get("objects", []) or []:
        if str(participant.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER and not participant.get("exit_cause"):
            return participant, None
    return None, "customer member not found in mpc"
