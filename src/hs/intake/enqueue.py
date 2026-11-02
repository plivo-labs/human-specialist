import json

from hs import jobs
from hs.conversations.models import ConvInsertModel
from hs.conversations.store import ActiveQueueEntry, emit_by_number
from hs.infra.db import SessionLocal
from hs.infra.helpers import is_valid_uuid
from hs.infra.log import logger
from hs.intake.models import ContactParameters, IncomingContactParams
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing.invitation import _ensure_request_context, invite_agents_to_channel
from hs.routing.redis_store import RoutingRedis
from hs.voice.phase_metrics import timed_phase

_CONTACT_PARAM_FIELDS = [
    "conversation_number",
    "flow_uuid",
    "flow_run_uuid",
    "contact_channel",
    "contact_uuid",
    "auth_id",
    "queue_and_route_run_uuid",
    "to",
    "flow_name",
    "node_uuid",
    "inbound",
]
_CREATE_FAILED = "child conversation creation failed"
_CHILD_FIELDS = [
    "generation_type",
    "parent_contact_uuid",
    "active_queue_entry_id",
    "sender_id",
    "contact_uuid",
    "channel",
]


class _EnqueueRefused(Exception):
    def __init__(self, err: str):
        super().__init__(err)
        self.err = err


def _refuse_on(err: str | None) -> None:
    if err is not None:
        raise _EnqueueRefused(err)


def check_queue_capacity(params: IncomingContactParams, db) -> str | None:
    queue_uuid, max_capacity = params.queue_and_route_config.queue, params.queue_config.queue_capacity
    try:
        current = aq.get_queue_capacity(db, queue_uuid, params.contact_channel)
    except Exception as e:
        logger.error(f"check_queue_capacity error queue_uuid={queue_uuid}: {e}")
        return "unable to fetch queue capacity"
    if current >= max_capacity:
        logger.error(f"queue capacity exceeded queue_uuid={queue_uuid} current={current} max={max_capacity}")
        return "queue capacity exceeded"
    return None


def set_queue_props_in_cache(params: IncomingContactParams, routing_redis: RoutingRedis) -> str | None:
    ttl = rc.get_cache_duration_for_contact_channel_session_flow(params.contact_channel) * 3600
    qarc, run = params.queue_and_route_config, (params.flow_run_uuid, params.queue_and_route_run_uuid)
    if qarc.routing == rc.SIMULTANEOUS_ROUTING:
        qarc.retry_limit = 0
    try:
        if routing_redis.set_queue_and_route_config(*run, qarc.to_compact_json(), ttl) is False:
            return "Could not save queue and route"
        if routing_redis.set_queue_config(*run, params.queue_config.to_compact_json(), ttl) is False:
            return "Could not save queue"
    except Exception as e:
        logger.error(f"set_queue_props_in_cache error contact_uuid={params.contact_uuid}: {e}")
        return "could not save queue properties"
    return None


def create_active_queue_entry(params: IncomingContactParams, db) -> tuple[int, str | None]:
    qarc, qc = params.queue_and_route_config, params.queue_config
    entry = ActiveQueueEntry(
        queue_priority=qc.queue_priority,
        queue_uuid=qarc.queue,
        contact_uuid=params.contact_uuid,
        flow_run_uuid=params.flow_run_uuid,
        queue_type=qc.dequeue_order,
        queue_channel=params.contact_channel,
        contact_phone_or_email=params.from_,
        qnr_run_uuid=params.queue_and_route_run_uuid,
        status=rc.CONTACT_STATE_IN_TRANSIT,
        auth_id=params.auth_id,
    )
    skills = [(sf.skill, sf.operator, sf.value) for sf in qarc.routing_skills_filter]
    try:
        active_queue_id = aq.create_active_queue_entry(
            db, entry, teams=list(qarc.routing_teams), skills_filter=skills, agents=list(qarc.final_routing_agents)
        )
    except Exception as e:
        logger.error(f"create_active_queue_entry error contact_uuid={params.contact_uuid}: {e}")
        return 0, "could not save queue entry"
    return active_queue_id, None


def store_contact_params_cache(
    params: IncomingContactParams, auth_token: str, routing_redis: RoutingRedis
) -> str | None:
    contact_params = ContactParameters(
        **{field: getattr(params, field) for field in _CONTACT_PARAM_FIELDS},
        queue_uuid=params.queue_and_route_config.queue,
        from_number=params.from_,
        auth_token=auth_token,
    )
    try:
        routing_redis.store_contact_params(
            params.contact_uuid, params.queue_and_route_run_uuid, contact_params.to_compact_json()
        )
    except Exception as e:
        logger.error(f"store_contact_params_cache error contact_uuid={params.contact_uuid}: {e}")
        return "unable to store contact params in cache"
    return None


def _build_child_payload(params: IncomingContactParams, active_queue_id: int, generation_type: str) -> dict:
    payload = {
        "generation_type": generation_type,
        "parent_contact_uuid": params.parent_contact_uuid,
        "active_queue_entry_id": active_queue_id,
        "sender_id": params.to,
    }
    if params.parent_contact_uuid != params.contact_uuid and params.contact_uuid:
        payload["contact_uuid"] = params.contact_uuid
    if params.contact_channel:
        payload["channel"] = params.contact_channel
    return payload


def create_qnr_child_conversation(payload: dict):
    from hs.conversations import transfer as cxfer

    try:
        _ensure_request_context()
        conv = ConvInsertModel(**{field: payload.get(field) for field in _CHILD_FIELDS})
        resp = cxfer.create_child_conversation(conv)
        if resp.error or (resp.status_code or 0) != 201:
            status = f"{resp.status_code} {resp.error}"
            logger.error(f"create_qnr_child_conversation: in-process create returned {status}")
            return _CREATE_FAILED
        return None
    except Exception:
        logger.error("create_qnr_child_conversation: in-process create failed", exc_info=True)
        return _CREATE_FAILED


def _resolve_routing_targets(params: IncomingContactParams) -> None:
    qarc = params.queue_and_route_config
    if qarc.routing_agents:
        emails = [a for a in qarc.routing_agents if not is_valid_uuid(a)]
        uuids = [a for a in qarc.routing_agents if is_valid_uuid(a)]
        final, err = ar.get_agent_details_for_email_or_aom_id(emails, uuids, params.auth_id)
        _refuse_on(err)
        qarc.final_routing_agents = final or []


@timed_phase("incoming_contact")
def handle_incoming_contact(params: IncomingContactParams) -> tuple[str, str, int, str | None]:
    rr, db, active_queue_id = RoutingRedis(), SessionLocal(), 0
    try:
        _refuse_on(check_queue_capacity(params, db))
        _resolve_routing_targets(params)
        _refuse_on(set_queue_props_in_cache(params, rr))
        active_queue_id, err = create_active_queue_entry(params, db)
        _refuse_on(err)
        db.commit()
        auth_token, _record, err = ar.get_organisation_auth_token_and_record_value(params.auth_id, True)
        _refuse_on(err)
        _refuse_on(store_contact_params_cache(params, auth_token, rr))
        try:
            rr.delete_contact_to_agent_mapping(params.contact_uuid)
        except Exception as e:
            logger.error(f"delete_contact_to_agent_mapping error contact_uuid={params.contact_uuid}: {e}")
            raise _EnqueueRefused("unable to delete contact to agent mapping")
        try:
            if params.is_queue_transfer:
                info = json.dumps({"transfer_type": rc.TRANSFER_TYPE_FLOW}, separators=(",", ":"))
                rr.set_transfer_in_progress(params.contact_uuid, info, rc.TRANSFER_KEY_TTL_SECONDS)
            else:
                rr.delete_transfer_in_progress(params.contact_uuid)
        except Exception as e:
            verb = "set" if params.is_queue_transfer else "delete"
            logger.error(f"{verb}_transfer_in_progress error contact_uuid={params.contact_uuid}: {e}")
        generation_type = "transfer_qnr_generated" if params.is_queue_transfer else "qnr_generated"
        _refuse_on(create_qnr_child_conversation(_build_child_payload(params, active_queue_id, generation_type)))
        queued = {
            "queue_id": params.queue_and_route_config.queue,
            "from": params.from_ or None,
            "to": params.to or None,
        }
        emit_by_number("conversation.queued", params.conversation_number, queued)
        msg, state, invite_err = invite_agents_to_channel(params, db, rr)
        db.commit()
        return msg, state, active_queue_id, invite_err
    except _EnqueueRefused as e:
        if active_queue_id:  # committed before the refusal: a job closes it, retried until it does
            jobs.schedule("close_refused_entry", {"run": [params.contact_uuid, params.queue_and_route_run_uuid]})
        return "", rc.QNR_STATE_ENQUEUE_FAILED, 0, e.err
    finally:
        db.close()


@jobs.handler("close_refused_entry")
def close_refused_entry(job: dict) -> None:
    with SessionLocal.begin() as db:
        aq.delete_active_queue_entry(db, *job["run"], rc.CONTACT_STATE_FAILED)
