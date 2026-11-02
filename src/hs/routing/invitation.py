import threading
import uuid

from hs.conversations.models import ConversationUpdateByContactUUID
from hs.infra.log import logger
from hs.routing import active_queue as aq
from hs.routing import constants as rc


def _ensure_request_context():
    from hs.infra.context import context_api_id, context_req_meta

    if context_req_meta.get() is None:
        api_id = context_api_id.get(None)
        if not api_id or api_id == "default":
            api_id = str(uuid.uuid4())
            context_api_id.set(api_id)
        context_req_meta.set({"api_id": api_id})


def _child_update(contact_uuid, child_conversation_id, **fields):
    from hs.conversations import child_update as ccu

    _ensure_request_context()
    child_id = int(child_conversation_id) if child_conversation_id else 0
    update = ConversationUpdateByContactUUID(contact_uuid=contact_uuid, child_conversation_id=child_id, **fields)
    return ccu.update_child_conversation(update)


def _update_unanswered_inprocess(contact_uuid, reason, child_conversation_id="", active_queue_id=0) -> None:
    try:
        resp = _child_update(
            contact_uuid, child_conversation_id, reason_for_unanswered=reason, active_queue_id=active_queue_id
        )
        if resp.status_code != 200:
            logger.error(
                f"update_unanswered non-200 ({resp.status_code}) contact_uuid={contact_uuid} "
                f"reason={reason} error={resp.error}"
            )
    except Exception as e:
        logger.error(f"update_unanswered in-process failed contact_uuid={contact_uuid} reason={reason}: {e}")


def _reverse_route_freed_agent(agent_sip, channel, conversation_id) -> None:
    """Release the freed agent's slot for conversation_id and offer them the next queued call. Runs on its
    own thread; the voice reverse routing opens its own DB session."""
    from hs.routing.capacity import release_reservation_best_effort
    from hs.routing.redis_store import RoutingRedis
    from hs.voice import hangup as voice_hangup

    where = f"agent_sip={agent_sip} channel={channel} conv={conversation_id}"
    try:
        _res, err = voice_hangup.handle_reverse_routing_and_agent_capacity(agent_sip, channel, conversation_id)
        if err is not None:
            logger.error(f"reverse-route freed {where}: {err}")
            if err == voice_hangup.RRLOCK_ACQUIRE_FAILED:
                # the one error where nothing was released; free the slot so it cannot strand
                release_reservation_best_effort(RoutingRedis(), agent_sip, channel, conversation_id)
    except Exception as e:
        logger.error(f"reverse-route freed {where}: {e}")
        try:
            release_reservation_best_effort(RoutingRedis(), agent_sip, channel, conversation_id)
        except Exception as e2:
            logger.error(f"fallback release failed agent_sip={agent_sip} conv={conversation_id}: {e2}")


def _handle_reverse_routing_inprocess(agent_sip, channel, conversation_id) -> str | None:
    # A decline/timeout/drain frees the agent: release their slot for THIS conversation (the agent-online
    # variant drops conversation_id and strands the reservation) on a daemon thread, off the request.
    if agent_sip:
        target, args = _reverse_route_freed_agent, (agent_sip, channel, conversation_id)
        threading.Thread(target=target, args=args, name=f"reverse-route-{agent_sip}", daemon=True).start()
    return None


def invite_agents_to_channel(params, db, routing_redis) -> tuple[str, str, str | None]:
    run = (params.contact_uuid, params.queue_and_route_run_uuid)
    if params.contact_channel != rc.CHANNEL_CALL:
        aq.delete_active_queue_entry(db, *run, rc.CONTACT_STATE_FAILED)
        return "", rc.QNR_STATE_ASSIGNMENT_FAILED, "invalid Contact Channel"
    from hs.voice import intake as voice_intake

    try:
        return voice_intake.handle_incoming_call(params) or "", "", None
    except Exception as e:
        logger.error(f"Error in handling incoming call contact_uuid={params.contact_uuid} msg= : {e}")
        try:
            aq.delete_active_queue_entry(db, *run, rc.CONTACT_STATE_FAILED)
        except Exception as de:
            logger.error(
                f"handle incoming call: Could not delete active queue entry contact_uuid={params.contact_uuid}: {de}"
            )
            return "", rc.QNR_STATE_ENQUEUE_FAILED, str(de)
        return "", rc.QNR_STATE_ENQUEUE_FAILED, None
