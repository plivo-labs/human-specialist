from hs.conversations.store import ActiveQueueEntry
from hs.infra.log import logger
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis


def _conference_active_unbuilt(qnr_run_uuid: str) -> bool:
    from hs.voice.redis_store import VoiceRedis

    return VoiceRedis().is_conference_active(qnr_run_uuid)[0]


def _is_known_routing_outcome(err: str) -> bool:
    # "known_error: ..." marks an EXPECTED outcome (empty queue when an agent comes online, no valid contact
    # after retries): logged once at origin as INFO, never re-logged by the layers it passes through
    return bool(err) and err.startswith("known_error")


def get_contact(
    agent: str,
    channel: list[str],
    db,
    routing_redis: RoutingRedis | None = None,
    is_conference_active=_conference_active_unbuilt,
) -> tuple[ActiveQueueEntry | None, str | None]:
    rr = routing_redis or RoutingRedis()
    if not ar.check_agent_availability(agent):
        logger.error(f"get_contact: agent is not available agent={agent}")
        return None, "agent is not available"
    teams = ar.get_agent_teams(agent, "sip")
    if teams is None:
        logger.error(f"get_contact: no teams found for agent={agent}")
        return None, "error fetching agent teams"
    aom_uuid = ar.get_aom_for_sip_endpoint(agent)
    if aom_uuid is None:
        logger.error(f"get_contact: no agent found for the SIP agent={agent}")
        return None, "no agent found for the SIP"
    skills, skip_contacts = ar.get_agent_skills(agent) or [], []
    for _attempt in range(3):
        contact, err = aq.get_best_contact(db, teams, skills, channel, skip_contacts, aom_uuid)
        if err is not None:
            log = logger.info if _is_known_routing_outcome(err) else logger.error
            log(f"get_contact: no queued contact found agent={agent}: {err}")
            return None, err
        skip_contacts.append(contact.contact_uuid)
        valid = aq.check_if_valid_contact(db, contact)
        # a contact whose params expired can never be offered: it goes back to the queue, not out as an error
        contact_params = rr.get_contact_parameters(contact.contact_uuid, contact.qnr_run_uuid)
        if (
            valid
            and contact_params is not None
            and agent not in rr.get_invited_agents(str(contact.flow_run_uuid), contact.qnr_run_uuid)
        ):
            if contact.queue_channel != rc.CHANNEL_CALL:
                return contact, None
            # a call is handed over only while its conference is still up
            if is_conference_active(contact.qnr_run_uuid):
                logger.info(f"get_contact: active conference contact={contact.contact_uuid}")
                return contact, None
        aq.update_queue_status(db, contact.contact_uuid, contact.qnr_run_uuid, rc.CONTACT_STATE_QUEUED)
        rr.set_recompute_flag_for_queue_position_calculation(
            (contact_params or {}).get("node_uuid", ""), (contact_params or {}).get("queue_uuid", "")
        )
    logger.info(f"get_contact: no valid contact found agent={agent}")
    return None, "known_error: no valid contact found"
