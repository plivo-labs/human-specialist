"""The inbound call path: a call's record, its conversation, consent, the actions it runs, enqueue and overflow."""

import json
import time
import uuid

from pydantic import TypeAdapter
from sqlalchemy import text

from hs import events
from hs.access import Principal, tenant_plivo
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.redis import Cache
from hs.intake.enqueue import handle_incoming_contact
from hs.intake.models import (
    AutoAcceptCallConfig,
    HoldMusic,
    IncomingContactParams,
    QueueAndRouteConfigModel,
    QueueConfigModel,
    SkillFilterModel,
)
from hs.routing import constants as rc
from hs.setup import actions as act
from hs.setup import api as setup

CALL_TTL = 6 * 3600
ACTION = TypeAdapter(act.Action)
HANGUP = {"type": "hangup"}
CONTEXT_MAX = 2048
# Overflow reason -> the call's result, should it end without another one (the others are their own).
RESULTS = {"wait_timeout": "queue_timeout", "dtmf": "queue_exit"}
STICKY = {"loose": rc.STICKY_LOOSELY_BIND, "strict": rc.STICKY_STRICTLY_BIND}
_CONSENT = text("SELECT voice_settings->'consent'->CAST(:direction AS text) FROM org_setting WHERE organization = :org")
_SET_CONTEXT = text(
    "UPDATE customer_conversation_data SET conversation_custom_data = CAST(:ctx AS json)"
    " WHERE conversation_parent_id = (SELECT id FROM conversation_parent WHERE uuid = CAST(:cid AS uuid))"
    " AND COALESCE(json_typeof(conversation_custom_data), 'null') = 'null'"
)


def redis():
    return Cache.get_instance().client


def e164(number: str) -> str:
    """Plivo sends PSTN numbers without the "+"."""
    return f"+{number}" if number.isdigit() else number


def load(call_uuid: str) -> dict:
    """What HS keeps about a call it answered or enqueued ({} when it has never seen it)."""
    return redis().hgetall(f"CALL:{call_uuid}")


def load_for(p: Principal, call_uuid: str, form: dict | None = None) -> dict:
    """`load` for a request Plivo signed for this tenant: {} unless the call is the tenant's own (and, given the
    form, the one the request is about)."""
    call = load(call_uuid)
    if (
        call
        and call.get("auth_id") == tenant_plivo(p.tenant_id)[0]
        and (form is None or form.get("CallUUID") == call_uuid)
    ):
        return call
    return {}


def take_move(call_uuid: str) -> str | None:
    """The action an agent moved the caller to, once: a replay finds nothing."""
    return redis().pipeline().hget(f"CALL:{call_uuid}", "move").hdel(f"CALL:{call_uuid}", "move").execute()[0]


def save(call_uuid: str, /, **fields) -> None:
    redis().pipeline().hset(f"CALL:{call_uuid}", mapping=fields).expire(f"CALL:{call_uuid}", CALL_TTL).execute()


def once(key: str, build) -> str:
    """`build()` for the first request with this key; a repeat gets the same answer (Plivo may retry)."""
    if redis().set(key, "", nx=True, ex=CALL_TTL):
        try:
            answer = build()
        except Exception:
            redis().delete(key)
            raise
        redis().set(key, answer, ex=CALL_TTL)
        return answer
    deadline = time.monotonic() + 10
    while not (answer := redis().get(key)) and time.monotonic() < deadline:
        time.sleep(0.05)
    return answer or response("<Hangup/>")


def response(*parts: str) -> str:
    return "<Response>" + "".join(parts) + "</Response>"


def context_of(raw: str | None) -> dict | None:
    """The `context` an integrator attached: a JSON object of at most 2 KB, without NUL (jsonb cannot hold it)."""
    if not raw:
        return None
    if len(raw.encode()) > CONTEXT_MAX:
        raise ApiError(422, "context_too_large", "context is limited to 2 KB")
    try:
        context = json.loads(raw)
    except ValueError as e:
        raise ApiError(422, "context_invalid", "context is not JSON") from e
    if not isinstance(context, dict) or "\\u0000" in raw or "\0" in raw:
        raise ApiError(422, "context_invalid", "context must be a JSON object without NUL characters")
    return context


def open_call(
    p: Principal, form: dict, context: dict | None = None, entry_point_id: str | None = None, hours: str = "none"
) -> dict:
    """The call's record, opening its conversation first when HS has not seen the call (as the answer URL does,
    or an enqueue/menu URL the tenant's own app redirected to). A new context is stored once per conversation."""
    from hs.conversations import management as cm
    from hs.conversations.models import ConversationDataDict, ConvParentInsertModel
    from hs.conversations.store import ConversationParent
    from hs.routing.invitation import _ensure_request_context

    call_uuid, auth_id = form["CallUUID"], tenant_plivo(p.tenant_id)[0]
    if call := load(call_uuid):
        if call.get("auth_id") != auth_id:
            raise ApiError(409, "call_not_live", "no such live call on this account")
        if context:
            with core_engine.begin() as conn:
                conn.execute(_SET_CONTEXT, {"ctx": json.dumps(context), "cid": call["conversation_id"]})
        return call
    # Outbound: the tenant's own app placed the call, so the customer is the number it dialled.
    inbound = form.get("Direction") != "outbound"
    caller, number, run_id = (
        e164(form["From" if inbound else "To"]),
        e164(form["To" if inbound else "From"]),
        str(uuid.uuid4()),
    )
    _ensure_request_context()
    opened = cm.create_parent_conversation(
        ConvParentInsertModel(
            parent_channel="call",
            phone_or_email=caller,
            parent_contact_uuid=call_uuid,
            org_auth_id=auth_id,
            sender_id=number,
            flow_run_uuid=run_id,
            direction_inbound=inbound,
            # an outbound conversation needs its agent or the request that placed the call: here, the call itself
            http_request_uuid=None if inbound else call_uuid,
            conversation_data=ConversationDataDict(conversation_custom_data=context) if context else None,
        )
    )
    if opened.error:
        raise RuntimeError(f"conversation open failed for {call_uuid}: {opened.error}")
    parent = ConversationParent.get_by_contact_uuid(call_uuid)
    call = {
        "call_uuid": call_uuid,
        "auth_id": auth_id,
        "conversation_id": str(parent.uuid),
        "conversation_number": str(parent.conversation_number),
        "run_id": run_id,
        "from": caller,
        "to": number,
        "direction": "inbound" if inbound else "outbound",
    }
    save(call_uuid, **call)
    emit("conversation.started", p, call, number=number, entry_point_id=entry_point_id, hours=hours)
    return call


def emit(type_: str, p: Principal, call: dict, agent_id: str | None = None, **data) -> None:
    events.emit(type_, tenant_id=p.tenant_id, conversation_id=call["conversation_id"], agent_id=agent_id, data=data)


def consent(p: Principal, call: dict, played: bool = False) -> str:
    """The tenant's inbound notice, the first time any HS response handles this call; `played`: the tenant's
    own app already played it."""
    if not redis().set(f"CONSENT:{call['call_uuid']}", 1, nx=True, ex=CALL_TTL) or played:
        return ""
    with core_engine.connect() as conn:
        notice = conn.execute(_CONSENT, {"org": p.org_id, "direction": call.get("direction", "inbound")}).scalar()
    return act.say(act.Say.model_validate(notice)) if notice else ""


def run(p: Principal, call: dict, action: dict | None) -> str:
    """The XML of an action for this call (no action: hang up)."""
    a = ACTION.validate_python(action or HANGUP)
    match a:
        case act.QueueAction():
            return enqueue(p, call, str(a.queue_id), [str(x) for x in a.agents])
        case act.MenuAction():
            menu = setup.get(setup.menus, p.org_id, a.menu_id)
            return act.menu_xml(p.tenant_id, str(a.menu_id), menu, 1) if menu else "<Hangup/>"
        case act.RedirectAction():
            emit("conversation.redirected", p, call, url=act.redirect_url(a, call["conversation_id"]))
        case act.ForwardAction():
            save(call["call_uuid"], result="forwarded")
    return act.xml(a, p.tenant_id, call["conversation_id"])


def overflow(p: Principal, call: dict, queue_id: str, reason: str, action: dict | None) -> str:
    """The caller left a queue without an agent: run the queue's overflow (or a key's) action."""
    save(call["call_uuid"], queued="", result=RESULTS.get(reason, reason))
    emit("conversation.overflowed", p, call, queue_id=queue_id, reason=reason, action=action)
    return run(p, call, action)


def contact_params(call: dict, queue_id: str, q: dict, agents: list[str]) -> IncomingContactParams:
    """The engine's enqueue input for this queue row; `agents` (if any) are the only agents offered. A call the
    calls code moved here from an agent carries `transfer` in its record."""
    sticky, accept, music = q["sticky"] or {}, q["auto_accept"], q["hold_music"]
    route = QueueAndRouteConfigModel(
        queue=queue_id,
        routing_teams=[] if agents else q["teams"],
        routing_skills_filter=[
            SkillFilterModel(skill=s["skill_id"], operator=s["operator"], value=s["level"]) for s in q["skills"]
        ],
        routing=q["strategy"],
        agent_limit=q["agents_at_once"],
        retry_limit=q["retries"],
        routing_agents=agents or q["agents"],
        sticky_routing_type=STICKY.get(sticky.get("mode"), rc.STICKY_NO_STICKY_ROUTING),
        sticky_routing_expiry=not sticky.get("expiry_hours"),
        sticky_routing_expiry_time=sticky.get("expiry_hours") or 0,
        sticky_routing_expiry_unit="hours",
        auto_accept_call=AutoAcceptCallConfig(allow_auto_accept=accept["enabled"], auto_accept_delay=accept["delay"]),
    )
    queue = QueueConfigModel(
        queue_priority=q["priority"],
        dequeue_order=q["dequeue_order"],
        queue_capacity=q["capacity"],
        queue_call_timeout=q["wait_timeout_sec"],
        customer_hold_music=HoldMusic(url=music["customer"] or ""),
        agent_hold_music=HoldMusic(url=music["agent"] or ""),
        queue_call_allowed_inputs=list(q["dtmf_actions"]),
    )
    return IncomingContactParams(
        conversation_number=call["conversation_number"],
        contact_uuid=call["call_uuid"],
        parent_contact_uuid=call["call_uuid"],
        auth_id=call["auth_id"],
        from_=call["from"],
        to=call["to"],
        flow_run_uuid=call["run_id"],
        queue_and_route_run_uuid=str(uuid.uuid4()),
        contact_channel=rc.CHANNEL_CALL,
        is_queue_transfer=call.get("transfer") == "1",
        inbound=call.get("direction") != "outbound",
        queue_config=queue,
        queue_and_route_config=route,
    )


def admit(p: Principal, call: dict, queue_id: str, agents: list[str]) -> tuple[int, str]:
    """Put the call in the queue: (active queue entry id, the room's MPC elements, started once here), or (0, "")
    when the queue refused it."""
    q = setup.get(setup.queues, p.org_id, queue_id)
    if q is None:
        return 0, ""
    room, state, entry_id, _err = handle_incoming_contact(contact_params(call, queue_id, q, agents))
    if state == rc.QNR_STATE_ENQUEUE_FAILED:
        return 0, ""
    save(call["call_uuid"], queued="1", queue_id=queue_id)
    return entry_id, room.removeprefix("<Response>").removesuffix("</Response>")


def unqueue(call: dict) -> None:
    """Close the queue entry of a call that never reached its room (Plivo could not transfer it)."""
    from hs.infra.db import owned_session
    from hs.routing import active_queue as aq
    from hs.voice.callbacks import _update_unanswered

    save(call["call_uuid"], queued="", move="")
    with owned_session(None) as (db, commit):
        if entry := aq.get_current_active_queue_entry_by_contact_uuid(db, call["call_uuid"]):
            _update_unanswered(call["call_uuid"], rc.QNR_STATE_ENQUEUE_FAILED, entry.id)
            aq.delete_active_queue_entry(db, call["call_uuid"], entry.qnr_run_uuid, rc.CONTACT_STATE_FAILED)
            commit()


def enqueue(p: Principal, call: dict, queue_id: str, agents: list[str]) -> str:
    """The caller joins the queue's room, or the queue's overflow action runs (enqueue_failed)."""
    entry_id, room = admit(p, call, queue_id, agents)
    if not entry_id:
        q = setup.get(setup.queues, p.org_id, queue_id) or {}
        return overflow(p, call, queue_id, "enqueue_failed", q.get("overflow_action"))
    return room


def close_unanswered(p: Principal, call: dict, ended: dict) -> None:
    """Close a call that ended with no queue or agent holding it: its result's system disposition, call.ended,
    then the conversation (whose close emits conversation.closed)."""
    from hs.conversations import child_update as ccu
    from hs.conversations.models import ConversationParentStates, NonQNRChunkInsert, UpdateParentByConversationNumber
    from hs.conversations.outcomes import system_disposition
    from hs.conversations.store import ConversationParent
    from hs.infra.context import bg_db_guard
    from hs.routing.invitation import _ensure_request_context

    @bg_db_guard
    def close() -> None:
        if (parent := ConversationParent.get_by_contact_uuid(call["call_uuid"], only_open_conv=True)) is None:
            return
        _ensure_request_context()
        name = system_disposition(parent.id, ended["result"])
        ccu.add_chunk_to_non_qnr_child(
            NonQNRChunkInsert(type="notes", disposition=name, parent_contact_uuid=call["call_uuid"])
        )
        emit("call.ended", p, call, **ended)
        closing = UpdateParentByConversationNumber(
            conversation_number=call["conversation_number"], state=ConversationParentStates.closed, is_flow_ended=True
        )
        ccu.update_parent_conversation_by_conversation_number(closing)

    close()


def store_voicemail(call_uuid: str, form: dict) -> str:
    """Keep a voicemail as a recording of the caller's own leg (its leg row, the recording and its chunk on the
    conversation); the recording's public id."""
    from hs.conversations.store import Recording
    from hs.infra.context import get_db_session
    from hs.voice.call_legs import create_call_leg, create_parent_call_leg
    from hs.voice.call_models import CallLegInsertModel
    from hs.voice.recording import handle_main_call_recording_event

    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    create_parent_call_leg(CallLegInsertModel(call_uuid=call_uuid, call_leg_uuid=call_uuid, start_time=now))
    create_call_leg(CallLegInsertModel(call_uuid=call_uuid, call_leg_uuid=call_uuid, role="customer", start_time=now))
    get_db_session().commit()
    _msg, err = handle_main_call_recording_event(call_uuid, form)
    if err is not None:
        raise RuntimeError(f"voicemail of {call_uuid} not stored: {err}")
    return str(Recording.get_by_recording_uuid(form["RecordingID"]).uuid)
