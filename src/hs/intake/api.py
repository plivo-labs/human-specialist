"""Plivo's answer and hangup URLs, the action continuations and the engine's callbacks (under /plivo/{tenant_id},
signed), and POST /api/v1/queues/{id}/enqueue (a Plivo form or a tenant key's JSON)."""

import json
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import text

from hs import access, jobs
from hs.access import AGENTS, Principal, bearer, owned, plivo, plivo_signed, redis_client, require, tenant_plivo
from hs.errors import ApiError
from hs.infra.db import SessionLocal, core_engine
from hs.intake import flow
from hs.plivo import voice as pv
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.setup import actions as act
from hs.setup import api as setup
from hs.setup.actions import E164
from hs.voice.constants import EXIT_WAIT_TIMEOUT

router = APIRouter()  # mounted under /plivo/{tenant_id}
public = APIRouter()  # mounted under /api/v1
SIGNED = Depends(plivo_signed)
CALLS = require("key:calls")
CLOSE_DELAY = 10  # a voicemail's record callback can land just after the hangup
_QUEUE_TENANT = text(
    "SELECT o.uuid::text FROM queues q JOIN organization o ON o.id = q.organization"
    " WHERE q.id = CAST(:id AS uuid) AND o.is_deleted IS NOT TRUE"
)


async def _form(request: Request) -> dict:
    """Plivo's form, a Browser SDK user read as the agent's endpoint."""
    return {k: access.agent_address(v) for k, v in (await request.form()).items()}


FORM = Depends(_form)


def _xml(*parts: str) -> Response:
    return Response(flow.response(*parts), media_type="text/xml")


def _outbound(form: dict, dial: str | None) -> bool:
    """An agent leg of POST /calls (`?dial=`) or a dial from one of the tenant's endpoints: the calls code's."""
    return bool(dial) or form.get("From", "").startswith("sip:")


@router.post("/answer")
def answer(p: Principal = SIGNED, form: dict = FORM, dial: str | None = None) -> Response:
    if _outbound(form, dial):
        from hs.voice import outbound

        xml = outbound.answer(p, form, dial)
    elif (old := flow.redis().get(f"ANS:{form['CallUUID']}")) and flow.load_for(p, form["CallUUID"]):
        xml = old  # the previous release's reply, keyed without the tenant: a retry across the upgrade must not enqueue
    else:
        xml = flow.once(f"ANS:{p.tenant_id}:{form['CallUUID']}", lambda: _answer(p, form))
    return Response(xml, media_type="text/xml")


def _answer(p: Principal, form: dict) -> str:
    """Entry point, conversation, hours, consent, then the open (or closed) action."""
    number = flow.e164(form.get("To", ""))
    if (found := setup.entry_point(p.org_id, number)) is None:
        return flow.response("<Hangup/>")
    entry_id, entry = found
    hours = entry["business_hours_id"] and setup.get(setup.hours_schedules, p.org_id, entry["business_hours_id"])
    state = "open" if hours and setup.is_open(hours) else "closed" if hours else "none"
    call = flow.open_call(p, form, entry_point_id=entry_id, hours=state)
    notice = flow.consent(p, call)
    if state != "closed":
        return flow.response(notice, flow.run(p, call, entry["open_action"]))
    action = entry["closed_action"] or flow.HANGUP
    flow.save(call["call_uuid"], result="after_hours")
    flow.emit("conversation.after_hours", p, call, number=number, action=action)
    return flow.response(notice, flow.run(p, call, action))


@router.post("/actions/menu")
def menu(p: Principal = SIGNED, form: dict = FORM, menu_id: str = "", attempt: int = 0) -> Response:
    """A menu's entry (its menu_url, no attempt) or one attempt's input (Digits, or none: no input)."""
    call = flow.open_call(p, form)
    notice = flow.consent(p, call)
    if (m := setup.get(setup.menus, p.org_id, menu_id)) is None:
        return _xml(notice, "<Hangup/>")
    if not attempt:
        return _xml(notice, act.menu_xml(p.tenant_id, menu_id, m, 1))
    digit = form.get("Digits", "")
    option = m["options"].get(digit)
    retry = option is None and attempt <= m["retries"]
    action = option or ({"type": "menu", "menu_id": menu_id} if retry else m["invalid_action"] or flow.HANGUP)
    said = digit if option else "invalid" if digit else "no_input"
    flow.emit("conversation.menu_input", p, call, menu_id=menu_id, input=said, attempt=attempt, action=action)
    if retry:
        return _xml(notice, act.menu_xml(p.tenant_id, menu_id, m, attempt + 1))
    return _xml(notice, flow.run(p, call, action))


@router.post("/actions/voicemail")
def voicemail(
    p: Principal = SIGNED, form: dict = FORM, queue_id: str | None = None, agent_id: str | None = None
) -> Response:
    """A voicemail's Record finished: keep the message and hang up."""
    if call := flow.load_for(p, form.get("CallUUID", "")):
        recording_id = flow.store_voicemail(call["call_uuid"], form)
        flow.save(call["call_uuid"], result="voicemail")
        mailbox = {"queue_id": queue_id} if queue_id else {"agent_id": agent_id}
        data = {"recording_id": recording_id, "mailbox": mailbox, "from": call["from"]}
        flow.emit("voicemail.received", p, call, mailbox.get("agent_id"), **data)
    return _xml("<Hangup/>")


@router.post("/actions/move")
def move(p: Principal = SIGNED, form: dict = FORM, call_uuid: str = "") -> Response:
    """Plivo transferred the caller here: into the room an API enqueue admitted it to, or out of a room to run the
    queue or voicemail action an agent chose."""
    call = flow.load_for(p, call_uuid, form)
    if not call or not (action := flow.take_move(call_uuid)):
        return _xml("<Hangup/>")
    action = json.loads(action)
    return _xml(action["room"] if "room" in action else flow.run(p, call, action))


@router.post("/actions/queue-exit")
def queue_exit(
    p: Principal = SIGNED,
    form: dict = FORM,
    call_uuid: str = "",
    qnr_run_uuid: str = "",
    result: str = "",
    digit: str = "",
) -> Response:
    """The caller left a queue: (a) wait timeout, (b) an exit key, (c) no agent took it -> overflow; (d) otherwise
    (the room ended after an agent call) hang up."""
    from hs.voice import callbacks as voice_callbacks

    if not (call := flow.load_for(p, call_uuid)):
        return _xml("<Hangup/>")
    params = RoutingRedis().get_contact_parameters(call_uuid, qnr_run_uuid) or {}
    queue_id = params.get("queue_uuid") or call.get("queue_id", "")
    q = setup.get(setup.queues, p.org_id, queue_id) or {}
    if form.get("ParticipantExitCause") == EXIT_WAIT_TIMEOUT:
        db = SessionLocal()
        try:
            voice_callbacks.handle_wait_timeout(call_uuid, qnr_run_uuid, db)
            db.commit()
        finally:
            db.close()
        reason, action = "wait_timeout", q.get("overflow_action")
    elif digit:
        reason, action = "dtmf", q.get("dtmf_actions", {}).get(digit)
    elif result == rc.QNR_STATE_ASSIGNMENT_FAILED:
        reason, action = "assignment_failed", q.get("overflow_action")
    else:
        return _xml("<Hangup/>")
    return _xml(flow.consent(p, call), flow.overflow(p, call, queue_id, reason, action))


@router.post("/hangup")
def hangup(p: Principal = SIGNED, form: dict = FORM, dial: str | None = None) -> Response:
    """A call HS answered ended. One a queue or an agent took is the MPC callbacks'; any other closes 10 s later."""
    if _outbound(form, dial):
        from hs.voice import outbound

        if left := outbound.hangup(p, form, dial):
            _engine().mpc_call_status_callback(*left)
        return Response(status_code=200)
    call = flow.load_for(p, form.get("CallUUID", ""))
    if call and not call.get("queued"):
        if form.get("HangupCauseCode") == "3020" and not call.get("result"):
            flow.save(call["call_uuid"], result="rejected")
        keep = ("HangupCauseName", "HangupCauseCode", "HangupSource", "Duration")
        payload = {"tenant_id": p.tenant_id, "org_id": p.org_id, "call_uuid": call["call_uuid"]}
        key = f"{p.tenant_id}:{call['call_uuid']}"
        jobs.schedule("close_call", payload | {k: form.get(k, "") for k in keep}, delay=CLOSE_DELAY, key=key)
    return Response(status_code=200)


@jobs.handler("close_call")
def close_call(job: dict) -> None:
    call = flow.load(job["call_uuid"])
    if not call or call.get("queued"):
        return
    code, seconds = job["HangupCauseCode"], job["Duration"]
    ended = {
        "from": call["from"],
        "to": call["to"],
        "result": call.get("result") or "abandoned",
        "queue_id": call.get("queue_id") or None,
        "hangup_cause": job["HangupCauseName"] or None,
        "hangup_cause_code": int(code) if code.isdigit() else None,
        "duration_sec": int(seconds) if seconds.isdigit() else None,
        "talk_sec": None,
        "ended_by": "customer" if job["HangupSource"] == "Caller" else "system",
    }
    flow.close_unanswered(Principal(job["tenant_id"], job["org_id"], "plivo"), call, ended)


# The engine's own callbacks, signed like every Plivo request; the handlers are the voice engine's.
def _engine():
    from hs.voice import dispatch

    dispatch.seed_context()
    return dispatch


def _own(p: Principal, call_uuid: str, qnr_run_uuid: str) -> None:
    """A callback names its call in a query any tenant can sign: only the call's own tenant acts on it. The raw client
    raises when the store does not answer, a 503 that Plivo retries; the wrapper's None would be a 404 it drops."""
    cp = redis_client().hget(rc.contact_params_key(call_uuid), qnr_run_uuid)
    if not cp or json.loads(cp).get("auth_id") != tenant_plivo(p.tenant_id)[0]:
        raise ApiError(404, "call_not_found", "no such call")


@router.post("/mpc-status")
def mpc_status(p: Principal = SIGNED, form: dict = FORM, call_uuid: str = "", qnr_run_uuid: str = "") -> Response:
    _own(p, call_uuid, qnr_run_uuid)
    return _engine().mpc_status_callback(call_uuid, qnr_run_uuid, form)


@router.post("/call-status")
def call_status(p: Principal = SIGNED, form: dict = FORM, call_uuid: str = "", qnr_run_uuid: str = "") -> Response:
    _own(p, call_uuid, qnr_run_uuid)
    return _engine().mpc_call_status_callback(call_uuid, qnr_run_uuid, form)


@router.post("/recording")
def recording(p: Principal = SIGNED, form: dict = FORM, call_uuid: str = "", qnr_run_uuid: str = "") -> Response:
    _own(p, call_uuid, qnr_run_uuid)
    return _engine().voice_mpc_recording_callback(call_uuid, form)


@router.post("/transcription")
def transcription(p: Principal = SIGNED, form: dict = FORM, call_uuid: str = "") -> Response:
    auth_id = tenant_plivo(p.tenant_id)[0]
    return _engine().voice_mpc_transcription_callback(auth_id, call_uuid or form.get("call_uuid", ""), form)


class EnqueueIn(BaseModel):
    call_uuid: str = Field(min_length=1, max_length=64)
    from_: E164 = Field(alias="from")
    to: E164
    agents: list[UUID] = []
    context: dict | None = None
    direction: Literal["inbound", "outbound"] = "inbound"


class Enqueued(BaseModel):
    active_queue_entry_id: int
    conversation_id: str


async def _enqueuer(request: Request, queue_id: UUID) -> Principal:
    """A tenant key with `calls` (JSON), else a form request Plivo signed with the queue's tenant's token."""
    if bearer(request) or request.headers.get("content-type", "").startswith("application/json"):
        return await CALLS(request)

    def tenant() -> str | None:
        with core_engine.connect() as conn:
            return conn.execute(_QUEUE_TENANT, {"id": str(queue_id)}).scalar()

    if (tenant_id := await run_in_threadpool(tenant)) is None:
        raise ApiError(401, "unauthorized", "bad Plivo signature")
    return await plivo(request, tenant_id)


async def _payload(request: Request) -> dict:
    if not request.headers.get("content-type", "").startswith("application/json"):
        return dict((await request.form()).items())
    try:
        body = await request.json()
    except ValueError as e:
        raise ApiError(400, "invalid_json", "the body is not valid JSON") from e
    return body if isinstance(body, dict) else {}


@public.post(
    "/queues/{queue_id}/enqueue",
    response_model=Enqueued,
    responses={200: {"content": {"text/xml": {}}, "description": "Plivo XML for a form request"}},
    # The body is read by hand (Plivo's form or this JSON) so auth runs first; its JSON shape is declared here.
    openapi_extra={"requestBody": {"content": {"application/json": {"schema": EnqueueIn.model_json_schema()}}}},
)
def enqueue(
    request: Request, queue_id: UUID, p: Principal = Depends(_enqueuer), data: dict = Depends(_payload)
) -> Response | dict:
    """Hand a live call to the queue. A Plivo form request (the queue's enqueue_url, query `agents`, `context`,
    `consent_played=1`) gets XML; a tenant key's JSON gets the queue entry."""
    if setup.get(setup.queues, p.org_id, queue_id) is None:
        raise ApiError(404, "queue_not_found", "no such queue")
    if p.kind == "plivo":
        if data.get("Event") == "Hangup":  # the call's hangup URL is this enqueue URL: the call is over
            return hangup(p, data)
        query = request.query_params
        agents = [a for v in query.getlist("agents") for a in v.split(",") if a]
        context = flow.context_of(query.get("context"))
        if agents:
            owned(AGENTS, agents, p, "agents")
        call = flow.open_call(p, data, context)
        notice = flow.consent(p, call, played=query.get("consent_played") == "1")
        return _xml(notice, flow.enqueue(p, call, str(queue_id), agents))
    body = setup.validated(EnqueueIn, data)
    agents = [str(a) for a in body.agents]
    if agents:
        owned(AGENTS, agents, p, "agents")
    context = flow.context_of(json.dumps(body.context)) if body.context else None
    form = {"CallUUID": body.call_uuid, "From": body.from_, "To": body.to, "Direction": body.direction}
    call = flow.open_call(p, form, context)
    entry_id, room = flow.admit(p, call, str(queue_id), agents)
    if not entry_id:
        raise ApiError(409, "enqueue_failed", "the queue refused the call")
    # The live call joins the room by Plivo's transfer: Plivo fetches the move URL, which hands it this room.
    flow.save(body.call_uuid, move=json.dumps({"room": room}))
    move = {"legs": "aleg", "aleg_url": act.plivo_url(p.tenant_id, "actions/move", call_uuid=body.call_uuid)}
    _b, status, err = pv.PlivoVoiceClient(*tenant_plivo(p.tenant_id)).call_update(body.call_uuid, move)
    if err is not None:
        flow.unqueue(call)
        raise ApiError(
            409, "call_not_live" if status == 404 else "transfer_failed", f"Plivo could not move the call: {err}"
        )
    return {"active_queue_entry_id": entry_id, "conversation_id": call["conversation_id"]}
