"""Recordings and transcripts: Plivo's recording and transcription callbacks, the recordings API, whose `audio_url`
is a short-lived signed link to HS (never Plivo's URL), and pausing or resuming a call's recording.

Each Plivo recording is one entry, kept once per Plivo id in any conversation state: a room's recording
(`recording`), a voicemail (`voicemail`, which voice/flow.py keeps), one leg's track (`track`, never listed) or an
empty take or mixed track (`dropped`, never listed or read). A transcript is kept on the entry it names."""

import hashlib
import hmac
import json
import math
import time
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import func, literal, select, update
from sqlalchemy.engine import Connection, Row
from starlette.background import BackgroundTask

from hs import access, config, conversations, events, jobs, plivo
from hs.config import models as m
from hs.core import PUBLIC, ApiError, Page, Paging, T, page, paging, read, settings, tx
from hs.voice import mpc

router = APIRouter()  # under /api/v1
callbacks = APIRouter()  # under /plivo/{tenant_id}
READ = access.require("key:recordings", "agent", "sup")
OWNER = access.require("key:recordings")
CALLS = access.require("key:calls", "agent")
TENANT = Depends(plivo.voice_signed)
LISTED = ("recording", "voicemail")
TEXT_AFTER = 240  # seconds Plivo has to produce timed segments before a callback's plain text is used
# A member's own recording is single-track; Plivo's default track type, `all`, names another one.
TRACK = {"record_track_type": "participant"}


class CallRecording(BaseModel):
    id: str
    conversation_id: str
    duration_sec: int | None
    format: str | None
    started_at: datetime | None
    ended_at: datetime | None
    created_at: datetime
    audio_url: str


class TranscriptionSegmentResponseModel(BaseModel):
    speaker: str
    start_time: int
    text: str


class Transcript(BaseModel):
    recording_id: str
    complete: bool
    segments: list[TranscriptionSegmentResponseModel]


ISSUES = {
    "poor_or_low_audio": ["AUDIO_LAG", "BROKEN_AUDIO", "ECHO", "LOW_AUDIO_LEVEL", "ROBOTIC_AUDIO"],
    "no_audio": ["ONE_WAY_AUDIO"],
    "call_disconnected": ["CALL_DROPPED"],
    "delay_in_connecting": ["HIGH_CONNECT_TIME"],
    "others": ["OTHERS"],
}


class Feedback(m.Body):
    rating: int = Field(ge=1, le=5)
    issue_type: Literal[tuple(ISSUES)] | None = None
    comment: str | None = None

    @model_validator(mode="after")
    def _issue_below_five(self):
        if self.rating <= 4 and not self.issue_type:
            raise ValueError("issue_type is required for a rating of 4 or below")
        return self


class RecordingAction(m.Body):
    action: Literal["pause", "resume"]


def _sig(rid, viewer: str, exp: int, download: bool) -> str:
    message = f"recording-audio.{rid}.{viewer}.{exp}.{int(download)}"
    return hmac.new(settings.secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()


def _out(db: Connection, e: Row, p: access.Principal, download: bool) -> dict:
    """The recording's read model, its audio_url signed for 5 minutes; downloading is signed in for tenant keys and
    the roles that hold `download_recording`."""
    if download and not access.permits(db, p, "download_recording"):
        raise ApiError(403, "download_not_allowed", "this caller's role may not download recordings")
    viewer, exp = p.key_id or p.agent_id or "", int(time.time()) + 300
    query = f"exp={exp}&v={viewer}&sig={_sig(e.id, viewer, exp, download)}" + ("&download=1" if download else "")
    url = f"{settings.public_base_url}{PUBLIC}/recordings/{e.id}/audio?{query}"
    found = {"id": str(e.id), "conversation_id": str(e.conversation_id), "created_at": e.at, "audio_url": url}
    when = {"duration_sec": e.body.get("duration"), "format": e.body.get("format")}
    return found | when | {"started_at": e.body.get("began"), "ended_at": e.body.get("ended")}


def _listed(db: Connection, p: access.Principal, *where, agent_id: UUID | None = None):
    """The tenant's recordings and voicemails, deleted ones too unless `where` drops them, of conversations `p` may
    see (and `agent_id` took, if given)."""
    E, A, C = T.entry, T.assignment, T.conversation
    q = select(E, C.c.call_uuid.label("customer"), C.c.channel).join(C, C.c.id == E.c.conversation_id)
    q = q.where(E.c.tenant_id == p.tenant_id, E.c.kind.in_(LISTED), *where)
    for agents in (access.visible_agents(db, p), agent_id and {str(agent_id)}):
        if agents is not None:
            took = (A.c.conversation_id == E.c.conversation_id, A.c.agent_id.in_(agents), A.c.started_at.isnot(None))
            q = q.where(select(A.c.id).where(*took).exists())
    return q


def _one(db: Connection, p: access.Principal, rid: UUID, *where) -> Row:
    if (e := db.execute(_listed(db, p, T.entry.c.id == rid, *where)).first()) is None:
        raise ApiError(404, "recording_not_found", "no such recording")
    return e


@router.get("/recordings", response_model=Page[CallRecording])
def list_recordings(
    conversation_id: UUID | None = None,
    agent_id: UUID | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    download: bool = False,
    p: access.Principal = READ,
    paged: Paging = Depends(paging),
):
    E = T.entry
    within = (conversation_id and E.c.conversation_id == conversation_id, created_after and E.c.at >= created_after)
    within += (created_before and E.c.at < created_before, E.c.deleted_at.is_(None))
    with read() as db:
        q = _listed(db, p, *(w for w in within if w is not None), agent_id=agent_id)
        rows = config.page_rows(db, q, paged, E.c.id)
        return page([_out(db, e, p, download) for e in rows], paged)


@router.get("/recordings/{rid}", response_model=CallRecording)
def get_recording(rid: UUID, download: bool = False, p: access.Principal = READ):
    with read() as db:
        return _out(db, _one(db, p, rid, T.entry.c.deleted_at.is_(None)), p, download)


@router.delete("/recordings/{rid}", status_code=204)
def delete_recording(rid: UUID, p: access.Principal = OWNER) -> Response:
    """The recording goes at Plivo too, and with the call's last recording its legs' tracks. Plivo's verdict is the
    answer, a refusal the job pool met included; a retry erases again whatever of the call is deleted here."""
    E = T.entry
    with tx() as db:
        e = _one(db, p, rid)
        conv = conversations.lock(db, e.conversation_id)
        call = (E.c.conversation_id == conv.id, E.c.kind.in_((*LISTED, "track")), E.c.provider_id.isnot(None))
        rows = db.execute(select(E).where(*call, E.c.deleted_at.is_(None))).all()
        last = e.kind == "recording" and not any(x.kind == "recording" and x.id != e.id for x in rows)
        going = [e.id] + [x.id for x in rows if last and x.kind == "track"]
        db.execute(update(E).where(E.c.id.in_(going), E.c.deleted_at.is_(None)).values(deleted_at=func.now()))
        conversations.touched(db, conv)
        erased = db.execute(select(E.c.provider_id).where(*call, E.c.deleted_at.isnot(None))).scalars().all()
        erase = {"tenant_id": p.tenant_id, "conversation_id": str(conv.id)}
        keys = [plivo.command(db, "erase", x, erase | {"plivo_id": x}, wait=True) for x in erased]
    plivo.verdict(*keys)
    return Response(status_code=204)


def _parts(db: Connection, e: Row) -> list[Row]:
    """What a recording's transcript is read from: its room's tracks, if it has any, else itself."""
    E = T.entry
    room = (E.c.conversation_id == e.conversation_id, E.c.kind == "track", E.c.body["mpc"].astext == e.body.get("mpc"))
    return (e.kind == "recording" and db.execute(select(E).where(*room)).all()) or [e]


@router.get("/recordings/{rid}/transcript", response_model=Transcript)
def get_transcript(rid: UUID, p: access.Principal = READ):
    """Each part's segments in time order, on the room recording's clock: the customer's, or the agent whose leg it
    is. A track's start is its own, so its segments move by how long after the room's recording it began, and what a
    track says before the room's recording or after its length is left out."""
    A, C = T.assignment, T.config
    with read() as db:
        e = _one(db, p, rid, T.entry.c.deleted_at.is_(None))
        parts = _parts(db, e)
        named = select(A.c.call_uuid, C.c.body["name"].astext).join(C, C.c.id == A.c.agent_id)
        agents = dict(db.execute(named.where(A.c.conversation_id == e.conversation_id)).all())
    if not any("segments" in x.body for x in parts):
        raise ApiError(404, "transcript_not_found", "no transcript for this recording")
    segments, end = [], e.body.get("duration") or math.inf
    for x in parts:
        leg, began, room = x.body.get("leg") or e.customer, x.body.get("began"), e.body.get("began")
        shift = (datetime.fromisoformat(began) - datetime.fromisoformat(room)).total_seconds() if began and room else 0
        who = "Customer" if leg == e.customer or (e.channel != "call" and leg not in agents) else agents.get(leg) or leg
        said = ((s["start"] + shift, s["text"]) for s in x.body.get("segments", []))
        segments += [
            {"speaker": who, "start_time": math.ceil(at), "text": words}
            for at, words in said
            if x.kind != "track" or 0 <= math.floor(at) <= end
        ]
    segments.sort(key=lambda s: s["start_time"])
    return {"recording_id": str(e.id), "complete": all("segments" in x.body for x in parts), "segments": segments}


@router.get("/recordings/{rid}/audio", openapi_extra={"security": [{}]})  # no bearer: the signed URL is the credential
def get_audio(
    rid: UUID, request: Request, exp: int = 0, v: str = "", sig: str = Query("", max_length=64), download: bool = False
):
    """The audio with HTTP Range, for whoever holds a signed URL that has not expired (an `<audio src>` sends no
    header), fetched from Plivo with the tenant's credentials."""
    if not sig:
        raise ApiError(401, "unauthorized", "a signed audio_url is required")
    if exp < time.time() or not hmac.compare_digest(sig, _sig(rid, v, exp, download)):
        raise ApiError(403, "invalid_signature", "this audio_url is invalid or expired")
    E = T.entry
    with read() as db:
        found = (E.c.id == rid, E.c.kind.in_(LISTED), E.c.deleted_at.is_(None))
        owner = T.tenant.c.id == E.c.tenant_id
        row = db.execute(select(E.c.body, T.tenant).join(T.tenant, owner).where(*found)).first()
    if row is None or not plivo.fetchable(url := row.body.get("url") or ""):
        raise ApiError(404, "recording_not_found", "no such recording")
    ranged = {"Range": request.headers["range"]} if "range" in request.headers else {}
    auth = (row.plivo_auth_id, plivo.decrypt(row.plivo_token_enc))
    try:
        upstream = plivo.HTTP.send(plivo.HTTP.build_request("GET", url, headers=ranged), auth=auth, stream=True)
    except httpx.HTTPError as e:
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    if upstream.status_code not in (200, 206, 416):
        upstream.close()
        raise ApiError(404 if upstream.status_code == 404 else 502, "recording_unavailable", "Plivo has no file")
    keep = ("content-type", "content-length", "content-range")
    headers = {k: upstream.headers[k] for k in keep if k in upstream.headers} | {"accept-ranges": "bytes"}
    headers |= {"content-disposition": f'attachment; filename="{rid}.mp3"'} if download else {}
    done = BackgroundTask(upstream.close)
    return StreamingResponse(upstream.iter_raw(), upstream.status_code, headers, background=done)


def _segments(found) -> list[dict] | None:
    """Plivo's transcript as [{start, text}], from segments as JSON; None when it holds none, plain text included."""
    if isinstance(found, str):
        try:
            found = json.loads(found)
        except ValueError:
            return None
    if not isinstance(found, list):
        return None
    return [{"start": float(s.get("start") or 0), "text": str(s.get("text", ""))} for s in found if isinstance(s, dict)]


def _announce(db: Connection, conv: Row, e: Row) -> None:
    """transcript.ready, once, for the recording `e` belongs to, when each of its parts has its transcript."""
    E = T.entry
    if e.kind == "track":
        room = (E.c.conversation_id == conv.id, E.c.kind == "recording", E.c.body["mpc"].astext == e.body["mpc"])
        e = db.execute(select(E).where(*room)).first()
    if e is None or e.kind == "dropped" or "settling" in e.body or not all("segments" in x.body for x in _parts(db, e)):
        return
    once = update(E).where(E.c.id == e.id, ~E.c.body.has_key("announced"))
    if db.execute(once.values(body=E.c.body.concat({"announced": True})).returning(E.c.id)).first():
        data = {"recording_id": str(e.id)}
        events.emit(db, "transcript.ready", str(conv.tenant_id), data, conversation_id=str(conv.id))


def _attach(db: Connection, conv: Row, plivo_id: str, segments: list[dict]) -> bool:
    E = T.entry
    named = update(E).where(E.c.conversation_id == conv.id, E.c.provider_id == plivo_id)
    if (e := db.execute(named.values(body=E.c.body.concat({"segments": segments})).returning(*E.c)).first()) is None:
        return False
    conversations.touched(db, conv)
    _announce(db, conv, e)
    return True


@callbacks.post("/recording")
def recording(c: UUID | None = None, tenant: Row = TENANT, form: dict = plivo.FORM) -> Response:
    """A room's recording or one leg's track is ready: kept once per Plivo recording, whatever the conversation's
    state or room, unless empty or a mixed track, which is kept apart and told of to no one. A track that comes once
    its call's recordings are all deleted goes at Plivo too."""
    event, rid = form.get("EventName", ""), form.get("RecordingUUID", "")
    if not rid or event not in ("MPCRecordingCompleted", "ParticipantRecordingCompleted"):
        return Response()
    kind, duration = "track" if event.startswith("Participant") else "recording", None
    with suppress(ValueError, TypeError):
        duration = int(form.get("RecordingDuration", ""))
    body = {"url": form.get("RecordingURL"), "duration": duration}
    body |= {"mpc": form.get("MPCUUID"), "format": form.get("RecordingFormat") or None}
    body |= {"began": plivo.utc(form.get("RecordingStartTime")), "ended": plivo.utc(form.get("RecordingEndTime"))}
    body |= {"leg": form.get("ParticipantCallUUID")} if kind == "track" else {"settling": True}
    if (duration is not None and duration < 1) or (kind == "track" and form.get("RecordTrackType") == "all"):
        kind = "dropped"
    E = T.entry
    with tx() as db:
        conv = mpc.find_conv(db, tenant, c)
        if conv is None or (e := conversations.entry(db, conv, kind, body, provider_id=rid)) is None:
            return Response()
        deleted = select(func.bool_and(E.c.deleted_at.isnot(None))).where(E.c.conversation_id == conv.id)
        if kind == "dropped":
            return Response()
        if kind == "recording":
            data = {"recording_id": str(e.id), "duration_sec": body["duration"]}
            events.emit(db, "recording.ready", str(conv.tenant_id), data, conversation_id=str(conv.id))
            settle = {"conversation_id": str(conv.id), "entry_id": str(e.id)}
            # Plivo sends the tracks beside a room's recording in any order. A track later than the grace misses
            # the announcement; the room's own list of legs would close that.
            at = db.execute(select(func.clock_timestamp())).scalar() + timedelta(seconds=mpc.CALLBACK_GRACE_SEC)
            jobs.schedule(db, "transcript_settle", settle, at=at, key=str(e.id))
        elif db.execute(deleted.where(E.c.kind == "recording")).scalar():
            db.execute(update(E).where(E.c.id == e.id).values(deleted_at=func.now()))
            erase = {"tenant_id": str(tenant.id), "conversation_id": str(conv.id), "plivo_id": rid}
            plivo.command(db, "erase", rid, erase)
            return Response()
        _announce(db, conv, e)
    return Response()


@callbacks.post("/transcription")
def transcription(c: UUID | None = None, tenant: Row = TENANT, form: dict = plivo.FORM) -> Response:
    """A recording's transcript, kept on it; a replay of one kept already changes nothing. Timed segments for a
    recording not kept yet wait for it. A voicemail's plain text is its transcript. Any other recording's plain
    text, or none, is read from Plivo's timed segments later, the text kept for when Plivo has none."""
    rid, text, E = form.get("recording_id", ""), form.get("transcription"), T.entry
    segments = _segments(text)
    plain = text.strip() if segments is None and isinstance(text, str) else ""
    held = select(E.c.kind, E.c.body["segments"]).where(E.c.tenant_id == tenant.id, E.c.provider_id == rid)
    with read() as db:
        kind, kept = db.execute(held).first() or (None, None)
    if kind == "voicemail" and plain:
        segments = [{"start": 0.0, "text": plain}]
    if kept is not None and (kept == segments or (segments is None and plain)):
        return Response()
    with tx() as db:
        conv = rid and mpc.find_conv(db, tenant, c)
        if conv and (segments is None or not _attach(db, conv, rid, segments)):
            fetch = {"tenant_id": str(tenant.id), "conversation_id": str(conv.id), "plivo_id": rid}
            held = fetch | {"segments": segments, "text": plain or None, "since": time.time()}
            jobs.schedule(db, "transcript_fetch", held, delay=5, key=f"{tenant.id}:{rid}")
    return Response()


@jobs.handler("transcript_fetch", lane="slow")
def _fetch(job: Row) -> None:
    p, tenant, segments = job.payload, plivo.account(job.payload["tenant_id"]), job.payload.get("segments")
    if segments is None:
        try:
            found = plivo.rest(tenant, "GET", f"Transcription/{p['plivo_id']}/", timeout_sec=30, type="raw")
        except plivo.Refused as e:
            if e.details.get("status") != 404:
                raise
            found = {}
        segments = _segments(found.get("transcription"))
    if segments is None and p.get("text") and time.time() - p["since"] >= TEXT_AFTER:
        segments = [{"start": 0.0, "text": p["text"]}]
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        if segments is None or not _attach(db, conv, p["plivo_id"], segments):
            raise jobs.NotYet(f"recording {p['plivo_id']} has no transcript at Plivo, or is not kept yet")


@jobs.handler("transcript_settle", lane="slow")
def _settled(job: Row) -> None:
    E = T.entry
    with tx() as db:
        conv = conversations.lock(db, UUID(job.payload["conversation_id"]))
        settled = update(E).where(E.c.id == job.payload["entry_id"]).values(body=E.c.body.op("-")(literal("settling")))
        if e := db.execute(settled.returning(*E.c)).first():
            _announce(db, conv, e)


@router.post("/calls/{cid}/recording", status_code=204)
def control_recording(cid: UUID, body: RecordingAction, p: access.Principal = CALLS) -> Response:
    """Pause or resume the call's recording and each member's own track, answered once each has; resuming what
    records nothing starts it. A failure is the answer and emits nothing; a retry finishes what it left."""
    ask = {"action": body.action, "by": p.agent_id, "request": uuid4().hex}
    with tx() as db:
        conv = access.conversation(db, p, cid, lock=True)
        off = conv.channel != "call" and mpc.dialed(db, conv) is None
        if conv.state not in conversations.LIVE or access.hung_up(conv) or off:
            raise ApiError(409, "call_not_live", "the call has ended")
        if not config.tenant_settings(db, p.tenant_id).recording.allow_pause:
            raise ApiError(409, "pause_not_allowed", "this tenant does not allow pausing a recording")
        access.conversation(db, p, cid, control=True)
        room = {"tenant_id": p.tenant_id, "conversation_id": str(conv.id), "room": conv.room}
        key = plivo.command(db, "recording", f"{conv.id}:{conv.room}", room | ask, wait=True)
    plivo.verdict(key, request=ask["request"])
    return Response(status_code=204)


def _toggle(tenant: Row, p: dict) -> dict:
    room, action = mpc.room_of(p), p["action"].capitalize()
    done, text = (plivo.url(str(tenant.id), path, c=p["conversation_id"]) for path in ("recording", "transcription"))
    start = {"recording_callback_url": done, "transcript": True, "transcription_url": text}
    listed = plivo.rest(tenant, "GET", f"{room}/Participant/").get("objects", [])
    members = [(f"{room}/Participant/{x['member_id']}", TRACK) for x in listed if not x.get("exit_cause")]
    for target, track in [(room, {}), *members]:
        try:
            plivo.rest(tenant, "POST", f"{target}/Record/{action}/", **track)
        except plivo.Refused as e:
            if e.details.get("status") != 409 or (action == "Pause" and target == room):
                raise
            if action == "Resume":
                plivo.rest(tenant, "POST", f"{target}/Record/", **track, **start)
    return {}


def _toggled(p: dict, _: dict) -> None:
    C, cid, data = T.conversation, p["conversation_id"], {"by_agent_id": p["by"]}
    mark = {"state": [p["room"], p["action"]]}
    with tx() as db:
        if (conv := mpc.still_live(db, p, conversations.LIVE)) is None:
            return
        was = conv.routing.get("recording") or {}
        db.execute(update(C).where(C.c.id == cid).values(routing=C.c.routing.concat({"recording": mark})))
        if was.get("state") != mark["state"]:
            agent = getattr(conversations.primary_agent(db, conv.id), "agent_id", None)
            events.emit(db, f"call.recording_{p['action']}d", p["tenant_id"], data, conversation_id=cid, agent_id=agent)


plivo.OPS["recording"] = plivo.Op(False, _toggle, _toggled)
plivo.OPS["erase"] = plivo.Op(False, lambda t, p: plivo.rest(t, "DELETE", f"Recording/{p['plivo_id']}/"))


@router.post("/calls/{cid}/feedback", status_code=204)
def call_feedback(cid: UUID, body: Feedback, p: access.Principal = CALLS) -> Response:
    """A rating of a call's quality, kept on it under the call's newest primary agent, whoever posts it, and passed to
    Plivo's call quality reports, which a Plivo refusal does not undo. A call no agent took has no agent to rate."""
    with tx() as db:
        conv = access.conversation(db, p, cid)
        if conv.channel != "call":
            raise ApiError(404, "conversation_not_found", "no such call")
        if not (took := [a for a in conversations.rows(db, conv.id) if a.role == "primary" and a.started_at]):
            raise ApiError(422, "call_not_answered", "no agent took this call")
        agent_id = max(took, key=lambda a: a.started_at).agent_id
        conversations.entry(db, conv, "feedback", body.model_dump(), agent_id=agent_id)
        sent = {"tenant_id": p.tenant_id, "path": f"Call/{conv.call_uuid}/Feedback/"}
        sent["params"] = {"rating": body.rating, "issues": ISSUES.get(body.issue_type, []), "notes": body.comment or ""}
        plivo.command(db, "feedback", f"{conv.id}:{uuid4().hex}", sent)
    return Response(status_code=204)


plivo.OPS["feedback"] = plivo.Op(True, lambda t, p: plivo.rest(t, "POST", p["path"], **p["params"]))
