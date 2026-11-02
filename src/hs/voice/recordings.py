"""Recordings: rows with a short-lived signed `audio_url` (never Plivo's URL), the audio itself with HTTP Range,
delete at Plivo, and the transcript."""

import hashlib
import hmac
import time
from datetime import datetime
from functools import cache
from urllib.parse import urlencode, urlsplit
from uuid import UUID

import httpx
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from starlette.background import BackgroundTask

from hs.access import Principal, require, tenant_plivo
from hs.conversations.api import visible_agents
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.settings import settings
from hs.paging import Page, Paging, page, paging
from hs.plivo import rest
from hs.voice import transcripts
from hs.voice.transcription_models import TranscriptionSegmentResponseModel

router = APIRouter()
READ = Depends(require("key:recordings", "agent", "sup"))
OWNER = Depends(require("key:recordings"))
URL_TTL = 300

# A recording reaches its conversation through the chunk that carries it; `agents`: one of them handled that
# conversation (primary on a child, or a secondary).
_FIND = text(
    "SELECT r.id AS cursor, r.uuid::text AS id, cp.uuid::text AS conversation_id, r.recording_duration,"
    " r.created_at, r.recording_uuid FROM recording r JOIN conversation_chunk ch ON ch.recording = r.id"
    " JOIN conversation c ON c.id = ch.conversation JOIN conversation_parent cp ON cp.id = c.parent"
    " WHERE r.is_deleted IS NOT TRUE AND cp.organization = :org"
    " AND (CAST(:rid AS uuid) IS NULL OR r.uuid = :rid) AND (CAST(:conv AS uuid) IS NULL OR cp.uuid = :conv)"
    " AND (CAST(:after AS timestamptz) IS NULL OR r.created_at >= :after)"
    " AND (CAST(:before AS timestamptz) IS NULL OR r.created_at < :before)"
    " AND (CAST(:cursor AS int) IS NULL OR r.id > :cursor)"
    " AND (CAST(:agents AS uuid[]) IS NULL OR EXISTS (SELECT 1 FROM conversation x JOIN account_org_mapping a"
    " ON a.id = x.primary_aom WHERE x.parent = cp.id AND a.uuid = ANY(CAST(:agents AS uuid[])))"
    " OR EXISTS (SELECT 1 FROM conversation_secondary_aom_mapping s JOIN account_org_mapping a ON a.id = s.aom"
    " WHERE s.conversation = cp.id AND a.uuid = ANY(CAST(:agents AS uuid[]))))"
    " ORDER BY r.id LIMIT :limit"
)
_MEDIA = text(
    "SELECT r.recording_url, o.uuid::text FROM recording r JOIN conversation_chunk ch ON ch.recording = r.id"
    " JOIN conversation c ON c.id = ch.conversation JOIN conversation_parent cp ON cp.id = c.parent"
    " JOIN organization o ON o.id = cp.organization WHERE r.is_deleted IS NOT TRUE AND r.uuid = :rid LIMIT 1"
)
# The recording, and once no other recording of its call is left, the call's per-participant tracks (the
# recordings on the legs under its call leg).
# Limit: a track names its MPC only by Plivo's MPCUUID, which is not stored; store it to delete per recording.
_WITH_TRACKS = text(
    "SELECT r.id, r.recording_uuid FROM recording r WHERE r.is_deleted IS NOT TRUE AND (r.id = :id OR r.call_leg_id"
    " IN (SELECT l.id FROM call_leg l JOIN recording pub ON pub.call_leg_id = l.parent_call_leg WHERE pub.id = :id"
    " AND NOT EXISTS (SELECT 1 FROM recording o WHERE o.call_leg_id = pub.call_leg_id AND o.id <> pub.id"
    " AND o.is_deleted IS NOT TRUE)))"
)
_DELETE = text("UPDATE recording SET is_deleted = true, updated_at = now() WHERE id = ANY(:ids)")
_HTTP = httpx.AsyncClient(timeout=30)


class CallRecording(BaseModel):
    id: str
    conversation_id: str
    duration_sec: int | None
    created_at: datetime
    audio_url: str


class Transcript(BaseModel):
    recording_id: str
    complete: bool
    segments: list[TranscriptionSegmentResponseModel]


@cache
def _url_key() -> bytes:
    return HKDF(SHA256(), 32, salt=None, info=b"recording-url").derive(settings.secret_key.encode())


def _sig(rid: str, viewer: str, exp: int, download: bool) -> str:
    return hmac.new(_url_key(), f"{rid}.{viewer}.{exp}.{int(download)}".encode(), hashlib.sha256).hexdigest()


def audio_url(rid: str, p: Principal, download: bool) -> str:
    viewer, exp = p.key_id or p.agent_id or "", int(time.time()) + URL_TTL
    query = {"exp": exp, "v": viewer, "sig": _sig(rid, viewer, exp, download)} | ({"download": 1} if download else {})
    return f"{settings.public_base_url}/api/v1/recordings/{rid}/audio?{urlencode(query)}"


def _download(p: Principal, download: bool) -> bool:
    """Download is signed in for tenant keys and admin and supervisor tokens only."""
    if download and p.kind == "agent" and p.role not in ("admin", "supervisor"):
        raise ApiError(403, "download_not_allowed", "only admins, supervisors and recordings keys may download")
    return download


def _out(row, p: Principal, download: bool) -> dict:
    duration = row.recording_duration or ""
    return {
        "id": row.id,
        "conversation_id": row.conversation_id,
        "duration_sec": int(duration) if duration.isdigit() else None,
        "created_at": row.created_at,
        "audio_url": audio_url(row.id, p, download),
        "cursor": row.cursor,
    }


def _find(p: Principal, limit: int = 1, agent_id: UUID | None = None, **filters) -> list:
    """The tenant's recordings that `p` may see: of conversations its agents (or `agent_id`) handled."""
    agents = visible_agents(p)
    if agent_id:
        agents = [a for a in agents or [str(agent_id)] if a == str(agent_id)]
    if agents == []:
        return []
    args = dict.fromkeys(("rid", "conv", "after", "before", "cursor")) | filters
    with core_engine.connect() as conn:
        return conn.execute(_FIND, args | {"org": p.org_id, "agents": agents, "limit": limit}).all()


def _recording(rid: UUID, p: Principal):
    if not (rows := _find(p, rid=str(rid))):
        raise ApiError(404, "recording_not_found", "no such recording")
    return rows[0]


@router.get("/recordings", response_model=Page[CallRecording])
def list_recordings(
    conversation_id: UUID | None = None,
    agent_id: UUID | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    download: bool = False,
    p: Principal = READ,
    pg: Paging = Depends(paging),
):
    download = _download(p, download)
    if pg.after is not None and not pg.after.isdigit():
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
    filters = {"conv": conversation_id and str(conversation_id), "after": created_after, "before": created_before}
    rows = _find(p, pg.limit + 1, agent_id, cursor=pg.after and int(pg.after), **filters)
    return page([_out(r, p, download) for r in rows], pg, key="cursor")


@router.get("/recordings/{rid}", response_model=CallRecording)
def get_recording(rid: UUID, download: bool = False, p: Principal = READ):
    return _out(_recording(rid, p), p, _download(p, download))


@router.delete("/recordings/{rid}", status_code=204)
def delete_recording(rid: UUID, p: Principal = OWNER) -> Response:
    """Deletes the recording at Plivo too, and with the call's last recording its per-participant tracks."""
    row, auth = _recording(rid, p), tenant_plivo(p.tenant_id)
    with core_engine.connect() as conn:
        rows = conn.execute(_WITH_TRACKS, {"id": row.cursor}).all()
    for _id, recording_uuid in rows:
        try:
            r = rest.call(auth, "DELETE", f"Recording/{recording_uuid}/")
        except httpx.HTTPError as e:
            raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
        if not r.is_success and r.status_code != 404:
            raise rest.refused(r)
    with core_engine.begin() as conn:
        conn.execute(_DELETE, {"ids": [i for i, _ in rows]})
    return Response(status_code=204)


@router.get("/recordings/{rid}/transcript", response_model=Transcript)
def get_transcript(rid: UUID, p: Principal = READ):
    row = _recording(rid, p)
    found = transcripts.get_transcriptions(row.recording_uuid)
    if found.error:
        raise ApiError(404, "transcript_not_found", "no transcript for this recording")
    return {"recording_id": row.id, "complete": found.data["all_fetched"], "segments": found.data["transcriptions"]}


def _fetchable(url: str) -> bool:
    """Only Plivo's media origin or an https Plivo host: never wherever a callback said the file is."""
    u, media = urlsplit(url), urlsplit(settings.plivo_media_url)
    return (u.scheme, u.netloc) == (media.scheme, media.netloc) or (
        u.scheme == "https" and (u.hostname or "").endswith(".plivo.com")
    )


def _media(rid: str) -> tuple[str, str]:
    with core_engine.connect() as conn:
        row = conn.execute(_MEDIA, {"rid": rid}).first()
    if row is None:
        raise ApiError(404, "recording_not_found", "no such recording")
    return row[0], row[1]


@router.get("/recordings/{rid}/audio", openapi_extra={"security": [{}]})  # no bearer: the signed URL is the credential
async def get_audio(
    rid: UUID, request: Request, exp: int = 0, v: str = "", sig: str = Query("", max_length=64), download: bool = False
):
    """The audio, for whoever holds a signed URL that has not expired (an `<audio src>` sends no header)."""
    if not sig:
        raise ApiError(401, "unauthorized", "a signed audio_url is required")
    if not hmac.compare_digest(sig, _sig(str(rid), v, exp, download)) or exp < time.time():
        raise ApiError(403, "invalid_signature", "this audio_url is invalid or expired")
    url, tenant_id = await run_in_threadpool(_media, str(rid))
    if not _fetchable(url):
        raise ApiError(502, "recording_unavailable", "the recording is not on a Plivo host")
    auth = await run_in_threadpool(tenant_plivo, tenant_id)
    ranged = {"Range": request.headers["range"]} if "range" in request.headers else {}
    try:
        upstream = await _HTTP.send(_HTTP.build_request("GET", url, headers=ranged), auth=auth, stream=True)
    except httpx.HTTPError as e:
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    if upstream.status_code not in (200, 206, 416):
        await upstream.aclose()
        raise ApiError(404 if upstream.status_code == 404 else 502, "recording_unavailable", "Plivo has no file")
    keep = ("content-type", "content-length", "content-range")
    headers = {k: upstream.headers[k] for k in keep if k in upstream.headers} | {"accept-ranges": "bytes"}
    if download:
        headers["content-disposition"] = f'attachment; filename="{rid}.mp3"'
    return StreamingResponse(
        upstream.aiter_raw(), upstream.status_code, headers, background=BackgroundTask(upstream.aclose)
    )
