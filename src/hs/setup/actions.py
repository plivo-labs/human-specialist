"""The action: one shape for entry points, overflow, menu options and queue keys, and its Plivo XML.

`queue` renders through intake (it enqueues the call); every other type renders here.
"""

from typing import Annotated, Literal
from urllib.parse import urlencode
from uuid import UUID
from xml.sax.saxutils import escape, quoteattr

from pydantic import BaseModel, ConfigDict, Field, model_validator

from hs.infra.settings import settings

E164_PATTERN = r"^\+[1-9]\d{6,14}$"
E164 = Annotated[str, Field(pattern=E164_PATTERN)]
HttpUrl = Annotated[str, Field(pattern=r"^https?://\S+$", max_length=2000)]


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Say(Body):
    """Text to speak (optionally with a voice and language), or an audio file to play."""

    text: str | None = Field(None, min_length=1, max_length=2000)
    voice: str | None = Field(None, max_length=100)
    language: str | None = Field(None, max_length=20)
    audio_url: HttpUrl | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.text is None) == (self.audio_url is None):
            raise ValueError("give either text or audio_url")
        return self


class QueueAction(Body):
    type: Literal["queue"]
    queue_id: UUID
    agents: list[UUID] = []


class MenuAction(Body):
    type: Literal["menu"]
    menu_id: UUID


class VoicemailBox(Body):
    queue_id: UUID | None = None
    agent_id: UUID | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.queue_id is None) == (self.agent_id is None):
            raise ValueError("give either queue_id or agent_id")
        return self


class VoicemailAction(Body):
    type: Literal["voicemail"]
    greeting: Say | None = None
    max_length_sec: int = Field(60, ge=1, le=3600)
    transcribe: bool = False
    mailbox: VoicemailBox


class ForwardAction(Body):
    type: Literal["forward"]
    number: E164


class RedirectAction(Body):
    type: Literal["redirect"]
    url: HttpUrl


class HangupAction(Body):
    type: Literal["hangup"]
    greeting: Say | None = None


Action = Annotated[
    QueueAction | MenuAction | VoicemailAction | ForwardAction | RedirectAction | HangupAction,
    Field(discriminator="type"),
]


def plivo_url(tenant_id: str, path: str, **query) -> str:
    """An absolute /plivo/{tenant_id}/... URL; empty query values are dropped."""
    q = urlencode({k: v for k, v in query.items() if v not in (None, "")})
    return f"{settings.public_base_url}/plivo/{tenant_id}/{path}" + (f"?{q}" if q else "")


def say(s: Say | None) -> str:
    if s is None:
        return ""
    if s.audio_url:
        return f"<Play>{escape(s.audio_url)}</Play>"
    attrs = "".join(f" {k}={quoteattr(v)}" for k, v in (("voice", s.voice), ("language", s.language)) if v)
    return f"<Speak{attrs}>{escape(s.text)}</Speak>"


def menu_xml(tenant_id: str, menu_id: str, menu: dict, attempt: int) -> str:
    """One attempt of a menu: the greeting inside GetDigits, then a redirect that reports "no input"."""
    url = plivo_url(tenant_id, "actions/menu", menu_id=menu_id, attempt=attempt)
    greeting, timeout = say(Say.model_validate(menu["greeting"])), menu["input_timeout_sec"]
    digits = f'<GetDigits action={quoteattr(url)} method="POST" numDigits="1" retries="1" timeout="{timeout}">'
    return f"{digits}{greeting}</GetDigits><Redirect>{escape(url)}</Redirect>"


def xml(action, tenant_id: str, conversation_id: str) -> str:
    """The XML of a non-queue, non-menu action (menu and queue need their stored rows)."""
    match action:
        case HangupAction():
            return f"{say(action.greeting)}<Hangup/>"
        case ForwardAction():
            return f"<Dial><Number>{action.number.removeprefix('+')}</Number></Dial>"
        case RedirectAction():
            return f"<Redirect>{escape(redirect_url(action, conversation_id))}</Redirect>"
        case VoicemailAction():
            box = action.mailbox
            query = {"queue_id": box.queue_id, "agent_id": box.agent_id}
            done = plivo_url(tenant_id, "actions/voicemail", **query)
            record = f' action={quoteattr(done)} method="POST" maxLength="{action.max_length_sec}"'
            if action.transcribe:
                text = plivo_url(tenant_id, "transcription")
                record += f' transcriptionType="auto" transcriptionUrl={quoteattr(text)} transcriptionMethod="POST"'
            return f'{say(action.greeting)}<Record{record} playBeep="true"/>'
    raise ValueError(f"{action!r} renders elsewhere")


def redirect_url(action: RedirectAction, conversation_id: str) -> str:
    return action.url + ("&" if "?" in action.url else "?") + urlencode({"conversation_id": conversation_id})
