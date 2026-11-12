"""WhatsApp templates: those of each WhatsApp Business Account the tenant's settings name, read from Plivo's Template
API, and the checks an agent's WhatsApp sender and template pass against them. A tenant that names no account gets no
checks: Plivo judges its templates on send."""

import json
import re
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ValidationError
from sqlalchemy.engine import Row

from hs import access, config, hot, plivo
from hs.access import Principal
from hs.config import models as m
from hs.core import ApiError, Page, Paging, page, paging, read

router = APIRouter()
READ = access.require("key:calls", "agent", "sup")
PAGE = 20  # Plivo lists 20 templates a page
HEADERS = {"TEXT": "text", "IMAGE": "media", "VIDEO": "media", "DOCUMENT": "media", "LOCATION": "location"}
_PLACEHOLDER = re.compile(r"{{\s*\w+\s*}}")
_LOADING = defaultdict(threading.Lock)  # one load of an account at a time per replica; the rest then hit the cache


class Template(BaseModel):
    id: str
    whatsapp_account_id: str
    name: str
    namespace: str
    language: str
    category: str | None = None
    status: str
    components: list[dict] = []


class _Part(BaseModel):
    type: str
    parameters: list[dict] = []


class _Sent(BaseModel):
    name: str
    language: str
    namespace: str | None = None
    components: list[_Part] = []


def catalog(tenant: Row, account_id: str) -> list[dict]:
    """The account's templates as the list shows them, each with its components: Plivo lists summaries, so each
    template's own read adds them. The whole is cached for a minute."""
    path = f"WhatsApp/Template/{account_id}/"

    def load() -> list[dict]:
        found = []
        for offset in range(0, 1000, PAGE):  # An account's first 1,000; Meta lets most hold 250
            listed = plivo.rest(tenant, "GET", path, limit=PAGE, offset=offset)
            found += listed.get("objects") or []
            if not (listed.get("meta") or {}).get("next"):
                break
        with ThreadPoolExecutor(8) as pool:
            full = list(pool.map(lambda t: plivo.rest(tenant, "GET", f"{path}{t['template_id']}/"), found))
        return [
            t
            | {"id": t["template_id"], "whatsapp_account_id": account_id, "status": t["status"].lower()}
            | {"namespace": f"{account_id}-{t['name']}-{t['language']}"}
            | {"components": f.get("components") or [], "order": f"{t['name']} {t['language']} {t['template_id']}"}
            for t, f in zip(found, full, strict=True)
        ]

    with _LOADING[tenant.id, account_id]:
        return json.loads(hot.cached(tenant.id, f"whatsapp:{account_id}", load))


def _tenant(tenant_id) -> tuple[m.Settings, Row]:
    with read() as db:
        return config.tenant_settings(db, tenant_id), config.live_tenant(db, tenant_id)


def _misfit(sent: _Sent, parts: list[dict]) -> str | None:
    """Why the message does not fill its template, else None: one parameter of the header's format, or one per
    placeholder of a text header or the body, each with its value."""
    given = {c.type.lower(): c.parameters for c in sent.components}
    if extra := sorted({"header", "body"} & given.keys() - {str(p.get("type")).lower() for p in parts}):
        return f"the template has no {' or '.join(extra)}"
    for part in parts:
        kind, form = str(part.get("type")).lower(), str(part.get("format", "TEXT")).upper()
        if kind not in ("header", "body") or form not in HEADERS:
            continue
        params, of = given.get(kind, []), HEADERS[form] if form != "TEXT" else None
        n = 1 if of else len(_PLACEHOLDER.findall(part.get("text") or ""))
        if len(params) != n or not all(p.get(str(p.get("type"))) and of in (None, p.get("type")) for p in params):
            return f"the template's {kind} takes {n} {of + ' ' if of else ''}parameters, each with its value"
    return None


def check(tenant_id, sender: str | None, template: dict | None) -> None:
    """With WhatsApp accounts in the settings: the WhatsApp `sender`, else default_from, is a sender of one (422
    from_not_allowed), and `template` is that account's, approved, filled as it reads and with the `namespace` its
    `{account id}-{name}-{language}` (422 invalid_template).
    Called outside any transaction, since a cold catalog is read from Plivo."""
    s, tenant = _tenant(tenant_id)
    sender = sender or s.default_from.get("whatsapp")
    if not s.whatsapp.accounts or sender is None:
        return
    if (account := next((a for a in s.whatsapp.accounts if sender in a.senders), None)) is None:
        raise ApiError(422, "from_not_allowed", "a WhatsApp goes from a sender of the tenant's WhatsApp accounts")
    if template is None:
        return
    try:
        sent = _Sent.model_validate(template)
    except ValidationError as e:
        raise ApiError(422, "invalid_template", "a template has a name, a language and components") from e
    if sent.namespace != f"{account.id}-{sent.name}-{sent.language}":
        raise ApiError(422, "invalid_template", "a namespace reads {account id}-{name}-{language}")
    named = (t for t in catalog(tenant, account.id) if (t["name"], t["language"]) == (sent.name, sent.language))
    if (found := next(named, None)) is None or found["status"] != "approved":
        raise ApiError(422, "invalid_template", "the template is not an approved one of the sender's account")
    if problem := _misfit(sent, found["components"]):
        raise ApiError(422, "invalid_template", problem)


@router.get("/whatsapp/templates", response_model=Page[Template])
def list_templates(
    status: Literal["approved", "pending", "rejected", "paused", "disabled"] | None = None,
    sender: Annotated[str | None, Query(pattern=m.E164_PATTERN)] = None,
    p: Principal = READ,
    paged: Paging = Depends(paging),
):
    """The templates of the WhatsApp accounts in the settings, by name and language; `sender` keeps its account's
    (404 sender_not_found), `status` those in it."""
    s, tenant = _tenant(p.tenant_id)
    accounts = [a for a in s.whatsapp.accounts if sender is None or sender in a.senders]
    if sender and not accounts:
        raise ApiError(404, "sender_not_found", "no WhatsApp account of the tenant has this sender")
    after = paged.after or ""
    found, failed = [], []
    for a in accounts:
        try:
            found += catalog(tenant, a.id)
        except ApiError as e:  # plivo.rest logged it; the others still list
            failed.append(e)
    if failed and len(failed) == len(accounts):
        raise failed[0]
    rows = [t for t in found if status in (None, t["status"]) and t["order"] > after]
    return page(sorted(rows, key=lambda t: t["order"])[: paged.limit + 1], paged, "order")
