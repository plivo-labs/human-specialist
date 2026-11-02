"""Inbound routing as configuration: queues, business hours, menus and number entry points."""

from datetime import UTC, datetime
from datetime import date as Date
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, Path, Response
from fastapi.exceptions import RequestValidationError
from pydantic import Field, ValidationError, create_model, field_validator, model_validator
from sqlalchemy import (
    TIMESTAMP,
    Column,
    ForeignKey,
    Index,
    Integer,
    Table,
    Text,
    UniqueConstraint,
    bindparam,
    delete,
    func,
    insert,
    literal,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.dialects.postgresql import insert as upsert

from hs.access import AGENTS, SKILLS, TEAMS, Principal, as_uuid, owned, require, tenant_plivo
from hs.errors import PUBLIC, ApiError
from hs.infra.db import DBBase, core_engine
from hs.infra.settings import settings
from hs.paging import Page, Paging, page, paging
from hs.plivo import rest
from hs.setup.actions import (
    E164_PATTERN,
    Action,
    Body,
    HttpUrl,
    MenuAction,
    QueueAction,
    Say,
    VoicemailAction,
    plivo_url,
)

router = APIRouter()
ADMIN = Depends(require("key:admin"))
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MenuDigit = Literal["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "*"]
Digit = Literal[MenuDigit, "#"]
_USED = text(
    "SELECT EXISTS (SELECT 1 FROM (SELECT id, body FROM entry_points WHERE organization = :org"
    " UNION ALL SELECT id, body FROM queues WHERE organization = :org"
    " UNION ALL SELECT id, body FROM menus WHERE organization = :org) r"
    " WHERE r.id <> :id AND jsonb_path_exists("
    " r.body, '$.** ? (@.queue_id == $id || @.menu_id == $id)', jsonb_build_object('id', cast(:id as text))))"
)
HHMM = Annotated[str, Field(pattern=r"^(([01]\d|2[0-3]):[0-5]\d|24:00)$")]
STRATEGIES = ("Availability", "Simultaneous", "Lowest Conversations Handled", "Lowest Conversation Handle Time")
_APP_ID = text("SELECT plivo_app_id FROM org_setting WHERE organization = :org")


def _table(name: str, *extra) -> Table:
    return Table(
        name,
        DBBase.metadata,
        Column("id", PgUUID, primary_key=True, server_default=text("gen_random_uuid()")),
        Column("organization", Integer, ForeignKey("organization.id"), nullable=False),
        *extra,
        Column("body", JSONB, nullable=False),
        Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()),
        Index(f"ix_{name}_organization_id", "organization", "id"),
    )


queues = _table("queues")
hours_schedules = _table("hours_schedules")
menus = _table("menus")
entry_points = _table(
    "entry_points",
    Column("number", Text, nullable=False),
    UniqueConstraint("organization", "number", name="uq_entry_points_organization_number"),
)


class Skill(Body):
    skill_id: UUID
    operator: Literal["=", "!=", "<", "<=", ">", ">="] = ">="
    level: int = Field(ge=0, le=100)


class Sticky(Body):
    mode: Literal["loose", "strict"]
    expiry_hours: int | None = Field(None, ge=1, le=8760)  # None: never expires


class AutoAccept(Body):
    enabled: bool = False
    delay: int = Field(0, ge=0, le=60)


class HoldMusic(Body):
    customer: HttpUrl | None = None
    agent: HttpUrl | None = None


class QueueIn(Body):
    name: str = Field(min_length=1, max_length=200)
    strategy: Literal[STRATEGIES] = "Availability"
    teams: list[UUID] = []
    agents: list[UUID] = []
    skills: list[Skill] = []
    agents_at_once: int = Field(3, ge=1, le=10)
    retries: int = Field(3, ge=0, le=3, description="Extra offer rounds; always 0 for Simultaneous.")
    sticky: Sticky | None = None
    auto_accept: AutoAccept = AutoAccept()
    priority: int = Field(5, ge=1, le=10)
    dequeue_order: Literal["first-in-first-out", "last-in-first-out"] = "first-in-first-out"
    capacity: int = Field(50, ge=50, le=100)
    wait_timeout_sec: int = Field(20, ge=0, le=1800)
    hold_music: HoldMusic = HoldMusic()
    overflow_action: Action | None = None
    dtmf_actions: dict[Digit, Action] = {}

    @model_validator(mode="after")
    def _simultaneous(self):
        if self.strategy == "Simultaneous":
            self.retries = 0
        return self


class Queue(QueueIn):
    id: UUID
    enqueue_url: str


class Slot(Body):
    start: HHMM
    end: HHMM

    @model_validator(mode="after")
    def _ordered(self):
        if self.start >= self.end:
            raise ValueError("start must be before end")
        return self


class WeeklySlot(Slot):
    day: Literal[DAYS]


class Holiday(Body):
    date: Date
    slots: list[Slot] = Field([], description="Open only inside these slots; none: closed all day.")


class HoursIn(Body):
    name: str = Field(min_length=1, max_length=200)
    time_zone: str
    slots: list[WeeklySlot] = []
    holidays: list[Holiday] = []

    @field_validator("time_zone")
    @classmethod
    def _zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError("not an IANA time zone") from e
        return value


class Hours(HoursIn):
    id: UUID


class HoursStatus(Body):
    open: bool


class MenuIn(Body):
    name: str = Field(min_length=1, max_length=200)
    greeting: Say
    options: dict[MenuDigit, Action] = Field({}, max_length=9)
    input_timeout_sec: int = Field(5, ge=1, le=60)
    retries: int = Field(3, ge=0, le=10)
    invalid_action: Action | None = None


class Menu(MenuIn):
    options: dict[Digit, Action] = {}
    id: UUID
    menu_url: str


class EntryPointIn(Body):
    open_action: Action
    business_hours_id: UUID | None = None
    closed_action: Action | None = None


class EntryPoint(EntryPointIn):
    id: UUID
    number: str


def _ids_in(table: Table):
    """(id, 0) rows of `table` for `:ids` in `:org`, as `owned` takes them."""
    t = table.c
    return select(t.id.cast(Text), literal(0)).where(
        t.organization == bindparam("org"), t.id.in_(bindparam("ids", expanding=True))
    )


REFS = {
    "agents": AGENTS,
    "teams": TEAMS,
    "skills": SKILLS,
    "queues": _ids_in(queues),
    "menus": _ids_in(menus),
    "business_hours_id": _ids_in(hours_schedules),
}


def _refs(body: Body) -> dict[str, set]:
    """The ids a setup body, or one action, names, by kind."""
    found = {kind: set() for kind in REFS}
    actions = [body]
    match body:
        case QueueIn():
            found["agents"] |= set(body.agents)
            found["teams"] |= set(body.teams)
            found["skills"] |= {s.skill_id for s in body.skills}
            actions = [body.overflow_action, *body.dtmf_actions.values()]
        case MenuIn():
            actions = [*body.options.values(), body.invalid_action]
        case EntryPointIn():
            found["business_hours_id"] |= {body.business_hours_id} - {None}
            actions = [body.open_action, body.closed_action]
    for a in actions:
        match a:
            case QueueAction():
                found["queues"].add(a.queue_id)
                found["agents"] |= set(a.agents)
            case MenuAction():
                found["menus"].add(a.menu_id)
            case VoicemailAction():
                found["queues" if a.mailbox.queue_id else "agents"].add(a.mailbox.queue_id or a.mailbox.agent_id)
    return found


def check_refs(body: Body, p: Principal) -> None:
    """Every id the body names is the tenant's (422 foreign_id otherwise)."""
    for kind, ids in _refs(body).items():
        if ids:
            owned(REFS[kind], list(ids), p, kind)


def get(table: Table, org_id: int, row_id) -> dict | None:
    """A config row's body inside the tenant, or None."""
    if (row_id := as_uuid(row_id)) is None:
        return None
    query = select(table.c.body).where(table.c.organization == org_id, table.c.id == row_id)
    with core_engine.connect() as conn:
        return conn.execute(query).scalar()


def entry_point(org_id: int, number: str) -> tuple[str, dict] | None:
    """(entry point id, body) for a number of the tenant, or None."""
    query = select(entry_points.c.id, entry_points.c.body).where(
        entry_points.c.organization == org_id, entry_points.c.number == number
    )
    with core_engine.connect() as conn:
        row = conn.execute(query).first()
    return (str(row.id), row.body) if row else None


def is_registered(org_id: int, number: str) -> bool:
    """Whether the tenant routes this E.164 number (PUT /numbers), which makes it usable as a caller id."""
    return entry_point(org_id, number) is not None


def is_open(hours: dict, now: datetime | None = None) -> bool:
    """Open when a slot of the local day has start <= now < end; on a holiday only its own slots count."""
    local = (now or datetime.now(UTC)).astimezone(ZoneInfo(hours["time_zone"]))
    t, today = local.strftime("%H:%M"), local.date().isoformat()
    holiday = next((h for h in hours["holidays"] if h["date"] == today), None)
    slots = holiday["slots"] if holiday else [s for s in hours["slots"] if s["day"] == DAYS[local.weekday()]]
    return any(s["start"] <= t < s["end"] for s in slots)


def validated(model: type[Body], data: dict) -> Body:
    try:
        return model.model_validate(data)
    except ValidationError as e:
        raise RequestValidationError(e.errors()) from e


def _partial(model: type[Body]) -> type[Body]:
    """The PATCH body: every field optional; what is sent is merged onto the stored row and re-validated."""
    fields = {name: (f.annotation | None, None) for name, f in model.model_fields.items()}
    return create_model(f"{model.__name__.removesuffix('In')}Patch", __base__=Body, **fields)


def _unused(conn, org: int, table: Table, row_id: UUID) -> None:
    """409 while any setup row names the queue or menu; the row lock orders this against `_pin`."""
    conn.execute(select(table.c.id).where(table.c.id == str(row_id)).with_for_update())
    if table in (queues, menus) and conn.execute(_USED, {"org": org, "id": str(row_id)}).scalar():
        raise ApiError(409, f"{table.name[:-1]}_in_use", "a number, queue or menu names this")


def _pin(conn, org: int, body: Body, own: UUID | None = None) -> None:
    """Locks the queues and menus the body names, but not the row being written (`own`), so a delete waits for this
    write and then sees its reference, or this write sees the delete (422). KEY SHARE conflicts only with the
    delete's FOR UPDATE: two writes that name each other's rows cannot deadlock."""
    found = _refs(body)
    for table in (queues, menus):
        ids = {str(i) for i in found[table.name]} - {str(own)}
        locked = select(table.c.id).where(table.c.organization == org, table.c.id.in_(ids))
        if len(conn.execute(locked.with_for_update(read=True, key_share=True)).all()) != len(ids):
            raise ApiError(422, "foreign_id", "a queue or menu this names was just deleted")


def _patched(model: type[Body], current: dict, sent: dict) -> tuple[Body, dict]:
    """The row with `sent` merged and validated, and the options saved on "#" before it was refused, which ride along."""
    kept = {} if "options" in sent else {k: v for k, v in current.get("options", {}).items() if k == "#"}
    merged = {**current, **sent}
    if kept:
        merged["options"] = {k: v for k, v in merged["options"].items() if k not in kept}
    return validated(model, merged), kept


def _crud(path: str, table: Table, model: type[Body], out: type[Body], links) -> None:
    """POST, GET (list and one), PATCH and DELETE for one config resource; `links(tenant_id, id)` are its URLs."""
    kind = path.strip("/").replace("-", "_")
    patch_model = _partial(model)

    def view(p: Principal, row_id, body: dict) -> dict:
        return {**body, "id": str(row_id), **links(p.tenant_id, str(row_id))}

    def found(p: Principal, row_id: UUID) -> dict:
        if (body := get(table, p.org_id, row_id)) is None:
            raise ApiError(404, f"{kind.removesuffix('s')}_not_found", f"no such {kind.removesuffix('s')}")
        return body

    @router.post(path, status_code=201, response_model=out, name=f"create_{kind}")
    def create(body: model, p: Principal = ADMIN):
        check_refs(body, p)
        values = body.model_dump(mode="json")
        with core_engine.begin() as conn:
            _pin(conn, p.org_id, body)
            row_id = conn.execute(
                insert(table).values(organization=p.org_id, body=values).returning(table.c.id)
            ).scalar_one()
        return view(p, row_id, values)

    @router.get(path, response_model=Page[out], name=f"list_{kind}")
    def listed(p: Principal = ADMIN, paged: Paging = Depends(paging)):
        if paged.after and not as_uuid(paged.after):
            raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
        query = select(table.c.id, table.c.body).where(table.c.organization == p.org_id)
        if paged.after:
            query = query.where(table.c.id > paged.after)
        with core_engine.connect() as conn:
            rows = conn.execute(query.order_by(table.c.id).limit(paged.limit + 1)).all()
        return page([view(p, r.id, r.body) for r in rows], paged)

    @router.get(f"{path}/{{row_id}}", response_model=out, name=f"get_{kind}")
    def one(row_id: UUID, p: Principal = ADMIN):
        return view(p, row_id, found(p, row_id))

    @router.patch(f"{path}/{{row_id}}", response_model=out, name=f"patch_{kind}")
    def patch(row_id: UUID, body: patch_model, p: Principal = ADMIN):
        merged, kept = _patched(model, found(p, row_id), body.model_dump(mode="json", exclude_unset=True))
        check_refs(merged, p)
        values = merged.model_dump(mode="json")
        if kept:
            values["options"] |= kept
        with core_engine.begin() as conn:
            _pin(conn, p.org_id, merged, row_id)
            conn.execute(update(table).where(table.c.id == str(row_id)).values(body=values))
        return view(p, row_id, values)

    @router.delete(f"{path}/{{row_id}}", status_code=204, name=f"delete_{kind}")
    def remove(row_id: UUID, p: Principal = ADMIN) -> Response:
        found(p, row_id)
        with core_engine.begin() as conn:
            _unused(conn, p.org_id, table, row_id)
            conn.execute(delete(table).where(table.c.id == str(row_id)))
        return Response(status_code=204)


_crud(
    "/queues",
    queues,
    QueueIn,
    Queue,
    lambda _t, qid: {"enqueue_url": f"{settings.public_base_url}{PUBLIC}/queues/{qid}/enqueue"},
)
_crud("/business-hours", hours_schedules, HoursIn, Hours, lambda _t, _id: {})
_crud("/menus", menus, MenuIn, Menu, lambda t, mid: {"menu_url": plivo_url(t, "actions/menu", menu_id=mid)})


@router.get("/business-hours/{row_id}/status", response_model=HoursStatus)
def hours_status(row_id: UUID, p: Principal = ADMIN):
    if (hours := get(hours_schedules, p.org_id, row_id)) is None:
        raise ApiError(404, "business_hour_not_found", "no such business hours")
    return {"open": is_open(hours)}


NumberPath = Annotated[str, Path(pattern=E164_PATTERN)]


def _number(p: Principal, number: str) -> dict:
    if (found := entry_point(p.org_id, number)) is None:
        raise ApiError(404, "number_not_found", "no entry point for this number")
    return {**found[1], "id": found[0], "number": number}


@router.put("/numbers/{number}", response_model=EntryPoint)
def put_number(number: NumberPath, body: EntryPointIn, p: Principal = ADMIN):
    """Checks at Plivo that the number is the sub-account's, points it at the tenant's application, stores it."""
    check_refs(body, p)
    auth, digits = tenant_plivo(p.tenant_id), number.removeprefix("+")
    try:
        rest.ok(auth, "GET", f"Number/{digits}/")
    except ApiError as e:
        if e.details.get("status") == 404:
            raise ApiError(422, "number_not_in_account", "the Plivo sub-account does not hold this number") from e
        raise
    with core_engine.connect() as conn:
        app_id = conn.execute(_APP_ID, {"org": p.org_id}).scalar()
    rest.ok(auth, "POST", f"Number/{digits}/", app_id=app_id)
    values = body.model_dump(mode="json")
    row = upsert(entry_points).values(organization=p.org_id, number=number, body=values)
    row = row.on_conflict_do_update(constraint="uq_entry_points_organization_number", set_={"body": values})
    with core_engine.begin() as conn:
        _pin(conn, p.org_id, body)
        conn.execute(row)
    return _number(p, number)


@router.get("/numbers", response_model=Page[EntryPoint])
def list_numbers(p: Principal = ADMIN, paged: Paging = Depends(paging)):
    t = entry_points.c
    query = select(t.id, t.number, t.body).where(t.organization == p.org_id, t.number > (paged.after or ""))
    with core_engine.connect() as conn:
        rows = conn.execute(query.order_by(t.number).limit(paged.limit + 1)).all()
    return page([{**r.body, "id": str(r.id), "number": r.number} for r in rows], paged, key="number")


@router.get("/numbers/{number}", response_model=EntryPoint)
def get_number(number: NumberPath, p: Principal = ADMIN):
    return _number(p, number)


@router.delete("/numbers/{number}", status_code=204)
def delete_number(number: NumberPath, p: Principal = ADMIN) -> Response:
    _number(p, number)
    t = entry_points.c
    with core_engine.begin() as conn:
        conn.execute(delete(entry_points).where(t.organization == p.org_id, t.number == number))
    return Response(status_code=204)
