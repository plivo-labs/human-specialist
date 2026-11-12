"""Agent statuses, holiday groups and queue names as a contact center configures them."""

from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import Json

from tests.acceptance.conftest import DAYS, error, hangup_action, noon_zone, ok, queue_action, set_status, settings

STATUSES = "/api/v1/agent-statuses"
GROUPS = "/api/v1/holiday-groups"


def statuses(tenant) -> list[dict]:
    return ok(tenant.api.get(STATUSES, params={"limit": 200}))["data"]


def ordered(tenant) -> list[str]:
    return [s["id"] for s in sorted(statuses(tenant), key=lambda s: s["sequence"])]


def test_a_status_has_a_color_an_order_and_new_ones_come_first(tenant):
    lunch = ok(tenant.api.post(STATUSES, json={"name": "Lunch", "availability": False, "color": "#EB5757"}))
    assert lunch["color"] == "#EB5757" and lunch["default"] is False
    assert ordered(tenant)[0] == lunch["id"]
    assert all(s["default"] for s in statuses(tenant) if s["id"] != lunch["id"])
    error(tenant.api.post(STATUSES, json={"name": "Odd", "availability": False, "color": "red"}), 422)


def test_a_default_status_cannot_be_deleted(tenant):
    idle = next(s for s in statuses(tenant) if s["name"] == "Idle")
    error(tenant.api.delete(f"{STATUSES}/{idle['id']}"), 409, "status_default")


def test_a_default_status_can_be_renamed_and_recolored_but_keeps_its_availability(tenant):
    away = next(s for s in statuses(tenant) if s["name"] == "Away")
    done = ok(tenant.api.patch(f"{STATUSES}/{away['id']}", json={"name": "Out", "color": "#EB5757"}))
    assert (done["name"], done["color"], done["default"]) == ("Out", "#EB5757", True)
    error(tenant.api.patch(f"{STATUSES}/{away['id']}", json={"availability": True}), 409, "status_default")


def test_a_status_name_is_unique(tenant):
    error(tenant.api.post(STATUSES, json={"name": "Away", "availability": False}), 409, "status_name_taken")


def test_a_queue_name_is_unique_in_its_tenant(world, tenant):
    sales, support = tenant.queue(name="Sales"), tenant.queue(name="Support")
    error(tenant.api.post("/api/v1/queues", json={"name": "Sales"}), 409, "queue_name_taken")
    error(tenant.api.patch(f"/api/v1/queues/{support['id']}", json={"name": "Sales"}), 409, "queue_name_taken")
    assert ok(tenant.api.patch(f"/api/v1/queues/{sales['id']}", json={"name": "Sales", "priority": 3}))["priority"] == 3
    assert world.new_tenant("Other").queue(name="Sales")["name"] == "Sales"


def test_statuses_reorder(tenant):
    ids = [s["id"] for s in statuses(tenant)]
    ok(tenant.api.post(f"{STATUSES}/reorder", json={"ids": ids[::-1]}))
    assert ordered(tenant) == ids[::-1]


def today(zone: str) -> str:
    return datetime.now(ZoneInfo(zone)).date().isoformat()


def group(tenant, *holidays: dict) -> dict:
    return ok(tenant.api.post(GROUPS, json={"name": "National", "holidays": list(holidays)}))


def hours_in(tenant, **fields) -> dict:
    zone = noon_zone()
    slots = [{"day": d, "start": "00:00", "end": "23:59"} for d in DAYS]
    body = {"name": "Hours", "time_zone": zone, "slots": slots} | fields
    return ok(tenant.api.post("/api/v1/business-hours", json=body))


def is_open(tenant, hours: dict) -> bool:
    return ok(tenant.api.get(f"/api/v1/business-hours/{hours['id']}/status"))["open"]


def test_a_holiday_group_closes_the_hours_that_select_its_holiday(tenant):
    day = today(noon_zone())
    g = group(tenant, {"name": "Today", "date": day}, {"name": "Later", "date": "2099-01-01", "slots": []})
    chosen = hours_in(tenant, holiday_group_id=g["id"], selected_holidays=[day])
    skipped = hours_in(tenant, holiday_group_id=g["id"], selected_holidays=["2099-01-01"])
    assert (is_open(tenant, chosen), is_open(tenant, skipped)) == (False, True)
    ok(
        tenant.api.patch(
            f"{GROUPS}/{g['id']}",
            json={"holidays": [{"name": "Today", "date": day, "slots": [{"start": "00:00", "end": "23:59"}]}]},
        )
    )
    assert is_open(tenant, chosen) is True


def test_a_holiday_group_has_unique_holiday_names_and_dates_and_is_guarded(tenant):
    error(
        tenant.api.post(
            GROUPS,
            json={"name": "G", "holidays": [{"name": "A", "date": "2099-01-01"}, {"name": "B", "date": "2099-01-01"}]},
        ),
        422,
    )
    error(
        tenant.api.post(
            GROUPS,
            json={"name": "G", "holidays": [{"name": "A", "date": "2099-01-01"}, {"name": "A", "date": "2099-01-02"}]},
        ),
        422,
    )
    g = group(tenant, {"name": "A", "date": "2099-01-01"})
    error(
        tenant.api.post(
            "/api/v1/business-hours",
            json={"name": "H", "time_zone": "UTC", "holiday_group_id": "00000000-0000-4000-8000-000000000000"},
        ),
        422,
    )
    h = hours_in(tenant, holiday_group_id=g["id"])
    error(tenant.api.delete(f"{GROUPS}/{g['id']}"), 409, "holiday_group_in_use")
    ok(tenant.api.delete(f"/api/v1/business-hours/{h['id']}"))
    ok(tenant.api.delete(f"{GROUPS}/{g['id']}"))


def test_a_document_an_entry_point_or_queue_names_cannot_be_deleted_and_the_refusal_names_who_uses_it(tenant):
    hours, number, fallback = hours_in(tenant), tenant.number(), tenant.queue(name="Fallback")
    point = tenant.route_number(number, queue_action(fallback), business_hours_id=hours["id"])
    busy = tenant.queue(name="Busy", overflow_action=queue_action(fallback))
    refused = error(tenant.api.delete(f"/api/v1/business-hours/{hours['id']}"), 409, "hours_in_use")
    assert refused["details"]["used_by"] == [{"kind": "number", "id": point["id"], "number": number}]
    refused = error(tenant.api.delete(f"/api/v1/queues/{fallback['id']}"), 409, "queue_in_use")
    users = [{"kind": "number", "id": point["id"], "number": number}, {"kind": "queue", "id": busy["id"]}]
    assert sorted(refused["details"]["used_by"], key=lambda u: u["kind"]) == users
    tenant.route_number(number, hangup_action())
    ok(tenant.api.delete(f"/api/v1/business-hours/{hours['id']}"))


def status(tenant, name: str, **fields) -> dict:
    return ok(tenant.api.post(STATUSES, json={"name": name, "availability": False} | fields))


def held(tenant, agent) -> str:
    return ok(tenant.api.get(f"/api/v1/agents/{agent.id}/status"))["status"]["name"]


def test_deleting_a_status_moves_what_names_it_to_a_default_one(tenant):
    away, ready = status(tenant, "Lunch"), status(tenant, "Ready", availability=True)
    settings(
        tenant, sign_in={"status_id": away["id"]}, sign_out={"status_id": away["id"]}, rona={"status_id": away["id"]}
    )
    agent = tenant.agent().login(available=False)
    ok(set_status(tenant, agent, "Lunch"))
    ok(tenant.api.delete(f"{STATUSES}/{away['id']}"))
    got = ok(tenant.api.get("/api/v1/settings"))
    assert (got["sign_in"], got["sign_out"]) == (None, None)
    assert got["rona"]["status_id"] == tenant.status_id("Break") and held(tenant, agent) == "Break"
    ok(set_status(tenant, agent, "Ready"))
    ok(tenant.api.delete(f"{STATUSES}/{ready['id']}"))
    assert held(tenant, agent) == "Available"


def test_deleting_a_status_moves_an_uppercase_rona_reference(tenant):
    away = status(tenant, "Lunch")
    settings(tenant, rona={"status_id": away["id"].upper()})
    ok(tenant.api.delete(f"{STATUSES}/{away['id']}"))
    assert ok(tenant.api.get("/api/v1/settings"))["rona"]["status_id"] == tenant.status_id("Break")


def stored_past_api(world, tenant, **fields) -> None:
    with psycopg2.connect(world._env["DATABASE_URL"]) as db, db.cursor() as cur:
        cur.execute("UPDATE hs.tenant SET settings = settings || %s WHERE id = %s", (Json(fields), tenant.id))


def test_a_stored_legacy_rona_status_id_still_reads_and_patches(world, tenant):
    stored_past_api(world, tenant, rona={"strikes": 2, "status_id": "legacy-status"})
    assert ok(tenant.api.get("/api/v1/settings"))["rona"]["status_id"] == "legacy-status"
    ok(tenant.api.patch("/api/v1/settings", json={"wrap_up": {"mode": "manual", "seconds": 30}}))


def test_a_stored_legacy_status_id_is_checked_once_it_moves_to_another_field(world, tenant):
    stored_past_api(world, tenant, rona={"strikes": 2, "status_id": "legacy-status"})
    error(tenant.api.patch("/api/v1/settings", json={"sign_in": {"status_id": "legacy-status"}}), 422, "foreign_id")
    rona = ok(tenant.api.patch("/api/v1/settings", json={"rona": {"strikes": 3}}))["rona"]
    assert rona == {"strikes": 3, "status_id": "legacy-status"}


def test_a_stored_legacy_idle_status_id_still_patches(world, tenant):
    stored_past_api(world, tenant, idle={"status_id": "legacy-idle"})
    ok(tenant.api.patch("/api/v1/settings", json={"wrap_up": {"mode": "manual", "seconds": 30}}))


def test_a_status_color_is_any_hex_rgb(tenant):
    assert status(tenant, "Teal", color="#12a4B6")["color"] == "#12a4B6"
    for bad in ("red", "#12345", "#1234567", "#12345G"):
        error(tenant.api.post(STATUSES, json={"name": "Odd", "availability": False, "color": bad}), 422)
    lunch = status(tenant, "Lunch")
    error(tenant.api.patch(f"{STATUSES}/{lunch['id']}", json={"color": "red"}), 422)


def test_hours_select_only_dates_of_their_group(tenant):
    g = group(tenant, {"name": "A", "date": "2099-01-01"})
    body = {"name": "H", "time_zone": "UTC", "holiday_group_id": g["id"], "selected_holidays": ["2099-02-02"]}
    error(tenant.api.post("/api/v1/business-hours", json=body), 422)
    h = hours_in(tenant, holiday_group_id=g["id"], selected_holidays=["2099-01-01"])
    error(tenant.api.patch(f"/api/v1/business-hours/{h['id']}", json={"selected_holidays": ["2099-02-02"]}), 422)
    ok(tenant.api.patch(f"/api/v1/business-hours/{h['id']}", json={"selected_holidays": []}))


def test_stored_settings_with_keys_the_model_lacks_still_read_log_in_and_patch(world, tenant):
    stored_past_api(world, tenant, gone=1, transfer={"removed_key": 3}, wrap_up={"old": True})
    assert ok(tenant.api.get("/api/v1/settings"))["ring_timeout_sec"] == 30
    tenant.agent().login()
    ok(tenant.api.patch("/api/v1/settings", json={"ring_timeout_sec": 20}))
    assert ok(tenant.api.get("/api/v1/settings"))["ring_timeout_sec"] == 20
    for body in ({"gone": 1}, {"transfer": {"removed_key": 3}}):
        assert tenant.api.patch("/api/v1/settings", json=body).status_code == 422
