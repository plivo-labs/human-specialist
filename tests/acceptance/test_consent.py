"""Consent: played before connect inbound and on answer outbound, exactly once per call."""

from tests.acceptance.conftest import (
    answer_params,
    connect,
    dial,
    enqueue_as_plivo,
    hangup_action,
    in_order,
    lead,
    menu_line,
    queue_action,
    redirecting_line,
    registered_number,
    say,
    settings,
    unavailable,
    with_query,
)

NOTICE = "This call may be recorded"


def test_inbound_consent_plays_before_the_agent_is_connected(tenant):
    settings(tenant, consent={"inbound": say(NOTICE)})
    live = connect(tenant)
    assert live.caller.said[0] == NOTICE


def test_inbound_consent_can_be_an_audio_file(tenant):
    audio = "https://cdn.example.com/consent.mp3"
    settings(tenant, consent={"inbound": {"audio_url": audio}})
    assert connect(tenant).caller.said[0] == audio


def test_inbound_consent_plays_before_a_menu(world, tenant):
    settings(tenant, consent={"inbound": say(NOTICE)})
    number, _ = menu_line(tenant, {"1": hangup_action("Bye")})
    caller = world.plivo.call(number, digits="1")
    caller.wait_state("completed")
    assert in_order(caller.said, NOTICE, "Press a key", "Bye") and caller.said.count(NOTICE) == 1


def test_outbound_consent_plays_to_the_lead_on_answer(world, tenant):
    settings(tenant, consent={"outbound": say(NOTICE)})
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, to=number)
    stream.wait_for("call.answered", conversation_id=cid)
    leg = phone.legs[0]
    world.wait(lambda: NOTICE in leg.said, what="consent to the lead")
    assert leg.said.count(NOTICE) == 1


def test_overflow_does_not_replay_consent(world, tenant):
    settings(tenant, consent={"inbound": say(NOTICE)})
    backup = tenant.agent("Backup").login()
    team_q = tenant.queue(name="Team", agents=[backup.id])
    first = tenant.queue(
        name="First", agents=[unavailable(tenant).id], wait_timeout_sec=1, overflow_action=queue_action(team_q)
    )
    number = tenant.number()
    tenant.route_number(number, queue_action(first))
    caller = world.plivo.call(number)
    backup.phone.wait_ring(timeout=10)
    assert caller.said.count(NOTICE) == 1


def test_an_owner_first_call_hears_consent_once(world, tenant):
    settings(tenant, consent={"inbound": say(NOTICE)})
    owner, teammate = tenant.agent("Owner").login(), tenant.agent("Teammate").login()
    owner.phone.on_ring("ignore")
    team_q = tenant.queue(name="Team", agents=[teammate.id])
    owner_q = tenant.queue(name="Owner", agents=[owner.id], wait_timeout_sec=1, overflow_action=queue_action(team_q))
    caller = world.plivo.call(redirecting_line(tenant, with_query(owner_q["enqueue_url"], agents=owner.id)))
    owner.phone.wait_ring()
    teammate.phone.wait_ring(timeout=10)
    assert caller.said.count(NOTICE) == 1


def test_an_enqueue_url_plays_consent_unless_the_tenant_app_already_did(world, tenant):
    settings(tenant, consent={"inbound": say(NOTICE)})
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id])
    played = enqueue_as_plivo(world, tenant, q["enqueue_url"])
    assert played.status_code == 200 and NOTICE in played.text and "<MultiPartyCall" in played.text
    world.wait(lambda: tenant.events(type="conversation.started"), what="conversation opened at enqueue")
    skipped = enqueue_as_plivo(world, tenant, with_query(q["enqueue_url"], consent_played=1))
    assert skipped.status_code == 200 and NOTICE not in skipped.text and "<MultiPartyCall" in skipped.text


def test_an_outbound_hand_off_takes_the_outbound_consent_and_recording(world, tenant):
    consent = {"inbound": say("Inbound notice"), "outbound": say("Outbound notice")}
    settings(tenant, consent=consent, recording={"inbound": False, "outbound": True})
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id])
    params = answer_params("+16465550123", from_=registered_number(tenant), Direction="outbound")
    url = q["enqueue_url"]
    xml = world.anon().post(url, data=params, headers=world.plivo.sign(url, params, tenant.auth_token)).text
    assert "Outbound notice" in xml and "Inbound notice" not in xml and ' record="true"' in xml
