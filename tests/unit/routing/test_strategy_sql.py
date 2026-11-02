"""get_agents_for_contact SQL builder (offline).

Pins the 4 strategies' ORDER BY, skill-is-a-filter (OR-combine + ' OR ' trim), the team-vs-agent
branch asymmetry (team: aom.status='active'; agent: aom.uuid IN), the UNION shape when both, and the
aom_routing_params join (non-Simultaneous only) — the structure that DETERMINES the returned agent
ORDER. The canonical byte-form is hand-written, so a structural/spacing drift is caught; every caller-supplied value is a bind parameter. No DB.
"""

import pytest

from hs.intake.models import SkillFilterModel
from hs.routing import constants as rc
from hs.routing import selection as S


def _build(routing_type, agents=("a1",), teams=(), skills=(), skip=()):
    return S.build_get_agents_query(list(agents), list(teams), list(skills), list(skip), routing_type, "MA_TEST")


def _q(routing_type, **kw):
    return _build(routing_type, **kw)[0]


def test_four_strategies_order_by():
    assert "ORDER BY" not in _q(rc.SIMULTANEOUS_ROUTING)  # Simultaneous: no ORDER BY
    assert _q(rc.AVAILABILITY_ROUTING).endswith("ORDER BY available_since")
    assert _q(rc.CONVERSATIONS_HANDLED_ROUTING).endswith("ORDER BY contacts_handled, available_since")
    assert _q(rc.CONVERSATIONS_HANDLE_TIME_ROUTING).endswith("ORDER BY contacts_handle_time, available_since")


def test_aom_routing_params_joined_only_for_metric_strategies():
    assert "aom_routing_params arp" not in _q(rc.SIMULTANEOUS_ROUTING)
    for rt in (rc.AVAILABILITY_ROUTING, rc.CONVERSATIONS_HANDLED_ROUTING, rc.CONVERSATIONS_HANDLE_TIME_ROUTING):
        assert "INNER JOIN aom_routing_params arp ON arp.aom=aom.id" in _q(rt)


def test_agent_only_simultaneous_byte_form_is_pinned():
    # Hand-derived from the builders (agent-only, Simultaneous, no skill/skip). The 2
    # leading spaces come from the empty `query` threaded through "%s %s %s %s FROM…".
    expected = (
        "  SELECT sip_endpoint from (SELECT DISTINCT aom.sip_endpoint as sip_endpoint "
        "FROM account_org_mapping aom LEFT JOIN aom_skills AS ask ON ask.aom=aom.id "
        "LEFT JOIN skills AS sk ON sk.id=ask.skill INNER JOIN org_statuses AS os ON os.id=aom.agent_status "
        "WHERE aom.uuid in :agents and aom.sip_endpoint is not NULL and os.availability = true AND aom.organization = "
        "(SELECT id FROM organization WHERE auth_id = :auth_id AND is_deleted IS NOT TRUE)) AS subquery"
    )
    assert _build(rc.SIMULTANEOUS_ROUTING, agents=("a1",)) == (expected, {"agents": ["a1"], "auth_id": "MA_TEST"})


def test_team_branch_requires_active_status_agent_branch_does_not():
    team = _q(rc.SIMULTANEOUS_ROUTING, agents=(), teams=("t1",))
    assert "INNER JOIN aom_team AS at ON at.aom=aom.id" in team
    assert "aom.status='active'" in team and "t.uuid IN :teams" in team
    agent = _q(rc.SIMULTANEOUS_ROUTING, agents=("a1",), teams=())
    assert "aom.status='active'" not in agent  # the agent branch filters by uuid, not status
    assert "aom.uuid in :agents" in agent


def test_union_when_both_team_and_agent():
    both = _q(rc.SIMULTANEOUS_ROUTING, agents=("a1",), teams=("t1",))
    assert "UNION " in both
    assert both.count("SELECT DISTINCT aom.sip_endpoint") == 2  # team subquery + agent subquery
    assert both.endswith(") AS subquery")  # outer subquery closed


def test_skill_filter_or_combines_and_trims_trailing_or():
    skills = [SkillFilterModel(skill="s1", operator=">", value=2), SkillFilterModel(skill="s2", operator="", value=0)]
    q, params = _build(rc.SIMULTANEOUS_ROUTING, agents=("a1",), skills=skills)
    assert " AND ((sk.uuid = :skill_0 AND ask.skill_level>:level_0) OR (sk.uuid = :skill_1))" in q
    assert params == {"agents": ["a1"], "auth_id": "MA_TEST", "skill_0": "s1", "level_0": 2, "skill_1": "s2"}
    assert ") OR )" not in q and " OR  AS" not in q  # the trailing ' OR ' was trimmed


def test_skip_agents_not_in():
    q, params = _build(rc.SIMULTANEOUS_ROUTING, agents=("a1",), skip=("sip:x", "sip:y"))
    assert "AND aom.sip_endpoint NOT IN :skip" in q and params["skip"] == ["sip:x", "sip:y"]


def test_caller_values_never_reach_the_sql_text():
    evil = "x') OR 1=1 --"
    q, params = _build(
        rc.SIMULTANEOUS_ROUTING,
        agents=(evil,),
        teams=(evil,),
        skills=[SkillFilterModel(skill=evil, operator=">", value=1)],
        skip=(evil,),
    )
    assert evil not in q
    assert params["agents"] == params["teams"] == params["skip"] == [evil] and params["skill_0"] == evil


def test_unknown_skill_operator_is_refused():
    with pytest.raises(ValueError):
        _q(rc.SIMULTANEOUS_ROUTING, skills=[SkillFilterModel(skill="s1", operator="> 0 OR 1=1 --", value=2)])


@pytest.mark.parametrize("blank", [" ", "  ", "\t"])
def test_blank_skill_operator_is_refused_with_the_query_error(blank, monkeypatch, caplog):
    import logging

    monkeypatch.setattr(S, "SessionLocal", lambda: type("DB", (), {"close": lambda self: None})())
    with caplog.at_level(logging.ERROR), pytest.raises(ValueError):
        S.get_agents_for_contact(
            ["a1"], [], [SkillFilterModel(skill="s1", operator=blank, value=2)], [], rc.SIMULTANEOUS_ROUTING, "MA_TEST"
        )
    assert any("get_agents_for_contact query error" in r.getMessage() for r in caplog.records)
    # the empty operator still means "no level filter"
    assert "skill_level" not in _q(rc.SIMULTANEOUS_ROUTING, skills=[SkillFilterModel(skill="s1", operator="", value=2)])


def test_padded_skill_operator_is_accepted_stripped():
    q = _q(rc.SIMULTANEOUS_ROUTING, skills=[SkillFilterModel(skill="s1", operator=" >= ", value=2)])
    assert " AND ((sk.uuid = :skill_0 AND ask.skill_level>=:level_0))" in q


def test_refused_operator_logs_the_query_error_and_raises(monkeypatch, caplog):
    import logging

    monkeypatch.setattr(S, "SessionLocal", lambda: type("DB", (), {"close": lambda self: None})())
    with caplog.at_level(logging.ERROR), pytest.raises(ValueError):
        S.get_agents_for_contact(
            ["a1"],
            [],
            [SkillFilterModel(skill="s1", operator="; DROP", value=2)],
            [],
            rc.SIMULTANEOUS_ROUTING,
            "MA_TEST",
        )
    assert any("get_agents_for_contact query error" in r.getMessage() for r in caplog.records)
