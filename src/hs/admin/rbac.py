"""Which agents a credential may see, from its role's level (fixed RBAC levels as a constant, no permission rows)."""

from sqlalchemy import text

from hs.access import LEVEL, Principal
from hs.infra.db import core_engine

_TEAMMATES = text(
    "SELECT mate.uuid::text FROM account_org_mapping me"
    " JOIN aom_team mine ON mine.aom = me.id AND mine.is_deleted IS NOT TRUE"
    " JOIN aom_team theirs ON theirs.team = mine.team AND theirs.is_deleted IS NOT TRUE"
    " JOIN account_org_mapping mate ON mate.id = theirs.aom AND mate.organization = :org"
    "  AND mate.is_deleted IS NOT TRUE"
    " WHERE me.uuid = CAST(:agent AS uuid) AND me.organization = :org"
)


def accessible_agents(p: Principal) -> set[str] | None:
    """The agent ids `p` may see: None for all of the tenant's (a tenant key, an org-level role), else itself and,
    for a team-level role, everyone in its teams."""
    level = "org" if p.kind == "key" else LEVEL.get(p.role)
    if level == "org":
        return None
    if level != "team":
        return {p.agent_id}
    with core_engine.connect() as conn:
        return {p.agent_id, *conn.execute(_TEAMMATES, {"org": p.org_id, "agent": p.agent_id}).scalars()}
