"""Built-in statuses take the colors a new tenant's are created with, by key: the sign-in one green, the sign-out one
gray, the others amber. Only a built-in still gray or with no color changes, since a status edit stored gray when it
named no color; a status an admin created, or a built-in given another color, keeps its own. Downgrade keeps them.

Revision ID: 0003_status_colors
Revises: 0002_agent_sign_in
Create Date: 2026-11-15
"""

from alembic import op

revision = "0003_status_colors"
down_revision = "0002_agent_sign_in"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE hs.config SET body = body || jsonb_build_object('color', CASE key WHEN 'online' THEN '#27AE7D'"
        " WHEN 'offline' THEN '#9B9FAA' ELSE '#EBA000' END) WHERE kind = 'status' AND body->>'default' = 'true'"
        " AND upper(coalesce(body->>'color', '#9B9FAA')) = '#9B9FAA'"
    )


def downgrade() -> None:
    pass
