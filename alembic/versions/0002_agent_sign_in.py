"""Agent sign-in: hs.password keeps each agent's scrypt hash, and credential.renewable_until ends a chain of renewed
sessions.

Revision ID: 0002_agent_sign_in
Revises: 0001_baseline
Create Date: 2026-11-10
"""

from alembic import op

revision = "0002_agent_sign_in"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None

# Every request reads hs.credential, so a queued ALTER gives up on its lock after 5 seconds and fails the run.
LOCK = "SET LOCAL lock_timeout = '5s'; "


def upgrade() -> None:
    op.execute(
        LOCK + "CREATE TABLE hs.password (agent_id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES hs.tenant,"
        " hash text NOT NULL); ALTER TABLE hs.credential ADD COLUMN renewable_until timestamptz"
    )


def downgrade() -> None:
    op.execute(LOCK + "ALTER TABLE hs.credential DROP COLUMN renewable_until; DROP TABLE hs.password")
