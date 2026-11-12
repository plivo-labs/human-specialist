"""baseline: schema hs, every table, index and trigger of the service"""

from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = """
CREATE TABLE hs.tenant (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    plivo_auth_id text NOT NULL UNIQUE,
    plivo_token_enc text NOT NULL,
    plivo_app_id text,
    settings jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    conversation_seq bigint
);

CREATE TABLE hs.credential (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sha256 text NOT NULL UNIQUE,
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    kind text NOT NULL,
    name text,
    prefix text,
    scopes text[] NOT NULL DEFAULT '{}',
    agent_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    revoked_at timestamptz
);
CREATE INDEX ix_credential_tenant_agent ON hs.credential (tenant_id, agent_id);
CREATE INDEX ix_credential_expires_at ON hs.credential (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE hs.idempotency (
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    key_sha256 text NOT NULL,
    fingerprint text NOT NULL,
    status int,
    body jsonb,
    resource_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, principal, key_sha256)
);

CREATE TABLE hs.config (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    kind text NOT NULL,
    key text,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_config_key ON hs.config (tenant_id, kind, key) WHERE key IS NOT NULL;
CREATE UNIQUE INDEX uq_config_agent_email ON hs.config (tenant_id, lower(body->>'email')) WHERE kind = 'agent';
CREATE INDEX ix_config_kind ON hs.config (tenant_id, kind, id);
CREATE INDEX ix_config_named ON hs.config
    USING gin ((jsonb_path_query_array(body, '$.**.queue_id') || jsonb_path_query_array(body, '$.**.menu_id')));

CREATE TABLE hs.presence (
    agent_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    status_id uuid NOT NULL,
    since timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz,
    beats int NOT NULL DEFAULT 0,
    misses int NOT NULL DEFAULT 0,
    signed_in_at timestamptz,
    first_available_at timestamptz,
    unreachable boolean NOT NULL DEFAULT false
);

CREATE SEQUENCE hs.conversation_number;
CREATE TABLE hs.conversation (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    number bigint NOT NULL DEFAULT nextval('hs.conversation_number'),
    channel text NOT NULL,
    direction text NOT NULL,
    state text NOT NULL,
    customer text NOT NULL,
    ours text NOT NULL DEFAULT '',
    call_uuid text UNIQUE,
    room int NOT NULL DEFAULT 0,
    queue_id uuid,
    queued_at timestamptz,
    priority int NOT NULL DEFAULT 0,
    round int NOT NULL DEFAULT 0,
    routing jsonb NOT NULL DEFAULT '{}',
    context jsonb,
    attributes jsonb NOT NULL DEFAULT '{}',
    result text,
    version bigint NOT NULL DEFAULT 0,
    opened_at timestamptz NOT NULL DEFAULT now(),
    answered_at timestamptz,
    closed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    ended_by text,
    team_ids jsonb,
    csat text,
    hold_count int NOT NULL DEFAULT 0,
    hold_sec int NOT NULL DEFAULT 0,
    held_at timestamptz,
    handle_sec int,
    wrap_sec int,
    response_sec int,
    first_reply_sec int,
    transfers int,
    disposition_id uuid,
    missed_offers int,
    UNIQUE (tenant_id, number)
);
CREATE INDEX ix_conversation_queued ON hs.conversation (tenant_id, queue_id, priority, queued_at)
    WHERE state = 'queued';
CREATE UNIQUE INDEX uq_conversation_open_text ON hs.conversation (tenant_id, channel, customer, ours)
    WHERE state IN ('open', 'queued', 'live') AND channel IN ('chat', 'sms', 'whatsapp', 'email');
CREATE INDEX ix_conversation_customer ON hs.conversation (tenant_id, customer, opened_at);
CREATE INDEX ix_conversation_opened ON hs.conversation (tenant_id, opened_at, id);
CREATE INDEX ix_conversation_closed ON hs.conversation (closed_at, id) WHERE state = 'closed';
CREATE INDEX ix_conversation_unreported ON hs.conversation (tenant_id, opened_at) WHERE transfers IS NULL;
CREATE INDEX ix_conversation_answered ON hs.conversation (tenant_id, queue_id, queued_at)
    WHERE channel = 'call' AND answered_at IS NOT NULL;
CREATE INDEX ix_conversation_attributes ON hs.conversation USING gin (attributes jsonb_path_ops);

CREATE TABLE hs.assignment (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    conversation_id uuid NOT NULL REFERENCES hs.conversation,
    agent_id uuid,
    role text NOT NULL,
    via text NOT NULL,
    mode text,
    channel text NOT NULL,
    state text NOT NULL,
    round int NOT NULL DEFAULT 0,
    ring uuid,
    address text,
    call_uuid text,
    member_id text,
    from_assignment uuid,
    offered_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    ended_at timestamptz,
    wrap_up_until timestamptz,
    disposition_id uuid,
    sub_disposition_id uuid,
    note text,
    reason text,
    transfer_id uuid,
    held_by_room int,
    submitted_at timestamptz,
    team uuid,
    transfers_told jsonb,
    dialed text,
    queue_id uuid,
    queued_at timestamptz
);
CREATE INDEX ix_assignment_capacity ON hs.assignment (agent_id, channel, state)
    WHERE state IN ('offered', 'active', 'wrap_up');
CREATE UNIQUE INDEX uq_assignment_one_primary ON hs.assignment (conversation_id)
    WHERE role = 'primary' AND state = 'active';
CREATE UNIQUE INDEX uq_assignment_ring_address ON hs.assignment (ring, address) WHERE ring IS NOT NULL;
CREATE UNIQUE INDEX uq_assignment_call_uuid ON hs.assignment (call_uuid) WHERE call_uuid IS NOT NULL;
CREATE INDEX ix_assignment_conversation ON hs.assignment (conversation_id);
CREATE INDEX ix_assignment_agent_ended ON hs.assignment (tenant_id, agent_id, ended_at);
CREATE INDEX ix_assignment_from ON hs.assignment (from_assignment) WHERE from_assignment IS NOT NULL;
CREATE INDEX ix_assignment_queued ON hs.assignment (queue_id, queued_at) WHERE queue_id IS NOT NULL;

CREATE TABLE hs.entry (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    conversation_id uuid NOT NULL REFERENCES hs.conversation,
    kind text NOT NULL,
    agent_id uuid,
    at timestamptz NOT NULL DEFAULT now(),
    body jsonb NOT NULL DEFAULT '{}',
    provider_id text,
    status text,
    deleted_at timestamptz,
    written_at timestamptz DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX uq_entry_provider_conversation ON hs.entry (tenant_id, provider_id, conversation_id)
    WHERE provider_id IS NOT NULL;
CREATE INDEX ix_entry_conversation ON hs.entry (conversation_id, at, id);
CREATE INDEX ix_entry_kind ON hs.entry (tenant_id, kind, at);

CREATE TABLE hs.task (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    conversation_id uuid NOT NULL REFERENCES hs.conversation,
    type text NOT NULL,
    title text NOT NULL,
    description text,
    status text NOT NULL,
    priority text NOT NULL DEFAULT 'medium',
    destination jsonb NOT NULL,
    due_at timestamptz,
    scheduled_at timestamptz,
    customer text,
    created_by uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    attachments jsonb NOT NULL DEFAULT '[]',
    origin_id uuid REFERENCES hs.conversation,
    schedule jsonb,
    updated_by uuid,
    channel text,
    routing_status text
);
CREATE INDEX ix_task_created ON hs.task (tenant_id, created_at, id);
CREATE INDEX ix_task_conversation ON hs.task (conversation_id);
CREATE INDEX ix_task_origin ON hs.task (origin_id) WHERE origin_id IS NOT NULL;

CREATE SEQUENCE hs.event_seq;
CREATE TABLE hs.events (
    id bigserial PRIMARY KEY,
    seq bigint,
    tenant_id text NOT NULL,
    type text NOT NULL,
    conversation_id text,
    agent_id text,
    direction text,
    context jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    data jsonb NOT NULL,
    queue_id uuid
);
CREATE INDEX ix_events_tenant_seq ON hs.events (tenant_id, seq);
CREATE INDEX ix_events_seq ON hs.events (seq) WHERE seq IS NOT NULL;
CREATE INDEX ix_events_unstamped ON hs.events (id) WHERE seq IS NULL;
CREATE INDEX ix_events_conversation_seq ON hs.events (conversation_id, seq);
CREATE INDEX ix_events_created_at ON hs.events (created_at);
CREATE INDEX ix_events_queued_queue ON hs.events (tenant_id, (data->>'queue_id'))
    WHERE type = 'conversation.queued';

CREATE TABLE hs.webhooks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id text NOT NULL,
    url text NOT NULL,
    secret text NOT NULL,
    previous_secret text,
    previous_secret_expires_at timestamptz,
    event_types text[] NOT NULL DEFAULT '{}',
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    queue_ids uuid[] NOT NULL DEFAULT '{}'
);
CREATE INDEX ix_webhooks_tenant ON hs.webhooks (tenant_id);

CREATE TABLE hs.jobs (
    id bigserial PRIMARY KEY,
    kind text NOT NULL,
    lane text NOT NULL DEFAULT 'control',
    key text,
    run_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    attempts int NOT NULL DEFAULT 0,
    locked_until timestamptz,
    last_error text,
    rev int NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX uq_jobs_kind_key ON hs.jobs (kind, key) WHERE key IS NOT NULL;
CREATE INDEX ix_jobs_lane_due ON hs.jobs (lane, run_at) WHERE locked_until IS NULL OR locked_until <> 'infinity';
CREATE INDEX ix_jobs_offer_agent ON hs.jobs ((payload->>'agent_id')) WHERE kind = 'offer_timeout';
CREATE INDEX ix_jobs_offer_conversation ON hs.jobs ((payload->>'conversation_id')) WHERE kind = 'offer_timeout';

CREATE TABLE hs.hook_cursor (
    name text PRIMARY KEY,
    seq bigint NOT NULL DEFAULT 0,
    failures int NOT NULL DEFAULT 0,
    last_error text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    held_until timestamptz,
    held_by uuid
);

CREATE TABLE hs.search (
    conversation_id uuid PRIMARY KEY REFERENCES hs.conversation,
    version bigint NOT NULL,
    team_ids text[] NOT NULL,
    disposition_ids text[] NOT NULL,
    words tsvector NOT NULL,
    content text,
    disposition text,
    wrap_up_note text,
    ended_by text,
    duration_sec int NOT NULL DEFAULT 0,
    talk_sec int NOT NULL DEFAULT 0,
    wrap_sec int NOT NULL DEFAULT 0,
    response_sec int NOT NULL DEFAULT 0,
    message_count int NOT NULL DEFAULT 0,
    conversation_state text,
    voicemail boolean NOT NULL DEFAULT false,
    hold_sec int NOT NULL DEFAULT 0,
    messages text[],
    types text[],
    hold_count int NOT NULL DEFAULT 0,
    csat text,
    ours text
);
CREATE INDEX ix_search_words ON hs.search USING gin (words);
CREATE INDEX ix_search_state ON hs.search (lower(conversation_state));
CREATE INDEX ix_search_csat ON hs.search (lower(csat));
CREATE INDEX ix_search_ours ON hs.search (lower(ours));
CREATE INDEX ix_search_voicemail ON hs.search (conversation_id) WHERE voicemail;

CREATE TABLE hs.status_history (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id bigint NOT NULL UNIQUE,
    tenant_id uuid NOT NULL REFERENCES hs.tenant,
    agent_id uuid NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    until timestamptz,
    status_id uuid NOT NULL,
    status text NOT NULL,
    available boolean
);
CREATE INDEX ix_status_history_at ON hs.status_history (tenant_id, at, id);
CREATE INDEX ix_status_history_agent ON hs.status_history (tenant_id, agent_id, at, id);
"""
# The UPDATE reads the table as it was before the INSERT, so the agent's latest row is the change this one ends.
HISTORY = """CREATE FUNCTION hs.write_status_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    WITH added AS (INSERT INTO hs.status_history (event_id, tenant_id, agent_id, status_id, status, available)
        SELECT e.id, t.id, CAST(e.agent_id AS uuid), CAST(e.data->'to_status'->>'id' AS uuid), e.data->'to_status'->>'name', coalesce(CAST(e.data->>'available' AS boolean), CAST(s.body->>'availability' AS boolean)) FROM hs.events e JOIN hs.tenant t ON CAST(t.id AS text) = e.tenant_id LEFT JOIN hs.config s ON s.id = CAST(e.data->'to_status'->>'id' AS uuid) WHERE e.id = NEW.id RETURNING tenant_id, agent_id, at)
    UPDATE hs.status_history h SET until = added.at FROM added WHERE h.id = (SELECT id FROM hs.status_history
        WHERE tenant_id = added.tenant_id AND agent_id = added.agent_id ORDER BY at DESC, id DESC LIMIT 1);
    RETURN NULL;
END $$;
CREATE TRIGGER write_status_history AFTER INSERT ON hs.events FOR EACH ROW WHEN (NEW.type = 'agent.status_changed')
    EXECUTE FUNCTION hs.write_status_history()"""


def upgrade() -> None:
    op.execute(SCHEMA)
    op.execute(HISTORY)


def downgrade() -> None:
    op.execute(
        "DROP TABLE hs.tenant, hs.credential, hs.idempotency, hs.config, hs.presence, hs.conversation, hs.assignment,"
        " hs.entry, hs.task, hs.events, hs.webhooks, hs.jobs, hs.hook_cursor, hs.search, hs.status_history;"
        " DROP SEQUENCE hs.conversation_number, hs.event_seq; DROP FUNCTION hs.write_status_history()"
    )
