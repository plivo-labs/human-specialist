"""initial schema: every table the engine reads and writes

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-10-30
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None

TABLES = (
    "account",
    "active_queue_entry",
    "agents",
    "call_leg",
    "closed_conversation_search",
    "number",
    "organization",
    "role",
    "skills",
    "skills_filter",
    "team",
    "teams",
    "conversation_custom_status_mapping",
    "conversation_parent",
    "conversation_priority",
    "dispositions",
    "org_setting",
    "org_statuses",
    "recording",
    "routing_settings",
    "transcription",
    "account_org_mapping",
    "customer_conversation_data",
    "org_event_actions",
    "transcription_segment",
    "aom_role_profile",
    "aom_routing_params",
    "aom_skills",
    "aom_team",
    "conversation",
    "conversation_secondary_aom_mapping",
    "conversation_chunk",
    "api_keys",
    "events",
    "webhooks",
    "jobs",
    "queues",
    "hours_schedules",
    "menus",
    "entry_points",
)
CONFIG_TABLES = ("queues", "hours_schedules", "menus", "entry_points")


def _core_table(name: str, *items) -> None:
    """A table with the shared CoreDBBase columns: a composite (id, uuid) key, both unique, plus timestamps."""
    op.create_table(
        name,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("is_deleted", sa.Boolean()),
        *items,
        sa.PrimaryKeyConstraint("id", "uuid"),
        sa.UniqueConstraint("id"),
        sa.UniqueConstraint("uuid"),
    )


def upgrade() -> None:
    _core_table(
        "account",
        sa.Column("email", sa.String()),
        sa.Column("name", sa.String()),
        sa.Column("first_name", sa.String()),
        sa.Column("last_name", sa.String()),
    )
    _core_table(
        "active_queue_entry",
        sa.Column("queue_priority", sa.Integer(), nullable=False),
        sa.Column("queue_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("contact_uuid", sa.String(), nullable=False),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("flow_run_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("queue_type", sa.String(), nullable=False),
        sa.Column("queue_channel", sa.String(), nullable=False),
        sa.Column("contact_phone_or_email", sa.String(), nullable=False),
        sa.Column("version", sa.BigInteger()),
        sa.Column("qnr_run_uuid", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("auth_id", sa.String(), nullable=False),
    )
    _core_table(
        "agents",
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("agent_id", sa.String(), nullable=False),
        sa.Column("active_queue_entry_id", sa.BigInteger()),
    )
    _core_table(
        "call_leg",
        sa.Column("call_uuid", sa.String()),
        sa.Column("participant", sa.String()),
        sa.Column("start_time", sa.String()),
        sa.Column("end_time", sa.String()),
        sa.Column("role", sa.String()),
        sa.Column("parent_call_leg", sa.Integer()),
        sa.ForeignKeyConstraint(["parent_call_leg"], ["call_leg.id"]),
    )
    op.create_index("ix_call_leg_call_uuid", "call_leg", ["call_uuid"])
    op.create_index("ix_call_leg_parent_call_leg", "call_leg", ["parent_call_leg"])
    _core_table(
        "closed_conversation_search",
        sa.Column("conv_number", sa.String(), nullable=False),
        sa.Column("organization", sa.Integer(), nullable=False),
        sa.Column("conv_created_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("conv_updated_at_ts", sa.TIMESTAMP(timezone=True)),
        sa.Column("agent_ids", postgresql.JSONB()),
        sa.Column("team_ids", postgresql.JSONB()),
        sa.Column("search_vector", postgresql.TSVECTOR()),
        sa.Column("customer_name", sa.String()),
        sa.Column("crm_contact_id", sa.String()),
        sa.Column("case_id", sa.String()),
        sa.Column("direction_inbound", sa.Boolean()),
        sa.Column("parent_channel", sa.String()),
        sa.Column("contact_identifier", sa.String()),
        sa.Column("conv_created_time", sa.String()),
        sa.Column("conv_created_date", sa.String()),
        sa.Column("conv_updated_at", sa.String()),
        sa.Column("conv_created_at_str", sa.String()),
        sa.Column("agent_details", postgresql.JSONB()),
        sa.Column("disposition", sa.String()),
        sa.Column("disposition_uuid", sa.String()),
        sa.Column("sub_disposition", sa.String()),
        sa.Column("sub_disposition_uuid", sa.String()),
        sa.Column("wrap_up_note", sa.Text()),
        sa.Column("conv_message_count", sa.Integer()),
        sa.Column("conv_channels", postgresql.JSONB()),
        sa.Column("conv_total_duration", sa.Numeric()),
        sa.Column("agent_customer_conv_duration", sa.Numeric()),
        sa.Column("conv_response_time", sa.Numeric()),
        sa.Column("conv_wrapup_duration", sa.Numeric()),
        sa.Column("conv_child_customer_contacts", postgresql.JSONB()),
        sa.Column("conv_child_sender_ids", postgresql.JSONB()),
        sa.Column("is_conv_answered", sa.Boolean()),
        sa.Column("conv_states", postgresql.JSONB()),
        sa.Column("conv_state", sa.String()),
        sa.Column("conv_state_uuid", sa.String()),
        sa.Column("conv_chunks", postgresql.JSONB()),
        sa.Column("org_uuid", sa.String()),
        sa.Column("conv_chunk_descriptions", postgresql.JSONB()),
        sa.Column("conv_chunk_message_types", postgresql.JSONB()),
        sa.Column("csat_feedback", sa.String()),
        sa.Column("hangup_source", sa.String()),
        sa.Column("custom_data", postgresql.JSONB()),
        sa.Column("customer_data", postgresql.JSONB()),
        sa.Column("customer_custom_data", postgresql.JSONB()),
        sa.Column("external_identifer", sa.String()),
        sa.Column("client_data", postgresql.JSONB()),
    )
    op.create_index(
        "ix_closed_conv_search_agent_ids", "closed_conversation_search", ["agent_ids"], postgresql_using="gin"
    )
    op.create_index(
        "ix_closed_conv_search_org_channel", "closed_conversation_search", ["organization", "parent_channel"]
    )
    op.create_index(
        "ix_closed_conv_search_org_created", "closed_conversation_search", ["organization", "conv_created_at"]
    )
    op.create_index("ix_closed_conv_search_org_dispo", "closed_conversation_search", ["organization", "disposition"])
    op.create_index("ix_closed_conv_search_org_state", "closed_conversation_search", ["organization", "conv_state"])
    op.create_index(
        "ix_closed_conv_search_vector", "closed_conversation_search", ["search_vector"], postgresql_using="gin"
    )
    op.create_index("ix_closed_conversation_search_conv_created_at", "closed_conversation_search", ["conv_created_at"])
    op.create_index(
        "ix_closed_conversation_search_conv_number", "closed_conversation_search", ["conv_number"], unique=True
    )
    op.create_index("ix_closed_conversation_search_organization", "closed_conversation_search", ["organization"])
    _core_table("number", sa.Column("number", sa.String()), sa.Column("is_sender_id", sa.Boolean()))
    _core_table(
        "organization",
        sa.Column("name", sa.String()),
        sa.Column("auth_id", sa.String()),
        sa.Column("auth_token", sa.String()),
        sa.Column("skip_contact_creation", sa.Boolean()),
        sa.Column("skip_case_creation", sa.Boolean()),
        sa.Column("allow_acw_post_transfer", sa.Boolean()),
        sa.Column("is_active", sa.Boolean()),
        sa.Column("allow_transfer_type", sa.Boolean()),
        sa.Column("default_transfer_type", sa.String()),
        sa.Column("call_recording_inbound", sa.Boolean()),
        sa.Column("call_recording_outbound", sa.Boolean()),
        sa.Column("allow_recording_pause_play", sa.Boolean()),
        sa.Column("plivo_token_enc", sa.Text()),
    )
    # One live tenant per sub-account: the engine looks tenants up by auth id.
    op.create_index(
        "ux_organization_live_auth_id",
        "organization",
        ["auth_id"],
        unique=True,
        postgresql_where=sa.text("is_deleted IS NOT TRUE"),
    )
    _core_table(
        "role",
        sa.Column("organization", sa.Integer()),
        sa.Column("name", sa.String()),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "skills",
        sa.Column("organization", sa.Integer()),
        sa.Column("name", sa.String()),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "skills_filter",
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("skill_id", sa.String(), nullable=False),
        sa.Column("operator", sa.String(), nullable=False),
        sa.Column("value", sa.Integer(), nullable=False),
        sa.Column("active_queue_entry_id", sa.BigInteger()),
    )
    _core_table(
        "team",
        sa.Column("organization", sa.Integer()),
        sa.Column("name", sa.String()),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "teams",
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("team_id", sa.String(), nullable=False),
        sa.Column("active_queue_entry_id", sa.BigInteger()),
    )
    _core_table(
        "conversation_custom_status_mapping",
        sa.Column("organization", sa.Integer()),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "conversation_parent",
        sa.Column("organization", sa.Integer()),
        sa.Column("case_id", sa.String()),
        sa.Column("contact_identifier", sa.String()),
        sa.Column("contact_uuid", sa.String()),
        sa.Column("crm_contact_id", sa.String()),
        sa.Column("end_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("last_activity_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("direction_inbound", sa.Boolean(), nullable=False),
        sa.Column("conversation_number", sa.String()),
        sa.Column("contact_name", sa.String()),
        sa.Column("flow_closure_flag", sa.Boolean()),
        sa.Column("is_hidden", sa.Boolean()),
        sa.Column("flow_run_uuid", postgresql.UUID(as_uuid=True)),
        sa.Column("external_identifier", sa.String()),
        sa.Column("result", sa.Text()),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    op.create_index("ix_conversation_parent_contact_uuid", "conversation_parent", ["contact_uuid"])
    _core_table(
        "conversation_priority",
        sa.Column("organization", sa.Integer()),
        sa.Column("label", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "dispositions",
        sa.Column("organization", sa.Integer()),
        sa.Column("name", sa.String()),
        sa.Column("sequence", sa.Integer()),
        sa.Column("answered", sa.Boolean()),
        sa.Column("internal", sa.Boolean()),
        sa.Column("state", sa.Boolean()),
        sa.Column("parent_disposition_id", sa.Integer()),
        sa.Column("note_required", sa.Boolean(), nullable=False, server_default="false"),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
        sa.ForeignKeyConstraint(["parent_disposition_id"], ["dispositions.id"]),
    )
    _core_table(
        "org_setting",
        sa.Column("organization", sa.Integer()),
        sa.Column("push_contact_details_async_flag", sa.Boolean()),
        sa.Column("conversation_timeout", postgresql.JSON()),
        sa.Column("plivo_app_id", sa.Text()),
        sa.Column("voice_settings", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'")),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "org_statuses",
        sa.Column("organization", sa.Integer()),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("availability", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "recording",
        sa.Column("recording_duration", sa.String()),
        sa.Column("recording_uuid", sa.String()),
        sa.Column("recording_url", sa.String()),
        sa.Column("recording_start_time", sa.String()),
        sa.Column("recording_end_time", sa.String()),
        sa.Column("recording_format", sa.String()),
        sa.Column("call_leg_id", sa.Integer()),
        sa.ForeignKeyConstraint(["call_leg_id"], ["call_leg.id"]),
    )
    _core_table(
        "routing_settings",
        sa.Column("organization", sa.Integer()),
        sa.Column("release_capacity_on_resolve", sa.Boolean()),
        sa.Column("wrap_up_configuration", postgresql.JSON()),
        sa.Column("capacity", postgresql.JSON()),
        sa.Column("reservation_timeout", postgresql.JSON()),
        sa.Column("simultaneous_agent_limit", sa.Integer()),
        sa.Column("auto_accept", sa.Boolean()),
        sa.Column("outbound_sticky_routing", postgresql.JSON()),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "transcription",
        sa.Column("call_leg_id", sa.Integer(), nullable=False),
        sa.Column("language", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["call_leg_id"], ["call_leg.id"]),
    )
    _core_table(
        "account_org_mapping",
        sa.Column("account", sa.Integer()),
        sa.Column("organization", sa.Integer()),
        sa.Column("sip_endpoint", sa.String()),
        sa.Column("sip_endpoint_id", sa.String()),
        sa.Column("status", sa.String()),
        sa.Column("agent_status", sa.Integer()),
        sa.Column("role", sa.Integer()),
        sa.Column("caller_id", sa.String()),
        sa.ForeignKeyConstraint(["agent_status"], ["org_statuses.id"]),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
        sa.ForeignKeyConstraint(["role"], ["role.id"]),
    )
    _core_table(
        "customer_conversation_data",
        sa.Column("conversation_parent_id", sa.Integer()),
        sa.Column("customer_data", postgresql.JSON()),
        sa.Column("customer_custom_data", postgresql.JSON()),
        sa.Column("conversation_custom_data", postgresql.JSON()),
        sa.Column("external_identifier", sa.String()),
        sa.Column("client_data", postgresql.JSON()),
        sa.ForeignKeyConstraint(["conversation_parent_id"], ["conversation_parent.id"]),
    )
    _core_table(
        "org_event_actions",
        sa.Column("organization", sa.Integer()),
        sa.Column("event", sa.String()),
        sa.Column("agent_status_action", sa.Integer()),
        sa.Column("event_metadata", sa.JSON()),
        sa.ForeignKeyConstraint(["agent_status_action"], ["org_statuses.id"]),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "transcription_segment",
        sa.Column("transcription_id", sa.Integer(), nullable=False),
        sa.Column("start_time", sa.Float(), nullable=False),
        sa.Column("end_time", sa.Float(), nullable=False),
        sa.Column("segment", sa.String(), nullable=False),
        sa.Column("start_ts", sa.Float()),
        sa.ForeignKeyConstraint(["transcription_id"], ["transcription.id"]),
    )
    _core_table(
        "aom_role_profile",
        sa.Column("organization", sa.Integer()),
        sa.Column("aom", sa.Integer()),
        sa.Column("role", sa.Integer()),
        sa.ForeignKeyConstraint(["aom"], ["account_org_mapping.id"]),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
        sa.ForeignKeyConstraint(["role"], ["role.id"]),
    )
    _core_table(
        "aom_routing_params",
        sa.Column("available_since", sa.TIMESTAMP(timezone=True)),
        sa.Column("contacts_handle_time", sa.Integer()),
        sa.Column("contacts_handled", sa.Integer()),
        sa.Column("aom", sa.Integer()),
        sa.ForeignKeyConstraint(["aom"], ["account_org_mapping.id"]),
    )
    _core_table(
        "aom_skills",
        sa.Column("aom", sa.Integer()),
        sa.Column("skill", sa.Integer()),
        sa.Column("skill_level", sa.Integer()),
        sa.ForeignKeyConstraint(["aom"], ["account_org_mapping.id"]),
        sa.ForeignKeyConstraint(["skill"], ["skills.id"]),
    )
    _core_table(
        "aom_team",
        sa.Column("organization", sa.Integer()),
        sa.Column("aom", sa.Integer()),
        sa.Column("team", sa.Integer()),
        sa.ForeignKeyConstraint(["aom"], ["account_org_mapping.id"]),
        sa.ForeignKeyConstraint(["team"], ["team.id"]),
        sa.ForeignKeyConstraint(["organization"], ["organization.id"]),
    )
    _core_table(
        "conversation",
        sa.Column("primary_aom", sa.Integer()),
        sa.Column("parent", sa.Integer(), nullable=False),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("generation_type", sa.String(), nullable=False),
        sa.Column("start_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("end_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("contact_creation_start_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("case_creation_start_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("wrap_up_start_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("contact_uuid", sa.String()),
        sa.Column("active_queue_entry_id", sa.Integer()),
        sa.Column("call_feedback_rating", sa.Integer()),
        sa.Column("custom_status_mapping", sa.Integer()),
        sa.Column("priority", sa.Integer()),
        sa.Column("status_change_timestamps", postgresql.JSONB()),
        sa.Column("direction_inbound", sa.Boolean()),
        sa.Column("sender_id", sa.String()),
        sa.Column("customer_contact", sa.String()),
        sa.Column("resolution_category", sa.String()),
        sa.ForeignKeyConstraint(["active_queue_entry_id"], ["active_queue_entry.id"]),
        sa.ForeignKeyConstraint(["custom_status_mapping"], ["conversation_custom_status_mapping.id"]),
        sa.ForeignKeyConstraint(["parent"], ["conversation_parent.id"]),
        sa.ForeignKeyConstraint(["primary_aom"], ["account_org_mapping.id"]),
        sa.ForeignKeyConstraint(["priority"], ["conversation_priority.id"]),
    )
    _core_table(
        "conversation_secondary_aom_mapping",
        sa.Column("aom", sa.Integer()),
        sa.Column("conversation", sa.Integer()),
        sa.Column("secondary_conversation", sa.Integer()),
        sa.Column("secondary_agent_state", sa.String()),
        sa.Column("participant_identifier", sa.String()),
        sa.ForeignKeyConstraint(["aom"], ["account_org_mapping.id"]),
        sa.ForeignKeyConstraint(["conversation"], ["conversation_parent.id"]),
        sa.ForeignKeyConstraint(["secondary_conversation"], ["conversation_parent.id"]),
    )
    _core_table(
        "conversation_chunk",
        sa.Column("conversation", sa.Integer()),
        sa.Column("from", sa.String(), nullable=False),
        sa.Column("to", sa.String(), nullable=False),
        sa.Column("type", sa.String()),
        sa.Column("disposition", sa.Text()),
        sa.Column("disposition_id", sa.Integer()),
        sa.Column("sub_disposition", sa.Text()),
        sa.Column("sub_disposition_id", sa.Integer()),
        sa.Column("description", sa.Text()),
        sa.Column("timestamp", sa.TIMESTAMP(timezone=True)),
        sa.Column("attributes", postgresql.JSON(none_as_null=True)),
        sa.Column("channel", sa.String()),
        sa.Column("message_id", sa.String()),
        sa.Column("channel_meta", postgresql.JSON()),
        sa.Column("recording", sa.Integer()),
        sa.ForeignKeyConstraint(["conversation"], ["conversation.id"]),
        sa.ForeignKeyConstraint(["disposition_id"], ["dispositions.id"]),
        sa.ForeignKeyConstraint(["recording"], ["recording.id"]),
        sa.ForeignKeyConstraint(["sub_disposition_id"], ["dispositions.id"]),
    )
    op.create_index(
        "ux_account_live_email",
        "account",
        [sa.text("lower(email)")],
        unique=True,
        postgresql_where=sa.text("is_deleted IS NOT TRUE"),
    )
    op.create_table(
        "api_keys",
        sa.Column("id", postgresql.UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization", sa.Integer(), sa.ForeignKey("organization.id"), nullable=False),
        sa.Column("prefix", sa.Text(), nullable=False),
        sa.Column("sha256", sa.Text(), nullable=False, unique=True),
        sa.Column("scopes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True)),
    )
    op.create_index("ix_api_keys_organization", "api_keys", ["organization"])
    op.execute("CREATE SEQUENCE event_seq")
    op.create_table(
        "events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("seq", sa.BigInteger()),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.Text()),
        sa.Column("agent_id", sa.Text()),
        sa.Column("direction", sa.Text()),
        sa.Column("context", postgresql.JSONB()),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("data", postgresql.JSONB(), nullable=False),
    )
    op.create_index("ix_events_tenant_seq", "events", ["tenant_id", "seq"])
    op.create_index("ix_events_unstamped", "events", ["id"], postgresql_where=sa.text("seq IS NULL"))
    op.create_index("ix_events_conversation_seq", "events", ["conversation_id", "seq"])
    op.create_index("ix_events_created_at", "events", ["created_at"])
    # a supervisor's stream finds the conversations queued on its team queues
    op.create_index(
        "ix_events_queued_queue",
        "events",
        ["tenant_id", sa.text("(data->>'queue_id')")],
        postgresql_where=sa.text("type = 'conversation.queued'"),
    )
    op.create_table(
        "webhooks",
        sa.Column("id", postgresql.UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        sa.Column("previous_secret", sa.Text()),
        sa.Column("previous_secret_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("event_types", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_webhooks_tenant", "webhooks", ["tenant_id"])
    op.create_table(
        "jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("key", sa.Text()),
        sa.Column("run_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("locked_until", sa.TIMESTAMP(timezone=True)),
        sa.Column("last_error", sa.Text()),
    )
    op.create_index(
        "uq_jobs_kind_key", "jobs", ["kind", "key"], unique=True, postgresql_where=sa.text("key IS NOT NULL")
    )
    op.create_index(
        "ix_jobs_due",
        "jobs",
        ["run_at"],
        postgresql_where=sa.text("locked_until IS NULL OR locked_until <> 'infinity'"),
    )

    # queues, business hours, menus and number entry points: each a tenant row with a validated JSONB body
    for name in CONFIG_TABLES:
        number = [sa.Column("number", sa.Text(), nullable=False)] if name == "entry_points" else []
        op.create_table(
            name,
            sa.Column("id", postgresql.UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
            sa.Column("organization", sa.Integer(), sa.ForeignKey("organization.id"), nullable=False),
            *number,
            sa.Column("body", postgresql.JSONB(), nullable=False),
            sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        )
        op.create_index(f"ix_{name}_organization_id", name, ["organization", "id"])
    op.create_unique_constraint("uq_entry_points_organization_number", "entry_points", ["organization", "number"])


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
    op.execute("DROP SEQUENCE event_seq")
