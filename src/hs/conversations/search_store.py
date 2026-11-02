"""Postgres-native closed-conversation search store (HS-owned table).

A denormalized, precomputed snapshot of each closed conversation, written once at
conversation-close, then read cheaply by the closed-conversation list/search.

The record's fields, plus a few EXTRACTED columns that exist only to make filtering/RBAC/sort/full-text efficient in
Postgres (they are not part of the returned record):

  - organization / conv_created_at : fast org-scoped, time-ordered, range-filtered reads
  - agent_ids / team_ids (JSONB)   : RBAC + agent/team filters without digging into agent_details
  - search_vector (TSVECTOR)       : GIN full-text over FULLTEXT_SOURCE_FIELDS

Its migration ships in alembic/versions (`alembic upgrade head`).
"""

from sqlalchemy import TIMESTAMP, Boolean, Column, Index, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR

from hs.infra.db import DB, CoreDBBase

# The free-text search fields: the precompute builds `search_vector` from the row's values for these.
FULLTEXT_SOURCE_FIELDS = [
    "customer_name",
    "wrap_up_note",
    "disposition",
    "conv_state",
    "contact_identifier",
    "conv_chunk_descriptions",
    "case_id",
    "crm_contact_id",
    "csat_feedback",
    "sub_disposition",
    "conv_child_customer_contacts",
    "conv_child_sender_ids",
    "hangup_source",
]


class ClosedConversationSearch(DB, CoreDBBase):
    __tablename__ = "closed_conversation_search"

    # --- natural key + scoping/sort/extracted columns (not part of the returned record) ---
    conv_number = Column(String, nullable=False, unique=True, index=True)
    organization = Column(Integer, nullable=False, index=True)  # internal org id (== conversation_parent.organization)
    conv_created_at = Column(TIMESTAMP(timezone=True), index=True)  # real timestamp for ORDER BY / range filters
    conv_updated_at_ts = Column(TIMESTAMP(timezone=True))  # real timestamp for conv_updated_at range filters
    agent_ids = Column(JSONB, default=list)  # [agent_uuid, ...] — RBAC + agent filter
    team_ids = Column(JSONB, default=list)  # [team_id, ...] — team filter
    search_vector = Column(TSVECTOR)  # GIN full-text over FULLTEXT_SOURCE_FIELDS

    # --- the record's fields (returned verbatim) ---
    customer_name = Column(String)
    crm_contact_id = Column(String)
    case_id = Column(String)
    direction_inbound = Column(Boolean, default=True)
    parent_channel = Column(String)
    contact_identifier = Column(String)
    conv_created_time = Column(String)  # 'HH:MM:SS.ffffff%z'
    conv_created_date = Column(String)  # 'YYYY-MM-DD'
    conv_updated_at = Column(String)  # ISO string (document field)
    conv_created_at_str = Column(String)  # ISO string (document field `conv_created_at`)
    agent_details = Column(JSONB, default=list)
    disposition = Column(String)
    disposition_uuid = Column(String)
    sub_disposition = Column(String)
    sub_disposition_uuid = Column(String)
    wrap_up_note = Column(Text)
    conv_message_count = Column(Integer, default=0)
    conv_channels = Column(JSONB, default=list)
    conv_total_duration = Column(Numeric, default=0)
    agent_customer_conv_duration = Column(Numeric, default=0)
    conv_response_time = Column(Numeric, default=0)
    conv_wrapup_duration = Column(Numeric, default=0)
    conv_child_customer_contacts = Column(JSONB, default=list)
    conv_child_sender_ids = Column(JSONB, default=list)
    is_conv_answered = Column(Boolean)
    conv_states = Column(JSONB, default=list)
    conv_state = Column(String)
    conv_state_uuid = Column(String)
    conv_chunks = Column(JSONB, default=list)
    org_uuid = Column(String)
    conv_chunk_descriptions = Column(JSONB, default=list)
    conv_chunk_message_types = Column(JSONB, default=list)
    csat_feedback = Column(String, default="Not Requested")
    hangup_source = Column(String)
    custom_data = Column(JSONB)
    customer_data = Column(JSONB)
    customer_custom_data = Column(JSONB)
    external_identifer = Column(String)  # the record field's spelling
    client_data = Column(JSONB)

    __table_args__ = (
        Index("ix_closed_conv_search_org_created", "organization", "conv_created_at"),
        Index("ix_closed_conv_search_vector", "search_vector", postgresql_using="gin"),
        Index("ix_closed_conv_search_agent_ids", "agent_ids", postgresql_using="gin"),
        Index("ix_closed_conv_search_org_dispo", "organization", "disposition"),
        Index("ix_closed_conv_search_org_state", "organization", "conv_state"),
        Index("ix_closed_conv_search_org_channel", "organization", "parent_channel"),
    )

    # Columns that are internal (filtering/sort/full-text), NOT part of the returned record.
    _NON_RECORD_COLUMNS = frozenset(
        {
            "id",
            "uuid",
            "created_at",
            "updated_at",
            "is_deleted",
            "organization",
            "conv_created_at",
            "conv_updated_at_ts",
            "agent_ids",
            "team_ids",
            "search_vector",
            "conv_created_at_str",
        }
    )
