import datetime

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    and_,
    case,
    func,
    or_,
    text,
)
from sqlalchemy.dialects.postgresql import JSON, JSONB, UUID
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, contains_eager, joinedload, relationship

from hs.admin.account import AccountOrgMapping, Organization
from hs.admin.dispositions import Dispositions
from hs.conversations.models import (
    AGENT_SPECIFIC_CHUNK_TYPES,
    NON_QNR_CHILDREN,
    OPEN_STATUSES,
    QNR_CHILDREN,
    RESOLVE_STATUSES,
    TRANSER_CHILDREN,
    ChunkAttributes,
    ConvChannel,
    ConversationChannels,
    ConversationChunkModel,
    ConversationChunkTypes,
    ConversationGenerationTypes,
    ConversationModel,
    ConversationParentModel,
    ConversationStates,
    ConversationStatus,
    ConvSecAgentMappingModel,
)
from hs.infra.constants import EMPTY_TO_USER, MONITOR_BARGE_COACH_STATE_LIST
from hs.infra.context import get_db_session
from hs.infra.db import DB, CoreDBBase, is_readonly_txn_error
from hs.infra.helpers import get_now, get_time_difference_in_seconds
from hs.infra.log import logger
from hs.infra.redis import Cache
from hs.voice.call_store import CallLeg

# Bound no-window chunk reads to the newest configured limit; explicit time windows keep their own scope.
DEFAULT_CONVERSATION_CHUNK_LIMIT = 1000


def _for_update_first_with_readonly_retry(query):
    """Run ``query.first()`` with ONE bounded retry when the statement dies with
    SQLSTATE 25006 (ReadOnlySqlTransaction: the connection reached a node that cannot
    serve writes, e.g. a demoted ex-writer during a failover).

    Scope is deliberately narrow: retry ONLY these
    parent-close ``SELECT ... FOR UPDATE`` executions — never the surrounding use case,
    which fires non-rollback-able side effects (Plivo calls) later in the same request.
    Two guards make the rollback+retry safe:
    - the FOR UPDATE is the first statement of its transaction by design (lock-order
      rule: parent BEFORE any child row), and
    - if the session unexpectedly carries pending ORM mutations, the retry REFUSES
      (rollback would silently discard them) and the original error propagates.
    The handle_error listener (hs/infra/db.py) has already invalidated the poisoned
    connection, so the retry dials a fresh one — with target_session_attrs=read-write
    walking to the current writer on a direct host."""
    try:
        return query.first()
    except DBAPIError as exc:
        if not is_readonly_txn_error(exc):
            raise
        session = query.session
        if session.new or session.dirty or session.deleted:
            logger.error(
                "[readonly-retry] SQLSTATE 25006 on FOR UPDATE but the session has pending ORM mutations — "
                "refusing to rollback+retry; propagating the original error"
            )
            raise
        logger.warning(
            "[readonly-retry] SELECT ... FOR UPDATE hit a read-only node (SQLSTATE 25006) — rolling back and "
            "retrying once on a fresh connection"
        )
        session.rollback()
        return query.first()


class ConversationCustomStatusMapping(DB, CoreDBBase):
    __tablename__ = "conversation_custom_status_mapping"
    organization = Column(Integer, ForeignKey(Organization.id))
    status = Column(String, nullable=False)
    state = Column(String, default="new", nullable=False)

    @classmethod
    def get_all_states_by_org(cls, org_id: int):
        db: Session = get_db_session()
        return db.query(cls).filter(cls.organization == org_id).all()

    @classmethod
    def get_by_state_and_org(cls, state: str, org_id: int):
        db: Session = get_db_session()
        return db.query(cls).filter(cls.state == state, cls.organization == org_id).first()


class ConversationPriority(DB, CoreDBBase):
    __tablename__ = "conversation_priority"
    organization = Column(Integer, ForeignKey(Organization.id))
    label = Column(String, default="medium", nullable=False)

    @classmethod
    def get_by_name_and_org(cls, org_id: int, name: str):
        db: Session = get_db_session()
        return db.query(cls).filter(cls.label == name, cls.organization == org_id).first()


class Recording(DB, CoreDBBase):
    __tablename__ = "recording"
    recording_duration = Column(String)
    recording_uuid = Column(String)
    recording_url = Column(String)
    recording_start_time = Column(String)
    recording_end_time = Column(String)
    recording_format = Column(String)
    call_leg_id = Column(Integer, ForeignKey(CallLeg.id))
    call_leg = relationship(CallLeg)
    # Bound filtered recordings even when limit is omitted; 500 is both the default and maximum.
    MAX_FILTERED_RECORDINGS_LIMIT = 500

    @classmethod
    def create_from_chunk(cls, chunk, call_leg_id=None):
        db: Session = get_db_session()
        attribute = chunk.attributes[0]
        recording = cls(
            recording_url=attribute.get("recording_url"),
            recording_duration=attribute.get("recording_duration"),
            recording_uuid=attribute.get("recording_uuid"),
            recording_format=attribute.get("recording_format"),
            recording_start_time=attribute.get("recording_start_time"),
            recording_end_time=attribute.get("recording_end_time"),
        )
        if call_leg_id:
            recording.call_leg_id = call_leg_id
        db.add(recording)
        db.flush()
        return recording

    @classmethod
    def create_recording(cls, recording):
        db: Session = get_db_session()
        db.add(recording)
        db.flush()
        return recording

    @classmethod
    def get_by_recording_uuid(cls, recording_uuid: str, with_call_leg: bool = False):
        db: Session = get_db_session()
        recording = cls.select_query(db).filter(cls.recording_uuid == recording_uuid)
        if with_call_leg:
            recording = recording.join(CallLeg).filter(CallLeg.is_deleted.is_(False))
        recording = recording.first()
        if not recording:
            return None
        return recording

    @classmethod
    def get_by_call_leg_id(cls, call_leg_id: int):
        db: Session = get_db_session()
        return cls.select_query(db).filter(cls.call_leg_id == call_leg_id).first()


class ActiveQueueEntry(DB, CoreDBBase):
    __tablename__ = "active_queue_entry"
    queue_priority = Column(Integer, nullable=False)
    queue_uuid = Column(UUID(as_uuid=True), nullable=False)
    contact_uuid = Column(String, nullable=False)
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    flow_run_uuid = Column(UUID(as_uuid=True), nullable=False)
    queue_type = Column(String, nullable=False)
    queue_channel = Column(String, nullable=False)
    contact_phone_or_email = Column(String, nullable=False)
    version = Column(BigInteger, nullable=True, default=0)
    qnr_run_uuid = Column(String, nullable=False)
    status = Column(String, nullable=False)
    auth_id = Column(String, nullable=False)

    @classmethod
    def active_query(cls, db: Session, columns=None):
        return cls.select_query(db, columns).filter(cls.deleted_at.is_(None))


class ActiveQueueTeam(DB, CoreDBBase):
    __tablename__ = "teams"
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    team_id = Column(String, nullable=False)
    active_queue_entry_id = Column(BigInteger, nullable=True)


class ActiveQueueSkillsFilter(DB, CoreDBBase):
    __tablename__ = "skills_filter"
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    skill_id = Column(String, nullable=False)
    operator = Column(String, nullable=False)
    value = Column(Integer, nullable=False)
    active_queue_entry_id = Column(BigInteger, nullable=True)


class ActiveQueueAgent(DB, CoreDBBase):
    __tablename__ = "agents"
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    agent_id = Column(String, nullable=False)
    active_queue_entry_id = Column(BigInteger, nullable=True)


class Team(DB, CoreDBBase):
    __tablename__ = "team"
    organization = Column(Integer, ForeignKey(Organization.id))
    name = Column(String)


class ConversationParent(DB, CoreDBBase):
    __tablename__ = "conversation_parent"
    organization = Column(Integer, ForeignKey(Organization.id))
    case_id = Column(String)
    contact_identifier = Column(String)
    contact_uuid = Column(String, name="contact_uuid", index=True)
    crm_contact_id = Column(String)
    end_at = Column(TIMESTAMP(timezone=True), name="end_at")
    last_activity_at = Column(TIMESTAMP(timezone=True))
    channel = Column(String, name="channel", nullable=False)
    direction_inbound = Column(Boolean, default=True, name="direction_inbound", nullable=False)
    conversation_number = Column(String)
    contact_name = Column(String, name="contact_name")
    flow_closure_flag = Column(Boolean, name="flow_closure_flag")
    is_hidden = Column(Boolean, name="is_hidden")
    flow_run_uuid = Column(UUID(as_uuid=True), name="flow_run_uuid")
    external_identifier = Column(String)
    result = Column(Text)  # the call result conversation.closed reports
    org = relationship(Organization)
    child_convs = relationship("Conversation", back_populates="parent_conversation")
    secondary_participants = relationship(
        "ConvSecondaryAgentMapping",
        backref="parent_conversation",
        foreign_keys="ConvSecondaryAgentMapping.conversation",
    )
    linked_conversation = relationship(
        "ConvSecondaryAgentMapping",
        backref="secondary_parent_conversation",
        foreign_keys="ConvSecondaryAgentMapping.secondary_conversation",
    )
    customer_conversation_data = relationship(
        "CustomerConversationData", uselist=False, back_populates="conversation_parent"
    )

    def __to_model(self) -> ConversationParentModel:
        self.parent_contact_uuid = self.contact_uuid
        self.source_direction_inbound = self.direction_inbound
        self.parent_channel = self.channel
        self.parent_end_at = self.end_at
        return ConversationParentModel.model_validate(self)

    def get_model(self) -> ConversationParentModel:
        return self.__to_model()

    @classmethod
    def __filter_for_active_conv(cls):
        return or_(Conversation.status.in_(OPEN_STATUSES), Conversation.end_at.is_(None))

    @classmethod
    def _parent_of_child(cls, child_filter):
        query = cls.select_query(get_db_session()).join(Conversation, ConversationParent.id == Conversation.parent)
        conversation_parent = query.filter(child_filter).first()
        return conversation_parent.__to_model() if conversation_parent else None

    @classmethod
    def create_conversation(cls, conversation) -> ConversationParentModel:
        db: Session = get_db_session()
        db.add(conversation)
        db.flush()
        return conversation.__to_model()

    @classmethod
    def get_by_contact_uuid(cls, contact_uuid: str, only_open_conv=False, for_update=False) -> ConversationParentModel:
        db: Session = get_db_session()
        conversation_query = cls.select_query(db).filter(cls.contact_uuid == contact_uuid)
        if only_open_conv:
            conversation_query = conversation_query.filter(cls.end_at.is_(None))
        conversation_query = conversation_query.order_by(cls.created_at.desc())
        if for_update:
            # Row lock held until the transaction commits — serializes the wrap-up ∥
            # parent-close race (lock order rule: parent BEFORE any child row).
            conversation_query = conversation_query.with_for_update(of=cls)
            conversation = _for_update_first_with_readonly_retry(conversation_query)
        else:
            conversation = conversation_query.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_by_conversation_number(
        cls, conversation_number: str, org_uuid: str | None = None, active: bool = False, for_update: bool = False
    ) -> ConversationParentModel | None:
        db: Session = get_db_session()
        conversation = cls.select_query(db).filter(cls.conversation_number == conversation_number)
        if org_uuid:
            conversation = conversation.join(Organization).filter(Organization.uuid == org_uuid)
        if active:
            conversation = conversation.filter(cls.end_at.is_(None))
        if for_update:
            # Row lock held until the transaction commits — serializes the wrap-up ∥
            # parent-close race (lock order rule: parent BEFORE any child row).
            conversation = conversation.with_for_update(of=cls)
            conversation = _for_update_first_with_readonly_retry(conversation)
        else:
            conversation = conversation.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_secondary_conversation_mappings_with_state(
        cls, agent_uuid: UUID, conv_channel: ConvChannel = None
    ) -> dict[str, str]:
        db: Session = get_db_session()
        base_query = (
            cls.select_query(db)
            .join(cls.secondary_participants.and_(ConvSecondaryAgentMapping.is_deleted.is_(False)))
            .join(AccountOrgMapping, AccountOrgMapping.id == ConvSecondaryAgentMapping.aom)
            .filter(
                AccountOrgMapping.uuid == agent_uuid,
                cls.__filter_for_active_conv(),
                ConvSecondaryAgentMapping.secondary_conversation.is_(None),
            )
            .join(
                cls.child_convs.and_(
                    Conversation.is_deleted.is_(False), Conversation.status != ConversationStatus.closed.name
                )
            )
        )
        if conv_channel and conv_channel.channel:
            base_query = base_query.filter(cls.channel == conv_channel.channel)
        active_conversations = base_query.all()
        conversation_number_to_state_map = {}
        for conversation in active_conversations:
            for mapping in conversation.secondary_participants:
                if mapping.is_deleted is False:
                    conversation_number_to_state_map[conversation.conversation_number] = mapping.secondary_agent_state
        return conversation_number_to_state_map

    @classmethod
    def get_parent_conversation_preloaded(cls, conversation_number, active: bool = False):
        db: Session = get_db_session()
        conversation = (
            cls.select_query(db)
            .outerjoin(cls.child_convs.and_(Conversation.is_deleted.is_(False)))
            .outerjoin(Conversation.chunks.and_(ConversationChunk.is_deleted.is_(False)))
            # eager: a lazy many-to-one skips SQL only while the earlier object survives GC
            .options(
                contains_eager(ConversationParent.child_convs, Conversation.chunks),
                contains_eager(ConversationParent.child_convs).joinedload(Conversation.primary_agent),
                contains_eager(ConversationParent.child_convs).joinedload(Conversation.active_queue_entry),
                joinedload(ConversationParent.org),
            )
            .filter(cls.conversation_number == conversation_number)
            .order_by(cls.created_at.desc(), Conversation.start_at.desc())
        )
        if active:
            conversation = conversation.filter(cls.__filter_for_active_conv())
        return conversation.all()

    @classmethod
    def get_conversation_parent_for_child_contact_uuid(cls, contact_uuid) -> ConversationParentModel:
        return cls._parent_of_child(Conversation.contact_uuid == contact_uuid)

    @classmethod
    def update_last_activity_at(cls, conversation_id: str, now=None):
        from hs.infra.db import core_engine

        now = now or get_now()
        db: Session = get_db_session()
        db.commit()
        # ConversationParent.__tablename__ == "conversation_parent" (fixed schema) — static query,
        # no f-string into text() (semgrep SQLi-safe). Values bound via :now / :conversation_id.
        query = text(
            "UPDATE conversation_parent "
            "SET last_activity_at = :now "
            "WHERE id = :conversation_id AND (last_activity_at < :now OR last_activity_at IS NULL)"
        )
        with core_engine.connect() as conn:
            transaction = conn.begin()
            conn.execute(query, {"now": now, "conversation_id": conversation_id})
            transaction.commit()
        db.commit()

    @classmethod
    def update_conversation(cls, conversation_id: str, update_dict: dict, now=None, commit_transaction: bool = True):
        now = now or get_now()
        db: Session = get_db_session()
        if commit_transaction:
            db.commit()
        values = dict(update_dict)
        values.update({cls.updated_at.name: now, cls.last_activity_at.name: now})
        cls.select_query(db).filter(cls.id == conversation_id).update(values)
        if commit_transaction:
            db.commit()
        else:
            # Parent-lock callers own the transaction boundary: flush the write while
            # retaining FOR UPDATE until their complete decision/write section finishes.
            db.flush()

    @classmethod
    def get_secondary_active_conversation(cls, conversation_number: str, agent_uuid) -> "ConversationModel":
        db: Session = get_db_session()
        base_query = (
            cls.select_query(db)
            .join(cls.secondary_participants.and_(ConvSecondaryAgentMapping.is_deleted.is_(False)))
            .join(AccountOrgMapping, AccountOrgMapping.id == ConvSecondaryAgentMapping.aom)
            .filter(AccountOrgMapping.uuid == agent_uuid, cls.conversation_number == conversation_number)
            .subquery("base_query")
        )
        conversation = (
            db.query(Conversation, base_query)
            .select_from(base_query)
            .join(Conversation)
            .filter(cls._ConversationParent__filter_for_active_conv(), Conversation.primary_aom.is_not(None))
            .first()
        )
        return conversation.Conversation.get_model() if conversation else None


class CustomerConversationData(DB, CoreDBBase):
    __tablename__ = "customer_conversation_data"
    conversation_parent_id = Column(Integer, ForeignKey(ConversationParent.id))
    customer_data = Column(JSON)
    customer_custom_data = Column(JSON)
    conversation_custom_data = Column(JSON)
    external_identifier = Column(String)
    client_data = Column(JSON)
    conversation_parent = relationship(ConversationParent, back_populates="customer_conversation_data")

    def __to_model(self):
        from hs.conversations.models import (
            ConversationCustomerData,
            ConversationDataDict,
            CustomerConversationDataModel,
        )

        model = CustomerConversationDataModel()
        model.conversation_parent_id = self.conversation_parent_id
        if self.customer_data:
            model.customer_data = (
                ConversationCustomerData.model_validate_json(self.customer_data).model_dump()
                if isinstance(self.customer_data, str)
                else ConversationCustomerData.model_validate(self.customer_data)
            )
            if self.customer_custom_data:
                model.customer_data.customer_custom_data = dict(self.customer_custom_data)
        model.conversation_data = ConversationDataDict()
        if self.conversation_custom_data:
            model.conversation_data.conversation_custom_data = dict(self.conversation_custom_data)
        if self.external_identifier:
            model.conversation_data.external_identifier = self.external_identifier
        return model

    @classmethod
    def create_customer_conversation_data(cls, customer_conversation_data):
        db: Session = get_db_session()
        db.add(customer_conversation_data)
        db.flush()
        return customer_conversation_data.__to_model()


class Conversation(DB, CoreDBBase):
    __tablename__ = "conversation"
    primary_aom = Column(Integer, ForeignKey(AccountOrgMapping.id))
    parent = Column(Integer, ForeignKey(ConversationParent.id), nullable=False)
    channel = Column(String, nullable=False)
    status = Column(String, default=ConversationStatus.open.value, nullable=False)
    state = Column(String, nullable=False)
    generation_type = Column(String, nullable=False)
    start_at = Column(TIMESTAMP(timezone=True))
    end_at = Column(TIMESTAMP(timezone=True))
    contact_creation_start_at = Column(TIMESTAMP(timezone=True))
    case_creation_start_at = Column(TIMESTAMP(timezone=True))
    wrap_up_start_at = Column(TIMESTAMP(timezone=True))
    contact_uuid = Column(String)
    active_queue_entry_id = Column(Integer, ForeignKey(ActiveQueueEntry.id))
    call_feedback_rating = Column(Integer)
    custom_status_mapping = Column(Integer, ForeignKey(ConversationCustomStatusMapping.id))
    priority = Column(Integer, ForeignKey(ConversationPriority.id))
    status_change_timestamps = Column(JSONB)
    primary_agent = relationship(AccountOrgMapping)
    parent_conversation = relationship(ConversationParent)
    active_queue_entry = relationship(ActiveQueueEntry)
    chunks = relationship("ConversationChunk", back_populates="conv")
    direction_inbound = Column(Boolean, default=True)
    sender_id = Column(String)
    customer_contact = Column(String)
    resolution_category = Column(String)

    def __to_model(self, parent_model=None):
        # Reuse the supplied parent model; callers without one keep the existing per-child build.
        # This avoids the explicit duplicate build, not the parent validation inside from_orm.
        if parent_model is None:
            parent_model = self.parent_conversation.get_model()
        conv_model = ConversationModel.model_validate(self)
        pri_agent_model = self.primary_agent.get_model() if self.primary_agent else None
        conv_model.parent_conversation = parent_model
        conv_model.primary_agent = pri_agent_model
        conv_model.child_direction_inbound = self.direction_inbound
        return conv_model

    def get_model(self, parent_model=None):
        return self.__to_model(parent_model=parent_model)

    @classmethod
    def _model_graph(cls):
        # eager: a lazy many-to-one (composite pk, no identity-map get) skips SQL only while an earlier copy of
        # this row survives GC in the weak identity map, so __to_model's statement count followed GC timing
        return joinedload(cls.parent_conversation), joinedload(cls.primary_agent)

    @classmethod
    def create_conversation(cls, conversation) -> ConversationModel:
        db: Session = get_db_session()
        db.add(conversation)
        db.flush()
        return conversation.__to_model()

    @classmethod
    def __filter_for_active_conv(cls):
        return or_(cls.status.in_(OPEN_STATUSES), cls.end_at.is_(None))

    @classmethod
    def get_by_id(cls, id):
        conv = cls.select_query(get_db_session()).options(*cls._model_graph()).filter(cls.id == id).first()
        return conv.get_model() if conv else None

    @classmethod
    def update_conversation(
        cls,
        conversation_id: str,
        update_dict: dict,
        now=None,
        update_status_change_timestamp_for_state: str | None = None,
        commit_transaction: bool = True,
    ) -> bool:
        db: Session = get_db_session()
        if update_status_change_timestamp_for_state is None:
            now = now or get_now()
            update_dict.update({cls.updated_at.name: now})
            cls.select_query(db).filter(cls.id == conversation_id).update(update_dict)
        else:
            allowed_columns = {
                "primary_aom",
                "channel",
                "status",
                "state",
                "generation_type",
                "start_at",
                "end_at",
                "contact_creation_start_at",
                "case_creation_start_at",
                "wrap_up_start_at",
                "active_queue_entry_id",
                "call_feedback_rating",
                "custom_status_mapping",
                "priority",
                "direction_inbound",
                "sender_id",
                "customer_contact",
                "resolution_category",
                "updated_at",
            }
            filtered_update_dict = {k: v for k, v in update_dict.items() if k in allowed_columns}
            if not filtered_update_dict:
                return True
            now = now or get_now()
            filtered_update_dict["updated_at"] = now
            # Apply the whitelisted column updates via the ORM (parameterized — same pattern as the
            # `update_status_change_timestamp_for_state is None` branch above). The column names come
            # ONLY from the `allowed_columns` whitelist; values are bound by SQLAlchemy. This removes
            # the former dynamic SET-clause f-string from the text() query below (semgrep SQLi-safe).
            cls.select_query(db).filter(cls.id == conversation_id).update(filtered_update_dict)
            # Recompute status_change_timestamps with a fully STATIC text() (no dynamic columns) —
            # the CTE reads the existing timestamps + :new_state, independent of the column updates above.
            current_timestamp = get_now()
            status_ts_query = text("""
                WITH current_status AS (
                    SELECT
                        COALESCE(status_change_timestamps, '[]'::jsonb) as status_change_timestamps,
                        CASE
                            WHEN status_change_timestamps IS NULL OR status_change_timestamps = '[]'::jsonb THEN NULL
                            ELSE (status_change_timestamps->-1->>'state')::text
                        END AS last_state
                    FROM conversation
                    WHERE id = :id LIMIT 1
                ),
                new_timestamp_entry AS (
                    SELECT
                        CASE
                            WHEN status_change_timestamps IS NULL OR status_change_timestamps = '[]'::jsonb THEN
                                -- First entry
                                jsonb_build_array(
                                    jsonb_build_object(
                                        'state', (:new_state)::text,
                                        'state_started_at', (:current_timestamp)::timestamptz,
                                        'state_ended_at', null
                                    )
                                )
                            WHEN cs.last_state = (:new_state)::text THEN
                                -- Same state, don't add new entry
                                status_change_timestamps
                            ELSE
                                -- Update previous entry's end time and add new entry
                                (
                                    SELECT
                                        jsonb_set(
                                            status_change_timestamps,
                                            ARRAY[
                                                jsonb_array_length(status_change_timestamps) - 1,
                                                'state_ended_at'
                                            ]::text[],
                                            to_jsonb((:current_timestamp)::timestamptz)
                                        )
                                ) ||
                                jsonb_build_array(
                                    jsonb_build_object(
                                        'state', (:new_state)::text,
                                        'state_started_at', (:current_timestamp)::timestamptz,
                                        'state_ended_at', null
                                    )
                                )
                        END AS new_status_timestamps
                    FROM current_status cs
                )
                UPDATE conversation
                SET
                    status_change_timestamps = nts.new_status_timestamps
                FROM new_timestamp_entry nts
                WHERE id = :id;
            """)
            db.execute(
                status_ts_query,
                {
                    "id": conversation_id,
                    "current_timestamp": current_timestamp,
                    "new_state": update_status_change_timestamp_for_state,
                },
            )
        if commit_transaction:
            db.commit()
        else:
            # Parent-lock callers own the transaction boundary. Flush both the child
            # update and any status-history write while retaining FOR UPDATE until the
            # complete parent/child closure decision is durable.
            db.flush()

    @classmethod
    def get_active_non_qnr_child_by_contact_uuid(cls, contact_uuid: str, filter_active=True) -> ConversationModel:
        db: Session = get_db_session()
        parent_id_subquery = cls.select_query(db, columns=[cls.parent]).filter(cls.contact_uuid == contact_uuid)
        conversation = (
            cls.select_query(db)
            .options(*cls._model_graph())
            .filter(cls.parent.in_(parent_id_subquery), cls.generation_type.in_(NON_QNR_CHILDREN))
        )
        if filter_active:
            conversation = conversation.filter(cls.__filter_for_active_conv())
        conversation = conversation.order_by(cls.created_at.desc()).first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_unanswered_qnr_child_by_contact_uuid(
        cls, contact_uuid: str, channel: str | None = None, active_queue_id: int = 0
    ) -> ConversationModel:
        db: Session = get_db_session()
        conversation_query = (
            cls.select_query(db)
            .filter(cls.contact_uuid == contact_uuid)
            .filter(
                cls.generation_type.in_(QNR_CHILDREN),
                cls.__filter_for_active_conv(),
                cls.state == ConversationStates.agent_unanswered.name,
            )
        )
        if channel:
            conversation_query = conversation_query.filter(cls.channel == channel)
        if active_queue_id > 0:
            conversation_query = conversation_query.filter(cls.active_queue_entry_id == active_queue_id)
        conversation = conversation_query.order_by(cls.created_at.desc(), cls.id.desc()).first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_outbound_child_by_contact_uuid(
        cls, contact_uuid: str, channel: str | None = None, filter_active=True
    ) -> ConversationModel:
        db: Session = get_db_session()
        conversation_query = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .filter(
                or_(
                    cls.generation_type == ConversationGenerationTypes.outbound_generated.name,
                    cls.generation_type == ConversationGenerationTypes.channel_switch.name,
                    cls.generation_type == ConversationGenerationTypes.transfer_generated.name,
                )
            )
        )
        if filter_active:
            conversation_query = conversation_query.filter(cls.__filter_for_active_conv())
        if channel:
            conversation_query = conversation_query.filter(cls.channel == channel)
        conversation = (
            conversation_query.filter(cls.contact_uuid == contact_uuid).order_by(cls.created_at.desc()).first()
        )
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_child_by_conv_parent(
        cls,
        parent_id,
        channel: str | None = None,
        inbound_direction: bool | None = None,
        filter_active=True,
        filter_qnr=False,
    ) -> ConversationModel:
        db: Session = get_db_session()
        conversation_query = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .join(cls.primary_agent.and_(AccountOrgMapping.is_deleted.is_(False)))
        )
        if filter_active:
            conversation_query = conversation_query.filter(cls.__filter_for_active_conv())
        if filter_qnr:
            conversation_query = conversation_query.filter(cls.generation_type.notin_(NON_QNR_CHILDREN))
        if channel:
            conversation_query = conversation_query.filter(cls.channel == channel)
        if inbound_direction is not None:
            if inbound_direction:
                conversation_query = conversation_query.filter(
                    or_(
                        cls.generation_type == ConversationGenerationTypes.qnr_generated.name,
                        cls.generation_type == ConversationGenerationTypes.transfer_generated.name,
                        cls.generation_type == ConversationGenerationTypes.channel_switch.name,
                        cls.generation_type == ConversationGenerationTypes.transfer_qnr_generated.name,
                    )
                )
            else:
                conversation_query = conversation_query.filter(
                    or_(
                        cls.generation_type == ConversationGenerationTypes.outbound_generated.name,
                        cls.generation_type == ConversationGenerationTypes.channel_switch.name,
                        cls.generation_type == ConversationGenerationTypes.transfer_generated.name,
                    )
                )
        conversation = conversation_query.filter(cls.parent == parent_id).order_by(cls.created_at.desc()).first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_all_active_conversations_for_parent(cls, parent_contact_uuid: str) -> list[ConversationModel]:
        db: Session = get_db_session()
        active_conversations_from_db = (
            cls.select_query(db)
            .join(
                cls.parent_conversation.and_(
                    ConversationParent.is_deleted.is_(False),
                    or_(ConversationParent.end_at.is_(None), ConversationParent.end_at > get_now()),
                )
            )
            .filter(ConversationParent.contact_uuid == parent_contact_uuid, cls.__filter_for_active_conv())
            .order_by(cls.created_at.desc())
            .all()
        )
        if not active_conversations_from_db:
            return []
        return [active_conv.__to_model() for active_conv in active_conversations_from_db]

    @classmethod
    def update_all_active_conversations_by_parent_id(
        cls, parent_conv_id: str, update_dict: dict, now=None, only_non_bot=False
    ):
        now = now or get_now()
        db: Session = get_db_session()
        update_dict.update({cls.updated_at.name: now})
        query = cls.select_query(db).filter(cls.parent == parent_conv_id, cls.__filter_for_active_conv())
        if only_non_bot:
            query = query.filter(cls.generation_type.not_in(NON_QNR_CHILDREN))
        query.update(update_dict)
        db.flush()

    @classmethod
    def update_all_non_qnr_for_parent(cls, parent_conv_number: str, update_dict: dict, now=None):
        now = now or get_now()
        db: Session = get_db_session()
        update_dict.update({cls.updated_at.name: now})
        parent_subquery = (
            ConversationParent.select_query(db, columns=[ConversationParent.id])
            .filter(
                ConversationParent.conversation_number == parent_conv_number, ConversationParent.is_deleted.is_(False)
            )
            .statement
        )
        cls.select_query(db).filter(
            cls.parent.in_(parent_subquery), cls.generation_type.in_(NON_QNR_CHILDREN), cls.__filter_for_active_conv()
        ).update(update_dict, synchronize_session=False)
        db.flush()

    @classmethod
    def get_active_conversations_by_parent_for_aom(
        cls, parent_conv_id: str, aom_id: str, exclude_channels: list[str] | None = None
    ) -> list[ConversationModel]:
        db: Session = get_db_session()
        query = cls.select_query(db).filter(
            cls.parent == parent_conv_id, cls.__filter_for_active_conv(), cls.primary_aom == aom_id
        )
        if exclude_channels:
            query = query.filter(cls.channel.notin_(exclude_channels))
        active_conversations_from_db = query.order_by(cls.created_at.desc()).all()
        return [active_conv.__to_model() for active_conv in active_conversations_from_db]

    @classmethod
    def update_active_conversations_by_parent_for_aom(
        cls, parent_conv_id: str, aom_id: str, update_dict: dict, now=None
    ) -> bool:
        now = now or get_now()
        db: Session = get_db_session()
        update_dict.update({cls.updated_at.name: now})
        cls.select_query(db).filter(
            cls.parent == parent_conv_id, cls.primary_aom == aom_id, cls.__filter_for_active_conv()
        ).update(update_dict)
        db.flush()

    @classmethod
    def get_active_conversations_for_primary_aom(
        cls, aom_uuid: UUID, conv_channel: ConvChannel = None, active: bool = True
    ) -> bool:
        # Return existence of an active primary-agent conversation without hydrating matching rows.
        db: Session = get_db_session()
        query = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .join(cls.primary_agent.and_(AccountOrgMapping.is_deleted.is_(False)))
            .filter(AccountOrgMapping.uuid == aom_uuid)
        )
        query = query.filter(cls.__filter_for_active_conv()) if active else query.filter(cls.status.in_(OPEN_STATUSES))
        if conv_channel and conv_channel.channel:
            query = query.filter(cls.channel == conv_channel.channel)
        return bool(db.query(query.exists()).scalar())

    @classmethod
    def get_last_used_number(
        cls,
        channel: str,
        aom_id: str,
        to_numbers: list,
        org_id: str | None = None,
        conversation_number: str | None = None,
        direction_inbound: bool | None = None,
    ) -> str:
        db: Session = get_db_session()
        last_used_number_query = (
            cls.select_query(db, columns=[cls.sender_id])
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .filter(cls.customer_contact.in_(to_numbers), cls.channel == channel, cls.sender_id is not None)
        )
        if org_id:
            last_used_number_query = last_used_number_query.filter(ConversationParent.organization == org_id)
        if conversation_number:
            last_used_number_query = last_used_number_query.filter(
                ConversationParent.conversation_number == conversation_number
            )
        if direction_inbound is not None:
            last_used_number_query = last_used_number_query.filter(cls.direction_inbound == direction_inbound)
        if len(to_numbers):
            last_used_number_query = last_used_number_query.filter(cls.customer_contact.in_(to_numbers))
        elif aom_id:
            last_used_number_query = last_used_number_query.filter(cls.primary_aom == aom_id)
        last_used_number = last_used_number_query.order_by(cls.created_at.desc()).first()
        if not last_used_number:
            return None
        return last_used_number.sender_id

    @classmethod
    def get_single_active_conversation_for_parent(
        cls, contact_uuid: str, channel: str | None = None, active: bool = True
    ) -> ConversationModel:
        db: Session = get_db_session()
        conversation = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .filter(or_(ConversationParent.contact_uuid == contact_uuid, cls.contact_uuid == contact_uuid))
        )
        if active:
            conversation = conversation.filter(cls.__filter_for_active_conv(), cls.primary_aom.is_not(None))
        if channel:
            conversation = conversation.filter(cls.channel == channel)
        conversation = conversation.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_conversation_for_hangup_chunk(cls, contact_uuid: str):
        db: Session = get_db_session()
        call_generation_types = [
            ConversationGenerationTypes.qnr_generated.name,
            ConversationGenerationTypes.transfer_generated.name,
            ConversationGenerationTypes.outbound_generated.name,
            ConversationGenerationTypes.channel_switch.name,
            ConversationGenerationTypes.transfer_qnr_generated.name,
        ]
        return (
            cls.select_query(db)
            .filter(
                cls.contact_uuid == contact_uuid,
                cls.generation_type.in_(call_generation_types),
                cls.channel == ConversationChannels.call.name,
            )
            .first()
        )

    @classmethod
    def get_first_child_conversation_for_parent(cls, conversation_number: str) -> ConversationModel:
        db: Session = get_db_session()
        conversation = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .filter(ConversationParent.conversation_number == conversation_number)
            .order_by(cls.created_at.desc())
        )
        conversation = conversation.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_by_conv_number_for_aom(
        cls, conversation_number: str, aom_uuid: str, contact_uuid: str | None = None
    ) -> list[ConversationModel]:
        db: Session = get_db_session()
        conversation_query = (
            cls.select_query(db)
            .join(
                cls.parent_conversation.and_(
                    ConversationParent.is_deleted.is_(False),
                    ConversationParent.conversation_number == conversation_number,
                )
            )
            .join(cls.primary_agent.and_(AccountOrgMapping.is_deleted.is_(False)))
            .filter(AccountOrgMapping.uuid == aom_uuid, cls.__filter_for_active_conv())
        )
        if contact_uuid:
            conversation_query = conversation_query.filter(cls.contact_uuid == contact_uuid)
        conversations = conversation_query.order_by(cls.created_at.desc()).all()
        return [conv.__to_model() for conv in conversations]

    @classmethod
    def get_primary_active_conversation(
        cls,
        conversation_number: str,
        agent_uuid=None,
        contact_uuid=None,
        channel=None,
        sender_id=None,
        customer_contact=None,
        asc_order=False,
        active=True,
    ) -> ConversationModel:
        db: Session = get_db_session()
        conversation_query = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .join(cls.primary_agent.and_(AccountOrgMapping.is_deleted.is_(False)))
            .options(contains_eager(cls.parent_conversation), contains_eager(cls.primary_agent))
            .filter(ConversationParent.conversation_number == conversation_number)
        )
        if active:
            conversation_query = conversation_query.filter(cls.__filter_for_active_conv())
        if agent_uuid:
            conversation_query = conversation_query.filter(AccountOrgMapping.uuid == agent_uuid)
        if contact_uuid:
            conversation_query = conversation_query.filter(cls.contact_uuid == contact_uuid)
        if channel:
            conversation_query = conversation_query.filter(cls.channel == channel.lower())
        if sender_id:
            conversation_query = conversation_query.filter(cls.sender_id == sender_id)
        if customer_contact:
            conversation_query = conversation_query.filter(cls.customer_contact == customer_contact)
        conversation_query = (
            conversation_query.order_by(cls.created_at.asc())
            if asc_order
            else conversation_query.order_by(cls.created_at.desc())
        )
        conversation = conversation_query.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_transferable_by_contact_uuid(
        cls, contact_uuid: str, channel: str | None = None, parent_contact_uuid: str | None = None
    ) -> ConversationModel:
        db: Session = get_db_session()
        if not parent_contact_uuid:
            parent_contact_uuid = contact_uuid
        conversation = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .join(cls.primary_agent.and_(AccountOrgMapping.is_deleted.is_(False)))
            .filter(or_(cls.contact_uuid == contact_uuid, ConversationParent.contact_uuid == parent_contact_uuid))
            .options(contains_eager(cls.parent_conversation), contains_eager(cls.primary_agent))
            .filter(
                cls.generation_type.notin_(NON_QNR_CHILDREN),
                cls.state != ConversationStates.transferred.name,
                cls.end_at.is_(None),
                cls.primary_aom.is_not(None),
            )
        )
        if channel:
            conversation = conversation.filter(cls.channel == channel)
        conversation = conversation.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_non_transfer_child_conversations(
        cls, conv_parent_id, transfer_child_id, transfer_primary_aom
    ) -> list[ConversationModel]:
        db: Session = get_db_session()
        conversations = (
            cls.select_query(db)
            .filter(cls.id != transfer_child_id)
            .filter(
                cls.generation_type.notin_(NON_QNR_CHILDREN),
                cls.state.notin_(
                    [ConversationStates.transferred.name, ConversationStates.external_transfer_initiated.name]
                ),
                cls.__filter_for_active_conv(),
                cls.parent == conv_parent_id,
                or_(
                    and_(
                        cls.channel.in_([ConversationChannels.task.name, ConversationChannels.callback.name]),
                        cls.primary_aom == transfer_primary_aom,
                        cls.primary_aom.is_not(None),
                    ),
                    and_(
                        cls.channel.notin_([ConversationChannels.task.name, ConversationChannels.callback.name]),
                        cls.primary_aom.is_not(None),
                    ),
                ),
            )
            .all()
        )
        return [conv.__to_model() for conv in conversations]

    @classmethod
    def get_conversation_for_child_contact_uuid(cls, contact_uuid: str, aom_id=None) -> ConversationModel:
        db: Session = get_db_session()
        query = (
            cls.select_query(db)
            .join(cls.parent_conversation.and_(ConversationParent.is_deleted.is_(False)))
            .filter(cls.contact_uuid == contact_uuid)
        )
        if aom_id:
            query = (
                query.join(AccountOrgMapping, cls.primary_aom == AccountOrgMapping.id)
                .filter(Conversation.primary_aom == aom_id)
                .order_by(cls.created_at.desc())
            )
        conversation = query.first()
        return conversation.__to_model() if conversation else None

    @classmethod
    def get_active_conversation(cls, sender_id: str, customer_contact: str, channel: str) -> ConversationModel:
        db: Session = get_db_session()
        active_conversation_from_db = (
            cls.select_query(db)
            .join(
                cls.parent_conversation.and_(
                    ConversationParent.is_deleted.is_(False),
                    or_(ConversationParent.end_at.is_(None), ConversationParent.end_at > get_now()),
                )
            )
            .filter(
                cls.sender_id == sender_id,
                cls.customer_contact == customer_contact,
                cls.channel == channel,
                cls.status.in_(OPEN_STATUSES),
                cls.generation_type.notin_(NON_QNR_CHILDREN),
            )
            .order_by(cls.created_at.desc())
            .first()
        )
        return active_conversation_from_db.__to_model() if active_conversation_from_db else None

    @classmethod
    def get_contacts_handle_time(cls, parent_conversation_id, aom_id) -> int:
        db: Session = get_db_session()
        row = (
            cls.select_query(
                db,
                columns=[
                    func.min(cls.start_at).label("start"),
                    func.max(cls.wrap_up_start_at).label("end"),
                    func.max(case((cls.generation_type.in_(TRANSER_CHILDREN), cls.start_at))).label("transfer_start"),
                ],
            )
            .filter(cls.parent == parent_conversation_id, cls.primary_aom == aom_id)
            .first()
        )
        if row:
            if row.transfer_start:
                return get_time_difference_in_seconds(row.end, row.transfer_start)
            return get_time_difference_in_seconds(row.end, row.start)
        return 0

    @classmethod
    def get_first_non_bot_by_contact_uuid(cls, contact_uuid: str) -> ConversationModel:
        db: Session = get_db_session()
        conversation = (
            cls.select_query(db)
            .filter(cls.contact_uuid == contact_uuid)
            .filter(
                cls.generation_type.in_(
                    [
                        ConversationGenerationTypes.qnr_generated.name,
                        ConversationGenerationTypes.transfer_qnr_generated.name,
                        ConversationGenerationTypes.transfer_generated.name,
                        ConversationGenerationTypes.outbound_generated.name,
                    ]
                ),
                cls.primary_aom.is_not(None),
            )
            .first()
        )
        return conversation.__to_model() if conversation else None

    @classmethod
    def filter_agents_in_acw(cls, agents: list[str], channel: str) -> list[str]:
        db: Session = get_db_session()
        agents_in_acw = (
            cls.select_query(db, columns=[func.distinct(AccountOrgMapping.sip_endpoint).label("sip_endpoint")])
            .join(cls.primary_agent.and_(AccountOrgMapping.sip_endpoint.in_(agents), cls.channel == channel))
            .filter(cls.status.in_(RESOLVE_STATUSES))
            .all()
        )
        return [agent_in_acw.sip_endpoint for agent_in_acw in agents_in_acw]


class ConversationChunk(DB, CoreDBBase):
    __tablename__ = "conversation_chunk"
    conversation = Column(Integer, ForeignKey(Conversation.id))
    from_user = Column(String, name="from", nullable=False)
    to_user = Column(String, name="to", nullable=False)
    type = Column(String)
    disposition = Column(Text)
    disposition_id = Column(Integer, ForeignKey(Dispositions.id))
    sub_disposition = Column(Text)
    sub_disposition_id = Column(Integer, ForeignKey(Dispositions.id))
    description = Column(Text)
    timestamp = Column(TIMESTAMP(timezone=True))
    attributes = Column(JSON(none_as_null=True))
    channel = Column(String)
    message_id = Column(String)
    channel_meta = Column(JSON)
    recording = Column(Integer, ForeignKey(Recording.id))
    conv = relationship("Conversation", back_populates="chunks")
    dispo = relationship("Dispositions", foreign_keys=[disposition_id])
    sub_dispo = relationship("Dispositions", foreign_keys=[sub_disposition_id])

    def __to_model(self, conv=None, conversation_model: ConversationModel = None) -> ConversationChunkModel:
        res_dict = {c.key: getattr(self, c.key) for c in self.__mapper__.column_attrs}
        res_dict["from"] = res_dict.pop("from_user", None)
        res_dict["to"] = res_dict.pop("to_user", None)
        res_dict.pop("recording", None)
        if conv:
            res_dict["sender_id"] = conv.sender_id if conv.sender_id and conv.sender_id not in ["None"] else None
            res_dict["customer_contact"] = conv.customer_contact
            res_dict["is_inbound_chunk"] = conv.direction_inbound
        if conversation_model:
            res_dict["sender_id"] = (
                conversation_model.sender_id
                if conversation_model.sender_id and conversation_model.sender_id not in ["None"]
                else None
            )
            res_dict["customer_contact"] = conversation_model.customer_contact
            res_dict["is_inbound_chunk"] = conversation_model.child_direction_inbound
        attributes = res_dict.pop("attributes", None)
        model = ConversationChunkModel.model_validate(res_dict)
        if attributes:
            model.attributes = ChunkAttributes(attributes=attributes).attributes
        return model

    def get_model(self, conv=None) -> ConversationChunkModel:
        return self.__to_model(conversation_model=conv)

    @classmethod
    def create_chunk(cls, chunk):
        db: Session = get_db_session()
        db.add(chunk)
        db.flush()

    @classmethod
    def get_conversation_chunks_by_conv_number(
        cls,
        conversation_number: str,
        start=None,
        end=None,
        filter_agent_specific_chunks=False,
        filter_chunk_types: list[str] | None = None,
        filter_empty_to_user=False,
        channel=None,
        ignore_chunk_types: list[str] | None = None,
        limit: int | None = None,
        include_customer_signals: bool = False,
    ) -> list[ConversationChunkModel]:
        db: Session = get_db_session()
        if start:
            start = datetime.datetime.fromtimestamp(start / 1000.0, tz=datetime.UTC)
        if end:
            end = datetime.datetime.fromtimestamp(end / 1000.0, tz=datetime.UTC)
        conversation_query = (
            cls.select_query(db, columns=[cls, Conversation])
            .join(cls.conv.and_(Conversation.is_deleted.is_(False)))
            .join(ConversationParent, ConversationParent.id == Conversation.parent)
            .filter(ConversationParent.conversation_number == conversation_number)
            .filter(cls.is_deleted == False)
        )
        if start and end:
            conversation_query = conversation_query.filter(start < cls.timestamp).filter(cls.timestamp < end)
        elif start:
            conversation_query = conversation_query.filter(start < cls.timestamp)
        elif end:
            conversation_query = conversation_query.filter(cls.timestamp < end)
        if filter_agent_specific_chunks:
            # The customer read (include_customer_signals) keeps `signal` rows so the
            # lifecycle dividers survive a refresh; a downstream event_name filter then drops the
            # non-customer-facing ones. notes/recording/transfer stay excluded either way.
            exclude_types = (
                [t for t in AGENT_SPECIFIC_CHUNK_TYPES if t != ConversationChunkTypes.signal.name]
                if include_customer_signals
                else AGENT_SPECIFIC_CHUNK_TYPES
            )
            conversation_query = conversation_query.filter(cls.type.notin_(exclude_types))
        if filter_chunk_types:
            conversation_query = conversation_query.filter(cls.type.in_(filter_chunk_types))
        if ignore_chunk_types:
            conversation_query = conversation_query.filter(cls.type.notin_(ignore_chunk_types))
        if filter_empty_to_user:
            conversation_query = conversation_query.filter(or_(cls.to_user == EMPTY_TO_USER, cls.to_user is None))
        if channel:
            conversation_query = conversation_query.filter(cls.channel == channel)
        # Apply the default chunk cap only without a time window; an explicit limit wins.
        effective_limit = (
            limit if limit is not None else (DEFAULT_CONVERSATION_CHUNK_LIMIT if not start and not end else None)
        )
        conversation_query = conversation_query.order_by(cls.timestamp.desc())
        if effective_limit is not None:
            conversation_query = conversation_query.limit(effective_limit)
        chunks_from_db = conversation_query.all()
        chunks: list[ConversationChunkModel] = [
            chunk_from_db.__to_model(conv=conv) for chunk_from_db, conv in chunks_from_db
        ]
        return chunks

    @classmethod
    def update_conversation_chunk_by_id(cls, chunk_id: str, update_dict: dict, now=None) -> bool:
        db: Session = get_db_session()
        if now:
            update_dict.update({cls.updated_at.name: now})
        cls.select_query(db).filter(cls.id == chunk_id).update(update_dict)
        db.flush()


class ConvSecondaryAgentMapping(DB, CoreDBBase):
    __tablename__ = "conversation_secondary_aom_mapping"
    aom = Column(Integer, ForeignKey(AccountOrgMapping.id))
    conversation = Column(Integer, ForeignKey("conversation_parent.id"))
    agent = relationship(AccountOrgMapping, foreign_keys=[aom])
    secondary_conversation = Column(Integer, ForeignKey("conversation_parent.id"))
    secondary_agent_state = Column(String)
    participant_identifier = Column(String)

    def __to_model(self) -> ConvSecAgentMappingModel:
        res = ConvSecAgentMappingModel.model_validate(self)
        if self.parent_conversation:
            res.parent_conversation = self.parent_conversation.get_model()
        if self.secondary_parent_conversation:
            res.secondary_parent_conversation = self.secondary_parent_conversation.get_model()
        return res

    @classmethod
    def _model_graph(cls):  # eager for the same reason as Conversation._model_graph
        return joinedload(cls.agent), joinedload(cls.parent_conversation), joinedload(cls.secondary_parent_conversation)

    @classmethod
    def create_mapping(cls, mapping):
        db: Session = get_db_session()
        db.add(mapping)
        db.flush()
        return mapping.__to_model()

    @classmethod
    def _matching(cls, db: Session, conversation_id, aom_id=None, participant_identifier=None, active=True, states=()):
        query = cls.select_query(db).filter(cls.conversation == conversation_id)
        if aom_id:
            query = query.filter(cls.aom == aom_id)
        if participant_identifier:
            query = query.filter(cls.participant_identifier == participant_identifier)
        if not active:
            query = query.filter(cls.secondary_conversation.is_(None))
        return query.filter(cls.secondary_agent_state.in_(states)) if states else query

    @classmethod
    def delete_mapping(
        cls, conversation_id, aom_id=None, participant_identifier=None, secondary_state=None, active=True, states=()
    ) -> int:
        db: Session = get_db_session()
        update_dict = {cls.is_deleted: True} | ({cls.secondary_agent_state: secondary_state} if secondary_state else {})
        rows = cls._matching(db, conversation_id, aom_id, participant_identifier, active, states).update(update_dict)
        db.flush()
        return rows or 0

    @classmethod
    def update_secondary_agent_state(
        cls, conversation_id, secondary_agent_state: str, aom_id=None, participant_identifier=None, now=None, states=()
    ):
        db: Session = get_db_session()
        update_dict = {cls.secondary_agent_state: secondary_agent_state} | ({cls.updated_at.name: now} if now else {})
        cls._matching(db, conversation_id, aom_id, participant_identifier, False, states).update(update_dict)
        db.flush()

    @classmethod
    def get_secondary_conv_by_identifiers(
        cls, conversation_id, aom_id=None, participant_identifier=None, active: bool = True, states=()
    ) -> ConvSecAgentMappingModel:
        query = cls._matching(get_db_session(), conversation_id, aom_id, participant_identifier, active, states)
        sec_mapping = query.options(*cls._model_graph()).first()
        return sec_mapping.__to_model() if sec_mapping else None

    @classmethod
    def get_monitor_barge_secondary_conversation_by_conv_id(cls, conversation_id) -> ConvSecAgentMappingModel:
        db: Session = get_db_session()
        sec_mapping = (
            db.query(cls)
            .join(cls.agent)
            .filter(cls.conversation == conversation_id, cls.secondary_agent_state.in_(MONITOR_BARGE_COACH_STATE_LIST))
            .order_by(cls.created_at.desc())
            .options(contains_eager(cls.agent))
            .first()
        )
        return sec_mapping.__to_model() if sec_mapping else None

    @classmethod
    def delete_all_secondary_mappings(cls, parent_conversation_id: str):
        db: Session = get_db_session()
        cls.select_query(db).filter(
            or_(
                and_(cls.conversation == parent_conversation_id, cls.secondary_conversation.is_(None)),
                cls.secondary_conversation == parent_conversation_id,
            )
        ).filter(cls.secondary_agent_state.notin_(MONITOR_BARGE_COACH_STATE_LIST)).update({cls.is_deleted: True})
        db.flush()


def _tenant(org_id) -> str:
    return str(Organization.get_by_id(int(org_id)).uuid)


def claim_offer_outcome(offer_id: str, agent: str) -> bool:
    """True for the first caller only: an offer reports one outcome (accepted, missed, rejected or withdrawn)."""
    return Cache.get_instance().set(f"OFFER_OUTCOME:{offer_id}:{agent}", "1", expiry_in_seconds=86400, nx=True)


def emit_by_number(type: str, conversation_number: str, data: dict, agent=None) -> None:
    """emit() for a call path that knows only the conversation number; nothing when it has no conversation."""
    if parent := ConversationParent.get_by_conversation_number(conversation_number):
        emit(type, parent, data, agent)


def emit(type: str, parent, data: dict, agent=None, session: Session | None = None) -> None:
    """Emit an event of the conversation `parent` (a conversation_parent row or model); `agent` is an aom uuid."""
    from hs import events

    events.emit(
        type,
        tenant_id=_tenant(parent.organization),
        data=data,
        conversation_id=str(parent.uuid),
        agent_id=str(agent) if agent else None,
        session=session,
    )
