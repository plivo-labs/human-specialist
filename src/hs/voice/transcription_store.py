from sqlalchemy import Column, Float, ForeignKey, Integer, String, select, text
from sqlalchemy.orm import Session, noload, relationship, selectinload

from hs.conversations.store import Recording
from hs.infra.context import get_db_session
from hs.infra.db import DB, CoreDBBase
from hs.voice.call_store import CallLeg
from hs.voice.transcription_models import TranscriptModel, TranscriptSegmentModel


def _first_by_leg(rows) -> dict:
    """First row per call_leg_id in scan order: the row an unordered LIMIT 1 on the same index returns."""
    first = {}
    for row in rows:
        first.setdefault(str(row.call_leg_id), row)
    return first


class Transcription(DB, CoreDBBase):
    __tablename__ = "transcription"
    call_leg_id = Column(Integer, ForeignKey(CallLeg.id), nullable=False)
    language = Column(String, nullable=False)
    transcription_segments = relationship("TranscriptionSegment", back_populates="transcription")

    def __to_model(self) -> TranscriptModel:
        return TranscriptModel.model_validate(self)

    @classmethod
    def _participant_stmts(cls, call_leg_ids):
        ids = [int(i) for i in call_leg_ids]
        return (
            select(Recording).where(Recording.call_leg_id.in_(ids), Recording.is_deleted.is_(False)),
            select(cls)
            .where(cls.call_leg_id.in_(ids), cls.is_deleted.is_(False))
            .options(selectinload(cls.transcription_segments)),
        )

    @classmethod
    def _bundle(cls, recording_rows, transcript_rows) -> tuple:
        # Keyed by str(call_leg_id) to match CallLegModel.id.
        transcripts = _first_by_leg(transcript_rows)
        return _first_by_leg(recording_rows), {leg: row.__to_model() for leg, row in transcripts.items()}

    @classmethod
    def get_participant_bundle(cls, call_leg_ids) -> tuple:
        """({leg_id: Recording row}, {leg_id: TranscriptModel}) for every participant leg of one MPC
        recording: two SELECTs plus one batched segment load, instead of 2N+K per-leg reads."""
        if not call_leg_ids:
            return {}, {}
        db: Session = get_db_session()
        recording_stmt, transcript_stmt = cls._participant_stmts(call_leg_ids)
        return cls._bundle(db.execute(recording_stmt).scalars().all(), db.execute(transcript_stmt).scalars().all())

    @classmethod
    def get_or_create_transcript(cls, call_leg_id: int) -> TranscriptModel:
        db: Session = get_db_session()
        transcript = (
            cls.select_query(db)
            .options(noload(cls.transcription_segments))
            .filter(cls.call_leg_id == call_leg_id)
            .first()
        )
        if transcript:
            return transcript.__to_model()
        from sqlalchemy.exc import IntegrityError

        try:
            transcript_model = TranscriptModel(call_leg_id=call_leg_id)
            transcript_db = transcript_model.build_db_model()
            db.add(transcript_db)
            db.flush()
            return transcript_db.__to_model()
        except IntegrityError:
            db.rollback()
            transcript = (
                cls.select_query(db)
                .options(noload(cls.transcription_segments))
                .filter(cls.call_leg_id == call_leg_id)
                .first()
            )
            return transcript.__to_model()

    @classmethod
    def acquire_ingestion_lock(cls, recording_uuid: str) -> None:
        """Serialize duplicate ingestion attempts for one provider recording.

        Plivo callbacks and transcript-attach job retries can overlap.  The advisory lock is
        transaction-scoped, so the second request waits for the first request's commit
        and then observes its segments before inserting.  SQLite-backed unit tests skip
        the Postgres-only statement.
        """
        db: Session = get_db_session()
        if db.get_bind().dialect.name == "postgresql":
            db.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:recording_uuid))"), {"recording_uuid": recording_uuid}
            )


class TranscriptionSegment(DB, CoreDBBase):
    __tablename__ = "transcription_segment"
    transcription_id = Column(Integer, ForeignKey(Transcription.id), nullable=False)
    start_time = Column(Float, nullable=False)
    end_time = Column(Float, nullable=False)
    segment = Column(String, nullable=False)
    transcription = relationship(Transcription)
    start_ts = Column(Float, nullable=True)

    def __to_model(self) -> TranscriptSegmentModel:
        return TranscriptSegmentModel.model_validate(self)

    @staticmethod
    def _ingestion_fingerprint(start_time, end_time, segment, start_ts):
        def _number(value):
            return None if value is None else round(float(value), 6)

        return (_number(start_time), _number(end_time), str(segment), _number(start_ts))

    @classmethod
    def create_missing_transcript_segments(cls, transcription_id: int, transcript_segments: list) -> int:
        """Insert only provider segments not already committed for this transcript.

        The caller holds ``Transcription.acquire_ingestion_lock(recording_uuid)``.
        Comparing the participant-relative offsets, text, and absolute timestamp keeps
        retries idempotent while still allowing a later recording on the same call leg
        to append distinct segments.
        """
        db: Session = get_db_session()
        existing = (
            db.query(cls.start_time, cls.end_time, cls.segment, cls.start_ts)
            .filter(cls.transcription_id == transcription_id, cls.is_deleted.is_(False))
            .all()
        )
        fingerprints = {
            cls._ingestion_fingerprint(row.start_time, row.end_time, row.segment, row.start_ts) for row in existing
        }
        missing = []
        for segment in transcript_segments:
            fingerprint = cls._ingestion_fingerprint(
                segment.start_time, segment.end_time, segment.segment, segment.start_ts
            )
            if fingerprint in fingerprints:
                continue
            fingerprints.add(fingerprint)
            missing.append(segment)
        if missing:
            db.add_all(missing)
            db.flush()
        return len(missing)
