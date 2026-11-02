from datetime import UTC

from sqlalchemy.exc import IntegrityError

from hs.admin.account import AccountOrgMapping
from hs.infra.context import get_db_session
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.infra.models import UseCaseResponse
from hs.voice.call_models import INTERNAL_PARTICIPANTS, CallLegInsertModel
from hs.voice.call_store import CallLeg


def _provider_time_is_earlier(candidate: str, current: str) -> bool:
    """Return true only when a provider timestamp is safely known to be earlier."""
    if not candidate:
        return False
    if not current:
        return True
    try:
        from datetime import datetime

        def _parse(value: str):
            normalized = str(value).strip()
            # A timestamp can carry a redundant zone name after +0000.
            parts = normalized.rsplit(" ", 1)
            if len(parts) == 2 and parts[1].isalpha():
                normalized = parts[0]
            normalized = normalized.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)

        return _parse(candidate) < _parse(current)
    except (TypeError, ValueError, OverflowError):
        # Never replace a non-empty persisted timestamp on an ambiguous comparison.
        return False


def is_duplicate_key_error(error: Exception) -> bool:
    error_str = str(error).lower()
    return (
        "duplicate key" in error_str
        or "unique constraint" in error_str
        or "violates unique constraint" in error_str
        or "23505" in error_str
    )


MSG_CREATE_CALL_LEG_SUCCESS = "Call leg created successfully"
MSG_CREATE_CALL_LEG_ERROR = "Failed to create call leg"
MSG_CREATE_CALL_LEG_ERROR_ROLE = "Role is required"
MSG_CREATE_CALL_LEG_ERROR_MEMBER_ADDRESS = "Member address is not a valid SIP endpoint"
MSG_CREATE_CALL_LEG_ERROR_ACCOUNT_ORG_MAPPING = "Account org mapping not found for SIP endpoint"
MSG_UPDATE_CALL_LEG_SUCCESS = "Call leg updated successfully"
MSG_UPDATE_CALL_LEG_ERROR = "Failed to update call leg"
MSG_CREATE_CALL_LEG_ERROR_PARENT_CALL_LEG_EXISTS = "Parent call leg already exists"
MSG_CREATE_CALL_LEG_ERROR_CALL_LEG_EXISTS = "Call leg already exists"
MSG_UPDATE_CALL_LEG_ERROR_PARENT_CALL_LEG_NOT_FOUND = "Parent call leg not found"
MSG_CALL_LEG_FOREIGN = "Call leg belongs to another call"


def _foreign(call_leg: CallLegInsertModel) -> bool:
    """A callback signed for its own call names a leg UUID it chose; it may act only on that call's legs."""
    if not CallLeg.outside_call(call_leg.call_leg_uuid, call_leg.call_uuid):
        return False
    logger.warning(f"refused a callback naming a call leg outside its call - {call_leg.call_uuid}")
    return True


def _set_terminal_end_time_once(call_uuid: str, terminal_end_time: str) -> UseCaseResponse:
    """Atomically persist the first non-empty terminal timestamp.

    Provider replays can carry older or newer timestamps. The first durable
    terminal fact is immutable so duplicate callbacks cannot drift billing
    duration in either direction.
    """
    if not terminal_end_time:
        return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CALL_LEG_SUCCESS)
    try:
        db = get_db_session()
        update_dict = {CallLeg.end_time.name: terminal_end_time, CallLeg.updated_at.name: get_now()}
        for empty_value in (None, ""):
            query = CallLeg.select_query(db).filter(CallLeg.call_uuid == call_uuid)
            if empty_value is None:
                query = query.filter(CallLeg.end_time.is_(None))
            else:
                query = query.filter(CallLeg.end_time == "")
            updated = query.update(update_dict, synchronize_session=False)
            db.flush()
            if updated:
                logger.info(f"in finalize_call_leg, persisted first terminal timestamp - {call_uuid}")
                return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CALL_LEG_SUCCESS)
        call_leg = CallLeg.get_any_by_call_uuid(call_uuid)
        if call_leg:
            logger.info(
                f"in finalize_call_leg, terminal timestamp already persisted; preserving first value - {call_uuid}"
            )
            return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CALL_LEG_SUCCESS)
        return UseCaseResponse(status_code=400, error=(MSG_UPDATE_CALL_LEG_ERROR_PARENT_CALL_LEG_NOT_FOUND))
    except Exception as e:
        logger.error(f"in finalize_call_leg, failed to persist terminal timestamp Error - {e}")
        return UseCaseResponse(status_code=503, msg=MSG_UPDATE_CALL_LEG_ERROR, error=str(e))


def _reconcile_existing_leg(call_leg: CallLegInsertModel, existing, participant: bool = False):
    """Move an existing leg's start earlier and, for a participant leg, fill an unresolved internal identity;
    returns the leg as stored."""
    updates = {}
    if _provider_time_is_earlier(call_leg.start_time, getattr(existing, "start_time", "")):
        updates[CallLeg.start_time.name] = call_leg.start_time
    # A terminal-first callback can only persist an unresolved internal leg. A later Join carries the SIP
    # identity; enrich the existing row rather than treating the placeholder as permanently complete.
    if (
        participant
        and call_leg.role in INTERNAL_PARTICIPANTS
        and not getattr(existing, "participant", None)
        and call_leg.member_address
        and call_leg.member_address.startswith("sip:")
    ):
        aom = AccountOrgMapping.get_by_sip_endpoint(call_leg.member_address)
        if aom:
            updates[CallLeg.participant.name] = aom.id
    if not updates:
        return existing
    CallLeg.update_call_leg(call_leg.call_leg_uuid, updates)
    return CallLeg.get_by_call_uuid(call_leg.call_leg_uuid, role=call_leg.role if participant else None)


def create_parent_call_leg(call_leg: CallLegInsertModel) -> UseCaseResponse:
    try:
        parent_call_leg = CallLeg.get_by_call_uuid(call_leg.call_leg_uuid)
        if parent_call_leg:
            # ParticipantJoin can arrive before MPCInitialised.  Its fallback
            # parent has the right canonical UUID but only knows the later
            # participant timestamp.  Reconcile to the authoritative, earlier
            # MPC start without ever moving an already-valid start forward.
            parent_call_leg = _reconcile_existing_leg(call_leg, parent_call_leg)
            logger.info(f"in create_parent_call_leg, parent call leg already exists - {call_leg.call_leg_uuid}")
            return UseCaseResponse(
                status_code=201, msg=MSG_CREATE_CALL_LEG_ERROR_PARENT_CALL_LEG_EXISTS, data=parent_call_leg
            )
        call_leg_db = call_leg.build_parent_call_leg_db_model()
        parent_call_leg = CallLeg.create_call_leg(call_leg_db)
        logger.info(f"in create_parent_call_leg, created successfully - {parent_call_leg}")
        return UseCaseResponse(status_code=201, msg=MSG_CREATE_CALL_LEG_SUCCESS, data=parent_call_leg)
    except (IntegrityError, Exception) as e:
        if is_duplicate_key_error(e):
            logger.info(
                f"in create_parent_call_leg, race condition detected, fetching existing - {call_leg.call_leg_uuid}"
            )
            existing_call_leg = CallLeg.get_by_call_uuid(call_leg.call_leg_uuid)
            if existing_call_leg:
                return UseCaseResponse(
                    status_code=201,
                    msg=MSG_CREATE_CALL_LEG_ERROR_PARENT_CALL_LEG_EXISTS,
                    data=_reconcile_existing_leg(call_leg, existing_call_leg),
                )
        logger.error(f"in create_parent_call_leg, failed to create call leg Error - {e}")
        return UseCaseResponse(status_code=400, msg=MSG_CREATE_CALL_LEG_ERROR, error=str(e))


def create_call_leg(call_leg: CallLegInsertModel, allow_unresolved_participant: bool = False) -> UseCaseResponse:
    try:
        if not call_leg.role:
            logger.error(f"in create_call_leg, role is required - {call_leg.call_leg_uuid}")
            return UseCaseResponse(status_code=400, error=MSG_CREATE_CALL_LEG_ERROR_ROLE)
        if _foreign(call_leg):
            return UseCaseResponse(status_code=400, error=MSG_CALL_LEG_FOREIGN)
        existing_call_leg = CallLeg.get_by_call_uuid(call_leg.call_leg_uuid, role=call_leg.role)
        if existing_call_leg:
            existing_call_leg = _reconcile_existing_leg(call_leg, existing_call_leg, participant=True)
            logger.info(f"in create_call_leg, call leg already exists - {existing_call_leg}")
            return UseCaseResponse(
                status_code=201, msg=MSG_CREATE_CALL_LEG_ERROR_CALL_LEG_EXISTS, data=existing_call_leg
            )
        call_leg.participant = call_leg.member_address
        if call_leg.role in INTERNAL_PARTICIPANTS:
            if not call_leg.member_address:
                if not allow_unresolved_participant:
                    logger.error(f"in create_call_leg, member address is required - {call_leg.call_leg_uuid}")
                    return UseCaseResponse(status_code=400, error=MSG_CREATE_CALL_LEG_ERROR_MEMBER_ADDRESS)
                # A provider retry repeats the same incomplete payload forever.
                # Keep the terminal fact linked to the canonical parent and let
                # a late ParticipantJoin fill the participant identity.
                call_leg.participant = None
                logger.warning(
                    f"in create_call_leg, persisting terminal internal leg without participant identity - "
                    f"{call_leg.call_leg_uuid}"
                )
            elif call_leg.member_address.startswith("sip:"):
                aom = AccountOrgMapping.get_by_sip_endpoint(call_leg.member_address)
                if not aom:
                    logger.error(
                        f"in create_call_leg, account org mapping not found for SIP endpoint - {call_leg.call_leg_uuid}"
                    )
                    return UseCaseResponse(status_code=400, error=MSG_CREATE_CALL_LEG_ERROR_ACCOUNT_ORG_MAPPING)
                call_leg.participant = aom.id
        parent_call_leg = CallLeg.get_by_call_uuid(call_leg.call_uuid)
        if not parent_call_leg:
            logger.info(
                f"in create_call_leg, canonical parent call leg not found - {call_leg.call_uuid}, creating it "
                f"before participant"
            )
            # Participant callbacks may beat MPC/main-leg creation. The fallback
            # parent identity is the MPC call_uuid, never the participant's
            # call_leg_uuid; otherwise later callbacks create a second parent and
            # the participant is permanently linked to the fabricated one.
            canonical_parent = CallLegInsertModel(
                call_uuid=call_leg.call_uuid,
                call_leg_uuid=call_leg.call_uuid,
                member_address=None,
                start_time=call_leg.start_time,
                end_time=None,
                role=None,
                participant=None,
            )
            parent_call_leg_response = create_parent_call_leg(canonical_parent)
            if parent_call_leg_response.status_code != 201:
                logger.error(f"in create_call_leg, failed to create parent call leg - {parent_call_leg_response}")
                return UseCaseResponse(status_code=400, error=parent_call_leg_response.error)
            parent_call_leg = parent_call_leg_response.data
        call_leg_db = call_leg.build_db_model(parent_call_leg.id)
        call_leg_model = CallLeg.create_call_leg(call_leg_db)
        logger.info(f"in create_call_leg, created successfully - {call_leg_model}")
        return UseCaseResponse(status_code=201, msg=MSG_CREATE_CALL_LEG_SUCCESS, data=call_leg_model)
    except Exception as e:
        if is_duplicate_key_error(e):
            logger.info(f"in create_call_leg, race condition detected, fetching existing - {call_leg.call_leg_uuid}")
            existing_call_leg = CallLeg.get_by_call_uuid(call_leg.call_leg_uuid, role=call_leg.role)
            if existing_call_leg:
                return UseCaseResponse(
                    status_code=201,
                    msg=MSG_CREATE_CALL_LEG_ERROR_CALL_LEG_EXISTS,
                    data=_reconcile_existing_leg(call_leg, existing_call_leg, participant=True),
                )
        logger.error(f"in create_call_leg, failed to create call leg Error - {e}")
        return UseCaseResponse(status_code=400, msg=MSG_CREATE_CALL_LEG_ERROR, error=str(e))


def finalize_call_leg(call_leg: CallLegInsertModel, is_parent: bool = False) -> UseCaseResponse:
    """Idempotently upsert a provider leg and persist its terminal timestamp.

    ParticipantExit/MPCEnd can beat the corresponding Join/Initialised callback.
    Finalization must therefore be a create-or-update operation, not an update-only
    detached task.  The existing create paths retain their validation, canonical
    parent linkage, and savepoint-isolated duplicate recovery.
    """
    leg_uuid = call_leg.call_leg_uuid
    if not leg_uuid:
        return UseCaseResponse(status_code=400, error=MSG_UPDATE_CALL_LEG_ERROR_PARENT_CALL_LEG_NOT_FOUND)
    existing = CallLeg.get_by_call_uuid(leg_uuid) if is_parent else CallLeg.get_any_by_call_uuid(leg_uuid)
    if existing is None:
        create_response = (
            create_parent_call_leg(call_leg)
            if is_parent
            else create_call_leg(call_leg, allow_unresolved_participant=True)
        )
        if create_response.error:
            return create_response
    elif not is_parent and _foreign(call_leg):
        return UseCaseResponse(status_code=400, error=MSG_CALL_LEG_FOREIGN)
    if call_leg.end_time:
        return _set_terminal_end_time_once(leg_uuid, call_leg.end_time)
    return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CALL_LEG_SUCCESS)
