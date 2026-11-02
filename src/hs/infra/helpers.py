import re as _re
import threading
import uuid as _uuid
from datetime import UTC, datetime

from hs.infra.constants import AOM_NAME_CACHE_PREFIX


class Singleton:
    _instances = {}
    # Construction is serialized: get_instance() is called from several threads, and an unlocked
    # check-then-set lets two first-callers construct concurrently.
    _lock = threading.Lock()

    def __init__(self, cls):
        self._cls = cls

    def get_instance(self):
        if self._cls not in self._instances:
            with Singleton._lock:
                if self._cls not in self._instances:  # double-checked: exactly one thread constructs
                    self._instances[self._cls] = self._cls()
        return self._instances[self._cls]

    def __call__(self):
        raise TypeError("Singletons must be accessed through `get_instance()`.")

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)


def is_valid_uuid(uuid_string) -> bool:
    # Pure predicate — no logging. It is routinely asked about NON-uuid identifiers by design
    # on normal calls, so logging here would be noise. Callers own any logging.
    try:
        _uuid.UUID(str(uuid_string), version=4)
    except Exception:
        return False
    return True


def get_now(utc=True):
    if utc:
        return datetime.now(UTC)
    return datetime.now()


def get_utc(d: datetime) -> datetime:
    return d.astimezone(UTC)


def get_time_difference_in_seconds(a: datetime, b: datetime) -> float:
    return (get_utc(a) - get_utc(b)).total_seconds()


def build_cache_key_without_integration(identifier_name: str, unique_id: str, entity_name: str | None = None) -> str:
    key_name = f"{str(identifier_name).lower()}:{unique_id}"
    if entity_name:
        key_name += f":{str(entity_name).lower()}"
    return key_name


_ANONYMOUS_CALLER_IDS = ["anonymous", "1000000000", "restricted"]
_PHONE_NUMBER_REGEX = r"^([+]?\d{1,15})(#\d{1,9})?$"
_E164 = _re.compile(r"\+[1-9]\d{1,14}")
_ANONYMOUS_NUMBER = "Anonymous"


def get_number_in_e164_format(number) -> str:
    """E.164 (`+` and 2-15 digits) with separators dropped and an optional `#extension`; anything else unchanged."""
    number = (number or "").strip().lower()
    if number in _ANONYMOUS_CALLER_IDS:
        return _ANONYMOUS_NUMBER
    digits, _, extension = number.partition("#")
    candidate = "+" + _re.sub(r"[\s().-]", "", digits).removeprefix("+")
    if not _E164.fullmatch(candidate):
        return number
    return f"{candidate}#{extension}" if extension.isdigit() else candidate


def validate_phone_number(number):
    if number and not _re.search(_PHONE_NUMBER_REGEX, number):
        raise ValueError("Invalid number")
    return number


def is_valid_phone_number(number) -> bool:
    try:
        if number:
            validate_phone_number(get_number_in_e164_format(number))
            return True
    except Exception:
        pass
    return False


# Voice only: a contact identifier is a phone number or anonymous.
is_valid_contact_identifier_for_chunk = is_valid_phone_number


def is_anonymous_identifier(contact_identifier) -> bool:
    return not is_valid_phone_number(contact_identifier)


def build_cache_key_for_aom_name(aom_uuid: str, org_id: str) -> str:
    return "{" + str(org_id) + "}" + f"{AOM_NAME_CACHE_PREFIX}:{aom_uuid}"


def sanitize_string_input(input_string: str) -> str:
    if not isinstance(input_string, str):
        return ""
    return " ".join(input_string.split())
