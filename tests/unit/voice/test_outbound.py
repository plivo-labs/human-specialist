import uuid
from urllib.parse import unquote

import pytest

from hs.access import Principal
from hs.errors import ApiError
from hs.voice import outbound

KEY = Principal("t", -1, "key", frozenset({"calls"}), key_id="k1")


def test_hs_headers_carry_only_what_plivo_accepts():
    value = "ab3c-0d.e_f~g h/é"
    encoded = outbound.hs_header(value)
    assert all(c.isalnum() or c == "%" for c in encoded) and unquote(encoded) == value


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({"to": "12345"}, "invalid_number"),
        ({"to": "sip:agent@phone.plivo.com"}, "invalid_number"),
        ({"to": "+14155550100", "context": {"note": "a\x00b"}}, "context_invalid"),
        ({"to": "+14155550100", "context": {"x": "y" * 2100}}, "context_too_large"),
    ],
)
def test_a_dial_is_checked_before_anything_is_opened(body, code):
    with pytest.raises(ApiError) as e:
        outbound._open(KEY, outbound.DialIn(agent_id=uuid.uuid4(), **body))
    assert (e.value.status, e.value.code) == (422, code)
