import pytest

from hs.infra.helpers import get_number_in_e164_format, is_anonymous_identifier


@pytest.mark.parametrize(
    "raw,want",
    [
        ("14155550100", "+14155550100"),
        ("+1 (415) 555-0100", "+14155550100"),
        ("+14155550100#12", "+14155550100#12"),
        ("anonymous", "Anonymous"),
        ("+0123", "+0123"),
        ("sip:agent@phone.plivo.com", "sip:agent@phone.plivo.com"),
        ("", ""),
    ],
)
def test_e164(raw, want):
    assert get_number_in_e164_format(raw) == want


def test_anything_but_a_number_is_anonymous():
    assert not is_anonymous_identifier("+14155550100")
    assert is_anonymous_identifier("restricted") and is_anonymous_identifier("bob@example.com")
