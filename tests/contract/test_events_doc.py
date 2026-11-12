from pathlib import Path

from hs.events.catalog import CATALOG


def test_every_event_type_is_in_the_events_doc():
    doc = Path(__file__).parents[2].joinpath("docs/events.md").read_text()
    assert [t for t in CATALOG if f"`{t}`" not in doc] == []
