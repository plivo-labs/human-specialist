"""Offers are read and written only in hs.conversations (`rows`, `desk`, `held`, `offer`, `dial`, `edit` and `move`),
so where an offer lives is that package's choice: an open one in Redis, witnessed by its `offer_timeout` row. Any
other code that names the assignment table reads stints alone, and any that names an offer's timer is listed here:
a read that joins open offers into a wider statement."""

import ast
import re
from pathlib import Path

import pytest

import hs

SRC = Path(hs.__file__).parent
TABLE = re.compile(r"\b(?:FROM|JOIN|UPDATE|INTO)\s+(?:\"?hs\"?\.)?\"?assignment\b|\bT\.assignment\b", re.IGNORECASE)
TIMER = re.compile(r"kind = 'offer_timeout'")
STINTS = {
    ("access.py", "_VISIBLE"),
    ("events/__init__.py", "FACTS"),
    ("export.py", "FACTS"),
    ("export.py", "ITEMS"),
    ("hot.py", "ACCEPTED"),
    ("reports.py", "FLOOR"),
    ("reports.py", "ROWS"),
    ("reports.py", "WORKED"),
    ("reports.py", "_IN"),
    ("routing.py", "_RANK"),
    ("routing.py", "_STUCK"),
    ("search.py", "FACTS"),
    ("search.py", "document"),
    ("tasks.py", "_FREE"),
    ("tasks.py", "_TASKS"),
    ("text/__init__.py", "_CAME"),
    ("text/__init__.py", "reply"),
    ("text/switch.py", "_FAMILY"),
    ("text/switch.py", "_UP"),
    ("voice/calls.py", "_coached"),
    ("voice/calls.py", "monitor"),
    ("voice/media.py", "_listed"),
    ("voice/media.py", "get_transcript"),
    ("voice/mpc.py", "_PLACE"),
    ("voice/outbound.py", "answer"),
}
# events sits below conversations in the import order; the rest join open offers into a wider statement, or find a
# lost tenant's conversations that hold one.
OFFERS = {
    ("access.py", "_VISIBLE"),
    ("events/__init__.py", "FACTS"),
    ("reports.py", "FLOOR"),
    ("routing.py", "_STRANDED"),
    ("routing.py", "_WAITING"),
    ("tasks.py", "_FREE"),
    ("tasks.py", "_TASKS"),
}


def naming(pattern: re.Pattern) -> set[tuple[str, str]]:
    """(module, top-level name) for each definition outside hs.conversations whose source matches `pattern`."""
    found = set()
    for path in SRC.rglob("*.py"):
        if (rel := path.relative_to(SRC).as_posix()).startswith("conversations/"):
            continue
        source = path.read_text()
        for node in ast.parse(source).body:
            if not isinstance(node, ast.Expr) and pattern.search(ast.get_source_segment(source, node) or ""):
                found.add((rel, getattr(node, "name", None) or ast.unparse(node.targets[0])))
    return found


def test_only_conversations_reads_or_writes_offers():
    assert naming(TABLE) == STINTS
    assert naming(TIMER) == OFFERS


@pytest.mark.parametrize(
    "source",
    [
        "SELECT 1 FROM assignment",
        "select 1 from assignment",
        "select 1 from hs.assignment",
        'select 1 from "assignment"',
        'select 1 from "hs"."assignment"',
        'SELECT 1 FROM x JOIN "assignment" a ON true',
        "update hs.assignment set x = 1",
        "INSERT INTO assignment (a) VALUES (1)",
        "delete from assignment where true",
        "T.assignment.c.id",
    ],
)
def test_the_matcher_catches_every_way_to_name_the_table(source):
    assert TABLE.search(source)
