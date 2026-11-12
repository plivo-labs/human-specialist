"""Every Redis key template, frozen. A changed template strands what the running release wrote, so a change ships in
two releases: write both, then read the new."""

import ast
from pathlib import Path

import hs
from hs import hot

FROZEN = {"EPOCH": "hs:{%s}:epoch", "HB": "hs:{%s}:hb:%s", "BEATS": "hs:{%s}:beats:%s"}
FROZEN |= {"GEN": "hs:{%s}:cfg", "CFG": "hs:{%s}:cfg:%s"}
FROZEN |= {"ROUTE": "hs:{%s}:route:%s", "TRIED": "hs:{%s}:tried:%s"}
FROZEN |= {"CAP": "hs:{%s}:cap:%s:%s", "FULL": "hs:{%s}:full:%s"}
FROZEN |= {"STICKY": "hs:{%s}:sticky:%s:%s", "OSTICKY": "hs:{%s}:osticky:%s"}
FROZEN |= {"UNREAD": "hs:{%s}:unread:%s", "WH": "hs:{%s}:wh:%s", "SIGNIN": "hs:{%s}:signin"}
FROZEN |= {
    "OFFERS": "hs:{%s}:offers:%s",
    "DESK": "hs:{%s}:desk:%s",
    "OFFER_CONVERSATION": "hs:{%s}:offer_conversation:%s",
}


def test_every_key_template_is_frozen_and_hash_tagged_by_its_tenant():
    assert {k: v for k, v in vars(hot).items() if isinstance(v, str) and v.startswith("hs:")} == FROZEN
    assert all(t.startswith("hs:{%s}:") for t in FROZEN.values())


def test_every_redis_key_the_code_names_is_hash_tagged():
    named = {
        (path.name, node.value)
        for path in Path(hs.__file__).parent.rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("hs:")
    }
    assert {(f, key) for f, key in named if not key.startswith("hs:{")} == set()
