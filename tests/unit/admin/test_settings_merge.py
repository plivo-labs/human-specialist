from hs.admin.api import merge_patch


def test_merge_patch_follows_rfc_7396():
    target = {"wrap_up": {"mode": "manual", "seconds": 60}, "consent": {"inbound": {"text": "hi"}}, "x": [1]}
    patch = {"wrap_up": {"seconds": 7}, "consent": {"inbound": None}, "x": [2], "new": {"a": None, "b": 1}}
    assert merge_patch(target, patch) == {
        "wrap_up": {"mode": "manual", "seconds": 7},
        "consent": {},
        "x": [2],
        "new": {"b": 1},
    }
    assert target["wrap_up"]["seconds"] == 60
