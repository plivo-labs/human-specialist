"""An accept stamps the child with the open and in-progress states without a None check, so register must seed both."""

import tests.conftest  # noqa: F401
from hs.admin import seeds


def test_register_seeds_the_states_an_accept_stamps(monkeypatch):
    written = {}

    def add(conn, name, rows, key):
        written[name] = [r[key] for r in rows]
        return {r[key]: (1, "u") for r in rows}

    monkeypatch.setattr(seeds, "_add", add)
    seeds.seed(None, 1, "app")
    assert {"open", "in_progress_human"} <= set(written["conversation_custom_status_mapping"])
