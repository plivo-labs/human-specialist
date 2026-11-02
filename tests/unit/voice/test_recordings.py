import uuid
from dataclasses import replace
from urllib.parse import parse_qsl, urlsplit

import pytest

from hs.access import Principal
from hs.errors import ApiError
from hs.voice import recordings

KEY = Principal("t", -1, "key", frozenset({"recordings"}), key_id="k1")
AGENT = Principal("t", -1, "agent", agent_id=str(uuid.uuid4()), role="agent")


def test_the_signature_covers_recording_viewer_expiry_and_download():
    q = dict(parse_qsl(urlsplit(recordings.audio_url("r1", KEY, download=False)).query))
    exp, sig = int(q["exp"]), q["sig"]
    assert recordings._sig("r1", "k1", exp, False) == sig
    changed = [
        ("r2", "k1", exp, False),
        ("r1", AGENT.agent_id, exp, False),
        ("r1", "k1", exp + 1, False),
        ("r1", "k1", exp, True),
    ]
    assert all(recordings._sig(*args) != sig for args in changed)


def test_audio_is_fetched_only_from_plivo_media_hosts(monkeypatch):
    monkeypatch.setattr(recordings, "settings", replace(recordings.settings, plivo_media_url="http://127.0.0.1:9000"))
    assert recordings._fetchable("http://127.0.0.1:9000/v1/Account/SA1/Recording/r.mp3")
    assert recordings._fetchable("https://media.plivo.com/v1/r.mp3")
    for url in ("http://media.plivo.com/r.mp3", "https://evil.example/r.mp3", "https://plivo.com.evil/r.mp3"):
        assert not recordings._fetchable(url), url


def test_only_admins_supervisors_and_keys_may_download():
    assert recordings._download(KEY, True) and recordings._download(AGENT, False) is False
    with pytest.raises(ApiError):
        recordings._download(AGENT, True)


def test_the_recording_query_binds_for_keys_and_agents():
    assert recordings._find(KEY, 10) == []
    assert recordings._find(AGENT, 10, conv=str(uuid.uuid4()), agent_id=uuid.UUID(AGENT.agent_id)) == []


def test_transcript_ready_query_binds_and_skips_unknown_recordings():
    from hs.voice import recording

    assert recording._transcript_ready(str(uuid.uuid4())) is None
