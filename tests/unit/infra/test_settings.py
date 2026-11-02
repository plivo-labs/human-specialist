import pytest

from hs.infra import settings as settings_module

REQUIRED = {
    "DATABASE_URL": "postgresql://h/db",
    "REDIS_URL": "redis://h/0",
    "PUBLIC_BASE_URL": "https://hs.example/",
    "HS_OPERATOR_KEY": "op",
    "HS_SECRET_KEY": "s" * 32,
}


def test_missing_required_env_names_every_variable(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://h/0")
    with pytest.raises(RuntimeError) as e:
        settings_module.load()
    assert "DATABASE_URL" in str(e.value) and "HS_SECRET_KEY" in str(e.value) and "REDIS_URL" not in str(e.value)


def test_a_short_secret_key_is_refused(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("HS_SECRET_KEY", "s" * 31)
    with pytest.raises(RuntimeError, match="32 bytes"):
        settings_module.load()


def test_defaults_and_lists(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    for name in ("PLIVO_API_URL", "PLIVO_MEDIA_URL", "EVENT_RETENTION_DAYS", "METRICS_PORT", "LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("WEBHOOK_PRIVATE_HOSTS", " 127.0.0.1, localhost ,")
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "")
    s = settings_module.load()
    assert s.public_base_url == "https://hs.example"
    assert (s.plivo_api_url, s.plivo_media_url) == ("https://api.plivo.com", "https://media.plivo.com")
    assert (s.event_retention_days, s.metrics_port, s.log_level) == (90, 9100, "INFO")
    assert s.webhook_private_hosts == ("127.0.0.1", "localhost") and s.cors_allowed_origins == ()


@pytest.mark.parametrize(
    "url",
    [
        "hs.example",
        "localhost:6004",
        "ftp://hs.example",
        "https://",
        "/plivo",
        "https://hs.example:bad",
        "https://[bad",
        "https://bad host",
        "https://%zz",
        "https://hs..example/hs",
        "https://-hs.example/hs",
        "https://hs-.example/hs",
        "https://hs.example/h\ns",
    ],
)
def test_a_public_base_url_that_is_not_an_absolute_http_url_is_refused(monkeypatch, url):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PUBLIC_BASE_URL", url)
    with pytest.raises(RuntimeError, match="PUBLIC_BASE_URL"):
        settings_module.load()


@pytest.mark.parametrize(
    "url",
    [
        "https://hs.example.com",
        "http://10.0.0.5:8080/hs",
        "https://[::1]:8443",
        "https://bücher.example/hs",
        "https://hs.example.com",
    ],
)
def test_a_public_base_url_a_self_hoster_uses_loads(monkeypatch, url):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PUBLIC_BASE_URL", url)
    assert settings_module.load().public_base_url == url
