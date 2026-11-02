from collections import namedtuple

from starlette.datastructures import Headers

from hs.infra.log import _Logger, safe_request_headers


def test_request_header_allowlist_drops_credentials_and_signatures():
    sanitized = safe_request_headers(
        Headers(
            {
                "Authorization": "Basic dXNlcjpwYXNz",
                "Cookie": "session=secret",
                "X-Plivo-Signature-V3": "provider-secret",
                "X-API-Key": "api-secret",
                "Content-Type": "application/json",
                "User-Agent": "pytest",
                "X-Request-ID": "request-1",
            }
        )
    )

    assert sanitized == {"content-type": "application/json", "user-agent": "pytest", "x-request-id": "request-1"}


def test_logger_redacts_nested_secret_fields_and_reapplies_header_allowlist(monkeypatch):
    emitted = []
    monkeypatch.setattr("hs.infra.log._base.log", lambda _level, message, *args, **kwargs: emitted.append(message))

    _Logger().info(
        msg="REQUEST_INITIATED",
        extra={
            "headers": {"Authorization": "Bearer secret", "Content-Type": "application/json"},
            "nested": {"auth_token": "secret-token", "password": "secret-password", "safe": "visible"},
        },
    )

    line = emitted[0]
    assert "Bearer secret" not in line
    assert "secret-token" not in line
    assert "secret-password" not in line
    assert "application/json" in line
    assert "visible" in line
    assert "[REDACTED]" in line


def test_logger_normalizes_namedtuple_and_list_subclasses_without_crashing(monkeypatch):
    emitted = []
    monkeypatch.setattr("hs.infra.log._base.log", lambda _level, message, *args, **kwargs: emitted.append(message))

    Pair = namedtuple("Pair", ["left", "right"])

    class RequiresMarker(list):
        def __init__(self, values, marker):
            super().__init__(values)
            self.marker = marker

    _Logger().info(
        msg="safe containers",
        extra={
            "tuple_payload": Pair({"auth": "tuple-secret"}, {"safe": "tuple-visible"}),
            "list_payload": RequiresMarker([{"credentials": "list-secret"}], marker="required"),
        },
    )

    line = emitted[0]
    assert "tuple-secret" not in line
    assert "list-secret" not in line
    assert "tuple-visible" in line
    assert line.count("[REDACTED]") == 2


def test_logger_breaks_cycles_caps_depth_and_redacts_sensitive_variants(monkeypatch):
    emitted = []
    monkeypatch.setattr("hs.infra.log._base.log", lambda _level, message, *args, **kwargs: emitted.append(message))

    cyclic = {
        "auth": "auth-secret",
        "basic_auth": "basic-auth-secret",
        "credentials": "credential-secret",
        "password_hash": "hash-secret",
        "passwordhash": "compact-hash-secret",
    }
    cyclic["self"] = cyclic
    cyclic_list = []
    cyclic_list.append(cyclic_list)
    deep = {}
    cursor = deep
    for _ in range(12):
        cursor["child"] = {}
        cursor = cursor["child"]
    cursor["password"] = "deep-secret"

    _Logger().info(msg="safe recursion", extra={"cyclic": cyclic, "cyclic_list": cyclic_list, "deep": deep})

    line = emitted[0]
    assert "auth-secret" not in line
    assert "basic-auth-secret" not in line
    assert "credential-secret" not in line
    assert "hash-secret" not in line
    assert "compact-hash-secret" not in line
    assert "deep-secret" not in line
    assert line.count("[CIRCULAR]") == 2
    assert "[TRUNCATED]" in line
