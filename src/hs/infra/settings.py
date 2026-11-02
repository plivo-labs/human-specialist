"""Process settings, read once from the environment at import. Compose passes the env; there is no .env loader."""

import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

_REQUIRED = {
    "database_url": "DATABASE_URL",
    "redis_url": "REDIS_URL",
    "public_base_url": "PUBLIC_BASE_URL",
    "operator_key": "HS_OPERATOR_KEY",
    "secret_key": "HS_SECRET_KEY",
}


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    public_base_url: str
    operator_key: str
    secret_key: str
    plivo_api_url: str
    plivo_media_url: str
    cors_allowed_origins: tuple[str, ...]
    event_retention_days: int
    webhook_private_hosts: tuple[str, ...]
    webhook_deadline: float
    metrics_port: int
    log_level: str
    hold_music_url: str


def _list(name: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in os.getenv(name, "").split(",") if v.strip())


def load() -> Settings:
    missing = [env for env in _REQUIRED.values() if not os.getenv(env, "").strip()]
    if missing:
        raise RuntimeError(f"missing required environment variables: {', '.join(missing)}")
    required = {attr: os.environ[env].strip() for attr, env in _REQUIRED.items()}
    required["public_base_url"] = required["public_base_url"].rstrip("/")
    try:
        base = urlsplit(required["public_base_url"])
        _ = base.port
        host = (base.hostname or "").encode("idna").decode()
    except ValueError as e:
        raise RuntimeError(f"PUBLIC_BASE_URL is not a valid URL: {e}") from e
    if (
        base.scheme not in ("http", "https")
        or re.search(r"[\s\x00-\x1f]", required["public_base_url"])
        or not re.fullmatch(r"(?!-)(?!.*-\.)(?!.*\.-)(?!.*-$)[A-Za-z0-9.-]+|[0-9A-Fa-f:.]+", host)
    ):
        raise RuntimeError(
            "PUBLIC_BASE_URL must be an absolute http or https URL with a host, such as https://hs.example.com"
        )
    if len(required["secret_key"].encode()) < 32:
        raise RuntimeError("HS_SECRET_KEY must be at least 32 bytes")
    return Settings(
        **required,
        plivo_api_url=os.getenv("PLIVO_API_URL", "https://api.plivo.com").rstrip("/"),
        plivo_media_url=os.getenv("PLIVO_MEDIA_URL", "https://media.plivo.com").rstrip("/"),
        cors_allowed_origins=_list("CORS_ALLOWED_ORIGINS"),
        event_retention_days=int(os.getenv("EVENT_RETENTION_DAYS", "90")),
        webhook_private_hosts=_list("WEBHOOK_PRIVATE_HOSTS"),
        webhook_deadline=float(os.getenv("WEBHOOK_DEADLINE", "10")),
        metrics_port=int(os.getenv("METRICS_PORT", "9100")),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        hold_music_url=os.getenv("HOLD_MUSIC_URL", "https://s3.amazonaws.com/plivocloud/music.mp3").strip(),
    )


settings = load()
