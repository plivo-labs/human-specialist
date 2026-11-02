"""Expiring Redis election shared by periodic workers."""


def acquire_worker_lock(key: str, interval_seconds: int) -> bool:
    from hs.infra.redis import Cache

    return bool(Cache.get_instance().set(key, "1", expiry_in_seconds=max(1, interval_seconds - 1), nx=True))
