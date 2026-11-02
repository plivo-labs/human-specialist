"""Singleton construction race.

hs.infra.helpers.Singleton.get_instance() is called from concurrent threads; an unlocked
check-then-set would let two first-callers construct concurrently. Pins: concurrent first access
constructs EXACTLY once and every caller gets the same instance.
"""

import threading
import time


def test_concurrent_first_access_constructs_exactly_once():
    from hs.infra.helpers import Singleton

    constructions = []

    @Singleton
    class _Probe:
        def __init__(self):
            constructions.append(threading.get_ident())
            time.sleep(0.05)  # widen the race window: unlocked code reliably double-constructs here

    results, errors = [], []
    barrier = threading.Barrier(8)

    def _grab():
        try:
            barrier.wait(timeout=5)  # release all threads into get_instance() at the same instant
            results.append(_Probe.get_instance())
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=_grab) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not any(t.is_alive() for t in threads), "worker thread hung (join timeout)"
    assert errors == [], f"worker exceptions: {errors}"
    assert len(results) == len(threads)  # every worker completed and grabbed an instance
    assert len(constructions) == 1, f"constructed {len(constructions)} times under concurrency"
    assert len({id(r) for r in results}) == 1  # every caller sees the same instance


def test_second_access_skips_the_lock_path_and_returns_same_instance():
    from hs.infra.helpers import Singleton

    @Singleton
    class _Probe2:
        pass

    first = _Probe2.get_instance()
    assert _Probe2.get_instance() is first
