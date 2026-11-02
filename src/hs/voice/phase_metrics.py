import functools
import time
from contextlib import contextmanager

from prometheus_client import Histogram

# The HTTP histogram only sees whole-endpoint latency. This histogram splits the routing path into a FIXED
# set of phases so cold-vs-warm p50/p95/p99 per hop is visible. The phase label is bounded (only
# the literals passed at the call sites below); never label with per-request values.
_PHASE_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
voice_routing_phase_seconds = Histogram(
    "hs_voice_routing_phase_seconds",
    "Per-phase latency of the incoming-call routing path (enqueue -> agent selection -> MPC add)",
    labelnames=("phase",),
    buckets=_PHASE_BUCKETS,
)


@contextmanager
def time_phase(phase: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        # Observed in finally so failed attempts are measured too — a slow failure on this
        # path (e.g. the ACW live-calls hop timing out) is what this histogram is for.
        voice_routing_phase_seconds.labels(phase=phase).observe(time.perf_counter() - start)


def timed_phase(phase: str):
    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            with time_phase(phase):
                return fn(*args, **kwargs)

        return _wrapped

    return _decorator
