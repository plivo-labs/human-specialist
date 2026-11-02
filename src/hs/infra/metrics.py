import threading

from prometheus_client import Gauge, Histogram

from hs.infra.log import logger

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_histogram = Histogram(
    "hs_http_request_response_seconds",
    "per-route response time (the add_response_time_metric middleware)",
    labelnames=("endpoint", "method", "response_code"),
    buckets=_LATENCY_BUCKETS,
)


class _HistogramFacade:
    def observe(self, labels, value):
        try:
            _histogram.labels(
                endpoint=str(labels.get("endpoint", "")),
                method=str(labels.get("method", "")),
                response_code=str(labels.get("response_code", "")),
            ).observe(float(value))
        except Exception as e:
            logger.error(f"http_request_response_metric.observe failed: {e}")


http_request_response_metric = _HistogramFacade()

# Log-sink health: a blocked write >= 1 s means the container's stdout consumer froze.
from hs.infra.log_nonblocking import sink_snapshot as _sink_snapshot  # noqa: E402


def _sink_gauge(name: str, doc: str, key: str) -> Gauge:
    g = Gauge(name, doc)
    g.set_function(lambda: float(_sink_snapshot().get(key, 0)))
    return g


_sink_gauge("hs_log_sink_queue_depth", "Log records waiting for the writer thread (non-blocking sink).", "depth")
_sink_gauge("hs_log_sink_records_dropped", "Log records dropped because the bounded log queue was full.", "dropped")
_sink_gauge("hs_log_sink_blocked_writes", "Log writes that took >= 1 s (stdout consumer stalled).", "blocked_writes")
_sink_gauge("hs_log_sink_last_block_seconds", "Duration of the most recent blocked log write.", "last_block_s")


Gauge("hs_python_threads", "Live Python threads in this worker (threading.active_count)").set_function(
    threading.active_count
)
