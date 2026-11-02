import enum

from prometheus_client import Counter as _PromCounter

from hs.infra.log import logger


class MetricName(str, enum.Enum):
    UNHANDLED_ERROR = "unhandled_error"
    HEARTBEAT_NOTIFY_FAILED = "heartbeat_notify_failed"
    QNR_EXECUTION_RESULT = "qnr_execution_result"
    MPC_FAILURE = "mpc_failure"
    AGENT_JOIN_FAILURE = "agent_join_failure"
    REVERSE_ROUTING_FAILURE = "reverse_routing_failure"
    MPC_ADD_AGENTS_FAILURE = "mpc_add_agents_failure"


_counter = _PromCounter("hs_events_total", "event counters (increase_counter)", labelnames=("metric", "detail"))


def increase_counter(metric, args):
    try:
        name = getattr(metric, "value", str(metric))
        detail = ",".join(f"{k}={v}" for k, v in sorted((args or {}).items()))[:120]
        _counter.labels(metric=name, detail=detail).inc()
    except Exception as e:
        logger.error(f"while incrementing counter exception occurred: {e}")


_REQUESTS = _PromCounter(
    "hs_request_outcome_total",
    "Requests by matched route template (or <unmatched>), method and status",
    labelnames=("route_family", "method", "status"),
)


def record_request_outcome(route: str, method: str, status: int) -> None:
    _REQUESTS.labels(route_family=route, method=method, status=str(status)).inc()
