from contextvars import ContextVar

# The request's log context; every log line carries it unless the call passes its own extra=.
context_req_meta: ContextVar[dict] = ContextVar("request_meta", default=None)
