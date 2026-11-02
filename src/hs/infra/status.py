from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from hs.infra.db import core_engine
from hs.infra.log import logger
from hs.infra.redis import Cache

router = APIRouter(tags=["status"])


def _db_ok() -> bool:
    try:
        with core_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"status: db check failed: {e}")
        return False
    return True


@router.get("/status")
def status() -> JSONResponse:
    """Liveness for load balancers: only booleans, never exception text or hosts."""
    body = {"db": _db_ok(), "redis": Cache.get_instance().get_status()}
    return JSONResponse(body, status_code=200 if all(body.values()) else 503)
