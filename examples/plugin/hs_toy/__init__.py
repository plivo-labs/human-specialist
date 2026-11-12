import logging

from fastapi import APIRouter

log = logging.getLogger("hs_toy")
router = APIRouter()


@router.get("/toy/hello")
def hello() -> dict:
    return {"hello": "world"}


def on_event(envelope: dict) -> None:
    log.info("event %s %s", envelope["id"], envelope["type"])


def setup(hs) -> None:
    hs.app.include_router(router)
    hs.on_event(on_event)
