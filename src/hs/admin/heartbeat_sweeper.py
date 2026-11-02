import threading
import time

from hs.infra import constants as non_env
from hs.infra.log import logger

_started = False
_start_lock = threading.Lock()


def _acquire_sweep_lock(interval_seconds: int) -> bool:
    # one replica sweeps per tick; the lock auto-expires, so a crashed holder never blocks later sweeps
    from hs.infra.locks import acquire_worker_lock

    return acquire_worker_lock(non_env.HEARTBEAT_SWEEP_LOCK_KEY, interval_seconds)


def _run_loop() -> None:
    # A tick's error is logged and the loop goes on; the thread dying alerts once, since nothing else would
    # notice offline sweeping stop.
    try:
        from hs.admin.heartbeat import sweep_offline_agents

        interval = non_env.HEARTBEAT_SWEEP_INTERVAL_SECONDS
        logger.info(f"heartbeat sweeper loop running every {interval}s")
        while True:
            time.sleep(interval)
            try:
                flipped = sweep_offline_agents() if _acquire_sweep_lock(interval) else 0
                if flipped:
                    logger.info(f"heartbeat sweeper marked {flipped} agent(s) offline")
            except Exception as e:
                logger.error(f"heartbeat sweeper tick error: {e}")
    except Exception as e:
        logger.error(f"heartbeat sweeper loop crashed, thread exiting: {e}", exc_info=True)
        raise


def start_heartbeat_sweeper() -> None:
    """Launch the offline sweeper once per process."""
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_run_loop, name="heartbeat-sweeper", daemon=True).start()
    logger.info("heartbeat sweeper started")
