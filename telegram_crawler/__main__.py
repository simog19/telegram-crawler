"""
Telegram crawler - entry point.

Initialises the database connection, spawns one worker subprocess per
configured API account, restores any queues that were persisted across
the previous run, and hands control to the master orchestration loop.

Run with::

    python -m telegram_crawler
"""

import multiprocessing
from datetime import datetime

from . import config
from .database import open_database
from .master import (
    build_leave_queue,
    build_wait_queue,
    recover_in_flight_links,
    run_master,
)
from .worker import launch_client


def _log(msg: str) -> None:
    print(f"{datetime.now()} - [MASTER] {msg}")


def main() -> None:
    # ------------------------------------------------------------------
    # 1. Connect to MongoDB
    # ------------------------------------------------------------------
    try:
        db = open_database(config.DB_URI, config.DB_NAME)
        _log("Database connection opened successfully")
    except Exception as e:
        _log(f"[!] Failed to open database connection: {e}")
        raise SystemExit(1)

    # ------------------------------------------------------------------
    # 2. Recover links that were interrupted in the previous run
    # ------------------------------------------------------------------
    _log("Recovering in-flight links from previous session...")
    recover_in_flight_links(db)

    # ------------------------------------------------------------------
    # 3. Create inter-process queues and restore persisted state
    # ------------------------------------------------------------------
    _log("Initialising queues and task list...")
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    process_queue: multiprocessing.Queue = multiprocessing.Queue()

    leave_queues: list[multiprocessing.Queue] = []
    wait_queues: list[multiprocessing.Queue] = []
    tasks: list[multiprocessing.Queue] = []

    for pid in range(config.N_PROCESSES):
        leave_queues.append(build_leave_queue(db, pid))
        wait_queues.append(build_wait_queue(db, pid))
        tasks.append(multiprocessing.Queue())
        process_queue.put(pid)  # all workers start as available

    # ------------------------------------------------------------------
    # 4. Launch worker subprocesses
    # ------------------------------------------------------------------
    _log(f"Launching {config.N_PROCESSES} worker(s)...")
    processes: list[multiprocessing.Process] = []
    for pid in range(config.N_PROCESSES):
        p = multiprocessing.Process(
            target=launch_client,
            args=(
                config.API_IDS[pid],
                config.API_HASHES[pid],
                pid,
                tasks[pid],
                result_queue,
                process_queue,
            ),
        )
        p.start()
        processes.append(p)

    # ------------------------------------------------------------------
    # 5. Run the master orchestration loop
    # ------------------------------------------------------------------
    run_master(db, processes, tasks, result_queue, leave_queues, wait_queues, process_queue)


if __name__ == "__main__":
    main()
