"""
Master process: orchestrates workers and persists crawl results to MongoDB.

The master runs in the main process and communicates with worker subprocesses
through multiprocessing queues.  Its responsibilities are:

1. Assign tasks to free workers in priority order:
   LEAVE  >  CHECK_WAIT  >  TRY_JOIN
2. Drain the result queue and route each outcome to the correct MongoDB write.
3. Maintain the per-worker leave and wait queues (in memory and in MongoDB) so
   state survives a restart.

Exit condition
--------------
The master loop exits only when ALL of the following hold simultaneously:

- ``tbp`` collection is empty (no new input)
- ``result_queue`` is empty (no pending writes)
- every worker is idle (every pid is back in ``process_queue``)

Persisted wait/leave items that are not yet due remain in MongoDB and are
picked up on the next run.
"""

import multiprocessing
import queue
import time
from datetime import datetime

from . import config
from .database import Database


def _log(msg: str) -> None:
    print(f"{datetime.now()} - [MASTER] {msg}")


# ---------------------------------------------------------------------------
# Queue helpers
# ---------------------------------------------------------------------------


def build_leave_queue(db: Database, pid: int) -> multiprocessing.Queue:
    """Restore the persisted leave queue for *pid* from MongoDB on startup."""
    q: multiprocessing.Queue = multiprocessing.Queue()
    element = db.leave.find_one({"_id": pid})
    if element:
        for el in element.get("queue", []):
            if el.get("id") and el.get("link_hash"):
                q.put(el)
    return q


def build_wait_queue(db: Database, pid: int) -> multiprocessing.Queue:
    """Restore the persisted wait queue for *pid* from MongoDB on startup."""
    q: multiprocessing.Queue = multiprocessing.Queue()
    element = db.wait.find_one({"_id": pid})
    if element:
        for el in element.get("queue", []):
            q.put(el)
    return q


def check_threshold(timestamp: datetime, threshold_hours: float) -> bool:
    """Return ``True`` if more than *threshold_hours* have elapsed since *timestamp*."""
    elapsed = (datetime.now() - timestamp).total_seconds() / 3600
    return elapsed >= threshold_hours


def _drain_leave_queue(
    pid: int, leave_queues: list, threshold: float
) -> tuple[list, multiprocessing.Queue]:
    """Split the leave queue for *pid* into items due for leaving and the rest."""
    due, remaining = [], multiprocessing.Queue()
    while not leave_queues[pid].empty():
        el = leave_queues[pid].get()
        if check_threshold(el["time_joined"], threshold):
            due.append(el)
        else:
            remaining.put(el)
    return due, remaining


def _drain_wait_queue(
    pid: int, wait_queues: list, threshold: float
) -> tuple[list, multiprocessing.Queue]:
    """Split the wait queue for *pid* into items due for re-checking and the rest."""
    due, remaining = [], multiprocessing.Queue()
    while not wait_queues[pid].empty():
        el = wait_queues[pid].get()
        if check_threshold(el["time_request"], threshold):
            due.append(el)
        else:
            remaining.put(el)
    return due, remaining


# ---------------------------------------------------------------------------
# Startup recovery
# ---------------------------------------------------------------------------


def recover_in_flight_links(db: Database) -> None:
    """Move links stuck in 'processing' back to the tbp queue.

    Called once at startup to handle a crash or ungraceful shutdown that left
    links marked as processing without a matching done record.
    """
    for x in db.done.find({"state": "processing"}):
        x["state"] = "tbp"
        x["time"] = None
        x["process_id"] = None
        db.done.delete_one({"link_hash": x["link_hash"]})
        db.analytics.update_one({"_id": 1}, {"$inc": {"total_counter": -1}})
        if db.tbp.find_one({"link_hash": x["link_hash"]}) is None:
            db.tbp.insert_one(x)


# ---------------------------------------------------------------------------
# Result persistence helpers
# ---------------------------------------------------------------------------


def _push_to_leave_queue(
    db: Database, result: dict, leave_queues: list
) -> None:
    pid = result["p_id"]
    item = {
        "link_hash": result["link"],
        "id": result["group_id"],
        "time_joined": result["time"],
    }
    db.leave.update_one({"_id": pid}, {"$push": {"queue": item}}, upsert=True)
    leave_queues[pid].put(item)
    _log("[RESULT] Item inserted in leave_queue")


def _insert_gathered_and_new_links(result: dict, db: Database) -> None:
    try:
        db.gathered.insert_many(result["link_gathered"])
    except Exception:
        pass
    try:
        db.tbp.insert_many(result["new_links"])
        _log("[RESULT] New links inserted in tbp")
    except Exception:
        _log("[RESULT] [!] No new links to insert")


def _insert_group_data(result: dict, db: Database) -> None:
    if db.groups.find_one({"id": str(result["group_id"])}) is None:
        db.groups.insert_one(result["data"])
        _log("[RESULT] Group data inserted")
        db.analytics.update_one({"_id": 0}, {"$inc": {"collect_counter": 1}})
    else:
        _log("[RESULT] [!] Group already in collection - skipping")


def _update_edge_graph(result: dict, db: Database) -> None:
    """Add an edge from every referring group to the newly processed group."""
    for el in db.gathered.find({"link_hash": result["link"]}):
        db.update_edges(result["group_id"], el["group_id"])


# ---------------------------------------------------------------------------
# Per-code result handlers
# ---------------------------------------------------------------------------


def _persist_join_success(
    result: dict, db: Database, leave_queues: list
) -> None:
    _insert_gathered_and_new_links(result, db)
    _insert_group_data(result, db)
    try:
        _push_to_leave_queue(db, result, leave_queues)
    except Exception:
        _log("[RESULT] [!] Error updating leave_collection")
    db.set_link_state(result["link"], "inside", result["time"])
    _update_edge_graph(result, db)


def _persist_bot_result(
    result: dict, db: Database, leave_queues: list
) -> None:
    _insert_gathered_and_new_links(result, db)
    try:
        bot_doc = {
            "process_id": str(result["p_id"]),
            "id": str(result["group_id"]),
            "link_hash": result["link"],
            "messages": result["messages_bot"],
            "time": result["time"],
        }
        if db.bots.find_one({"id": str(result["group_id"])}) is None:
            db.bots.insert_one(bot_doc)
            _log("[RESULT] Bot data inserted")
            db.analytics.update_one({"_id": 3}, {"$inc": {"bot_counter": 1}})
            db.analytics.update_one({"_id": 0}, {"$inc": {"collect_counter": 1}})
        else:
            _log("[RESULT] [!] Bot data already present - skipping")
    except Exception:
        _log("[RESULT] [!] Error inserting into bot_collection")
    try:
        _push_to_leave_queue(db, result, leave_queues)
    except Exception:
        _log("[RESULT] [!] Error updating leave_collection")
    db.set_link_state(result["link"], "inside", result["time"])
    _update_edge_graph(result, db)


def _persist_request_sent(
    result: dict, db: Database, wait_queues: list
) -> None:
    try:
        pid = result["p_id"]
        item = {"link_hash": result["link"], "time_request": result["time"]}
        db.wait.update_one({"_id": pid}, {"$push": {"queue": item}}, upsert=True)
        wait_queues[pid].put(item)
        _log("[RESULT] Item inserted in wait_queue")
        db.analytics.update_one({"_id": 4}, {"$inc": {"request_counter": 1}})
        db.set_link_state(result["link"], "waiting")
    except Exception:
        _log("[RESULT] [!] Error updating wait_collection")


def _requeue_leave(result: dict, db: Database, leave_queues: list) -> None:
    """Re-add a group to the leave queue after a failed leave attempt."""
    try:
        pid = result["p_id"]
        item = {
            "link_hash": result["link"],
            "id": result.get("group_id", ""),
            "time_joined": datetime.now(),
        }
        db.leave.update_one({"_id": pid}, {"$pull": {"queue": {"id": result["id"]}}})
        db.leave.update_one({"_id": pid}, {"$push": {"queue": item}})
        leave_queues[pid].put(item)
    except Exception:
        _log("[RESULT] [!] Error re-queuing leave after LEAVE_FAILED")


def _persist_request_accepted(
    result: dict, db: Database, leave_queues: list
) -> None:
    db.analytics.update_one({"_id": 5}, {"$inc": {"request_accepted": 1}})
    _insert_gathered_and_new_links(result, db)
    _insert_group_data(result, db)
    try:
        _push_to_leave_queue(db, result, leave_queues)
    except Exception:
        _log("[RESULT] [!] Error updating leave_collection")
    db.set_link_state(result["link"], "inside", result["time"])
    db.wait.update_one(
        {"_id": result["p_id"]},
        {"$pull": {"queue": {"link_hash": result["link"]}}},
    )
    _update_edge_graph(result, db)


# ---------------------------------------------------------------------------
# Result dispatcher
# ---------------------------------------------------------------------------


def get_result(
    result_queue,
    db: Database,
    leave_queues: list,
    wait_queues: list,
) -> None:
    """Drain *result_queue* and persist each result to the appropriate collections."""
    while not result_queue.empty():
        result = result_queue.get()
        code = result["code"]
        _log(
            f"[RESULT] Worker {result['p_id']} | "
            f"link: {result['link']} | code: {code}"
        )

        if code == "JOIN_SUCCESS":
            _persist_join_success(result, db, leave_queues)

        elif code == "JOIN_FAILED":
            db.set_link_state(result["link"], "join_failed", result["time"])

        elif code == "BOT_RESULT":
            _persist_bot_result(result, db, leave_queues)

        elif code == "REQUEST_SENT":
            _persist_request_sent(result, db, wait_queues)

        elif code == "LEAVE_SUCCESS":
            db.leave.update_one(
                {"_id": result["p_id"]},
                {"$pull": {"queue": {"id": result["id"]}}},
            )
            db.set_link_state(result["link"], "done", result["time"])

        elif code == "LEAVE_FAILED":
            db.set_link_state(result["link"], "leave_failed")
            _requeue_leave(result, db, leave_queues)

        elif code == "REQUEST_ACCEPTED":
            _persist_request_accepted(result, db, leave_queues)

        elif code == "STILL_WAITING":
            wait_queues[result["p_id"]].put(
                {"link_hash": result["link"], "time_request": datetime.now()}
            )

        else:
            _log(
                f"[RESULT] Unknown code for link: {result['link']} "
                f"- {result.get('error_messages', '')}"
            )


# ---------------------------------------------------------------------------
# Task assignment helpers
# ---------------------------------------------------------------------------


def _try_assign_leave(
    pid, db, tasks, leave_queues, result_queue, wait_queues, threshold
) -> bool:
    due, leave_queues[pid] = _drain_leave_queue(pid, leave_queues, threshold)
    if not due:
        return False
    _log(f"Assigning LEAVE task to worker {pid} ({len(due)} groups)")
    tasks[pid].put({"name": "LEAVE", "data": due})
    if not result_queue.empty():
        get_result(result_queue, db, leave_queues, wait_queues)
    return True


def _try_assign_check_wait(
    pid, db, tasks, wait_queues, result_queue, leave_queues, threshold
) -> bool:
    due, wait_queues[pid] = _drain_wait_queue(pid, wait_queues, threshold)
    if not due:
        return False
    _log(f"Assigning CHECK_WAIT task to worker {pid} ({len(due)} links)")
    tasks[pid].put({"name": "CHECK_WAIT", "data": due})
    if not result_queue.empty():
        get_result(result_queue, db, leave_queues, wait_queues)
    return True


def _try_assign_try_join(
    pid, db, tasks, result_queue, leave_queues, wait_queues, process_queue
) -> bool:
    """Assign the next tbp link to *pid*.  Returns ``True`` if tbp is non-empty."""
    x = db.tbp.find_one()
    if x is None:
        return False

    _log(f"Taking from tbp: {x['link_hash']}")
    if db.done.find_one({"link_hash": x["link_hash"]}) is not None:
        _log(f"Already in done - removing duplicate from tbp: {x['link_hash']}")
        db.tbp.delete_many({"link_hash": x["link_hash"]})
        process_queue.put(pid)
        return True  # tbp may still have more items

    db.analytics.update_one({"_id": 1}, {"$inc": {"total_counter": 1}})
    _log(f"Assigning TRY_JOIN to worker {pid}")
    tasks[pid].put({"name": "TRY_JOIN", "data": [x["link_hash"]]})

    x["state"] = "processing"
    x["time"] = datetime.now()
    x["process_id"] = pid
    db.tbp.delete_many({"link_hash": x["link_hash"]})
    if db.done.find_one({"link_hash": x["link_hash"]}) is None:
        try:
            db.done.insert_one(x)
        except Exception:
            _log(f"Error inserting {x['link_hash']} in done_collection")

    if not result_queue.empty():
        get_result(result_queue, db, leave_queues, wait_queues)
    return True


# ---------------------------------------------------------------------------
# Main orchestration loop
# ---------------------------------------------------------------------------


def _drain_free_workers(process_queue, free_workers: set) -> None:
    """Move all currently-free worker pids from *process_queue* into the set."""
    while True:
        try:
            free_workers.add(process_queue.get_nowait())
        except queue.Empty:
            return


def run_master(
    db: Database,
    processes: list,
    tasks: list,
    result_queue,
    leave_queues: list,
    wait_queues: list,
    process_queue,
) -> None:
    """Main orchestration loop.

    Continuously assigns work to free workers and persists any results that
    arrive.  Exits when there is no more work to do AND no work is in flight.
    """
    threshold_leave = config.THRESHOLD_LEAVE_HOURS
    threshold_request = config.THRESHOLD_REQUEST_HOURS
    total_workers = len(processes)
    free_workers: set[int] = set()

    while True:
        _drain_free_workers(process_queue, free_workers)

        progress = False

        # 1) Assign work to a free worker, if any
        if free_workers:
            pid = next(iter(free_workers))
            assigned = (
                _try_assign_leave(
                    pid, db, tasks, leave_queues, result_queue, wait_queues, threshold_leave
                )
                or _try_assign_check_wait(
                    pid, db, tasks, wait_queues, result_queue, leave_queues, threshold_request
                )
                or _try_assign_try_join(
                    pid, db, tasks, result_queue, leave_queues, wait_queues, process_queue
                )
            )
            if assigned:
                free_workers.discard(pid)
                progress = True

        # 2) Drain results
        if not result_queue.empty():
            get_result(result_queue, db, leave_queues, wait_queues)
            progress = True

        # 3) Exit when nothing more to do AND no work in flight
        if (
            db.tbp.find_one() is None
            and result_queue.empty()
            and len(free_workers) == total_workers
        ):
            break

        # 4) Avoid a busy-wait when there is nothing to do this tick
        if not progress:
            time.sleep(0.5)

    _log("No more work - shutting down workers")
    for p in processes:
        p.terminate()
    for p in processes:
        p.join()
