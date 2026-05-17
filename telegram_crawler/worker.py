"""
Worker subprocess: connects to Telegram and processes tasks from the master.

Each worker runs as a separate OS process (via multiprocessing) and communicates
with the master exclusively through multiprocessing queues:

- ``task_queue``    - receives tasks (TRY_JOIN / LEAVE / CHECK_WAIT)
- ``result_queue``  - sends results back to the master
- ``process_queue`` - signals availability after completing a task
"""

import asyncio
import random
from datetime import datetime

import telethon
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest

from . import config
from .telegram_ops import evaluate_link, leave_group, process_link


def _log(pid: int, msg: str) -> None:
    print(f"{datetime.now()} - [WORKER n.{pid}] {msg}")


def _wait_range(pid: int) -> tuple[int, int]:
    """Return the appropriate random sleep range for this worker's index."""
    return config.WAIT_PRIMARY if pid < 2 else config.WAIT_SECONDARY


def _empty_result(pid: int, link: str = "") -> dict:
    """Return a blank result dict for *pid* pre-populated with default values."""
    return {
        "p_id": pid,
        "link": link,
        "group_id": "",
        "code": "",
        "data": None,
        "messages_bot": [],
        "link_gathered": [],
        "new_links": [],
        "error_messages": "",
        "time": None,
    }


# ---------------------------------------------------------------------------
# Task handlers
# ---------------------------------------------------------------------------


async def _handle_try_join(
    task: dict, client, result_queue, process_queue, pid: int
) -> None:
    """Evaluate and attempt to join the link in *task*, then publish the result."""
    link = task["data"][0]
    result = _empty_result(pid, link)
    _log(pid, f"[EVAL_LINK] Evaluating: {link}")
    await evaluate_link(client, link, result)
    result["time"] = datetime.now()
    _log(pid, f"[RESULT - TRY_JOIN] Done: {link}")
    result_queue.put(result)

    wait = random.randint(*_wait_range(pid))
    _log(pid, f"Waiting {wait}s before next join...")
    await asyncio.sleep(wait)
    process_queue.put(pid)


async def _handle_leave(
    task: dict, client, result_queue, process_queue, pid: int
) -> None:
    """Leave each group in *task['data']* and publish one result per group."""
    for entry in task["data"]:
        result = {
            "p_id": pid,
            "code": "",
            "link": "",
            "id": "",
            "error_messages": "",
            "time": None,
        }
        _log(pid, f"Leaving: {entry['link_hash']} ({entry['id']})")
        await asyncio.sleep(random.randint(*config.WAIT_LEAVE))
        await leave_group(entry, client, result)
        result["id"] = entry["id"]
        result["link"] = entry["link_hash"]
        result["time"] = datetime.now()
        _log(pid, f"[RESULT - LEAVE] Done: {entry['id']}")
        result_queue.put(result)
    process_queue.put(pid)


async def _handle_check_wait(
    task: dict, client, result_queue, process_queue, pid: int
) -> None:
    """Re-check pending join requests in *task['data']* and publish results."""
    for el in task["data"]:
        result = _empty_result(pid)
        _log(pid, f"Checking pending request: {el['link_hash']}")
        try:
            await client(JoinChannelRequest(el["link_hash"]))
            result["code"] = "REQUEST_ACCEPTED"
            entity = await client.get_entity(el["link_hash"])
            await process_link(el["link_hash"], entity.id, client, result)
        except telethon.errors.UserAlreadyParticipantError:
            result["code"] = "REQUEST_ACCEPTED"
            entity = await client.get_entity(el["link_hash"])
            await process_link(el["link_hash"], entity.id, client, result)
        except Exception as e:
            result["code"] = "STILL_WAITING"
            result["error_messages"] = str(e)

        result["link"] = el["link_hash"]
        result["time"] = datetime.now()
        _log(pid, f"[RESULT - CHECK_WAIT] Done: {el['link_hash']}")
        result_queue.put(result)

        wait = random.randint(*_wait_range(pid))
        _log(pid, f"Waiting {wait}s...")
        await asyncio.sleep(wait)

    process_queue.put(pid)


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

_TASK_HANDLERS = {
    "TRY_JOIN": _handle_try_join,
    "LEAVE": _handle_leave,
    "CHECK_WAIT": _handle_check_wait,
}


async def _crawl_worker(
    client, task_queue, result_queue, pid: int, process_queue
) -> None:
    """Async main loop: pull tasks from *task_queue* and dispatch to handlers."""
    _log(pid, "------------- START TIME -------------")
    while True:
        task = task_queue.get()
        handler = _TASK_HANDLERS.get(task["name"])
        if handler:
            await handler(task, client, result_queue, process_queue, pid)
        else:
            _log(pid, f"[!] Unknown task: {task['name']!r}")


async def _worker_main(
    client, task_queue, result_queue, pid: int, process_queue
) -> None:
    me = await client.get_me()
    _log(pid, f"Connected - username: {me.username} | phone: {me.phone}")
    await _crawl_worker(client, task_queue, result_queue, pid, process_queue)


def launch_client(
    api_id: str,
    api_hash: str,
    pid: int,
    task_queue,
    result_queue,
    process_queue,
) -> None:
    """Entry point for a worker subprocess.

    Creates a :class:`~telethon.TelegramClient` with a per-process session file
    and runs the async worker loop until the process is terminated.
    """
    client = TelegramClient(f"session_{pid}", api_id, api_hash)
    with client:
        client.loop.run_until_complete(
            _worker_main(client, task_queue, result_queue, pid, process_queue)
        )
