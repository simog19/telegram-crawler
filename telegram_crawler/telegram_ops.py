"""
Async Telegram operations: joining groups, collecting data, and bot interactions.

All functions in this module are coroutines that operate on a single
TelegramClient. They are called exclusively from within worker processes.
"""

import asyncio
from datetime import datetime

import telethon
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Dialog

from .link_utils import (
    extract_telegram_link,
    is_message_link,
    is_proxy_link,
    links_to_tbp_documents,
)


def _log(pid: int, msg: str) -> None:
    print(f"{datetime.now()} - [WORKER n.{pid}] {msg}")


def _parse_flood_wait(error: telethon.errors.FloodWaitError) -> int:
    """Return the mandatory wait duration in seconds from a FloodWaitError."""
    return error.seconds


def _set_failed(result: dict, error: BaseException) -> None:
    """Write a JOIN_FAILED outcome into *result*."""
    result["code"] = "JOIN_FAILED"
    result["error_messages"] = str(error)
    result["time"] = datetime.now()


# ---------------------------------------------------------------------------
# Link gathering
# ---------------------------------------------------------------------------


async def gather_links(
    dialog: Dialog, client, pid: int
) -> tuple[set, list]:
    """Scan messages in *dialog* for t.me links and return collected data.

    Searches up to 1,000,000 messages using the Telegram API's server-side
    text filter. Duplicate (link, group) pairs are deduplicated in memory.

    Returns
    -------
    unique_links : set
        Distinct t.me links found.
    gathered : list
        Documents ready for insertion into the ``gathered`` collection, each
        carrying the link, originating message, group id/name, and timestamp.
    """
    unique_links: set = set()
    gathered: list = []
    seen: set = set()  # (link, group_id) pairs already recorded

    try:
        async for m in client.iter_messages(
            dialog.entity.id, search="t.me/", limit=1_000_000
        ):
            try:
                link = extract_telegram_link(m.text)
                if link is None or is_message_link(link) or is_proxy_link(link):
                    continue

                unique_links.add(link)
                key = (link, dialog.entity.id)
                if key not in seen:
                    seen.add(key)
                    gathered.append(
                        {
                            "link_hash": link,
                            "message": m.to_dict(),
                            "group_id": dialog.entity.id,
                            "group_name": dialog.title,
                            "date": datetime.now(),
                        }
                    )
            except AttributeError as e:
                _log(pid, f"[!] gather_links: {e}")

        _log(pid, f"[] Links collected in {dialog.entity.id}")

    except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
        _log(pid, f"[!] gather_links - private channel: {e}")
    except TypeError as e:
        _log(pid, f"[!] gather_links - type error: {e}")

    return unique_links, gathered


async def collect_data(dialog: Dialog, link: str, client, pid: int) -> dict:
    """Fetch members and recent messages for a group or channel.

    Collects up to 2,000 members (skipped for broadcast channels) and up to
    500 recent messages.  Returns a document ready for the ``groups`` collection.
    """
    group = dialog.entity
    members: list = []
    messages: list = []

    if isinstance(group, (telethon.tl.types.Channel, telethon.tl.types.Chat)):
        try:
            is_broadcast = getattr(group, "broadcast", False)
            if group.id is not None and not is_broadcast:
                async for m in client.iter_participants(dialog.id, limit=2000):
                    members.append(m.to_dict())
            if group.id is not None:
                async for m in client.iter_messages(dialog.id, limit=500):
                    messages.append(m.to_dict())
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            _log(pid, f"[!] collect_data - private channel: {e}")

    _log(pid, f"[] Data collected in {dialog.entity.id}")
    return {
        "id": str(group.id),
        "name": group.title,
        "username": getattr(group, "username", None),
        "link_hash": link,
        "date": str(group.date),
        "date_inserted": datetime.now(),
        "is_scam": str(getattr(group, "scam", False)),
        "members": members,
        "messages": messages,
    }


async def process_link(
    link_ext: str, entity_id: int, client, result: dict
) -> None:
    """Populate *result* with gathered links and group data for *entity_id*.

    Iterates over the client's dialogs until it finds the one matching
    *entity_id*, then calls :func:`gather_links` and :func:`collect_data`.
    """
    async for dialog in client.iter_dialogs():
        if dialog.entity.id != entity_id:
            continue

        unique_links, gathered = await gather_links(dialog, client, result["p_id"])
        result["new_links"] = links_to_tbp_documents(unique_links)
        result["link_gathered"] = gathered
        result["group_id"] = entity_id
        result["link"] = link_ext
        result["data"] = await collect_data(dialog, link_ext, client, result["p_id"])
        break


# ---------------------------------------------------------------------------
# Joining and leaving groups
# ---------------------------------------------------------------------------


async def _scrape_already_inside(
    link_ext: str, client, result: dict
) -> None:
    """Handle the 'already a participant' case: resolve entity and scrape.

    Mirrors what :func:`process_link` does after a successful join - sets the
    result code to JOIN_SUCCESS, populates group_id / time, and harvests
    links + group snapshot.
    """
    try:
        entity = await client.get_entity(link_ext)
        result["code"] = "JOIN_SUCCESS"
        result["group_id"] = entity.id
        result["time"] = datetime.now()
        await process_link(link_ext, entity.id, client, result)
    except BaseException as inner:
        _set_failed(result, inner)


async def join_group_by_hash(
    link_ext: str, link_hash: str, client, result: dict
):
    """Join a private Telegram group via its invite hash.

    Falls back to :func:`join_group_public_by_link` if the hash is expired.
    Respects Telegram's flood-wait mechanism by sleeping for the required
    duration and retrying in a loop (avoids unbounded recursion).

    Returns the raw Telegram ``Updates`` object on success, ``None`` otherwise.
    If we discover we are already a participant, the group is still scraped
    (result code becomes JOIN_SUCCESS) before returning ``None``.
    """
    pid = result["p_id"]
    while True:
        try:
            g = await client(ImportChatInviteRequest(link_hash))
            _log(pid, f"[+] [HASH] Joined: {g.chats[0].title} ({link_hash})")
            return g

        except telethon.errors.rpcerrorlist.InviteHashExpiredError:
            _log(pid, "Invite hash expired - trying public join")
            return await join_group_public_by_link(link_ext, link_hash, client, pid, result)

        except telethon.errors.rpcerrorlist.UserAlreadyParticipantError:
            _log(pid, f"Already a participant: {link_ext} - scraping anyway")
            await _scrape_already_inside(link_ext, client, result)
            return None

        except telethon.errors.rpcerrorlist.PeerIdInvalidError as e:
            _set_failed(result, e)
            return None

        except telethon.errors.FloodWaitError as e:
            wait = _parse_flood_wait(e)
            _log(pid, f"[!] Flood wait: {wait}s")
            await asyncio.sleep(wait + 10)
            # loop retries

        except telethon.errors.rpcerrorlist.InviteRequestSentError as e:
            result["code"] = "REQUEST_SENT"
            result["error_messages"] = str(e)
            result["time"] = datetime.now()
            _log(pid, f"[!] {e}")
            return None

        except BaseException as e:
            _set_failed(result, e)
            return None


async def join_group_public_by_link(
    link_ext: str, link: str, client, pid: int, result: dict
):
    """Join a public Telegram channel or group by its username or public link.

    Returns the raw ``Updates`` object on success, ``None`` otherwise. If the
    user is already a participant, scrapes the group and sets the result code
    to JOIN_SUCCESS before returning ``None``.
    """
    while True:
        try:
            g = await client(JoinChannelRequest(link))
            _log(pid, f"[+] [PUBLIC] Joined: {g.chats[0].title} ({link})")
            return g

        except (telethon.errors.ChannelInvalidError, telethon.errors.ChannelPrivateError) as e:
            _set_failed(result, e)
            return None

        except telethon.errors.rpcerrorlist.UserAlreadyParticipantError:
            _log(pid, f"Already a participant: {link_ext} - scraping anyway")
            await _scrape_already_inside(link_ext, client, result)
            return None

        except telethon.errors.FloodWaitError as e:
            wait = _parse_flood_wait(e)
            _log(pid, f"[!] Flood wait: {wait}s")
            await asyncio.sleep(wait + 10)
            # loop retries

        except BaseException as e:
            _set_failed(result, e)
            return None


async def leave_group(group_entry: dict, client, result: dict) -> None:
    """Delete the dialog for *group_entry* and write the outcome into *result*."""
    try:
        await client.delete_dialog(group_entry["id"])
        result["code"] = "LEAVE_SUCCESS"
        result["time"] = datetime.now()
    except BaseException as err:
        _log(
            result["p_id"],
            f"[!] Leaving [{group_entry['link_hash']}] failed: {err}",
        )
        result["code"] = "LEAVE_FAILED"
        result["error_messages"] = str(err)


# ---------------------------------------------------------------------------
# Bot interactions
# ---------------------------------------------------------------------------


async def bot_interaction(
    link_extended: str,
    param: str,
    link: str,
    entity,
    client,
    result: dict,
) -> None:
    """Send a /start command to a bot and harvest any t.me links from its reply.

    Waits 10 seconds after sending the message to allow the bot to respond,
    then reads the 10 most recent messages in the conversation.
    """
    pid = result["p_id"]
    message = f"/start {param}"
    await client.send_message(link, message)
    _log(pid, f"Sent: {message!r} - waiting 10s for reply...")
    await asyncio.sleep(10)

    messages_dict: list = []
    new_links: set = set()

    async for m in client.iter_messages(entity.id, limit=10):
        messages_dict.append(m.to_dict())
        link_found = extract_telegram_link(m.text)
        if link_found:
            result["link_gathered"].append(
                {
                    "link_hash": link_found,
                    "message": m.to_dict(),
                    "group_id": entity.id,
                    "group_name": entity.first_name,
                }
            )
            new_links.add(link_found)

    result["code"] = "BOT_RESULT"
    result["link"] = link_extended
    result["group_id"] = entity.id
    result["new_links"] = links_to_tbp_documents(new_links)
    result["messages_bot"] = messages_dict


# ---------------------------------------------------------------------------
# Link evaluation (entry point for each task)
# ---------------------------------------------------------------------------


async def evaluate_link(client, link: str, result: dict) -> None:
    """Classify *link* and take the appropriate crawling action.

    Three cases are handled in priority order:

    1. **Query-string link** (``?`` present) - resolve the entity; if it is a
       bot, call :func:`bot_interaction`; otherwise mark as failed.
    2. **Bot username** (link ends in ``bot`` case-insensitively) - resolve the
       entity and call :func:`bot_interaction` without parameters.
    3. **Group / channel link** - extract the invite hash or username, attempt
       to join via :func:`join_group_by_hash`, then call :func:`process_link`.
    """
    pid = result["p_id"]

    if "?" in link:
        param = link[link.rfind("=") + 1:]
        base_link = link[: link.rfind("?")]
        try:
            entity = await client.get_entity(base_link)
            if isinstance(entity, telethon.tl.types.User) and entity.bot:
                result["code"] = "BOT_RESULT"
                await bot_interaction(link, param, base_link, entity, client, result)
            else:
                result["code"] = "JOIN_FAILED"
        except BaseException as e:
            _log(pid, f"[!] get_entity failed: {e}")
            result["code"] = "JOIN_FAILED"

    elif link.lower().endswith("bot"):
        try:
            entity = await client.get_entity(link)
            result["code"] = "BOT_RESULT"
            await bot_interaction(link, "", link, entity, client, result)
        except BaseException as e:
            _log(pid, f"[!] get_entity failed: {e}")
            result["code"] = "JOIN_FAILED"

    else:
        link_hash = link[link.rfind("/") + 1:]
        link_hash = link_hash[link_hash.rfind("+") + 1:]
        try:
            update = await join_group_by_hash(link, link_hash, client, result)
            if update is not None:
                result["code"] = "JOIN_SUCCESS"
                entity_id = update.chats[0].id
                result["group_id"] = entity_id
                result["time"] = datetime.now()
                await process_link(link, entity_id, client, result)
        except BaseException as e:
            _log(pid, f"[!] {e}")
