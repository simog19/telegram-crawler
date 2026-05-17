"""
Utility functions for parsing and filtering Telegram links.

All regex patterns are compiled once at import time.
"""

import re

# Matches any t.me link, including invite links with a leading '+'
_TELEGRAM_LINK_RE = re.compile(
    r'(https\:\/\/)?t\.me\/\+?[a-zA-Z0-9\.\&\/\?\:@\-_=#]*'
)

# Matches links that point to a specific message (e.g. t.me/channel/42)
_MESSAGE_LINK_RE = re.compile(r't\.me\/.*?\/(\d+)')

# Matches Telegram proxy configuration links
_PROXY_LINK_RE = re.compile(r't\.me\/proxy\?')


def extract_telegram_link(text: str | None) -> str | None:
    """Return the first t.me link found in *text*, or ``None``."""
    if not text:
        return None
    match = _TELEGRAM_LINK_RE.search(text)
    return match.group() if match else None


def is_message_link(link: str) -> bool:
    """Return ``True`` if *link* points to a specific message rather than a group."""
    return bool(_MESSAGE_LINK_RE.search(link))


def is_proxy_link(link: str) -> bool:
    """Return ``True`` if *link* is a Telegram proxy configuration link."""
    return bool(_PROXY_LINK_RE.search(link))


def links_to_tbp_documents(links) -> list[dict]:
    """Convert an iterable of link strings into tbp-collection documents."""
    return [{"link_hash": str(link), "process_id": None, "state": "tbp"} for link in links]
