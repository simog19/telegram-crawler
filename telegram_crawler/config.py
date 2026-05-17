"""
Centralised configuration for the Telegram crawler.

Secrets (Telethon credentials + MongoDB URI) are loaded from environment
variables. A ``.env`` file in the project root is auto-loaded if
``python-dotenv`` is installed; otherwise variables must be exported by
the shell or the orchestrator.

Required environment variables
------------------------------
TELEGRAM_API_IDS      Comma-separated Telethon api_ids, one per worker.
TELEGRAM_API_HASHES   Comma-separated Telethon api_hashes, one per worker.
MONGODB_URI           Full MongoDB connection string.

Optional environment variables
------------------------------
MONGODB_DB              Database name. Default: "MyCrawler".
N_PROCESSES             Number of worker processes. Default: 2.
THRESHOLD_LEAVE_HOURS   Hours inside a group before leaving. Default: 24.
THRESHOLD_REQUEST_HOURS Hours before re-checking a pending join. Default: 24.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _required(key: str) -> str:
    """Read a required environment variable; raise if missing or empty."""
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {key}. "
            "See .env.example for the list of required variables."
        )
    return value


def _required_list(key: str) -> list[str]:
    """Parse a comma-separated required environment variable into a list."""
    return [item.strip() for item in _required(key).split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Worker pool
# ---------------------------------------------------------------------------

N_PROCESSES: int = int(os.environ.get("N_PROCESSES", "2"))

API_IDS: list[str] = _required_list("TELEGRAM_API_IDS")
API_HASHES: list[str] = _required_list("TELEGRAM_API_HASHES")

if len(API_IDS) != N_PROCESSES or len(API_HASHES) != N_PROCESSES:
    raise RuntimeError(
        f"TELEGRAM_API_IDS ({len(API_IDS)}) and TELEGRAM_API_HASHES "
        f"({len(API_HASHES)}) must each provide exactly N_PROCESSES "
        f"({N_PROCESSES}) values."
    )

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_URI: str = _required("MONGODB_URI")
DB_NAME: str = os.environ.get("MONGODB_DB", "MyCrawler")

# ---------------------------------------------------------------------------
# Timing thresholds
# ---------------------------------------------------------------------------

# How long a worker must have been inside a group before it leaves (hours)
THRESHOLD_LEAVE_HOURS: float = float(os.environ.get("THRESHOLD_LEAVE_HOURS", "24.0"))

# How long to wait before re-checking a pending join request (hours)
THRESHOLD_REQUEST_HOURS: float = float(os.environ.get("THRESHOLD_REQUEST_HOURS", "24.0"))

# Random sleep range (seconds) after each join attempt, indexed by worker priority
WAIT_PRIMARY: tuple[int, int] = (100, 120)    # for worker ids < 2
WAIT_SECONDARY: tuple[int, int] = (220, 240)  # for worker ids >= 2

# Random sleep range (seconds) between consecutive leave operations
WAIT_LEAVE: tuple[int, int] = (0, 5)
