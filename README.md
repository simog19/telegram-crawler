# Telegram Crawler

A multi-process Python crawler that discovers and archives Telegram groups and channels by following `t.me` links found in messages.  Data is stored in MongoDB across nine purpose-specific collections.

---

## Master's thesis context

This repository contains the engineering side of my Master's thesis at the **Politecnico di Torino**.  The crawler was built end-to-end to collect the dataset, and the [`plots/`](plots/) folder produced the operational figures used in the dissertation.  The thesis itself (and any other publications listed under my name) is available on the Politecnico institutional repository:

> <https://webthesis.biblio.polito.it/view/creators/Galota=3ASimone=3A=3A.default.html>

---

## Architecture

The crawler follows a **master-worker** pattern:

```
┌─────────────────────────────────────────────────────────┐
│                    Master process                        │
│  Orchestrates task assignment and result persistence     │
│                                                          │
│  Queues (in-memory + persisted in MongoDB):              │
│    tbp      → links waiting to be processed              │
│    leave    → groups to leave after THRESHOLD_LEAVE_HOURS│
│    wait     → pending join requests to re-check          │
│    process  → ids of currently available workers         │
└────────────────┬───────────────────┬────────────────────┘
                 │  task_queue       │  result_queue
        ┌────────▼────────┐  ┌───────▼────────┐
        │   Worker  0     │  │   Worker  1    │
        │  TelegramClient │  │ TelegramClient │
        │  (session_0)    │  │  (session_1)   │
        └─────────────────┘  └────────────────┘
```

Each worker runs as a separate OS process with its own Telegram session.  Workers communicate with the master exclusively through `multiprocessing.Queue` objects.

### Task priority

For each free worker the master assigns tasks in this order:

1. **LEAVE** - leave groups that have been joined for longer than `THRESHOLD_LEAVE_HOURS`
2. **CHECK_WAIT** - re-attempt joining groups whose request has been pending for longer than `THRESHOLD_REQUEST_HOURS`
3. **TRY_JOIN** - pick the next link from `tbp` and attempt to join

### Exit condition

The master loop exits only when **all** of the following hold simultaneously:

- `tbp` collection is empty (no new input),
- `result_queue` is empty (no pending writes), and
- every worker is idle.

Persisted wait/leave items that are not yet due remain in MongoDB and are resumed on the next run.

---

## MongoDB schema

| Collection      | Purpose |
|-----------------|---------|
| `tbp`           | Input queue: links to be processed (`{link_hash, process_id, state}`) |
| `groups`        | Group/channel snapshots: metadata, up to 2 000 members, up to 500 messages |
| `done`          | State-machine record for every link ever attempted (see lifecycle below) |
| `wait`          | Per-worker persistent queue of pending join requests |
| `gathered`      | Every `t.me` link found in scraped messages, with full message context |
| `edges`         | Directed graph: `{dest: group_id, sources: [group_id, ...]}` |
| `analytics`     | Operational counters (total, collected, bots, requests sent/accepted) |
| `bot_collection`| Bot interaction data (sent command + received messages) |
| `leave`         | Per-worker persistent queue of groups to leave |

### Link lifecycle

```
tbp → processing → inside      (join succeeded; data collected)
                 → join_failed  (could not join)
                 → waiting      (join request sent, not yet accepted)
                 → done         (left the group after threshold)
                 → leave_failed (leave attempt failed; re-queued)
```

### Edge graph

The `edges` collection records the provenance of each group: whenever group **A** is scraped and its messages contain a link to group **B**, an edge `A → B` is written.  This allows post-hoc analysis of how groups refer to one another.

### Why MongoDB and not a relational store

The choice of MongoDB over a relational database was driven by the shape of the data and the access patterns of the crawler, not by hype:

- **Heterogeneous group documents** - Telegram groups, channels and supergroups expose different metadata fields (e.g. `participants_count` only for groups, `linked_chat_id` only for channels).  A relational schema would have required either many `NULL` columns or a base+specialised table arrangement, both adding friction during ingest.
- **Variable-length nested data** - each `groups` document embeds up to 2 000 member sub-documents and up to 500 message sub-documents directly inline.  In SQL this would need at least two child tables with foreign keys and a `JOIN` on every read; in MongoDB it is one document, retrieved in a single round-trip.
- **A directed graph of provenance** - the `edges` collection stores `{dest, sources: [...]}` and grows by appending to the `sources` array.  The single atomic `update_one(..., {"$push": ...}, upsert=True)` operation has no clean SQL equivalent - it would be either `INSERT ... ON CONFLICT` plus an array merge, or a separate edge table with extra round-trips and locking overhead under concurrent worker writes.
- **Schema evolution** - during the thesis work the document shape changed several times (new flags, extra timestamps, bot conversation history).  MongoDB absorbed these changes without migrations, which mattered for an iterative research project.

For the volumes handled here (~10⁴ groups, ~10⁵ links, ~10⁶ messages over multi-week runs) the ergonomics of a document store outweighed the consistency guarantees that a relational engine would have provided.

### Database work in detail

The crawler exercises a representative slice of MongoDB operations across all nine collections:

**CRUD primitives**

- `insert_one` / `insert_many` for batch ingest of gathered links, group snapshots, bot interactions, new TBP entries.
- `find_one` / `find` with predicate filters for deduplication (`find_one({"link_hash": ...})`) and crash recovery (`find({"state": "processing"})` re-queues in-flight links on startup).
- `delete_many` to consume an input queue item once it has been moved into `processing`.

**Atomic mutations**

- `update_one(..., {"$set": ...})` for state-machine transitions in `done` (`tbp → processing → inside / waiting / done / leave_failed`).
- `update_one(..., {"$inc": ...})` for operational counters in `analytics` (links seen, groups collected, bots interacted with, requests sent / accepted) incremented concurrently from multiple worker processes.
- `update_one(..., {"$push": ...}, upsert=True)` for both the provenance graph (`edges`) and the persisted per-worker queues (`leave`, `wait`).
- `update_one(..., {"$pull": ...})` to remove a queue element after it has been processed.

**Concurrency design**

- Nine collections accessed concurrently by N worker processes plus the master, all through `pymongo.MongoClient` connection pooling.
- Per-worker queue documents (`{_id: pid, queue: [...]}`) keyed by process id eliminate cross-worker contention on shared documents.
- Atomic upsert + `$push` removes any read-modify-write pattern that would otherwise need an application-level lock.

**Recovery and persistence**

- On startup `recover_in_flight_links` re-queues every `done` document still in the `processing` state from a previous run.
- `build_leave_queue` and `build_wait_queue` reconstruct the in-memory worker queues from their MongoDB counterparts, so a restart resumes the exact same schedule of pending leaves and pending join re-checks.

---

## Data processing and visualisation

The [`plots/`](plots/) directory contains a self-contained two-stage pipeline that turns raw crawler output into the operational figures published in the thesis.

**Stage 1 - ETL (`re` + `datetime`)**

Three extractor scripts parse the crawler's structured log lines and emit normalised JSON:

| Script | Extracts | Output |
|---|---|---|
| `extract_link_evaluations.py` | "Data collected" events per day and hour | `link_evaluations.json` |
| `extract_join_throughput.py` | successful join events per day and hour | `join_throughput.json` |
| `extract_fwe.py` | `FloodWaitError` durations per day | `fwe_data.json` |

**Stage 2 - Visualisation (`numpy` + `matplotlib`)**

Four plotting scripts consume those JSON files (or work standalone with embedded data):

| Script | Figure |
|---|---|
| `plot_link_evaluations.py` | Hourly link-evaluation throughput - 5-worker run |
| `plot_join_throughput.py` | Hourly join throughput across 18 days - 2-worker run |
| `plot_fwe_cdf.py` | Empirical CDF (log-scale) of ~1 000 Telegram `FloodWaitError` penalties |
| `plot_fwe_days.py` | Per-day bar chart of `FloodWaitError` durations |

**Tools used across the full project**

| Tool | Role |
|---|---|
| `Telethon` | Async Telegram MTProto client (join, leave, gather messages, bot interactions) |
| `pymongo` | All MongoDB access (CRUD, atomic update operators, find with predicates) |
| Python `multiprocessing` | N-worker parallelism with `Queue`-based IPC between master and workers |
| `python-dotenv` | Environment-variable loading for credentials and connection strings |
| `re` + `datetime` | Log parsing and timestamp normalisation in the ETL stage |
| `numpy` | Sorting, empirical CDF computation, bar-position layout |
| `matplotlib` | All figures (line charts, log-scale scatter for the CDF, grouped bar chart) |
| `argparse` + `json` | CLI surface and intermediate storage between the ETL and plotting stages |

**Selected empirical findings** (full discussion in the thesis)

- The 2-worker crawler sustained **40-55 group joins per hour** consistently over 18 consecutive days.
- ~95 % of `FloodWaitError` penalties fall between 60 s and 720 s; three multi-hour outliers (max ≈ 29 368 s ≈ 8 h) reveal Telegram's adaptive rate-limiting.
- After roughly 48 h of operation the penalty distribution narrows from ~700 s to ~300 s, suggesting the rate-limiter calibrates as request patterns stabilise.

---

## Project layout

```
.
├── README.md
├── pyproject.toml          Build + package metadata
├── requirements.txt        Runtime dependencies
├── .env.example            Template for required env vars (copy to .env)
├── .gitignore
├── telegram_crawler/       Crawler package
│   ├── __init__.py
│   ├── __main__.py         Entry point - `python -m telegram_crawler`
│   ├── config.py           Reads env vars (with optional .env support)
│   ├── database.py         Database wrapper + collection helpers
│   ├── link_utils.py       Regex-based link parsing / filtering
│   ├── telegram_ops.py     Async Telegram ops: join, leave, gather, eval
│   ├── worker.py           Worker subprocess: task dispatch + handlers
│   └── master.py           Master loop: orchestration + result persistence
└── plots/                  Stand-alone analysis & visualisation scripts
```

---

## Setup

### 1. Install dependencies

`requirements.txt` covers everything (crawler runtime + analysis scripts under `plots/`):

```bash
pip install -r requirements.txt
```

Alternatively, install the project itself.  The `analysis` extra is opt-in so a
production crawler install stays slim (no `numpy` / `matplotlib`):

```bash
pip install -e .                # crawler only
pip install -e ".[analysis]"    # crawler + plots dependencies
```

The editable install also exposes the `telegram-crawler` console script.

### 2. Configure secrets

Copy the template and fill in real values:

```bash
cp .env.example .env
$EDITOR .env
```

Required variables:

| Variable | Purpose |
|---|---|
| `TELEGRAM_API_IDS` | Comma-separated Telethon api_ids, one per worker |
| `TELEGRAM_API_HASHES` | Comma-separated Telethon api_hashes, one per worker |
| `MONGODB_URI` | Full MongoDB connection string |

Optional variables (defaults in parentheses):

| Variable | Default |
|---|---|
| `MONGODB_DB` | `MyCrawler` |
| `N_PROCESSES` | `2` |
| `THRESHOLD_LEAVE_HOURS` | `24.0` |
| `THRESHOLD_REQUEST_HOURS` | `24.0` |

Get Telethon credentials at <https://my.telegram.org/apps>.

### 3. Seed the input queue

```python
import os, pymongo
db = pymongo.MongoClient(os.environ["MONGODB_URI"])[os.environ.get("MONGODB_DB", "MyCrawler")]
db["tbp"].insert_one({
    "link_hash": "https://t.me/some_public_group",
    "process_id": None,
    "state": "tbp",
})
```

### 4. Run

```bash
python -m telegram_crawler
# or, if installed:
telegram-crawler
```

The crawler will authenticate each session interactively on first run (Telethon stores the session in `session_0.session` / `session_1.session`, both git-ignored).

---

## Key design decisions

- **Flood-wait compliance** - every `FloodWaitError` is caught and the worker sleeps for exactly the duration Telegram mandates before retrying (iterative, not recursive - avoids unbounded stack growth).
- **Random inter-join delays** - workers wait a random interval (100-120 s or 220-240 s) between joins to avoid triggering Telegram's anti-spam heuristics.
- **Queue persistence** - the `leave` and `wait` collections mirror the in-memory queues so the crawler can resume from the exact same state after a restart.
- **Deduplication** - gathered links are deduplicated by `(link, group_id)` in memory before insertion; the `groups` and `bot_collection` collections are checked before insert to avoid duplicate documents.
- **Atomic edge upsert** - `edges.update_one(..., upsert=True)` writes new destinations and appends to existing ones in a single atomic operation, avoiding the race window of a find-then-insert.
- **Idle backoff** - when there is no work to assign and no result to drain, the master loop sleeps 0.5 s instead of busy-spinning the database.
