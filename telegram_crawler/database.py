"""
MongoDB collections wrapper for the Telegram crawler.

All persistence is document-oriented (MongoDB). The nine collections and their
roles in the crawl pipeline are described in the Database docstring below.
"""

from datetime import datetime

import pymongo


class Database:
    """Thin wrapper that exposes each MongoDB collection as a named attribute.

    Collections
    -----------
    tbp         Links to be processed (the input queue).
    groups      Full group / channel snapshots (members, messages, metadata).
    done        Every link that has been attempted, with its current state and
                timestamps (state machine: tbp → processing → inside / join_failed
                / waiting / done / leave_failed).
    wait        Per-worker queue of links whose join request was sent but not yet
                accepted; persisted across restarts.
    gathered    Every t.me link discovered inside a scraped group, stored with the
                originating message and group context.
    edges       Directed graph: for each group id (dest) the list of group ids that
                contained a reference to it (sources). Used for provenance analysis.
    analytics   Operational counters (total links seen, groups collected, bots
                interacted with, requests sent / accepted).
    bots        Conversation data from bot interactions (/start responses).
    leave       Per-worker queue of groups to leave once THRESHOLD_LEAVE_HOURS have
                elapsed since joining; persisted across restarts.
    """

    def __init__(self, uri: str, db_name: str) -> None:
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        self.tbp = db["tbp"]
        self.groups = db["groups"]
        self.done = db["done"]
        self.wait = db["wait"]
        self.gathered = db["gathered"]
        self.edges = db["edges"]
        self.analytics = db["analytics"]
        self.bots = db["bot_collection"]
        self.leave = db["leave"]

    # ------------------------------------------------------------------
    # Graph helpers
    # ------------------------------------------------------------------

    def update_edges(self, entity_id: int, source_group_id: int) -> None:
        """Record that *source_group_id* contained a reference to *entity_id*.

        Creates the destination node on first encounter; otherwise appends to
        the existing sources list. Single atomic upsert - avoids the race
        window of a separate find + insert.
        """
        self.edges.update_one(
            {"dest": entity_id},
            {"$push": {"sources": source_group_id}},
            upsert=True,
        )

    # ------------------------------------------------------------------
    # State-machine helpers
    # ------------------------------------------------------------------

    def set_link_state(
        self,
        link_hash: str,
        state: str,
        timestamp: datetime | None = None,
    ) -> None:
        """Update the *state* field of a link in the done collection.

        Optionally sets *time* in the same atomic operation.
        """
        fields: dict = {"state": state}
        if timestamp is not None:
            fields["time"] = timestamp
        self.done.update_one({"link_hash": link_hash}, {"$set": fields})


def open_database(uri: str, db_name: str) -> Database:
    """Connect to MongoDB and return a ready-to-use Database instance."""
    print(f"{datetime.now()} - [MASTER] Opening MongoDB connection...")
    return Database(uri, db_name)
