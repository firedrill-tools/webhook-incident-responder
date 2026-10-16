"""SQLite journal, sequence fence, incident state, and notification outbox."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .schema import IncidentAssessment, ServiceEvent


class EventStore:
    def __init__(self, path: str | Path, clock=time.time):
        self.path = str(path)
        self.clock = clock
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY, service TEXT NOT NULL, sequence INTEGER NOT NULL,
                    kind TEXT NOT NULL, observed_at TEXT NOT NULL, details_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT, lease_until REAL, error TEXT,
                    UNIQUE(service, sequence)
                );
                CREATE TABLE IF NOT EXISTS service_state (
                    service TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL, health TEXT NOT NULL,
                    incident_id TEXT, summary TEXT
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id),
                    message TEXT NOT NULL, status TEXT NOT NULL,
                    sent_at REAL
                );
            """)

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _event(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        result = dict(row)
        result["details"] = json.loads(result.pop("details_json"))
        return result

    def submit(self, event: ServiceEvent) -> dict:
        serialized = event.model_dump_json()
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT * FROM events WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if previous:
                if previous["payload_sha256"] != digest:
                    raise ValueError("event ID reused with different payload")
                return self._event(previous)
            try:
                db.execute(
                    """
                    INSERT INTO events (event_id,service,sequence,kind,observed_at,details_json,
                        payload_sha256,status) VALUES (?,?,?,?,?,?,?,'pending')
                """,
                    (
                        event.event_id,
                        event.service,
                        event.sequence,
                        event.kind,
                        event.observed_at,
                        json.dumps(event.details, sort_keys=True),
                        digest,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(
                    "service sequence already assigned to another event"
                ) from exc
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event.event_id,)
                ).fetchone()
            )

    def _now(self) -> float:
        value = self.clock()
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError("clock must return finite numeric seconds")
        return float(value)

    def claim(self, lease_seconds: float = 60, *, guard=None) -> dict | None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or not 0 < lease_seconds <= 3600
        ):
            raise ValueError("lease seconds must be finite and in (0,3600]")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if guard is not None:
                guard()
            now = self._now()
            if not math.isfinite(now + lease_seconds) or now + lease_seconds <= now:
                raise ValueError("lease deadline is not representable")
            db.execute(
                """
                UPDATE events SET status='failed', error='lease expired after three attempts',
                    lease_token=NULL, lease_until=NULL
                WHERE status='processing' AND lease_until <= ? AND attempt >= 3
            """,
                (now,),
            )
            row = db.execute(
                """
                SELECT * FROM events WHERE status='pending'
                OR (status='processing' AND lease_until <= ? AND attempt < 3)
                ORDER BY rowid LIMIT 1
            """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            if guard is not None:
                guard()
            token = str(uuid.uuid4())
            db.execute(
                """
                UPDATE events SET status='processing', attempt=attempt+1,
                    lease_token=?, lease_until=?, error=NULL WHERE event_id=?
            """,
                (token, now + lease_seconds, row["event_id"]),
            )
            if guard is not None:
                guard()
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (row["event_id"],)
                ).fetchone()
            )

    def context(self, service: str, sequence: int) -> dict:
        with self._db() as db:
            state = db.execute(
                "SELECT * FROM service_state WHERE service=?", (service,)
            ).fetchone()
            rows = db.execute(
                """
                SELECT event_id,sequence,kind,observed_at,details_json FROM events
                WHERE service=? AND sequence<=? ORDER BY sequence DESC LIMIT 5
            """,
                (service, sequence),
            ).fetchall()
            events = [
                {**dict(row), "details": json.loads(row["details_json"])}
                for row in rows
            ]
            for event in events:
                event.pop("details_json")
            return {"state": dict(state) if state else None, "recent_events": events}

    def commit(
        self,
        event_id: str,
        token: str,
        assessment: IncidentAssessment | None,
        *,
        guard=None,
    ) -> dict:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if guard is not None:
                guard()
            row = db.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            now = self._now()
            if (
                row is None
                or row["status"] != "processing"
                or row["lease_token"] != token
                or not isinstance(row["lease_until"], (int, float))
                or not math.isfinite(row["lease_until"])
                or row["lease_until"] <= now
            ):
                raise ValueError("event lease is no longer current")
            event = self._event(row)
            state_row = db.execute(
                "SELECT * FROM service_state WHERE service=?", (event["service"],)
            ).fetchone()
            state = dict(state_row) if state_row else None
            if state and event["sequence"] <= state["last_sequence"]:
                db.execute(
                    "UPDATE events SET status='ignored_stale', lease_token=NULL, lease_until=NULL WHERE event_id=?",
                    (event_id,),
                )
                if guard is not None:
                    guard()
                return self._event(
                    db.execute(
                        "SELECT * FROM events WHERE event_id=?", (event_id,)
                    ).fetchone()
                )

            message = None
            if event["kind"] == "degraded":
                if assessment is None:
                    raise ValueError("degraded event needs an assessment")
                allowed = {
                    row["event_id"]
                    for row in db.execute(
                        "SELECT event_id FROM events WHERE service=? AND sequence<=?",
                        (event["service"], event["sequence"]),
                    )
                }
                if (
                    event_id not in assessment.evidence_event_ids
                    or not set(assessment.evidence_event_ids) <= allowed
                ):
                    raise ValueError("assessment cites unobserved event IDs")
                incident_id = (
                    state["incident_id"]
                    if state and state["health"] == "degraded"
                    else (f"{event['service']}-{event['sequence']}")
                )
                health = "degraded"
                summary = assessment.summary
                message = (
                    f"[{assessment.severity.upper()}] {event['service']} incident {incident_id}: "
                    f"{summary} Cause: {assessment.suspected_cause}. "
                    f"Next: {assessment.recommended_action}. Evidence: "
                    f"{', '.join(assessment.evidence_event_ids)}"
                )
            else:
                health = "healthy"
                incident_id = None
                summary = None
                if state and state["health"] == "degraded":
                    message = f"[RECOVERED] {event['service']} incident {state['incident_id']} at event {event_id}"

            if guard is not None:
                guard()
            db.execute(
                """
                INSERT INTO service_state VALUES (?,?,?,?,?)
                ON CONFLICT(service) DO UPDATE SET last_sequence=excluded.last_sequence,
                health=excluded.health, incident_id=excluded.incident_id, summary=excluded.summary
            """,
                (event["service"], event["sequence"], health, incident_id, summary),
            )
            db.execute(
                "UPDATE events SET status='applied', lease_token=NULL, lease_until=NULL WHERE event_id=?",
                (event_id,),
            )
            if message:
                db.execute(
                    "INSERT INTO outbox (event_id,message,status) VALUES (?,?,'pending')",
                    (event_id, message),
                )
            if guard is not None:
                guard()
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
            )

    def fail(self, event_id: str, token: str, error: str) -> bool:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            updated = db.execute(
                """
                UPDATE events SET status='failed', error=?, lease_token=NULL, lease_until=NULL
                WHERE event_id=? AND status='processing' AND lease_token=? AND lease_until>?
            """,
                (error[:500], event_id, token, self._now()),
            ).rowcount
            return updated == 1

    def retry(self, event_id: str) -> dict:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is None:
                raise KeyError(event_id)
            if row["status"] != "failed" or row["attempt"] >= 3:
                raise ValueError(
                    "only failed events with fewer than three attempts can retry"
                )
            db.execute(
                "UPDATE events SET status='pending', error=NULL WHERE event_id=?",
                (event_id,),
            )
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
            )

    def get_event(self, event_id: str) -> dict | None:
        with self._db() as db:
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
            )

    def state(self, service: str) -> dict | None:
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM service_state WHERE service=?", (service,)
            ).fetchone()
            return dict(row) if row else None

    def outbox(self) -> list[dict]:
        with self._db() as db:
            return [
                dict(row)
                for row in db.execute("SELECT * FROM outbox ORDER BY notification_id")
            ]

    def mark_sent(self, notification_id: int) -> None:
        with self._db() as db:
            db.execute(
                "UPDATE outbox SET status='sent', sent_at=? WHERE notification_id=? AND status='pending'",
                (self.clock(), notification_id),
            )
