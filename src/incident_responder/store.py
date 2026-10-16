"""SQLite journal, sequence fence, incident state, and notification outbox."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .schema import IncidentAssessment, ServiceEvent
from .delivery_state import capture_notification
from .delivery_store import DeliveryStore
from .source import (
    InvalidSource,
    validate,
    canonical_event,
    initialize,
    identity,
    source_digest,
)


class EventStore(DeliveryStore):
    def __init__(self, path: str | Path, clock=time.time):
        self.path = str(path)
        self.clock = clock
        self._file_identity = None
        self._instance_id = None
        if self.path == ":memory:":
            raise ValueError("a persistent local journal path is required")
        self.path = os.path.abspath(self.path)
        with self._db(initialize_source=True) as db:
            self._instance_id = identity(db)
        info = Path(self.path).lstat()
        self._file_identity = (info.st_dev, info.st_ino)

    def _file_info(self):
        try:
            info = Path(self.path).lstat()
        except FileNotFoundError:
            if self._file_identity is not None:
                raise InvalidSource("incident journal disappeared")
            return None
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_size > 64 * 1024 * 1024
        ):
            raise InvalidSource("journal requires a bounded regular local file")
        if (
            self._file_identity is not None
            and (info.st_dev, info.st_ino) != self._file_identity
        ):
            raise InvalidSource("incident journal file was replaced")
        return info

    @contextmanager
    def _db(self, *, initialize_source=False):
        before = self._file_info()
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            db.row_factory = sqlite3.Row
            if before is None:
                os.chmod(self.path, 0o600)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("BEGIN IMMEDIATE")
            if initialize_source:
                initialize(db, self._now)
            validate(db)
            if self._instance_id is not None and identity(db) != self._instance_id:
                raise InvalidSource("incident journal ownership identity changed")
            yield db
            validate(db)
            self._file_info()
            if self._instance_id is not None and identity(db) != self._instance_id:
                raise InvalidSource("incident journal ownership identity changed")
            db.commit()
        except sqlite3.Error as error:
            if db is not None:
                db.rollback()
            if getattr(error, "sqlite_errorcode", 0) & 255 in (
                sqlite3.SQLITE_BUSY,
                sqlite3.SQLITE_LOCKED,
            ):
                raise RuntimeError("incident journal is busy") from error
            raise InvalidSource("incident journal operation failed") from error
        except BaseException:
            if db is not None:
                db.rollback()
            raise
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _event(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        result = dict(row)
        result["details"] = json.loads(result.pop("details_json"))
        return result

    def submit(self, event: ServiceEvent) -> dict:
        event = ServiceEvent.model_validate(event)
        serialized = event.model_dump_json()
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        with self._db() as db:
            previous = db.execute(
                "SELECT * FROM events WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if previous:
                original = self._event(previous)
                original_event = ServiceEvent.model_validate(
                    {
                        key: original[key]
                        for key in (
                            "event_id",
                            "service",
                            "sequence",
                            "kind",
                            "observed_at",
                            "details",
                        )
                    }
                )
                if canonical_event(original_event) != canonical_event(event):
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
                        json.dumps(
                            event.details,
                            sort_keys=True,
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                        digest,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(
                    "service sequence already assigned to another event"
                ) from exc
            db.execute(
                "INSERT INTO event_sources VALUES (?,?,?,?,?)",
                (
                    event.event_id,
                    source_digest(event, digest),
                    serialized,
                    "application_serialized",
                    self._now(),
                ),
            )
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
            or abs(value) > 2**53
            or not math.isfinite(value)
        ):
            raise ValueError("clock must return finite numeric seconds")
        return float(value)

    def claim(self, lease_seconds: float = 60, *, guard=None) -> dict | None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not 0 < lease_seconds <= 3600
            or not math.isfinite(lease_seconds)
        ):
            raise ValueError("lease seconds must be finite and in (0,3600]")
        with self._db() as db:
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
                assessment = IncidentAssessment.model_validate(assessment)
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
                item = dict(
                    db.execute(
                        "SELECT * FROM outbox WHERE event_id=?", (event_id,)
                    ).fetchone()
                )
                capture_notification(db, item, "application_created", self._now)
            if guard is not None:
                guard()
            return self._event(
                db.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
            )

    def fail(self, event_id: str, token: str, error: str) -> bool:
        with self._db() as db:
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
