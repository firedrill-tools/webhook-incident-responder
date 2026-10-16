"""Bounded validation of the known local journal without repairing original facts."""

from __future__ import annotations
from functools import lru_cache
import json
import math
import re
import sqlite3
import uuid
import hashlib
from .schema import ServiceEvent

MAX_EVENTS = 1000
MAX_SERVICES = 128
MAX_BYTES = 8 * 1024 * 1024
LEGACY_DDL = (
    """CREATE TABLE events (
 event_id TEXT PRIMARY KEY, service TEXT NOT NULL, sequence INTEGER NOT NULL,
 kind TEXT NOT NULL, observed_at TEXT NOT NULL, details_json TEXT NOT NULL,
 payload_sha256 TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
 lease_token TEXT, lease_until REAL, error TEXT, UNIQUE(service, sequence))""",
    """CREATE TABLE service_state (
 service TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL, health TEXT NOT NULL,
 incident_id TEXT, summary TEXT)""",
    """CREATE TABLE outbox (
 notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
 event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id),
 message TEXT NOT NULL, status TEXT NOT NULL, sent_at REAL)""",
)


EXTRA_DDL = (
    "CREATE TABLE journal_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    """CREATE TABLE event_sources (
        event_id TEXT PRIMARY KEY REFERENCES events(event_id),
        canonical_sha256 TEXT NOT NULL, original_payload_json TEXT,
        provenance TEXT NOT NULL, captured_at REAL NOT NULL)""",
)
DDL = LEGACY_DDL + EXTRA_DDL
TABLES = ("events", "service_state", "outbox", "journal_metadata", "event_sources")


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def business_event(row):
    return ServiceEvent.model_validate(
        {
            key: row[key]
            for key in ("event_id", "service", "sequence", "kind", "observed_at")
        }
        | {"details": strict_json(row["details_json"])}
    )


def source_digest(event, original_digest):
    return digest(canonical_event(event) + "\n" + original_digest)


def identity(db):
    return db.execute(
        "SELECT value FROM journal_metadata WHERE key='instance_id'"
    ).fetchone()[0]


def initialize(db, clock):
    version = db.execute("PRAGMA user_version").fetchone()[0]
    names = {
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }
    if version == 0:
        if names:
            _validate(db, version=0)
            statements = EXTRA_DDL
        else:
            statements = DDL
        for statement in statements:
            db.execute(statement)
        db.executemany(
            "INSERT INTO journal_metadata VALUES (?,?)",
            [
                ("application", "firedrill-tools.incident-responder"),
                ("instance_id", str(uuid.uuid4())),
            ],
        )
        captured = clock()
        for raw in db.execute("SELECT * FROM events"):
            row = dict(raw)
            event = business_event(row)
            db.execute(
                "INSERT INTO event_sources VALUES (?,?,?,?,?)",
                (
                    event.event_id,
                    source_digest(event, row["payload_sha256"]),
                    None,
                    "legacy_snapshot",
                    captured,
                ),
            )
        db.execute("PRAGMA user_version=1")
    elif version != 1:
        raise InvalidSource("unsupported incident journal version")
    validate(db)


class InvalidSource(RuntimeError):
    pass


def strict_json(value):
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate source JSON key")
            result[key] = item
        return result

    def nonfinite(value):
        raise ValueError("nonfinite source JSON")

    return json.loads(value, object_pairs_hook=pairs, parse_constant=nonfinite)


def canonical_event(event):
    return json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _schema(db, version):
    result = {}
    for row in db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ):
        key = (row[0], row[1], row[2])
        result[key] = re.sub(r"\s+", "", row[3].lower()) if row[3] else None
    for table in TABLES if version == 1 else TABLES[:3]:
        result[("columns", table)] = tuple(
            tuple(row) for row in db.execute(f"PRAGMA table_xinfo({table})")
        )
        indexes = []
        for row in db.execute(f"PRAGMA index_list({table})"):
            indexes.append(
                (
                    row[2],
                    row[3],
                    row[4],
                    tuple(
                        tuple(item)[1:]
                        for item in db.execute(f'PRAGMA index_xinfo("{row[1]}")')
                    ),
                )
            )
        result[("indexes", table)] = tuple(sorted(indexes, key=str))
    for table in ("outbox", "event_sources") if version == 1 else ("outbox",):
        result[("foreign_keys", table)] = tuple(
            tuple(row) for row in db.execute(f"PRAGMA foreign_key_list({table})")
        )
    return result


@lru_cache(maxsize=2)
def expected_schema(version):
    db = sqlite3.connect(":memory:")
    try:
        for statement in DDL if version == 1 else LEGACY_DDL:
            db.execute(statement)
        return _schema(db, version)
    finally:
        db.close()


def validate(db):
    try:
        _validate(db)
    except InvalidSource:
        raise
    except (
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        sqlite3.DatabaseError,
    ) as error:
        raise InvalidSource(
            "invalid incident journal; original records were not repaired"
        ) from error


def _validate(db, version=1):
    if db.execute("PRAGMA user_version").fetchone()[0] != version:
        raise InvalidSource("unsupported incident journal version")
    expected_tables = set(TABLES if version == 1 else TABLES[:3])
    objects = list(
        db.execute(
            "SELECT type,name,LENGTH(CAST(sql AS BLOB)) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 6"
        )
    )
    if len(objects) != len(expected_tables) or any(
        row[0] != "table" or row[1] not in expected_tables or row[2] > 16384
        for row in objects
    ):
        raise InvalidSource("unexpected incident journal objects")
    if _schema(db, version) != expected_schema(version):
        raise InvalidSource(
            "incident journal schema differs from the supported contract"
        )
    if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise InvalidSource("incident journal has invalid references")
    counts = {
        name: db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        for name in ("events", "service_state", "outbox")
    }
    if (
        counts["events"] > MAX_EVENTS
        or counts["outbox"] > MAX_EVENTS
        or counts["service_state"] > MAX_SERVICES
    ):
        raise InvalidSource("incident journal exceeds supported history bounds")
    limits = {
        "events": {
            "event_id": 128,
            "service": 128,
            "kind": 32,
            "observed_at": 256,
            "payload_sha256": 64,
            "status": 32,
            "lease_token": 64,
            "error": 2000,
        },
        "service_state": {
            "service": 128,
            "health": 32,
            "incident_id": 256,
            "summary": 2000,
        },
        "outbox": {"event_id": 128, "status": 32},
    }
    if version == 1:
        limits.update(
            journal_metadata={"key": 64, "value": 256},
            event_sources={"event_id": 128, "canonical_sha256": 64, "provenance": 64},
        )
    for table, columns in limits.items():
        for column, maximum in columns.items():
            if db.execute(
                f"SELECT 1 FROM {table} WHERE LENGTH(CAST({column} AS BLOB))>? LIMIT 1",
                (maximum,),
            ).fetchone():
                raise InvalidSource("incident journal has an oversized scalar field")
    source_bytes = (
        db.execute(
            "SELECT COALESCE(SUM(LENGTH(CAST(details_json AS BLOB))),0) FROM events"
        ).fetchone()[0]
        + db.execute(
            "SELECT COALESCE(SUM(LENGTH(CAST(message AS BLOB))),0) FROM outbox"
        ).fetchone()[0]
    )
    if (
        source_bytes > MAX_BYTES
        or db.execute(
            "SELECT 1 FROM events WHERE LENGTH(CAST(details_json AS BLOB))>65536 LIMIT 1"
        ).fetchone()
        or db.execute(
            "SELECT 1 FROM outbox WHERE LENGTH(CAST(message AS BLOB))>32768 LIMIT 1"
        ).fetchone()
    ):
        raise InvalidSource("incident journal exceeds bounded record bytes")
    events = {}
    by_service = {}
    for raw in db.execute("SELECT * FROM events"):
        row = dict(raw)
        if not isinstance(row["details_json"], str):
            raise InvalidSource("source details must be JSON text")
        details = strict_json(row["details_json"])
        event = ServiceEvent.model_validate(
            {
                key: row[key]
                for key in ("event_id", "service", "sequence", "kind", "observed_at")
            }
            | {"details": details}
        )
        if not re.fullmatch(r"[a-f0-9]{64}", row["payload_sha256"]):
            raise InvalidSource("invalid original event digest")
        status = row["status"]
        attempt = row["attempt"]
        if (
            status
            not in {"pending", "processing", "failed", "applied", "ignored_stale"}
            or type(attempt) is not int
            or not 0 <= attempt <= 3
        ):
            raise InvalidSource("invalid event lifecycle")
        if status == "pending" and attempt >= 3 or status != "pending" and attempt < 1:
            raise InvalidSource("event attempt and status disagree")
        if status == "processing":
            if (
                not isinstance(row["lease_token"], str)
                or str(uuid.UUID(row["lease_token"])) != row["lease_token"]
                or type(row["lease_until"]) not in (int, float)
                or not math.isfinite(row["lease_until"])
            ):
                raise InvalidSource("invalid processing lease")
        elif row["lease_token"] is not None or row["lease_until"] is not None:
            raise InvalidSource("terminal/unclaimed event retains a lease")
        if status == "failed":
            if not isinstance(row["error"], str) or len(row["error"]) > 500:
                raise InvalidSource("failed event error is invalid")
        elif row["error"] is not None:
            raise InvalidSource("nonfailed event retains an error")
        events[event.event_id] = row
        by_service.setdefault(event.service, []).append(row)
    if len(by_service) > MAX_SERVICES:
        raise InvalidSource("too many source services")
    outbox = {}
    last_notification = {}
    for raw in db.execute("SELECT * FROM outbox ORDER BY notification_id"):
        row = dict(raw)
        event = events.get(row["event_id"])
        if (
            type(row["notification_id"]) is not int
            or row["notification_id"] < 1
            or event is None
            or event["status"] != "applied"
            or not isinstance(row["message"], str)
            or not row["message"].strip()
        ):
            raise InvalidSource("notification lacks an applied event")
        if row["status"] not in {"pending", "sent"}:
            raise InvalidSource("invalid notification status")
        if (
            row["status"] == "pending"
            and row["sent_at"] is not None
            or row["status"] == "sent"
            and (
                type(row["sent_at"]) not in (int, float)
                or not math.isfinite(row["sent_at"])
            )
        ):
            raise InvalidSource("notification delivery facts disagree")
        previous = last_notification.get(event["service"])
        if previous is not None and previous >= event["sequence"]:
            raise InvalidSource("notification order contradicts service sequence")
        last_notification[event["service"]] = event["sequence"]
        outbox[row["event_id"]] = row
    states = {
        row["service"]: dict(row) for row in db.execute("SELECT * FROM service_state")
    }
    for service, rows in by_service.items():
        applied = sorted(
            (row for row in rows if row["status"] == "applied"),
            key=lambda row: row["sequence"],
        )
        current = states.pop(service, None)
        if not applied:
            if current is not None:
                raise InvalidSource("service state has no applied source")
            if any(row["status"] == "ignored_stale" for row in rows):
                raise InvalidSource("stale disposition has no later applied source")
            continue
        active = None
        last = None
        for row in applied:
            notification = outbox.get(row["event_id"])
            if row["kind"] == "degraded":
                active = active or f"{service}-{row['sequence']}"
                if notification is None or not any(
                    notification["message"].startswith(
                        f"[{severity}] {service} incident {active}: "
                    )
                    for severity in ("WARNING", "CRITICAL")
                ):
                    raise InvalidSource(
                        "degradation notification is missing or inconsistent"
                    )
                marker = " Evidence: "
                if marker not in notification["message"]:
                    raise InvalidSource("degradation lacks original citation text")
                cited = notification["message"].rsplit(marker, 1)[1].split(", ")
                allowed = {
                    item["event_id"]
                    for item in rows
                    if item["sequence"] <= row["sequence"]
                }
                if row["event_id"] not in cited or not set(cited) <= allowed:
                    raise InvalidSource("notification citations do not match source")
            else:
                if active:
                    if (
                        notification is None
                        or notification["message"]
                        != f"[RECOVERED] {service} incident {active} at event {row['event_id']}"
                    ):
                        raise InvalidSource("recovery notification contradicts history")
                elif notification is not None:
                    raise InvalidSource("notification invents a recovery")
                active = None
            last = row
        if (
            current is None
            or current["last_sequence"] != last["sequence"]
            or current["health"] != ("degraded" if active else "healthy")
            or current["incident_id"] != active
        ):
            raise InvalidSource("service state contradicts applied history")
        if active:
            if (
                not isinstance(current["summary"], str)
                or not current["summary"].strip()
                or len(current["summary"]) > 500
                or not any(
                    outbox[last["event_id"]]["message"].startswith(
                        f"[{severity}] {service} incident {active}: {current['summary']} Cause: "
                    )
                    for severity in ("WARNING", "CRITICAL")
                )
            ):
                raise InvalidSource("current summary differs from its notification")
        elif current["summary"] is not None:
            raise InvalidSource("healthy state retains an incident summary")
        if any(
            row["status"] == "ignored_stale" and row["sequence"] > last["sequence"]
            for row in rows
        ):
            raise InvalidSource("stale event is newer than committed state")
    if states:
        raise InvalidSource("service state has no source history")

    if version == 1:
        meta = {
            row["key"]: row["value"]
            for row in db.execute("SELECT * FROM journal_metadata")
        }
        if (
            set(meta) != {"application", "instance_id"}
            or meta["application"] != "firedrill-tools.incident-responder"
            or str(uuid.UUID(meta["instance_id"])) != meta["instance_id"]
            or uuid.UUID(meta["instance_id"]).version != 4
        ):
            raise InvalidSource("invalid incident journal ownership metadata")
        count = db.execute("SELECT COUNT(*) FROM event_sources").fetchone()[0]
        raw_size = db.execute(
            "SELECT COALESCE(SUM(LENGTH(CAST(original_payload_json AS BLOB))),0) FROM event_sources"
        ).fetchone()[0]
        if (
            count != len(events)
            or raw_size > MAX_BYTES
            or db.execute(
                "SELECT 1 FROM event_sources WHERE LENGTH(CAST(original_payload_json AS BLOB))>65536 LIMIT 1"
            ).fetchone()
        ):
            raise InvalidSource("event source provenance is incomplete or oversized")
        for raw in db.execute("SELECT * FROM event_sources"):
            proof = dict(raw)
            row = events.get(proof["event_id"])
            if (
                row is None
                or type(proof["captured_at"]) not in (int, float)
                or not math.isfinite(proof["captured_at"])
            ):
                raise InvalidSource("invalid event source capture")
            event = business_event(row)
            if proof["canonical_sha256"] != source_digest(event, row["payload_sha256"]):
                raise InvalidSource(
                    "immutable event facts differ from their captured source"
                )
            if proof["provenance"] == "legacy_snapshot":
                if proof["original_payload_json"] is not None:
                    raise InvalidSource(
                        "legacy provenance invents original serialized bytes"
                    )
            elif proof["provenance"] == "application_serialized":
                original = proof["original_payload_json"]
                if (
                    not isinstance(original, str)
                    or digest(original) != row["payload_sha256"]
                    or canonical_event(
                        ServiceEvent.model_validate(strict_json(original))
                    )
                    != canonical_event(event)
                ):
                    raise InvalidSource(
                        "original serialized event does not match stored facts"
                    )
            else:
                raise InvalidSource("unknown event source provenance")
