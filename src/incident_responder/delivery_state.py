"""Delivery journal contract; an uncertain POST is never an automatic retry."""

import hashlib
import json
import math
import re
import uuid

DELIVERY_DDL = (
    """CREATE TABLE notification_sources (
        notification_id INTEGER PRIMARY KEY REFERENCES outbox(notification_id),
        message_sha256 TEXT NOT NULL, provenance TEXT NOT NULL,
        captured_status TEXT NOT NULL, captured_sent_at REAL, captured_at REAL NOT NULL)""",
    """CREATE TABLE delivery_attempts (
        attempt_id TEXT PRIMARY KEY, notification_id INTEGER NOT NULL REFERENCES outbox(notification_id),
        attempt INTEGER NOT NULL, payload_sha256 TEXT NOT NULL, target_sha256 TEXT NOT NULL,
        started_at REAL NOT NULL, lease_until REAL NOT NULL, dispatched_at REAL,
        finished_at REAL, outcome TEXT NOT NULL, acknowledgement TEXT, error_code TEXT,
        resolution TEXT, resolution_note TEXT, resolved_at REAL,
        UNIQUE(notification_id,attempt))""",
)
ERROR_CODES = {
    "operation_failed",
    "cancelled",
    "deadline",
    "invalid_ack",
    "lease_expired",
    "recording_failed",
}


def digest(value):
    return hashlib.sha256(
        value if isinstance(value, bytes) else value.encode()
    ).hexdigest()


def message_digest(item):
    return digest(
        json.dumps(
            [item["notification_id"], item["event_id"], item["message"]],
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


def payload(message):
    # Slack control characters must be escaped before passing untrusted text.
    escaped = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return json.dumps(
        {"text": escaped}, ensure_ascii=False, separators=(",", ":")
    ).encode()


def capture_notification(db, item, provenance, clock):
    db.execute(
        "INSERT INTO notification_sources VALUES (?,?,?,?,?,?)",
        (
            item["notification_id"],
            message_digest(item),
            provenance,
            item["status"],
            item["sent_at"],
            clock(),
        ),
    )


def validate_delivery(db):
    from .source import InvalidSource

    def require(test, message):
        if not test:
            raise InvalidSource(message)

    def number(value):
        return (
            type(value) in (int, float) and abs(value) <= 2**53 and math.isfinite(value)
        )

    def hex_hash(value):
        return (
            isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None
        )

    count = db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0]
    require(count <= 3000, "delivery history exceeds supported bound")
    for column, maximum in {
        "attempt_id": 36,
        "payload_sha256": 64,
        "target_sha256": 64,
        "outcome": 32,
        "acknowledgement": 16,
        "error_code": 32,
        "resolution": 32,
        "resolution_note": 500,
    }.items():
        require(
            not db.execute(
                f"SELECT 1 FROM delivery_attempts WHERE LENGTH(CAST({column} AS BLOB))>? LIMIT 1",
                (maximum,),
            ).fetchone(),
            "oversized delivery scalar",
        )
    for column, maximum in {
        "message_sha256": 64,
        "provenance": 32,
        "captured_status": 32,
    }.items():
        require(
            not db.execute(
                f"SELECT 1 FROM notification_sources WHERE LENGTH(CAST({column} AS BLOB))>? LIMIT 1",
                (maximum,),
            ).fetchone(),
            "oversized notification provenance",
        )
    require(
        db.execute("SELECT COUNT(*) FROM notification_sources").fetchone()[0] <= 1000,
        "notification provenance exceeds supported bound",
    )
    sources = {
        row["notification_id"]: dict(row)
        for row in db.execute("SELECT * FROM notification_sources")
    }
    notifications = {
        row["notification_id"]: dict(row) for row in db.execute("SELECT * FROM outbox")
    }
    require(set(sources) == set(notifications), "notification provenance is incomplete")
    attempts = {}
    for raw in db.execute(
        "SELECT * FROM delivery_attempts ORDER BY notification_id,attempt"
    ):
        row = dict(raw)
        require(
            isinstance(row["attempt_id"], str)
            and str(uuid.UUID(row["attempt_id"])) == row["attempt_id"]
            and uuid.UUID(row["attempt_id"]).version == 4,
            "invalid delivery identity",
        )
        item = notifications.get(row["notification_id"])
        history = attempts.setdefault(row["notification_id"], [])
        require(
            item is not None
            and type(row["attempt"]) is int
            and row["attempt"] == len(history) + 1
            and row["attempt"] <= 3,
            "invalid delivery attempt sequence",
        )
        require(
            row["payload_sha256"] == digest(payload(item["message"]))
            and hex_hash(row["target_sha256"]),
            "delivery request facts differ from source",
        )
        require(
            number(row["started_at"])
            and number(row["lease_until"])
            and row["lease_until"] > row["started_at"],
            "invalid delivery lease",
        )
        require(
            row["dispatched_at"] is None or number(row["dispatched_at"]),
            "invalid dispatch observation",
        )
        require(
            row["outcome"]
            in {"claimed", "dispatching", "not_dispatched", "unknown", "acknowledged"},
            "invalid delivery outcome",
        )
        active = row["outcome"] in {"claimed", "dispatching"}
        require(
            (row["finished_at"] is None) if active else number(row["finished_at"]),
            "invalid delivery completion facts",
        )
        require(
            (row["dispatched_at"] is None)
            if row["outcome"] in {"claimed", "not_dispatched"}
            else number(row["dispatched_at"]),
            "delivery phase disagrees with dispatch intent",
        )
        require(
            row["acknowledgement"]
            == ("200_ok" if row["outcome"] == "acknowledged" else None),
            "invalid delivery acknowledgement",
        )
        require(
            (row["error_code"] in ERROR_CODES)
            if row["outcome"] in {"unknown", "not_dispatched"}
            else row["error_code"] is None,
            "invalid delivery error",
        )
        if row["resolution"] is None:
            require(
                row["resolution_note"] is None and row["resolved_at"] is None,
                "delivery resolution is incomplete",
            )
        else:
            require(
                row["outcome"] == "unknown"
                and row["resolution"] in {"delivered", "not_delivered"}
                and isinstance(row["resolution_note"], str)
                and bool(row["resolution_note"].strip())
                and len(row["resolution_note"].encode()) <= 500
                and number(row["resolved_at"]),
                "invalid operator resolution",
            )
        if history:
            previous = history[-1]
            require(
                previous["outcome"] == "not_dispatched"
                or previous["outcome"] == "unknown"
                and previous["resolution"] == "not_delivered",
                "delivery retries an unresolved or acknowledged attempt",
            )
        history.append(row)
    for identifier, item in notifications.items():
        proof = sources[identifier]
        require(
            proof["message_sha256"] == message_digest(item)
            and number(proof["captured_at"]),
            "original notification facts changed",
        )
        require(
            proof["provenance"] in {"legacy_snapshot", "application_created"}
            and proof["captured_status"] in {"pending", "sent"},
            "invalid notification provenance",
        )
        require(
            number(proof["captured_sent_at"])
            if proof["captured_status"] == "sent"
            else proof["captured_sent_at"] is None,
            "invalid original delivery facts",
        )
        require(
            proof["provenance"] != "application_created"
            or proof["captured_status"] == "pending",
            "application notification invents historical delivery",
        )
        history = attempts.get(identifier, [])
        if not history:
            require(
                item["status"] == proof["captured_status"]
                and item["sent_at"] == proof["captured_sent_at"],
                "notification changed without delivery evidence",
            )
            continue
        require(
            proof["captured_status"] == "pending",
            "historically sent notification was resent",
        )
        service = db.execute(
            "SELECT service FROM events WHERE event_id=?", (item["event_id"],)
        ).fetchone()[0]
        require(
            not db.execute(
                """SELECT 1 FROM outbox o JOIN events e ON e.event_id=o.event_id
            WHERE e.service=? AND o.notification_id<? AND o.status!='sent' LIMIT 1""",
                (service, identifier),
            ).fetchone(),
            "delivery overtakes an unresolved service notification",
        )
        latest = history[-1]
        if latest["outcome"] in {"claimed", "dispatching"}:
            expected, sent_at = "sending", None
        elif latest["outcome"] == "acknowledged":
            expected, sent_at = "sent", latest["finished_at"]
        elif latest["outcome"] == "unknown" and latest["resolution"] is None:
            expected, sent_at = "delivery_unknown", None
        elif latest["resolution"] == "delivered":
            expected, sent_at = "sent", latest["resolved_at"]
        else:
            expected, sent_at = (
                ("delivery_failed" if latest["attempt"] == 3 else "pending"),
                None,
            )
        require(
            item["status"] == expected and item["sent_at"] == sent_at,
            "notification status contradicts delivery history",
        )
