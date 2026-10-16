"""Transactional delivery claims, expiry fences and explicit operator resolution."""

import math
import uuid
from .delivery_state import ERROR_CODES, digest, payload


class DeliveryStore:
    def _expire_deliveries(self, db, now):
        rows = db.execute(
            "SELECT * FROM delivery_attempts WHERE outcome IN ('claimed','dispatching') AND lease_until<=?",
            (now,),
        ).fetchall()
        for row in rows:
            outcome = "unknown" if row["outcome"] == "dispatching" else "not_dispatched"
            self._end_delivery(db, row, outcome, now, "lease_expired")

    @staticmethod
    def _end_delivery(db, row, outcome, now, error_code=None):
        status = (
            "sent"
            if outcome == "acknowledged"
            else "delivery_unknown"
            if outcome == "unknown"
            else "delivery_failed"
            if row["attempt"] == 3
            else "pending"
        )
        db.execute(
            "UPDATE delivery_attempts SET outcome=?,finished_at=?,acknowledgement=?,error_code=? WHERE attempt_id=?",
            (
                outcome,
                now,
                "200_ok" if outcome == "acknowledged" else None,
                error_code,
                row["attempt_id"],
            ),
        )
        db.execute(
            "UPDATE outbox SET status=?,sent_at=? WHERE notification_id=?",
            (status, now if status == "sent" else None, row["notification_id"]),
        )

    def claim_notification(self, target_sha256, lease_seconds=90, *, guard=None):
        if (
            not isinstance(target_sha256, str)
            or len(target_sha256) != 64
            or any(c not in "0123456789abcdef" for c in target_sha256)
        ):
            raise ValueError("delivery target requires a SHA256 identity")
        if (
            type(lease_seconds) not in (int, float)
            or not 0 < lease_seconds <= 3600
            or not math.isfinite(lease_seconds)
        ):
            raise ValueError("delivery lease seconds must be finite and in (0,3600]")
        with self._db() as db:
            if guard:
                guard()
            now = self._now()
            if not math.isfinite(now + lease_seconds) or now + lease_seconds <= now:
                raise ValueError("delivery lease deadline is not representable")
            self._expire_deliveries(db, now)
            row = db.execute("""SELECT o.* FROM outbox o JOIN events e ON e.event_id=o.event_id
                WHERE o.status='pending' AND NOT EXISTS (
                  SELECT 1 FROM outbox earlier JOIN events prior ON prior.event_id=earlier.event_id
                  WHERE earlier.notification_id<o.notification_id AND prior.service=e.service AND earlier.status!='sent')
                ORDER BY o.notification_id LIMIT 1""").fetchone()
            if row is None:
                if guard:
                    guard()
                return None
            item = dict(row)
            attempt = (
                db.execute(
                    "SELECT COUNT(*) FROM delivery_attempts WHERE notification_id=?",
                    (item["notification_id"],),
                ).fetchone()[0]
                + 1
            )
            token = str(uuid.uuid4())
            db.execute(
                "INSERT INTO delivery_attempts (attempt_id,notification_id,attempt,payload_sha256,target_sha256,started_at,lease_until,outcome) VALUES (?,?,?,?,?,?,?,'claimed')",
                (
                    token,
                    item["notification_id"],
                    attempt,
                    digest(payload(item["message"])),
                    target_sha256,
                    now,
                    now + lease_seconds,
                ),
            )
            db.execute(
                "UPDATE outbox SET status='sending' WHERE notification_id=?",
                (item["notification_id"],),
            )
            if guard:
                guard()
            return {
                "notification": item,
                "attempt": dict(
                    db.execute(
                        "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
                    ).fetchone()
                ),
            }

    def dispatch_notification(self, token, *, guard=None):
        with self._db() as db:
            if guard:
                guard()
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
            ).fetchone()
            now = self._now()
            if row is None or row["outcome"] != "claimed" or row["lease_until"] <= now:
                raise ValueError("delivery lease is no longer current")
            db.execute(
                "UPDATE delivery_attempts SET outcome='dispatching',dispatched_at=? WHERE attempt_id=?",
                (now, token),
            )
            if guard:
                guard()

    def acknowledge_notification(self, token, *, guard=None):
        with self._db() as db:
            if guard:
                guard()
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
            ).fetchone()
            now = self._now()
            if (
                row is None
                or row["outcome"] != "dispatching"
                or row["lease_until"] <= now
            ):
                raise ValueError("delivery lease is no longer current")
            self._end_delivery(db, row, "acknowledged", now)
            if guard:
                guard()

    def abandon_notification(self, token, error_code):
        if error_code not in ERROR_CODES:
            raise ValueError("unknown delivery error code")
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
            ).fetchone()
            if row is None or row["outcome"] not in {"claimed", "dispatching"}:
                return False
            self._end_delivery(
                db,
                row,
                "unknown" if row["outcome"] == "dispatching" else "not_dispatched",
                self._now(),
                error_code,
            )
            return True

    def resolve_delivery(self, token, decision, note, *, guard=None):
        if (
            decision not in {"delivered", "not_delivered"}
            or not isinstance(note, str)
            or not note.strip()
            or len(note.encode()) > 500
        ):
            raise ValueError(
                "resolution requires delivered/not_delivered and a nonblank note of at most 500 UTF8 bytes"
            )
        with self._db() as db:
            if guard:
                guard()
            row = db.execute(
                "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
            ).fetchone()
            if (
                row is None
                or row["outcome"] != "unknown"
                or row["resolution"] is not None
            ):
                raise ValueError(
                    "only an unresolved unknown delivery attempt can be resolved"
                )
            now = self._now()
            db.execute(
                "UPDATE delivery_attempts SET resolution=?,resolution_note=?,resolved_at=? WHERE attempt_id=?",
                (decision, note, now, token),
            )
            status = (
                "sent"
                if decision == "delivered"
                else "delivery_failed"
                if row["attempt"] == 3
                else "pending"
            )
            db.execute(
                "UPDATE outbox SET status=?,sent_at=? WHERE notification_id=?",
                (
                    status,
                    now if decision == "delivered" else None,
                    row["notification_id"],
                ),
            )
            if guard:
                guard()
            return dict(
                db.execute(
                    "SELECT * FROM delivery_attempts WHERE attempt_id=?", (token,)
                ).fetchone()
            )

    def deliveries(self):
        with self._db() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM delivery_attempts ORDER BY notification_id,attempt"
                )
            ]
