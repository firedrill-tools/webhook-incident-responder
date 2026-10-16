import hashlib
import json
import os
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from pydantic import ValidationError
from incident_responder.schema import ServiceEvent, IncidentAssessment
from incident_responder.graph import run_once
from incident_responder.store import EventStore
from incident_responder.source import InvalidSource, LEGACY_DDL


def source(details=None):
    return ServiceEvent(
        event_id="evt-1",
        service="checkout-api",
        sequence=1,
        kind="degraded",
        observed_at="2026-10-15T12:00:00Z",
        details=details or {"error_rate_pct": 12.4, "region": "synthetic-east"},
    )


def assess(event, context):
    return IncidentAssessment(
        severity="critical",
        summary="Synthetic degradation",
        suspected_cause="Unknown",
        recommended_action="Inspect traces",
        evidence_event_ids=[event["event_id"]],
    )


def applied(tmp_path):
    path = tmp_path / "events.db"
    store = EventStore(path)
    store.submit(source())
    assert run_once(store, assess)["status"] == "applied"
    return path, store


MUTATIONS = {
    "current sequence": "UPDATE service_state SET last_sequence=99",
    "current health": "UPDATE service_state SET health='healthy'",
    "current summary": "UPDATE service_state SET summary='Changed after commit'",
    "immutable event facts": """UPDATE events SET details_json='{"error_rate_pct":0.2,"region":"synthetic-east"}' """,
    "original digest": "UPDATE events SET payload_sha256=printf('%064d',0)",
    "source capture digest": "UPDATE event_sources SET canonical_sha256=printf('%064d',0)",
    "missing source provenance": "DELETE FROM event_sources",
    "fabricated source bytes": "UPDATE event_sources SET original_payload_json='{}'",
    "source provenance label": "UPDATE event_sources SET provenance='paid_model'",
    "unsafe source number": """UPDATE events SET details_json='{"count":9007199254740992}' """,
    "nonfinite source number": """UPDATE events SET details_json='{"rate":NaN}' """,
    "duplicate JSON key": """UPDATE events SET details_json='{"rate":1,"rate":2}' """,
    "unsupported source BLOB": "UPDATE events SET details_json=CAST(details_json AS BLOB)",
    "invalid sequence": "UPDATE events SET sequence=0",
    "attempt overflow": "UPDATE events SET attempt=4",
    "terminal lease": "UPDATE events SET lease_token='unowned',lease_until=123",
    "unknown event state": "UPDATE events SET status='done'",
    "missing notification": "DELETE FROM outbox",
    "notification source": "UPDATE outbox SET event_id='absent'",
    "notification history": "UPDATE outbox SET message='Invented historical output'",
    "delivery status": "UPDATE outbox SET status='delivered'",
    "delivery provenance": "UPDATE outbox SET status='sent',sent_at=NULL",
    "orphan service": "INSERT INTO service_state VALUES('orphan',1,'healthy',NULL,NULL)",
    "unknown schema table": "CREATE TABLE unowned(value TEXT)",
    "unknown schema column": "ALTER TABLE events ADD COLUMN secret TEXT",
    "unknown schema trigger": "CREATE TRIGGER unowned AFTER INSERT ON events BEGIN UPDATE service_state SET health='healthy'; END",
    "unknown schema index": "CREATE INDEX unowned ON events(kind)",
    "unsupported schema version": "PRAGMA user_version=99",
    "oversized current summary": "UPDATE service_state SET summary=printf('%03000d',0)",
}


@pytest.mark.parametrize("name", MUTATIONS)
def test_corrupt_journal_is_refused_without_repair_before_read_or_write(tmp_path, name):
    path, store = applied(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute(MUTATIONS[name])
        db.commit()
    before = path.read_bytes()
    for action in (
        lambda: EventStore(path),
        lambda: store.state("checkout-api"),
        lambda: store.get_event("evt-1"),
        store.outbox,
        lambda: store.submit(source()),
        store.claim,
    ):
        with pytest.raises(InvalidSource):
            action()
        assert path.read_bytes() == before


def test_semantic_replay_preserves_original_serialized_bytes_digest_and_effects(
    tmp_path,
):
    path, store = applied(tmp_path)
    original = store.get_event("evt-1")
    outbox = store.outbox()
    reordered = source({"region": "synthetic-east", "error_rate_pct": 12.4})
    assert (
        hashlib.sha256(reordered.model_dump_json().encode()).hexdigest()
        != original["payload_sha256"]
    )
    assert store.submit(reordered) == original
    assert store.outbox() == outbox and len(outbox) == 1
    with sqlite3.connect(path) as db:
        row = db.execute(
            "SELECT original_payload_json,provenance FROM event_sources"
        ).fetchone()
    assert (
        hashlib.sha256(row[0].encode()).hexdigest() == original["payload_sha256"]
        and row[1] == "application_serialized"
    )


def test_known_legacy_journal_preserves_original_rows_and_truthful_unknown_provenance(
    tmp_path,
):
    path = tmp_path / "legacy.db"
    event = source({"region": "synthetic-east", "error_rate_pct": 12.4})
    raw = event.model_dump_json()
    digest = hashlib.sha256(raw.encode()).hexdigest()
    with sqlite3.connect(path) as db:
        for statement in LEGACY_DDL:
            db.execute(statement)
        db.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?,?,'applied',1,NULL,NULL,NULL)",
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
        db.execute(
            "INSERT INTO service_state VALUES ('checkout-api',1,'degraded','checkout-api-1','Original legacy summary')"
        )
        db.execute(
            "INSERT INTO outbox VALUES (17,'evt-1','[WARNING] checkout-api incident checkout-api-1: Original legacy summary Cause: Unknown. Next: Inspect. Evidence: evt-1','sent',1000)"
        )
        before = {
            name: db.execute(f"SELECT * FROM {name}").fetchall()
            for name in ("events", "service_state", "outbox")
        }
    store = EventStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        for name, values in before.items():
            assert db.execute(f"SELECT * FROM {name}").fetchall() == values
        proof = db.execute(
            "SELECT provenance,original_payload_json FROM event_sources"
        ).fetchone()
    assert proof == ("legacy_snapshot", None)
    assert store.submit(event)["payload_sha256"] == digest
    assert store.outbox()[0]["notification_id"] == 17
    assert store.outbox()[0]["sent_at"] == 1000


@pytest.mark.parametrize(
    "mutation",
    [
        {"sequence": True},
        {"sequence": 1.0},
        {"sequence": 2**53},
        {"event_id": " padded"},
        {"service": " "},
        {"details": {"rate": float("nan")}},
        {"details": {"count": 2**53}},
        {"details": {"text": "x" * 2001}},
    ],
)
def test_unsafe_event_contract_cannot_enter_the_journal(tmp_path, mutation):
    store = EventStore(tmp_path / "events.db")
    invalid = source().model_copy(update=mutation)
    with pytest.raises(ValidationError):
        store.submit(invalid)
    assert store.claim() is None


def test_source_instance_and_file_replacement_are_fenced_during_a_native_attempt(
    tmp_path,
):
    path, store = applied(tmp_path)
    with sqlite3.connect(path) as db:
        db.execute(
            "UPDATE journal_metadata SET value='123e4567-e89b-42d3-a456-426614174000' WHERE key='instance_id'"
        )
        db.commit()
    with pytest.raises(InvalidSource, match="identity"):
        store.state("checkout-api")
    reopened = EventStore(path)
    replacement = tmp_path / "replacement.db"
    EventStore(replacement)
    os.replace(replacement, path)
    with pytest.raises(InvalidSource, match="replaced"):
        reopened.outbox()


def test_alias_or_unrelated_database_is_not_adopted_or_modified(tmp_path):
    path = tmp_path / "other.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
        db.execute("INSERT INTO unrelated VALUES('preserve')")
        db.commit()
    before = path.read_bytes()
    with pytest.raises(InvalidSource):
        EventStore(path)
    assert path.read_bytes() == before
    path, store = applied(tmp_path)
    alias = tmp_path / "alias.db"
    alias.symlink_to(path)
    with pytest.raises(InvalidSource):
        EventStore(alias)
    hard = tmp_path / "hard.db"
    os.link(path, hard)
    with pytest.raises(InvalidSource):
        store.outbox()


def test_independent_connections_share_one_claim_and_original_replay(tmp_path):
    path = tmp_path / "events.db"
    first = EventStore(path)
    second = EventStore(path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        submitted = list(
            pool.map(lambda store: store.submit(source()), [first, second])
        )
    assert all(item["event_id"] == "evt-1" for item in submitted)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda store: store.claim(), [first, second]))
    assert sum(item is not None for item in claims) == 1
    claimed = next(item for item in claims if item)
    first.commit(claimed["event_id"], claimed["lease_token"], assess(claimed, {}))
    assert len(second.outbox()) == 1


def test_real_processes_share_one_claim_and_one_atomic_outbox_effect(tmp_path):
    import subprocess
    import sys
    import time

    path = tmp_path / "events.db"
    store = EventStore(path)
    store.submit(source())
    script = """import json,sys,time\nfrom pathlib import Path\nfrom incident_responder.store import EventStore\nfrom incident_responder.schema import IncidentAssessment\npath,ready,start=sys.argv[1:]\nstore=EventStore(path);Path(ready).write_text('ready')\ndeadline=time.monotonic()+8\nwhile not Path(start).exists():\n if time.monotonic()>deadline:raise RuntimeError('barrier deadline')\n time.sleep(.01)\nevent=store.claim()\nif event:\n assessment=IncidentAssessment(severity='warning',summary='Controlled process',suspected_cause='Unknown',recommended_action='Inspect',evidence_event_ids=[event['event_id']])\n event=store.commit(event['event_id'],event['lease_token'],assessment)\nprint(json.dumps({'claimed':event is not None,'status':event['status'] if event else None}))\n"""
    start = tmp_path / "start"
    children = []
    try:
        for index in range(2):
            children.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        script,
                        str(path),
                        str(tmp_path / f"ready-{index}"),
                        str(start),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        deadline = time.monotonic() + 8
        while not all((tmp_path / f"ready-{index}").exists() for index in range(2)):
            if time.monotonic() > deadline:
                raise AssertionError("processes did not reach the barrier")
            time.sleep(0.01)
        start.write_text("start")
        results = []
        for child in children:
            stdout, stderr = child.communicate(timeout=8)
            assert child.returncode == 0, stderr
            results.append(json.loads(stdout))
        assert sum(item["claimed"] for item in results) == 1
        assert (
            store.get_event("evt-1")["status"] == "applied" and len(store.outbox()) == 1
        )
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=2)


def test_extreme_integer_details_are_rejected_before_serialization_or_write(tmp_path):
    store = EventStore(tmp_path / "events.db")
    with pytest.raises(ValidationError):
        store.submit(source().model_copy(update={"details": {"count": 10**1000}}))
    assert store.claim() is None


def test_extreme_lease_parameter_is_a_controlled_rejection(tmp_path):
    store = EventStore(tmp_path / "events.db")
    store.submit(source())
    with pytest.raises(ValueError, match="lease seconds"):
        store.claim(10**1000)
    assert store.get_event("evt-1")["status"] == "pending"
