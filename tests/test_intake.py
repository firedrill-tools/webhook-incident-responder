import asyncio
import hashlib
import hmac
import json
import sqlite3
from fastapi.testclient import TestClient
from incident_responder.web import create_app
from incident_responder.store import EventStore


def signature(raw):
    return "sha256=" + hmac.new(b"synthetic-secret", raw, hashlib.sha256).hexdigest()


def test_signed_semantic_replay_accepts_reordered_fields_but_preserves_original_payload(
    tmp_path,
):
    store = EventStore(tmp_path / "events.db")
    client = TestClient(create_app(store, "synthetic-secret"))
    source = {
        "event_id": "evt-1",
        "service": "checkout-api",
        "sequence": 1,
        "kind": "degraded",
        "observed_at": "2026-10-15T12:00:00Z",
        "details": {"rate": 12.4, "region": "synthetic-east"},
    }
    raw = json.dumps(source).encode()
    assert (
        client.post(
            "/events", content=raw, headers={"X-Incident-Signature": signature(raw)}
        ).status_code
        == 202
    )
    original = store.get_event("evt-1")
    source["details"] = {"region": "synthetic-east", "rate": 12.4}
    reordered = json.dumps(source, sort_keys=True).encode()
    assert signature(raw) != signature(reordered)
    assert (
        client.post(
            "/events",
            content=reordered,
            headers={"X-Incident-Signature": signature(raw)},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/events",
            content=reordered,
            headers={"X-Incident-Signature": signature(reordered)},
        ).status_code
        == 202
    )
    assert store.get_event("evt-1") == original and store.outbox() == []


def test_duplicate_keys_and_nonfinite_signed_input_cannot_enter_source(tmp_path):
    store = EventStore(tmp_path / "events.db")
    client = TestClient(create_app(store, "synthetic-secret"))
    for raw in [b'{"event_id":"one","event_id":"two"}', b'{"sequence":NaN}', b"\xff"]:
        assert (
            client.post(
                "/events", content=raw, headers={"X-Incident-Signature": signature(raw)}
            ).status_code
            == 422
        )
    assert store.claim() is None


def test_intake_stops_reading_at_its_cap_and_checks_missing_auth_before_read(tmp_path):
    store = EventStore(tmp_path / "events.db")
    app = create_app(store, "synthetic-secret")

    async def exercise(headers, chunks, expected, maximum_reads):
        reads = [0]
        messages = []

        async def receive():
            index = reads[0]
            reads[0] += 1
            assert index < len(chunks), "application read past supplied source"
            return {
                "type": "http.request",
                "body": chunks[index],
                "more_body": index < len(chunks) - 1,
            }

        async def send(message):
            messages.append(message)

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/events",
            "raw_path": b"/events",
            "query_string": b"",
            "root_path": "",
            "headers": headers,
            "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 80),
        }
        await app(scope, receive, send)
        assert (
            next(
                message["status"]
                for message in messages
                if message["type"] == "http.response.start"
            )
            == expected
        )
        assert reads[0] <= maximum_reads

    asyncio.run(exercise([], [b"x" * 100000], 401, 0))
    header = [(b"x-incident-signature", b"sha256=" + b"0" * 64)]
    asyncio.run(exercise(header, [b"x" * 40000, b"x" * 40000, b"never read"], 413, 2))
    asyncio.run(
        exercise(header + [(b"content-length", b"100000")], [b"never read"], 413, 0)
    )
    assert store.claim() is None


def test_open_intake_refuses_corrupted_source_without_repair(tmp_path):
    path = tmp_path / "events.db"
    store = EventStore(path)
    client = TestClient(create_app(store, "synthetic-secret"))
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
        db.commit()
    before = path.read_bytes()
    raw = json.dumps(
        {
            "event_id": "evt-1",
            "service": "checkout-api",
            "sequence": 1,
            "kind": "degraded",
            "observed_at": "2026-10-15T12:00:00Z",
        }
    ).encode()
    assert (
        client.post(
            "/events", content=raw, headers={"X-Incident-Signature": signature(raw)}
        ).status_code
        == 503
    )
    assert path.read_bytes() == before
