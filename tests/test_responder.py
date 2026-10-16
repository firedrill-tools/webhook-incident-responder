import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from incident_responder.graph import run_once
from incident_responder.schema import IncidentAssessment, ServiceEvent
from incident_responder.store import EventStore
from incident_responder.web import create_app


class Clock:
    now = 1000.0

    def __call__(self):
        return self.now


def event(sequence=1, kind="degraded", event_id=None, service="checkout-api"):
    return ServiceEvent(
        event_id=event_id or f"api-{sequence:03}",
        service=service,
        sequence=sequence,
        kind=kind,
        observed_at="2026-10-15T10:05:00Z",
        details={"error_rate_pct": 12.4 if kind == "degraded" else 0.2},
    )


def assess(item, context):
    return IncidentAssessment(
        severity="critical",
        summary="Checkout errors rose to 12.4%",
        suspected_cause="Cause unconfirmed; investigate upstream dependency",
        recommended_action="Inspect request traces",
        evidence_event_ids=[item["event_id"]],
    )


@pytest.fixture
def store(tmp_path):
    return EventStore(tmp_path / "events.db")


def test_langgraph_opens_and_recovers_incident(store):
    store.submit(event())
    assert run_once(store, assess)["status"] == "applied"
    state = store.state("checkout-api")
    assert state["health"] == "degraded"
    assert state["incident_id"] == "checkout-api-1"
    assert len(store.outbox()) == 1
    assert "api-001" in store.outbox()[0]["message"]
    store.submit(event(2, "recovered"))
    assert run_once(store, assess)["status"] == "applied"
    assert store.state("checkout-api")["health"] == "healthy"
    assert store.state("checkout-api")["last_sequence"] == 2
    assert len(store.outbox()) == 2
    assert "RECOVERED" in store.outbox()[1]["message"]


def test_duplicate_delivery_and_sequence_conflict(store):
    original = event()
    assert store.submit(original)["status"] == "pending"
    assert store.submit(original)["event_id"] == "api-001"
    with pytest.raises(ValueError, match="different payload"):
        store.submit(original.model_copy(update={"details": {"error_rate_pct": 99}}))
    with pytest.raises(ValueError, match="sequence already assigned"):
        store.submit(event(event_id="other-001"))
    assert run_once(store, assess)["status"] == "applied"
    assert run_once(store, assess) is None
    assert len(store.outbox()) == 1


def test_recovery_processed_before_delayed_degradation_stays_healthy(store):
    store.submit(event(2, "recovered"))
    assert run_once(store, assess)["status"] == "applied"
    assert store.outbox() == []
    store.submit(event(1, "degraded"))

    def unexpected(*_):
        raise AssertionError("stale event must not invoke the model")

    assert run_once(store, unexpected)["status"] == "ignored_stale"
    assert store.state("checkout-api")["health"] == "healthy"
    assert store.outbox() == []


def test_expired_worker_is_fenced_and_reclaimed(tmp_path):
    clock = Clock()
    store = EventStore(tmp_path / "fenced.db", clock=clock)
    store.submit(event())
    first = store.claim(lease_seconds=10)
    assert store.claim() is None
    clock.now += 11
    second = store.claim(lease_seconds=10)
    assert second["attempt"] == 2
    assert second["lease_token"] != first["lease_token"]
    with pytest.raises(ValueError, match="lease is no longer current"):
        store.commit(first["event_id"], first["lease_token"], assess(first, {}))
    assert (
        store.commit(second["event_id"], second["lease_token"], assess(second, {}))[
            "status"
        ]
        == "applied"
    )
    assert len(store.outbox()) == 1


def test_model_failure_retries_same_event_but_stops_after_three(store):
    store.submit(event())

    def broken(*_):
        raise RuntimeError("synthetic provider outage")

    for attempt in (1, 2, 3):
        failed = run_once(store, broken)
        assert failed["status"] == "failed"
        assert failed["attempt"] == attempt
        if attempt < 3:
            assert store.retry("api-001")["status"] == "pending"
    with pytest.raises(ValueError, match="fewer than three attempts"):
        store.retry("api-001")
    assert store.state("checkout-api") is None
    assert store.outbox() == []


def test_invalid_model_citation_cannot_commit(store):
    store.submit(event())

    def invented(*_):
        return IncidentAssessment(
            severity="warning",
            summary="Issue",
            suspected_cause="Unknown",
            recommended_action="Check logs",
            evidence_event_ids=["other"],
        )

    assert run_once(store, invented)["status"] == "failed"
    assert store.state("checkout-api") is None
    assert store.outbox() == []


def test_signed_webhook_accepts_valid_payload_and_rejects_invalid(store):
    client = TestClient(create_app(store, "test-secret"))
    raw = event().model_dump_json().encode()
    signature = "sha256=" + hmac.new(b"test-secret", raw, hashlib.sha256).hexdigest()
    assert client.post("/events", content=raw).status_code == 401
    accepted = client.post(
        "/events", content=raw, headers={"X-Incident-Signature": signature}
    )
    assert accepted.status_code == 202
    assert accepted.json() == {"event_id": "api-001", "status": "pending"}
    assert (
        client.post(
            "/events", content=raw, headers={"X-Incident-Signature": signature}
        ).status_code
        == 202
    )
    changed = (
        event(event_id="api-001")
        .model_copy(update={"details": {"new": True}})
        .model_dump_json()
        .encode()
    )
    changed_sig = (
        "sha256=" + hmac.new(b"test-secret", changed, hashlib.sha256).hexdigest()
    )
    assert (
        client.post(
            "/events", content=changed, headers={"X-Incident-Signature": changed_sig}
        ).status_code
        == 409
    )
