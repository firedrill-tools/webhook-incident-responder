import math
import pytest
from incident_responder.store import EventStore
from incident_responder.schema import IncidentAssessment, ServiceEvent


def source():
    return ServiceEvent(
        event_id="evt-1",
        service="checkout-api",
        sequence=1,
        kind="degraded",
        observed_at="2026-10-15T12:00:00Z",
    )


def assessment():
    return IncidentAssessment(
        severity="critical",
        summary="Synthetic degradation",
        suspected_cause="Unknown",
        recommended_action="Inspect traces",
        evidence_event_ids=["evt-1"],
    )


@pytest.mark.parametrize("elapsed", [10, 11])
def test_expiry_itself_fences_commit_and_failure_without_replacement(tmp_path, elapsed):
    clock = [1000.0]
    store = EventStore(tmp_path / "events.db", clock=lambda: clock[0])
    store.submit(source())
    first = store.claim(lease_seconds=10)
    clock[0] += elapsed
    with pytest.raises(ValueError, match="lease is no longer current"):
        store.commit("evt-1", first["lease_token"], assessment())
    assert store.fail("evt-1", first["lease_token"], "late failure") is False
    assert store.state("checkout-api") is None
    assert store.outbox() == []
    replacement = store.claim(lease_seconds=10)
    assert replacement["attempt"] == 2
    assert replacement["lease_token"] != first["lease_token"]
    assert (
        store.commit("evt-1", replacement["lease_token"], assessment())["status"]
        == "applied"
    )


@pytest.mark.parametrize("duration", [True, 0, -1, float("nan"), float("inf"), 3601])
def test_invalid_lease_duration_cannot_claim_an_event(tmp_path, duration):
    store = EventStore(tmp_path / "events.db")
    store.submit(source())
    with pytest.raises(ValueError, match="lease seconds"):
        store.claim(duration)
    assert store.get_event("evt-1")["status"] == "pending"
    assert store.get_event("evt-1")["attempt"] == 0
