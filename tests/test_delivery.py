import asyncio
import base64
import json
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import httpx2
import pytest
from incident_responder.schema import ServiceEvent, IncidentAssessment
from incident_responder.store import EventStore
from incident_responder.delivery import send_once, send_once_async, target
from incident_responder.delivery_state import digest, payload
from incident_responder.source import InvalidSource

URL = "http://127.0.0.1:19/services/synthetic-secret"


class Clock:
    value = 1000.0

    def __call__(self):
        return self.value


def add(store, sequence=1, service="checkout", summary="Synthetic degradation"):
    identifier = f"{service}-{sequence}"
    store.submit(
        ServiceEvent(
            event_id=identifier,
            service=service,
            sequence=sequence,
            kind="degraded",
            observed_at="2026-10-16T12:00:00Z",
        )
    )
    row = store.claim()
    store.commit(
        row["event_id"],
        row["lease_token"],
        IncidentAssessment(
            severity="critical",
            summary=summary,
            suspected_cause="Unknown",
            recommended_action="Inspect",
            evidence_event_ids=[identifier],
        ),
    )


@pytest.fixture
def store(tmp_path):
    result = EventStore(tmp_path / "events.db")
    add(result)
    return result


class Transport(httpx2.AsyncBaseTransport):
    def __init__(
        self, status=200, body=b"ok", delay=None, error=None, close_error=False
    ):
        self.status = status
        self.body = body
        self.delay = delay
        self.error = error
        self.requests = []
        self.closed = False
        self.close_error = close_error

    async def handle_async_request(self, request):
        self.requests.append(
            {
                "url": str(request.url),
                "body": await request.aread(),
                "headers": dict(request.headers),
            }
        )
        if self.delay:
            await self.delay()
        if self.error:
            raise self.error
        return httpx2.Response(
            self.status,
            content=self.body,
            headers={"Location": "http://127.0.0.1:19/redirect-target"}
            if self.status == 302
            else None,
        )

    async def aclose(self):
        self.closed = True
        if self.close_error:
            raise RuntimeError("synthetic close failure")


def run(store, tmp_path, transport, **options):
    result = send_once(
        store, URL, transport=transport, output_root=tmp_path / "runs", **options
    )
    data = json.loads(Path(result["receipt_path"]).read_bytes())
    return result, data


def test_valid_ack_has_private_original_request_response_and_no_webhook_secret(
    store, tmp_path
):
    transport = Transport()
    result, data = run(store, tmp_path, transport)
    assert (
        result["status"] == "sent"
        and result["run_status"] == "completed"
        and result["delivery_basis"] == "http_200_ok"
    )
    assert len(transport.requests) == 1 and transport.closed and data["client_closed"]
    assert data["target"] == target(URL) and "synthetic-secret" not in json.dumps(data)
    assert (
        base64.b64decode(data["response"]["decoded_body_base64"]) == b"ok"
        and data["response"]["complete"]
    )
    assert data["response"]["body_sha256"] == digest(b"ok")
    assert data["request"]["body_sha256"] == digest(transport.requests[0]["body"])
    assert (
        data["cost_usd"] is None
        and store.deliveries()[0]["acknowledgement"] == "200_ok"
    )
    assert store.outbox()[0]["status"] == "sent"
    assert Path(result["receipt_path"]).stat().st_mode & 0o777 == 0o600
    assert Path(result["receipt_path"]).parent.stat().st_mode & 0o777 == 0o700
    assert (
        send_once(store, URL, transport=Transport(), output_root=tmp_path / "runs")
        is None
    )


@pytest.mark.parametrize(
    "status,body",
    [
        (200, b"not-an-ack"),
        (200, b"ok\n"),
        (201, b"ok"),
        (204, b""),
        (302, b"ok"),
        (400, b"invalid_payload"),
        (403, b"action_prohibited"),
        (404, b"no_service"),
        (429, b"rate_limited"),
        (503, b"outage"),
        (200, b"x" * 4097),
    ],
)
def test_non_ack_is_unknown_and_cannot_automatically_resend(
    store, tmp_path, status, body
):
    transport = Transport(status, body)
    result, data = run(store, tmp_path, transport)
    assert result["status"] == "delivery_unknown" and result["run_status"] == "failed"
    assert len(transport.requests) == 1 and transport.closed and data["client_closed"]
    assert store.deliveries()[0]["error_code"] == "invalid_ack"
    second = Transport()
    assert (
        send_once(store, URL, transport=second, output_root=tmp_path / "runs") is None
    )
    assert second.requests == [] and len(store.deliveries()) == 1


def test_untrusted_message_slack_controls_are_escaped_without_rewriting_source(
    tmp_path,
):
    store = EventStore(tmp_path / "events.db")
    add(
        store, summary="Synthetic <@U123> & <!channel> <https://untrusted.example|link>"
    )
    original = store.outbox()[0]["message"]
    transport = Transport()
    run(store, tmp_path, transport)
    sent = json.loads(transport.requests[0]["body"])["text"]
    assert (
        "<" not in sent
        and ">" not in sent
        and "&lt;!channel&gt;" in sent
        and "&amp;" in sent
    )
    assert store.outbox()[0]["message"] == original


def test_operator_resolution_is_compare_and_set_with_original_unknown_preserved(
    store, tmp_path
):
    run(store, tmp_path, Transport(body=b"wrong"))
    token = store.deliveries()[0]["attempt_id"]
    resolved = store.resolve_delivery(
        token,
        "not_delivered",
        "Local receiver confirmed the original POST was rejected",
    )
    assert (
        resolved["outcome"] == "unknown"
        and resolved["acknowledgement"] is None
        and resolved["resolution"] == "not_delivered"
    )
    with pytest.raises(ValueError, match="unresolved"):
        store.resolve_delivery(token, "delivered", "Stale operator input")
    result, _ = run(store, tmp_path, Transport())
    assert result["status"] == "sent" and [
        r["attempt"] for r in store.deliveries()
    ] == [1, 2]


def test_operator_delivered_is_labeled_as_assertion_without_fabricating_http_ack(
    store, tmp_path
):
    run(store, tmp_path, Transport(body=b"wrong"))
    token = store.deliveries()[0]["attempt_id"]
    row = store.resolve_delivery(
        token, "delivered", "Operator inspected the synthetic receiving journal"
    )
    assert (
        row["resolution"] == "delivered"
        and row["acknowledgement"] is None
        and row["outcome"] == "unknown"
    )
    assert store.outbox()[0]["status"] == "sent"
    assert (
        send_once(store, URL, transport=Transport(), output_root=tmp_path / "runs")
        is None
    )


def test_three_explicitly_resolved_unknowns_stop_delivery_retry(store, tmp_path):
    for attempt in range(1, 4):
        run(store, tmp_path, Transport(body=b"wrong"))
        row = store.deliveries()[-1]
        assert row["attempt"] == attempt
        store.resolve_delivery(
            row["attempt_id"],
            "not_delivered",
            f"Synthetic rejection confirmed for attempt {attempt}",
        )
    assert store.outbox()[0]["status"] == "delivery_failed"
    assert (
        send_once(store, URL, transport=Transport(), output_root=tmp_path / "runs")
        is None
    )


def test_service_order_holds_unknown_and_allows_other_services(tmp_path):
    store = EventStore(tmp_path / "events.db")
    add(store)
    add(store, 2)
    add(store, 1, "search")
    run(store, tmp_path, Transport(body=b"wrong"))
    result, _ = run(store, tmp_path, Transport())
    assert result["notification_id"] == 3
    assert [r["status"] for r in store.outbox()] == [
        "delivery_unknown",
        "pending",
        "sent",
    ]
    assert (
        send_once(store, URL, transport=Transport(), output_root=tmp_path / "runs")
        is None
    )
    store.resolve_delivery(
        store.deliveries()[0]["attempt_id"],
        "delivered",
        "Synthetic receiver confirms receipt",
    )
    result, _ = run(store, tmp_path, Transport())
    assert result["notification_id"] == 2 and result["status"] == "sent"


@pytest.mark.parametrize("dispatched", [False, True])
def test_expired_attempt_does_not_accept_stale_ack_and_only_undispatched_can_reclaim(
    tmp_path, dispatched
):
    clock = Clock()
    store = EventStore(tmp_path / "events.db", clock=clock)
    add(store)
    first = store.claim_notification(digest(URL), lease_seconds=10)
    token = first["attempt"]["attempt_id"]
    if dispatched:
        store.dispatch_notification(token)
    clock.value += 10
    with pytest.raises(ValueError, match="no longer current"):
        store.acknowledge_notification(token)
    second = store.claim_notification(digest(URL), lease_seconds=10)
    if dispatched:
        assert second is None and store.outbox()[0]["status"] == "delivery_unknown"
    else:
        assert (
            second["attempt"]["attempt"] == 2
            and store.deliveries()[0]["outcome"] == "not_dispatched"
        )
    with pytest.raises(ValueError, match="no longer current"):
        store.dispatch_notification(token)
    with pytest.raises(ValueError, match="no longer current"):
        store.acknowledge_notification(token)


def test_two_connections_claim_once(store):
    def claim(_):
        return EventStore(store.path).claim_notification(digest(URL))

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(claim, range(2)))
    assert sum(row is not None for row in results) == 1 and len(store.deliveries()) == 1


@pytest.mark.parametrize(
    "stage",
    [
        "prepared",
        "delivery_claimed",
        "dispatch_prepared",
        "dispatching",
        "ack_commit_prepared",
        "after_ack",
    ],
)
def test_recording_failure_prevents_later_effect_and_preserves_actual_delivery(
    store, tmp_path, stage
):
    failed = False

    def writer(path, data):
        nonlocal failed
        trigger = (
            data["status"] == stage
            if stage != "after_ack"
            else data["result"] is not None
        )
        if trigger and not failed:
            failed = True
            raise OSError("synthetic disk unavailable")
        path.write_text(json.dumps(data))

    transport = Transport()
    if stage == "prepared":
        with pytest.raises(Exception, match="recording_failed"):
            run(store, tmp_path, transport, writer=writer)
        assert (
            store.outbox()[0]["status"] == "pending"
            and store.deliveries() == []
            and transport.requests == []
        )
        return
    result, data = run(store, tmp_path, transport, writer=writer)
    assert result["run_status"] == "failed"
    expected = (
        "pending"
        if stage in {"delivery_claimed", "dispatch_prepared"}
        else "sent"
        if stage == "after_ack"
        else "delivery_unknown"
    )
    assert store.outbox()[0]["status"] == expected
    assert len(transport.requests) == (
        1 if stage in {"ack_commit_prepared", "after_ack"} else 0
    )
    if stage == "after_ack":
        assert store.deliveries()[0]["outcome"] == "acknowledged"


def test_actual_transport_failure_after_dispatch_has_sanitized_error_and_unknown_state(
    store, tmp_path
):
    transport = Transport(error=httpx2.ConnectError("Connection failed at " + URL))
    result, data = run(store, tmp_path, transport)
    assert result["status"] == "delivery_unknown" and transport.closed
    assert "synthetic-secret" not in json.dumps(data) and "ConnectError" in json.dumps(
        data
    )


def test_client_close_failure_cannot_release_acknowledgement(store, tmp_path):
    result, data = run(store, tmp_path, Transport(close_error=True))
    assert (
        result["status"] == "delivery_unknown"
        and result["run_status"] == "failed"
        and not data["client_closed"]
    )
    assert store.deliveries()[0]["acknowledgement"] is None


@pytest.mark.parametrize("cancel", [False, True])
def test_owned_request_cancellation_or_deadline_drains_before_unknown_result(
    store, tmp_path, cancel
):
    async def scenario():
        ready = asyncio.Event()
        cancelled = asyncio.Event()

        async def delay():
            ready.set()
            try:
                await asyncio.sleep(30)
            finally:
                cancelled.set()

        transport = Transport(delay=delay)
        task = asyncio.create_task(
            send_once_async(
                store,
                URL,
                transport=transport,
                output_root=tmp_path / "runs",
                timeout_seconds=10 if cancel else 0.25,
            )
        )
        await asyncio.wait_for(ready.wait(), 3)
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError) as error:
                await task
            data = json.loads(Path(error.value.receipt_path).read_text())
        else:
            result = await task
            assert result["status"] == "delivery_unknown"
            data = json.loads(Path(result["receipt_path"]).read_text())
        assert (
            cancelled.is_set()
            and transport.closed
            and data["client_closed"]
            and data["status"] == "failed"
        )
        before = store.deliveries()
        await asyncio.sleep(0.02)
        assert (
            store.deliveries() == before
            and store.outbox()[0]["status"] == "delivery_unknown"
        )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/secret",
        "https://user:pass@example.com/secret",
        "https://example.com/secret?token=bad",
        "https://example.com/secret#bad",
        "https://example.com:invalid/secret",
        "https://example.com/secret\n",
        "",
    ],
)
def test_invalid_target_before_claim_does_not_disclose_it(store, tmp_path, url):
    with pytest.raises(ValueError) as error:
        send_once(store, url, output_root=tmp_path / "runs")
    assert "secret" not in str(error.value) and "pass" not in str(error.value)
    assert store.outbox()[0]["status"] == "pending" and store.deliveries() == []


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE delivery_attempts SET payload_sha256=printf('%064d',0)",
        "UPDATE delivery_attempts SET target_sha256='invalid'",
        "UPDATE delivery_attempts SET outcome='acknowledged'",
        "UPDATE delivery_attempts SET acknowledgement='200_ok'",
        "UPDATE delivery_attempts SET resolution='delivered'",
        "UPDATE delivery_attempts SET lease_until=started_at",
        "UPDATE delivery_attempts SET attempt=3",
        "UPDATE delivery_attempts SET error_code='unbounded'",
        "UPDATE delivery_attempts SET dispatched_at=NULL",
        "UPDATE notification_sources SET message_sha256=printf('%064d',0)",
        "UPDATE notification_sources SET captured_status='sent',captured_sent_at=1000",
        "DELETE FROM delivery_attempts",
        "DELETE FROM notification_sources",
        "UPDATE outbox SET status='pending'",
    ],
)
def test_delivery_corruption_is_refused_without_repair(store, tmp_path, mutation):
    run(store, tmp_path, Transport(body=b"wrong"))
    with sqlite3.connect(store.path) as db:
        db.execute(mutation)
        db.commit()
    before = Path(store.path).read_bytes()
    for action in (
        store.outbox,
        store.deliveries,
        lambda: EventStore(store.path),
        lambda: store.claim_notification(digest(URL)),
    ):
        with pytest.raises(InvalidSource):
            action()
        assert Path(store.path).read_bytes() == before


def test_known_version_one_migration_preserves_existing_business_and_source_facts(
    store,
):
    with sqlite3.connect(store.path) as db:
        db.execute("DROP TABLE delivery_attempts")
        db.execute("DROP TABLE notification_sources")
        db.execute("PRAGMA user_version=1")
        original = {
            name: db.execute(f"SELECT * FROM {name}").fetchall()
            for name in (
                "events",
                "outbox",
                "service_state",
                "event_sources",
                "journal_metadata",
            )
        }
        db.commit()
    migrated = EventStore(store.path)
    with sqlite3.connect(store.path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        for name, rows in original.items():
            assert db.execute(f"SELECT * FROM {name}").fetchall() == rows
        assert (
            db.execute("SELECT provenance FROM notification_sources").fetchone()[0]
            == "legacy_snapshot"
        )
    assert migrated.deliveries() == []


def test_new_source_message_is_immutable_even_before_first_delivery(store):
    with sqlite3.connect(store.path) as db:
        db.execute(
            "UPDATE outbox SET message=REPLACE(message,'Cause: Unknown','Cause: Changed historical cause')"
        )
        db.commit()
    with pytest.raises(InvalidSource):
        store.outbox()


@pytest.mark.parametrize("lease", [True, 0, -1, float("nan"), float("inf"), 10**400])
def test_invalid_delivery_lease_cannot_claim(store, lease):
    with pytest.raises(ValueError):
        store.claim_notification(digest(URL), lease_seconds=lease)
    assert store.outbox()[0]["status"] == "pending" and store.deliveries() == []


def test_permanent_receipt_failure_retains_actual_effect_with_unknown_durable_receipt(
    store, tmp_path
):
    writes = 0

    def writer(path, data):
        nonlocal writes
        writes += 1
        if data["result"] is not None or data["status"] == "failed":
            raise OSError("synthetic permanent storage failure")
        path.write_text(json.dumps(data))

    result = send_once(
        store, URL, transport=Transport(), output_root=tmp_path / "runs", writer=writer
    )
    assert result["status"] == "sent" and result["run_status"] == "failed"
    assert result["receipt_durable_status"] == "unknown"
    assert store.deliveries()[0]["outcome"] == "acknowledged"
    saved = json.loads(Path(result["receipt_path"]).read_text())
    assert saved["status"] == "ack_commit_prepared" and saved["result"] is None
    assert writes > 1
