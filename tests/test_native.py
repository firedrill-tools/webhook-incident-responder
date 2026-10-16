import asyncio
import json
from pathlib import Path
import httpx2
import pytest
from incident_responder.graph import run_once, run_once_async
from incident_responder.schema import ServiceEvent
from incident_responder.store import EventStore


def event():
    return ServiceEvent(
        event_id="evt-1",
        service="checkout-api",
        sequence=1,
        kind="degraded",
        observed_at="2026-10-15T12:00:00Z",
        details={"error_rate_pct": 12.4},
    )


def response(stop="tool_use", usage=None):
    return {
        "id": "msg_synthetic",
        "type": "message",
        "role": "assistant",
        "model": "claude-controlled-observation",
        "content": [
            {
                "type": "tool_use",
                "id": "tool_1",
                "name": "IncidentAssessment",
                "input": {
                    "severity": "critical",
                    "summary": "Errors increased",
                    "suspected_cause": "Unknown; inspect upstream",
                    "recommended_action": "Inspect traces",
                    "evidence_event_ids": ["evt-1"],
                },
            }
        ],
        "stop_reason": stop,
        "stop_sequence": None,
        "usage": usage
        if usage is not None
        else {"input_tokens": 11, "output_tokens": 5},
    }


class Transport(httpx2.AsyncBaseTransport):
    def __init__(self, body=None, status=200, delay=None):
        self.body = body or response()
        self.status = status
        self.requests = []
        self.closed = False
        self.delay = delay

    async def handle_async_request(self, request):
        self.requests.append(json.loads(await request.aread()))
        if self.delay is not None:
            await self.delay()
        return httpx2.Response(
            self.status, json=self.body, headers={"request-id": "req-controlled"}
        )

    async def aclose(self):
        self.closed = True


def run(tmp_path, transport, **options):
    store = EventStore(tmp_path / "events.db")
    store.submit(event())
    result = run_once(
        store,
        model="claude-synthetic",
        api_key="synthetic-not-a-real-key",
        base_url="http://127.0.0.1:19",
        transport=transport,
        output_root=tmp_path / "runs",
        **options,
    )
    data = json.loads(Path(result["receipt_path"]).read_text())
    return store, result, data


def test_actual_native_langgraph_assessment_commits_only_after_original_completion(
    tmp_path,
):
    transport = Transport()
    store, result, data = run(tmp_path, transport)
    assert result["status"] == "applied" and result["run_status"] == "completed"
    assert (
        len(store.outbox()) == 1 and len(transport.requests) == 1 and transport.closed
    )
    assert data["native_clients_closed"] is True
    raw = data["native"][0]
    assert raw["status"] == "accepted" and raw["response"]["stop_reason"] == "tool_use"
    assert raw["reported_usage"]["fields"]["input_tokens"] == {
        "state": "observed",
        "value": 11,
    }
    assert (
        raw["reported_usage"]["fields"]["cache_creation_input_tokens"]["state"]
        == "unknown"
    )
    assert data["sdk_message"]["usage_metadata"]["input_tokens"] == 11
    assert raw["response_model"] == "claude-controlled-observation"
    assert data["requested_model"] == "claude-synthetic"
    assert data["cost_usd"] is None
    assert "synthetic-not-a-real-key" not in json.dumps(data)
    content = transport.requests[0]["messages"][0]["content"]
    text = content if isinstance(content, str) else content[0]["text"]
    supplied = json.loads(text)
    assert supplied["event"]["event_id"] == "evt-1"
    assert "lease_token" not in supplied["event"]
    assert Path(result["receipt_path"]).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "mode",
    [
        "max_tokens",
        "end_turn",
        "pause_turn",
        "refusal",
        "wrong-tool",
        "two-tools",
        "pending-tool",
        "unknown-content",
        "empty-content",
    ],
)
def test_actual_native_unfinished_or_inconsistent_assessment_cannot_commit(
    tmp_path, mode
):
    body = response(
        stop=mode
        if mode in {"max_tokens", "end_turn", "pause_turn", "refusal"}
        else "tool_use"
    )
    if mode == "wrong-tool":
        body["content"][0]["name"] = "send_slack"
    if mode == "two-tools":
        body["content"].append({**body["content"][0], "id": "tool_2"})
    if mode == "pending-tool":
        body["stop_reason"] = None
    if mode == "unknown-content":
        body["content"].append({"type": "refusal", "text": "No"})
    if mode == "empty-content":
        body["content"] = []
    transport = Transport(body)
    store, result, data = run(tmp_path, transport)
    assert result["status"] == "failed" and result["run_status"] == "failed"
    assert store.state("checkout-api") is None and store.outbox() == []
    assert len(transport.requests) == 1 and transport.closed
    assert data["native_clients_closed"] is True
    assert data["native"][0]["response"] == body
    assert data["native"][0]["status"] == "rejected"
    assert data["native"][0]["reported_usage"]["fields"]["input_tokens"]["value"] == 11


@pytest.mark.parametrize(
    "usage",
    [
        {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
        },
        {"input_tokens": 11, "output_tokens": 5},
        {"input_tokens": -1, "output_tokens": "bad"},
    ],
)
def test_original_provider_usage_is_separate_from_sdk_defaults_even_on_failure(
    tmp_path, usage
):
    transport = Transport(response(stop="max_tokens", usage=usage))
    store, result, data = run(tmp_path, transport)
    fields = data["native"][0]["reported_usage"]["fields"]
    assert fields["input_tokens"]["state"] == (
        "invalid" if usage["input_tokens"] == -1 else "observed"
    )
    assert fields["cache_creation_input_tokens"]["state"] == (
        "observed" if "cache_creation_input_tokens" in usage else "unknown"
    )
    assert data["native"][0]["response"]["usage"] == usage and data["cost_usd"] is None
    assert store.outbox() == [] and transport.closed


def test_native_http_failure_retains_original_error_and_does_not_retry(tmp_path):
    body = {
        "type": "error",
        "error": {"type": "overloaded_error", "message": "Controlled outage"},
        "usage": {"input_tokens": 7},
    }
    transport = Transport(body, status=503)
    store, result, data = run(tmp_path, transport)
    assert result["status"] == "failed" and len(transport.requests) == 1
    assert transport.closed and store.outbox() == []
    assert data["native"][0]["response"] == body
    assert data["native"][0]["http_status"] == 503
    assert data["native"][0]["reported_usage"]["fields"]["input_tokens"]["value"] == 7


def test_native_deadline_cancels_owned_transport_before_return_and_cannot_commit_late(
    tmp_path,
):
    async def exercise():
        cancelled = asyncio.Event()

        async def hold():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        transport = Transport(delay=hold)
        store = EventStore(tmp_path / "events.db")
        store.submit(event())
        result = await run_once_async(
            store,
            model="claude-synthetic",
            api_key="synthetic-key",
            base_url="http://127.0.0.1:19",
            transport=transport,
            output_root=tmp_path / "runs",
            timeout_seconds=0.05,
        )
        assert result["status"] == "failed" and result["run_status"] == "failed"
        assert cancelled.is_set() and transport.closed and store.outbox() == []
        data = json.loads(Path(result["receipt_path"]).read_text())
        assert data["native_clients_closed"] is True
        before = Path(result["receipt_path"]).read_bytes()
        await asyncio.sleep(0.02)
        assert (
            Path(result["receipt_path"]).read_bytes() == before and store.outbox() == []
        )

    asyncio.run(exercise())


def test_native_source_becomes_stale_before_commit_without_releasing_an_alert(tmp_path):
    store = EventStore(tmp_path / "events.db")
    store.submit(event())

    class Racing(Transport):
        async def handle_async_request(self, request):
            store.submit(
                ServiceEvent(
                    event_id="evt-2",
                    service="checkout-api",
                    sequence=2,
                    kind="recovered",
                    observed_at="2026-10-15T12:01:00Z",
                )
            )
            newer = store.claim()
            store.commit(newer["event_id"], newer["lease_token"], None)
            return await super().handle_async_request(request)

    transport = Racing()
    result = run_once(
        store,
        model="claude-synthetic",
        api_key="synthetic-key",
        base_url="http://127.0.0.1:19",
        transport=transport,
        output_root=tmp_path / "runs",
    )
    assert result["status"] == "ignored_stale" and result["run_status"] == "completed"
    assert store.state("checkout-api")["health"] == "healthy"
    assert store.outbox() == [] and transport.closed


def test_recording_failure_after_model_completion_blocks_commit(tmp_path):
    store = EventStore(tmp_path / "events.db")
    store.submit(event())
    transport = Transport()
    tripped = [False]

    def writer(path, data):
        if not tripped[0] and data.get("status") == "commit_prepared":
            tripped[0] = True
            raise OSError("Controlled recording fault")
        path.write_text(json.dumps(data))
        path.chmod(0o600)

    result = run_once(
        store,
        model="claude-synthetic",
        api_key="synthetic-key",
        base_url="http://127.0.0.1:19",
        transport=transport,
        output_root=tmp_path / "runs",
        writer=writer,
    )
    assert result["status"] == "failed" and result["run_status"] == "failed"
    assert (
        store.outbox() == []
        and store.state("checkout-api") is None
        and transport.closed
    )
    data = json.loads(Path(result["receipt_path"]).read_text())
    assert data["status"] == "failed" and data["native"][0]["status"] == "accepted"


def test_persistent_initial_recording_fault_leaves_event_unclaimed(tmp_path):
    store = EventStore(tmp_path / "events.db")
    store.submit(event())
    transport = Transport()

    def broken(*_):
        raise OSError("Persistent recording fault")

    with pytest.raises(OSError) as caught:
        run_once(
            store,
            model="claude-synthetic",
            api_key="synthetic-key",
            base_url="http://127.0.0.1:19",
            transport=transport,
            output_root=tmp_path / "runs",
            writer=broken,
        )
    assert (
        caught.value.evidence["status"] == "failed"
        and caught.value.evidence["durable_status"] == "unknown"
    )
    assert (
        store.get_event("evt-1")["status"] == "pending"
        and store.get_event("evt-1")["attempt"] == 0
    )
    assert transport.requests == [] and store.outbox() == []


def test_owned_cleanup_failure_cannot_release_a_valid_assessment(tmp_path):
    class BrokenClose(Transport):
        async def aclose(self):
            raise RuntimeError("Controlled transport close fault")

    transport = BrokenClose()
    store, result, data = run(tmp_path, transport)
    assert result["status"] == "failed" and store.outbox() == []
    assert data["native"][0]["status"] == "accepted"
    assert data["native_clients_closed"] is False
    assert data["native_cleanup_error"]["name"] == "RuntimeError"


def test_explicit_native_key_overrides_credential_environment_and_auth_headers(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "synthetic-other-token")
    monkeypatch.setenv(
        "ANTHROPIC_CUSTOM_HEADERS",
        "authorization: Bearer synthetic-other-header\nx-api-key: synthetic-other-key",
    )

    class HeaderProbe(Transport):
        async def handle_async_request(self, request):
            assert request.headers["x-api-key"] == "synthetic-not-a-real-key"
            assert "authorization" not in request.headers
            return await super().handle_async_request(request)

    store, result, data = run(tmp_path, HeaderProbe())
    assert result["status"] == "applied" and len(store.outbox()) == 1
    assert "synthetic-other-" not in json.dumps(data)


def test_parent_cancellation_drains_native_work_and_publishes_a_failed_receipt(
    tmp_path,
):
    async def exercise():
        ready = asyncio.Event()

        async def hold():
            ready.set()
            await asyncio.Event().wait()

        transport = Transport(delay=hold)
        store = EventStore(tmp_path / "events.db")
        store.submit(event())
        task = asyncio.create_task(
            run_once_async(
                store,
                model="claude-synthetic",
                api_key="synthetic-key",
                base_url="http://127.0.0.1:19",
                transport=transport,
                output_root=tmp_path / "runs",
            )
        )
        await asyncio.wait_for(ready.wait(), 2)
        task.cancel("parent interrupted")
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        assert transport.closed and store.get_event("evt-1")["status"] == "failed"
        receipt = Path(caught.value.receipt_path)
        data = json.loads(receipt.read_text())
        assert data["status"] == "failed" and data["native_clients_closed"] is True
        assert data["native"][0]["status"] == "failed"
        assert store.outbox() == []

    asyncio.run(exercise())


def test_final_recording_failure_preserves_committed_effect_and_cannot_report_success(
    tmp_path,
):
    store = EventStore(tmp_path / "events.db")
    store.submit(event())
    transport = Transport()
    tripped = [False]

    def writer(path, data):
        if not tripped[0] and data.get("status") == "finished":
            tripped[0] = True
            raise OSError("Controlled final receipt fault")
        path.write_text(json.dumps(data))
        path.chmod(0o600)

    with pytest.raises(OSError) as caught:
        run_once(
            store,
            model="claude-synthetic",
            api_key="synthetic-key",
            base_url="http://127.0.0.1:19",
            transport=transport,
            output_root=tmp_path / "runs",
            writer=writer,
        )
    assert store.get_event("evt-1")["status"] == "applied" and len(store.outbox()) == 1
    assert caught.value.evidence["result"]["status"] == "applied"
    assert json.loads(Path(caught.value.receipt_path).read_text())["status"] == "failed"
    assert transport.closed
    assert (
        run_once(
            store,
            lambda *_: pytest.fail("applied work must not be retried"),
            output_root=tmp_path / "later",
        )
        is None
    )
    assert len(store.outbox()) == 1


def test_native_assessment_cannot_cite_journal_events_omitted_from_its_context(
    tmp_path,
):
    store = EventStore(tmp_path / "events.db")
    for sequence in (7, 1, 2, 3, 4, 5, 6):
        store.submit(
            ServiceEvent(
                event_id=f"evt-{sequence}",
                service="checkout-api",
                sequence=sequence,
                kind="degraded",
                observed_at="2026-10-15T12:00:00Z",
            )
        )
    body = response()
    body["content"][0]["input"]["evidence_event_ids"] = ["evt-7", "evt-1"]
    transport = Transport(body)
    result = run_once(
        store,
        model="claude-synthetic",
        api_key="synthetic-key",
        base_url="http://127.0.0.1:19",
        transport=transport,
        output_root=tmp_path / "runs",
    )
    assert result["status"] == "failed" and store.outbox() == []
    data = json.loads(Path(result["receipt_path"]).read_text())
    assert [item["event_id"] for item in data["context"]["recent_events"]] == [
        "evt-7",
        "evt-6",
        "evt-5",
        "evt-4",
        "evt-3",
    ]
    assert data["native"][0]["status"] == "accepted"
    assert "outside supplied context" in result["error"]
