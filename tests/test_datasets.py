import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
import pytest
from incident_responder.datasets import (
    list_cases,
    load_case,
    initialize_workspace,
    inspect_workspace,
    reset_workspace,
    data_root,
)
from incident_responder.schema import ServiceEvent, IncidentAssessment
from incident_responder.store import EventStore
from incident_responder.graph import run_once, run_once_async
from test_native import Transport


def assess(event, context):
    return IncidentAssessment(
        severity="warning",
        summary="Controlled assessment of supplied source",
        suspected_cause="Unconfirmed",
        recommended_action="Inspect traces",
        evidence_event_ids=[event["event_id"]],
    )


@pytest.mark.parametrize(
    "identifier", [entry["case_id"] for entry in list_cases()["cases"]]
)
def test_every_case_initializes_private_and_preserves_original_imported_rows(
    tmp_path, identifier
):
    case, _ = load_case(identifier)
    initialized = initialize_workspace(identifier, tmp_path / identifier)
    directory = Path(initialized["directory"])
    store = EventStore(initialized["database"], clock=lambda: case["clock"])
    assert initialized["identity"]["case_id"] == identifier
    assert directory.stat().st_mode & 0o777 == 0o700
    for path in directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(store.path) as db:
        db.row_factory = sqlite3.Row
        for table, rows in case["journal"].items():
            assert [
                dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")
            ] == rows
        assert {
            row[0] for row in db.execute("SELECT provenance FROM event_sources")
        } == {"legacy_snapshot"}
        assert {
            row[0]
            for row in db.execute("SELECT original_payload_json FROM event_sources")
        } == {None}
    previous = store.outbox()
    for submission in case["submissions"]:
        store.submit(ServiceEvent.model_validate(submission))
    processed = []
    while True:
        result = run_once(store, assess, output_root=tmp_path / "runs")
        if result is None:
            break
        assert result["run_status"] == "completed"
        processed.append(result)
        assert len(processed) <= 12
    counts = {
        "checkout-degradation": 1,
        "delayed-recovery": 1,
        "semantic-replay": 1,
        "regional-busy": 12,
        "sparse-telemetry": 1,
        "hostile-log": 1,
        "recovery-before-degradation": 2,
        "stale-degradation": 1,
        "maximum-sequence": 1,
        "expired-worker": 1,
        "exhausted-failure": 0,
        "already-current": 0,
    }
    assert len(processed) == counts[identifier]
    assert store.outbox()[: len(previous)] == previous
    if identifier in {"recovery-before-degradation", "stale-degradation"}:
        assert (
            processed[-1]["status"] == "ignored_stale"
            and store.state("checkout-api")["health"] == "healthy"
        )
    if identifier == "exhausted-failure":
        with pytest.raises(ValueError):
            store.retry("checkout-api-002")
    if identifier == "expired-worker":
        assert processed[0]["attempt"] == 2
        with pytest.raises(ValueError, match="no longer current"):
            store.commit(
                "checkout-api-002",
                "123e4567-e89b-42d3-a456-426614174000",
                assess({"event_id": "checkout-api-002"}, {}),
            )
    assert inspect_workspace(directory)["identity"] == initialized["identity"]


def test_busy_history_has_coherent_dates_metrics_incidents_and_truthful_legacy_measurement(
    tmp_path,
):
    case, _ = load_case("regional-busy")
    journal = case["journal"]
    assert (
        len(journal["events"]) == 72
        and len(journal["outbox"]) == 48
        and len(journal["service_state"]) == 12
    )
    assert len({row["service"] for row in journal["events"]}) == 12
    assert all(
        row["status"] == "sent" and row["sent_at"] < case["clock"]
        for row in journal["outbox"]
    )
    assert all(row["status"] == "pending" for row in journal["events"][-12:])
    assert all(
        row["last_sequence"] == 5 and row["health"] == "healthy"
        for row in journal["service_state"]
    )
    assert all("model" not in row and "usage" not in row for row in journal["events"])
    for row in journal["events"]:
        details = json.loads(row["details_json"])
        assert details["request_count"] > 0 and 0 <= details["error_rate_pct"] <= 100


@pytest.mark.parametrize(
    "identifier",
    ["regional-busy", "hostile-log", "sparse-telemetry", "maximum-sequence"],
)
def test_actual_pinned_native_investigation_receives_only_case_business_context(
    tmp_path, identifier
):
    case, _ = load_case(identifier)
    initialized = initialize_workspace(identifier, tmp_path / identifier)
    store = EventStore(initialized["database"], clock=lambda: case["clock"])
    event = next(row for row in case["journal"]["events"] if row["status"] == "pending")
    from test_native import response

    body = response()
    body["content"][0]["input"]["evidence_event_ids"] = [event["event_id"]]
    transport = Transport(body=body)
    result = run_once(
        store,
        model="claude-synthetic",
        api_key="synthetic-key",
        base_url="http://127.0.0.1:19",
        transport=transport,
        output_root=tmp_path / "runs",
    )
    assert (
        result["status"] == "applied"
        and transport.closed
        and len(transport.requests) == 1
    )
    sent = json.dumps(transport.requests[0])
    assert (
        case["task"] not in sent
        and "case_sha256" not in sent
        and "database_instance" not in sent
    )
    assert "lease_token" not in sent and "legacy_snapshot" not in sent
    if identifier == "regional-busy":
        assert "edge-12" not in sent and "edge-01" in sent
    if identifier == "hostile-log":
        assert (
            "IGNORE SYSTEM" in sent
        )  # Source placement, not semantic obedience proof.
    saved = json.loads(Path(result["receipt_path"]).read_text())
    assert saved["context"]["recent_events"][0]["event_id"] == event["event_id"]


def test_keyfree_cli_init_readback_reset_preserves_existing_workspace_and_refuses_collisions(
    tmp_path, monkeypatch
):
    env = dict(os.environ)
    for key in (
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_MODEL",
        "SLACK_WEBHOOK_URL",
        "INCIDENT_WEBHOOK_SECRET",
    ):
        env.pop(key, None)

    def cli(*arguments):
        result = subprocess.run(
            [sys.executable, "-m", "incident_responder", *arguments],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result

    listed = cli("datasets")
    assert listed.returncode == 0, listed.stderr
    assert (
        len(json.loads(listed.stdout)["cases"]) == 12
        and not (tmp_path / "incidents.db").exists()
    )
    initialized = cli(
        "init", "--case", "regional-busy", "--directory", str(tmp_path / "workspace")
    )
    assert initialized.returncode == 0, initialized.stderr
    original = json.loads(initialized.stdout)
    directory = Path(original["directory"])
    before = {
        str(x.relative_to(directory)): x.read_bytes() for x in directory.iterdir()
    }
    collision = cli(
        "init", "--case", "checkout-degradation", "--directory", str(directory)
    )
    assert collision.returncode == 1
    inspected = cli("workspace", str(directory))
    assert inspected.returncode == 0
    assert json.loads(inspected.stdout)["identity"] == original["identity"]
    bad = cli(
        "reset",
        str(directory),
        "--to",
        str(tmp_path / "bad-reset"),
        "--instance",
        "wrong",
    )
    assert bad.returncode == 1 and not (tmp_path / "bad-reset").exists()
    reset = cli(
        "reset",
        str(directory),
        "--to",
        str(tmp_path / "fresh"),
        "--instance",
        original["identity"]["database_instance"],
    )
    assert reset.returncode == 0, reset.stderr
    fresh = json.loads(reset.stdout)
    assert (
        fresh["counts"] == original["counts"]
        and fresh["identity"]["database_instance"]
        != original["identity"]["database_instance"]
    )
    assert {
        str(x.relative_to(directory)): x.read_bytes() for x in directory.iterdir()
    } == before


@pytest.mark.parametrize("index_changed", [False, True])
def test_corrupt_dataset_is_rejected_before_creating_destination(
    tmp_path, index_changed
):
    root = tmp_path / "data"
    shutil.copytree(data_root(), root)
    target = root / ("index.json" if index_changed else "checkout-degradation.json")
    target.write_bytes(target.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash differs"):
        initialize_workspace("checkout-degradation", tmp_path / "uncreated", root=root)
    assert not (tmp_path / "uncreated").exists()
    with pytest.raises(ValueError, match="hash differs"):
        list_cases(root=root)


def test_workspace_identity_or_missing_database_is_refused_without_repair(tmp_path):
    initial = initialize_workspace("checkout-degradation", tmp_path / "workspace")
    directory = Path(initial["directory"])
    identity = directory / "workspace.json"
    original = identity.read_bytes()
    value = json.loads(original)
    value["case_sha256"] = "0" * 64
    identity.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="identity changed"):
        inspect_workspace(directory)
    identity.write_bytes(original)
    database = Path(initial["database"])
    database.unlink()
    with pytest.raises(ValueError, match="missing"):
        inspect_workspace(directory)
    assert not database.exists()


def test_case_regeneration_matches_pinned_manifest_without_git(tmp_path):
    root = Path(__file__).resolve().parents[1]
    if not (root / "scripts/build_datasets.py").is_file():
        pytest.skip("source-only regeneration check")
    result = subprocess.run(
        [sys.executable, str(root / "scripts/build_datasets.py"), "--check"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
