import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import pytest
from incident_responder.schema import ServiceEvent
from incident_responder.store import EventStore


@pytest.mark.parametrize(
    "stop,code,status", [("tool_use", 0, "applied"), ("max_tokens", 1, "failed")]
)
def test_normal_cli_real_sdk_http_preserves_success_or_failure(
    tmp_path, stop, code, status
):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            message = {
                "id": "msg_controlled",
                "type": "message",
                "role": "assistant",
                "model": "claude-reported",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool_controlled",
                        "name": body["tools"][0]["name"],
                        "input": {
                            "severity": "critical",
                            "summary": "Controlled degradation",
                            "suspected_cause": "Unknown",
                            "recommended_action": "Inspect traces",
                            "evidence_event_ids": ["evt-1"],
                        },
                    }
                ],
                "stop_reason": stop,
                "stop_sequence": None,
                "usage": {"input_tokens": 11, "output_tokens": 5},
            }
            raw = json.dumps(message).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    database = tmp_path / "events.db"
    store = EventStore(database)
    store.submit(
        ServiceEvent(
            event_id="evt-1",
            service="checkout-api",
            sequence=1,
            kind="degraded",
            observed_at="2026-10-15T12:00:00Z",
        )
    )
    env = {
        **os.environ,
        "ANTHROPIC_API_KEY": "synthetic-cli-key",
        "ANTHROPIC_MODEL": "claude-synthetic",
        "LANGSMITH_TRACING": "false",
        "LANGCHAIN_TRACING_V2": "false",
    }
    for key in ("LANGSMITH_API_KEY", "LANGCHAIN_API_KEY"):
        env.pop(key, None)
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "incident_responder",
                "--db",
                str(database),
                "work",
                "--once",
                "--provider-base-url",
                f"http://127.0.0.1:{server.server_port}",
                "--output-root",
                str(tmp_path / "runs"),
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
    assert result.returncode == code, result.stderr
    output = json.loads(result.stdout)
    assert output["status"] == status
    assert len(requests) == 1
    data = json.loads(Path(output["receipt_path"]).read_text())
    assert (
        data["native_clients_closed"] is True
        and data["native"][0]["response"]["stop_reason"] == stop
    )
    assert "synthetic-cli-key" not in json.dumps(data)
    assert len(store.outbox()) == (1 if code == 0 else 0)


@pytest.mark.parametrize("name", ["SIGINT", "SIGTERM", "SIGHUP"])
def test_normal_cli_signal_closes_native_work_and_retains_failure(tmp_path, name):
    import signal

    if not hasattr(signal, name):
        pytest.skip("POSIX signal required")
    ready = threading.Event()
    release = threading.Event()
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            )
            ready.set()
            release.wait(10)
            try:
                self.send_response(503)
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    database = tmp_path / "events.db"
    store = EventStore(database)
    store.submit(
        ServiceEvent(
            event_id="evt-1",
            service="checkout-api",
            sequence=1,
            kind="degraded",
            observed_at="2026-10-15T12:00:00Z",
        )
    )
    env = {
        **os.environ,
        "ANTHROPIC_API_KEY": "synthetic-cli-key",
        "ANTHROPIC_MODEL": "claude-synthetic",
        "LANGSMITH_TRACING": "false",
        "LANGCHAIN_TRACING_V2": "false",
    }
    for key in ("LANGSMITH_API_KEY", "LANGCHAIN_API_KEY"):
        env.pop(key, None)
    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "incident_responder",
            "--db",
            str(database),
            "work",
            "--once",
            "--provider-base-url",
            f"http://127.0.0.1:{server.server_port}",
            "--output-root",
            str(tmp_path / "runs"),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert ready.wait(8), "normal worker did not dispatch its native request"
        child.send_signal(getattr(signal, name))
        stdout, stderr = child.communicate(timeout=8)
        assert child.returncode == 128 + int(getattr(signal, name)), stderr
        output = json.loads(stdout)
        assert output["status"] == "interrupted" and output["signal"] == name
        data = json.loads(Path(output["receipt_path"]).read_text())
        assert data["status"] == "failed" and data["native_clients_closed"] is True
        assert (
            len(requests) == 1
            and store.outbox() == []
            and store.state("checkout-api") is None
        )
        assert store.get_event("evt-1")["status"] == "failed"
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=2)
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(2)
