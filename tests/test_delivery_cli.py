"""Normal send CLI over owned local HTTP, including real process ownership."""

import json
import os
import signal
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import pytest
from test_delivery import add
from incident_responder.store import EventStore


@contextmanager
def receiver(body=b"ok", status=200, block=False):
    ready = threading.Event()
    release = threading.Event()
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(
                {
                    "path": self.path,
                    "body": json.loads(
                        self.rfile.read(int(self.headers["Content-Length"]))
                    ),
                    "headers": dict(self.headers),
                }
            )
            ready.set()
            if block:
                release.wait(15)
            try:
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                if status == 302:
                    self.send_header(
                        "Location",
                        f"http://127.0.0.1:{self.server.server_port}/redirect-target",
                    )
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "url": f"http://127.0.0.1:{server.server_port}/services/synthetic-secret",
            "ready": ready,
            "release": release,
            "requests": requests,
        }
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(2)


def command(tmp_path, store):
    return [
        sys.executable,
        "-m",
        "incident_responder",
        "--db",
        store.path,
        "send",
        "--confirm",
        "--once",
        "--output-root",
        str(tmp_path / "runs"),
    ]


def environment(server):
    return {
        **os.environ,
        "SLACK_WEBHOOK_URL": server["url"],
        "HTTP_PROXY": "http://127.0.0.1:1",
        "HTTPS_PROXY": "http://127.0.0.1:1",
        "NO_PROXY": "",
    }


@pytest.mark.parametrize(
    "status,body,code,expected",
    [
        (200, b"ok", 0, "sent"),
        (200, b"wrong", 1, "delivery_unknown"),
        (302, b"ok", 1, "delivery_unknown"),
    ],
)
def test_cli_original_http_ack_status_and_no_redirect_or_proxy(
    tmp_path, status, body, code, expected
):
    store = EventStore(tmp_path / "events.db")
    add(store)
    with receiver(body, status) as server:
        child = subprocess.run(
            command(tmp_path, store),
            env=environment(server),
            text=True,
            capture_output=True,
            timeout=15,
        )
        assert child.returncode == code, child.stderr
        result = json.loads(child.stdout)
        assert result["status"] == expected and len(server["requests"]) == 1
        assert "synthetic-secret" not in child.stdout + child.stderr
        assert store.outbox()[0]["status"] == expected
        receipt = json.loads(Path(result["receipt_path"]).read_text())
        assert receipt["response"]["status"] == status and receipt["client_closed"]


def test_concurrent_normal_cli_processes_send_only_one_post(tmp_path):
    store = EventStore(tmp_path / "events.db")
    add(store)
    with receiver(block=True) as server:
        first = subprocess.Popen(
            command(tmp_path, store),
            env=environment(server),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert server["ready"].wait(8)
            second = subprocess.run(
                command(tmp_path, store),
                env=environment(server),
                text=True,
                capture_output=True,
                timeout=8,
            )
            assert second.returncode == 0 and json.loads(second.stdout) is None, (
                second.stderr
            )
            assert (
                len(server["requests"]) == 1
                and store.outbox()[0]["status"] == "sending"
            )
            server["release"].set()
            stdout, stderr = first.communicate(timeout=8)
            assert first.returncode == 0 and json.loads(stdout)["status"] == "sent", (
                stderr
            )
            assert len(server["requests"]) == 1 and len(store.deliveries()) == 1
        finally:
            if first.poll() is None:
                first.kill()
                first.communicate(timeout=2)


@pytest.mark.parametrize("name", ["SIGINT", "SIGTERM", "SIGHUP"])
def test_normal_cli_signal_drains_delivery_and_publishes_unknown_without_resend(
    tmp_path, name
):
    store = EventStore(tmp_path / "events.db")
    add(store)
    with receiver(block=True) as server:
        child = subprocess.Popen(
            command(tmp_path, store),
            env=environment(server),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert server["ready"].wait(8)
            child.send_signal(getattr(signal, name))
            stdout, stderr = child.communicate(timeout=8)
            assert child.returncode == 128 + int(getattr(signal, name)), stderr
            result = json.loads(stdout)
            assert result["status"] == "interrupted" and result["signal"] == name
            data = json.loads(Path(result["receipt_path"]).read_text())
            assert data["status"] == "failed" and data["client_closed"]
            assert (
                store.outbox()[0]["status"] == "delivery_unknown"
                and len(server["requests"]) == 1
            )
            server["release"].set()
            retry = subprocess.run(
                command(tmp_path, store),
                env=environment(server),
                text=True,
                capture_output=True,
                timeout=8,
            )
            assert (
                retry.returncode == 0
                and json.loads(retry.stdout) is None
                and len(server["requests"]) == 1
            )
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=2)


def test_cli_operator_resolution_requires_note_and_does_not_send(tmp_path):
    store = EventStore(tmp_path / "events.db")
    add(store)
    with receiver(b"wrong") as server:
        result = subprocess.run(
            command(tmp_path, store),
            env=environment(server),
            text=True,
            capture_output=True,
            timeout=15,
        )
        token = json.loads(result.stdout)["attempt_id"]
        base = [
            sys.executable,
            "-m",
            "incident_responder",
            "--db",
            store.path,
            "resolve-delivery",
            token,
            "--decision",
            "delivered",
        ]
        missing = subprocess.run(base, text=True, capture_output=True, timeout=8)
        assert (
            missing.returncode == 2
            and store.outbox()[0]["status"] == "delivery_unknown"
        )
        resolved = subprocess.run(
            base
            + ["--note", "Operator inspected the local synthetic receiver journal"],
            text=True,
            capture_output=True,
            timeout=8,
        )
        assert resolved.returncode == 0, resolved.stderr
        row = json.loads(resolved.stdout)
        assert (
            row["outcome"] == "unknown"
            and row["resolution"] == "delivered"
            and row["acknowledgement"] is None
        )
        assert len(server["requests"]) == 1 and store.outbox()[0]["status"] == "sent"


def test_killed_sender_expires_to_unknown_without_a_second_post(tmp_path):
    import time
    from incident_responder.delivery import send_once

    store = EventStore(tmp_path / "events.db")
    add(store)
    with receiver(block=True) as server:
        child = subprocess.Popen(
            command(tmp_path, store),
            env=environment(server),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert server["ready"].wait(8)
            child.kill()
            child.communicate(timeout=5)
            assert store.outbox()[0]["status"] == "sending"
            recovered = EventStore(store.path, clock=lambda: time.time() + 300)
            assert (
                send_once(
                    recovered, server["url"], output_root=tmp_path / "recovery-runs"
                )
                is None
            )
            assert recovered.outbox()[0]["status"] == "delivery_unknown"
            assert (
                recovered.deliveries()[0]["error_code"] == "lease_expired"
                and len(server["requests"]) == 1
            )
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=2)
