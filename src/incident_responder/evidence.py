"""Private prepared/final attempt evidence, independent of the business journal."""

from __future__ import annotations
import hashlib
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat()


def sha(value):
    return hashlib.sha256(
        value
        if isinstance(value, bytes)
        else json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


class Receipt:
    def __init__(self, store, output_root, timeout_seconds, writer=None):
        self.deadline = time.monotonic() + timeout_seconds
        self.recording_error = None
        self.writer = writer
        directory = (
            Path(output_root)
            if output_root is not None
            else Path(store.path).resolve().parent / ".runs"
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.run_id = str(uuid.uuid4())
        directory = directory / self.run_id
        directory.mkdir(mode=0o700)
        self.path = directory / "trace.json"
        self.data = {
            "schema_version": 1,
            "run_id": self.run_id,
            "started_at": now(),
            "status": "prepared",
            "database": str(Path(store.path).resolve()),
            "event": None,
            "context": None,
            "native": [],
            "sdk_message": None,
            "assessment": None,
            "result": None,
            "error": None,
            "cost_usd": None,
        }

    def guard(self):
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        if task is not None and task.cancelling():
            raise asyncio.CancelledError("worker cancellation requested")
        if self.recording_error is not None:
            raise self.recording_error
        if time.monotonic() >= self.deadline:
            raise TimeoutError("worker operation deadline exceeded")

    def save(self, *, final=False):
        if not final and self.recording_error is not None:
            raise self.recording_error
        staged = self.path.with_name(f".{uuid.uuid4()}.tmp")
        try:
            value = json.loads(json.dumps(self.data, allow_nan=False))
            if self.writer is not None:
                self.writer(self.path, value)
                return
            raw = (
                json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
            ).encode()
            with staged.open("xb") as file:
                os.chmod(staged, 0o600)
                file.write(raw)
                file.flush()
                os.fsync(file.fileno())
            os.replace(staged, self.path)
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except BaseException as error:
            self.recording_error = error
            raise
        finally:
            staged.unlink(missing_ok=True)

    def finish(self, result, error):
        self.data.update(
            status="failed" if error else "finished",
            finished_at=now(),
            result=result,
            error=None
            if error is None
            else {"name": type(error).__name__, "message": str(error)},
        )
        if self.recording_error:
            self.data["recording_error"] = {
                "name": type(self.recording_error).__name__,
                "message": str(self.recording_error),
            }
            self.data["status"] = "failed"
        self.save(final=True)
