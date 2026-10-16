"""Hash-bound synthetic journals and exclusive private model-free workspaces."""

import json
import os
import re
import sqlite3
import stat
from importlib.resources import files
from pathlib import Path
from ._dataset_manifest import INDEX_SHA256
from .delivery_state import digest
from .evidence import now
from .source import LEGACY_DDL, strict_json
from .store import EventStore

DATASET = "synthetic-incident-cases"
VERSION = "1.0.0"


def data_root():
    installed = files("incident_responder").joinpath("sample_data")
    if installed.is_dir():
        return installed
    return Path(__file__).resolve().parents[2] / "data"


def read_data(name, maximum, root):
    if not re.fullmatch("[a-z0-9-]+[.]json", name):
        raise ValueError("invalid dataset filename")
    value = root.joinpath(name).read_bytes()
    if len(value) > maximum:
        raise ValueError("dataset exceeds supported bytes")
    return value


def index(*, root=None):
    root = root or data_root()
    raw = read_data("index.json", 65536, root)
    if digest(raw) != INDEX_SHA256:
        raise ValueError("dataset index hash differs from source authority")
    result = strict_json(raw)
    if (
        result.get("schema_version") != 1
        or result.get("dataset") != DATASET
        or result.get("version") != VERSION
        or not isinstance(result.get("cases"), list)
        or not 1 <= len(result["cases"]) <= 100
    ):
        raise ValueError("unsupported dataset index")
    return result


def load_case(identifier, *, root=None):
    root = root or data_root()
    manifest = index(root=root)
    entry = next(
        (row for row in manifest["cases"] if row["case_id"] == identifier), None
    )
    if entry is None:
        raise ValueError("unknown incident dataset case")
    raw = read_data(entry["file"], 4 * 1024 * 1024, root)
    if digest(raw) != entry["sha256"]:
        raise ValueError("dataset case hash differs from index")
    case = strict_json(raw)
    if (
        case.get("schema_version") != 1
        or case.get("dataset") != DATASET
        or case.get("version") != VERSION
        or case.get("case_id") != identifier
        or case.get("profile") != entry["profile"]
        or set(case.get("journal", {})) != {"events", "service_state", "outbox"}
    ):
        raise ValueError("unsupported incident dataset case")
    return case, entry


def list_cases(*, root=None):
    result = index(root=root)
    # Listing validates every source file, not only the requested subset.
    for entry in result["cases"]:
        load_case(entry["case_id"], root=root)
    return result


def write_private(path, raw):
    with path.open("xb") as file:
        os.chmod(path, 0o600)
        file.write(raw)
        file.flush()
        os.fsync(file.fileno())


def initialize_workspace(identifier, directory, *, root=None):
    case, entry = load_case(identifier, root=root)
    directory = Path(os.path.abspath(directory))
    directory.mkdir(mode=0o700)  # Exclusive; no existing path/file/symlink is replaced.
    database = directory / "incidents.db"
    write_private(database, b"")
    with sqlite3.connect(database) as db:
        for statement in LEGACY_DDL:
            db.execute(statement)
        for table, rows in case["journal"].items():
            for row in rows:
                columns = ",".join(row)
                slots = ",".join("?" for _ in row)
                # Columns come only from a source-hash-bound case, never caller SQL.
                db.execute(
                    f"INSERT INTO {table} ({columns}) VALUES ({slots})",
                    tuple(row.values()),
                )
        db.commit()
    store = EventStore(database, clock=lambda: case["clock"])
    metadata = {
        "schema_version": 1,
        "dataset": DATASET,
        "version": VERSION,
        "case_id": identifier,
        "case_sha256": entry["sha256"],
        "index_sha256": INDEX_SHA256,
        "database_instance": store._instance_id,
        "initialized_at": now(),
        "source_clock": case["clock"],
    }
    write_private(
        directory / "workspace.json", (json.dumps(metadata, indent=2) + "\n").encode()
    )
    write_private(
        directory / "TASK.md",
        (case["task"] + "\n\n" + case["provenance"] + "\n").encode(),
    )
    write_private(
        directory / "submissions.json",
        (json.dumps(case["submissions"], indent=2, ensure_ascii=False) + "\n").encode(),
    )
    return inspect_workspace(directory, root=root)


def inspect_workspace(directory, *, root=None):
    directory = Path(os.path.abspath(directory))
    path = directory / "workspace.json"
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
        raise ValueError("workspace identity requires a bounded regular file")
    metadata = strict_json(path.read_bytes())
    if (
        set(metadata)
        != {
            "schema_version",
            "dataset",
            "version",
            "case_id",
            "case_sha256",
            "index_sha256",
            "database_instance",
            "initialized_at",
            "source_clock",
        }
        or metadata["schema_version"] != 1
        or metadata["dataset"] != DATASET
        or metadata["version"] != VERSION
        or metadata["index_sha256"] != INDEX_SHA256
    ):
        raise ValueError("unsupported workspace identity")
    case, entry = load_case(metadata["case_id"], root=root)
    if (
        metadata["case_sha256"] != entry["sha256"]
        or metadata["source_clock"] != case["clock"]
    ):
        raise ValueError("workspace dataset identity changed")
    database = directory / "incidents.db"
    if not database.is_file():
        raise ValueError("workspace journal is missing")
    store = EventStore(database)
    if store._instance_id != metadata["database_instance"]:
        raise ValueError("workspace database ownership differs from initialization")
    with store._db() as db:
        counts = {
            table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("events", "service_state", "outbox", "delivery_attempts")
        }
    return {
        "directory": str(directory),
        "database": store.path,
        "identity": metadata,
        "counts": counts,
    }


def reset_workspace(directory, destination, expected_instance, *, root=None):
    original = inspect_workspace(directory, root=root)
    if expected_instance != original["identity"]["database_instance"]:
        raise ValueError("reset source instance is no longer current")
    # Reset materializes the original case into a fresh private destination, preserving all prior work/evidence.
    return initialize_workspace(original["identity"]["case_id"], destination, root=root)
