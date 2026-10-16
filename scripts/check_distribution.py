"""Fresh locked wheel/sdist/complete-source installs and real CLI outside checkout."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path
from zipfile import ZipFile
from package_source import build

ROOT = Path(__file__).resolve().parents[1]
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
UV = os.environ.get("UV") or shutil.which("uv")
if not UV:
    raise RuntimeError("uv is required")
ENV = {**os.environ, "LANGSMITH_TRACING": "false", "LANGCHAIN_TRACING_V2": "false"}
for key in (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_MODEL",
    "SLACK_WEBHOOK_URL",
    "INCIDENT_WEBHOOK_SECRET",
    "VIRTUAL_ENV",
    "PYTHONPATH",
    "PYTHONHOME",
    "LANGSMITH_API_KEY",
    "LANGCHAIN_API_KEY",
):
    ENV.pop(key, None)


def run(command, cwd, check=True):
    result = subprocess.run(
        [str(x) for x in command],
        cwd=cwd,
        env=ENV,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed: {command[0]}\n{result.stdout[-10000:]}\n{result.stderr[-10000:]}"
        )
    return result


PROGRAM = r"""
import json, os, subprocess, sys, threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import incident_responder
from incident_responder.datasets import list_cases,initialize_workspace,inspect_workspace,reset_workspace
from incident_responder.store import EventStore
base=Path(sys.argv[1]);assert Path(incident_responder.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
assert len(list_cases()['cases'])==12
for case in list_cases()['cases']:
 result=initialize_workspace(case['case_id'],base/case['case_id'])
 assert result['identity']['case_sha256']==case['sha256'] and Path(result['database']).stat().st_mode&0o777==0o600
original=inspect_workspace(base/'regional-busy');assert original['counts']=={'events':72,'service_state':12,'outbox':48,'delivery_attempts':0}
fresh=reset_workspace(base/'regional-busy',base/'fresh',original['identity']['database_instance']);assert fresh['identity']['database_instance']!=original['identity']['database_instance']
requests=[]
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_POST(self):
  body=self.rfile.read(int(self.headers['Content-Length']));requests.append((self.path,json.loads(body)))
  if self.path=='/services/synthetic-secret': raw=b'ok'
  else:
   raw=json.dumps({'id':'msg_dist','type':'message','role':'assistant','model':'claude-controlled-dist','content':[{'type':'tool_use','id':'tool_dist','name':'IncidentAssessment','input':{'severity':'warning','summary':'Controlled installed investigation','suspected_cause':'Unconfirmed','recommended_action':'Inspect traces','evidence_event_ids':['checkout-api-002']}}],'stop_reason':'tool_use','stop_sequence':None,'usage':{'input_tokens':11,'output_tokens':5}}).encode()
  self.send_response(200);self.send_header('Content-Type','application/json' if self.path!='/services/synthetic-secret' else 'text/plain');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
cli=[sys.executable,'-I','-m','incident_responder','--db',str(base/'checkout-degradation/incidents.db')]
env={**os.environ,'ANTHROPIC_API_KEY':'synthetic-dist-key','ANTHROPIC_MODEL':'claude-synthetic-dist','SLACK_WEBHOOK_URL':f'http://127.0.0.1:{server.server_port}/services/synthetic-secret'}
try:
 worked=subprocess.run([*cli,'work','--once','--provider-base-url',f'http://127.0.0.1:{server.server_port}','--output-root',str(base/'runs')],env=env,capture_output=True,text=True,timeout=20)
 assert worked.returncode==0,worked.stderr
 result=json.loads(worked.stdout);assert result['status']=='applied'
 trace=json.loads(Path(result['receipt_path']).read_text());assert trace['native_clients_closed'] and trace['native'][0]['response_model']=='claude-controlled-dist'
 sent=subprocess.run([*cli,'send','--confirm','--once','--output-root',str(base/'runs')],env=env,capture_output=True,text=True,timeout=15)
 assert sent.returncode==0,sent.stderr
 result=json.loads(sent.stdout);assert result['status']=='sent' and 'synthetic-secret' not in sent.stdout+sent.stderr
 assert len(requests)==2
 assert EventStore(base/'checkout-degradation/incidents.db').deliveries()[0]['outcome']=='acknowledged'
finally:server.shutdown();server.server_close();thread.join(2)
print('installed twelve cases, reset, original native HTTP and acknowledged controlled delivery passed')
"""


def exercise(python, base):
    base.mkdir()
    run([python.parent / "incident-responder", "--help"], base)
    listed = json.loads(
        run([python, "-I", "-m", "incident_responder", "datasets"], base).stdout
    )
    assert len(listed["cases"]) == 12
    run([python, "-I", "-c", PROGRAM, base], base)
    preflight = run(
        [
            python,
            "-I",
            "-m",
            "incident_responder",
            "--db",
            base / "sparse-telemetry/incidents.db",
            "work",
            "--once",
            "--model",
            "claude-synthetic-dist",
            "--output-root",
            base / "missing-key-runs",
        ],
        base,
        False,
    )
    assert preflight.returncode == 1
    failed = json.loads(preflight.stdout)
    assert failed["status"] == "failed" and failed["run_status"] == "failed"
    receipt = json.loads(Path(failed["receipt_path"]).read_text())
    assert receipt["native"] == [] and receipt["status"] == "failed"


def extract(path, destination):
    destination.mkdir()
    with tarfile.open(path) as archive:
        assert all(member.isfile() or member.isdir() for member in archive.getmembers())
        archive.extractall(destination, filter="data")
    (source,) = destination.iterdir()
    return source


def verify_source(source):
    manifest = json.loads((source / "source-manifest.json").read_text())
    listed = {row["path"] for row in manifest["files"]}
    actual = {
        str(path.relative_to(source)) for path in source.rglob("*") if path.is_file()
    }
    assert actual == listed | {"source-manifest.json"}
    for row in manifest["files"]:
        path = source / row["path"]
        assert (
            path.stat().st_size == row["bytes"]
            and hashlib.sha256(path.read_bytes()).hexdigest() == row["sha256"]
            and path.stat().st_mode & 0o777 == row["mode"]
        )
    return manifest


def main():
    wheel = ROOT / "dist" / f"webhook_incident_responder-{VERSION}-py3-none-any.whl"
    sdist = ROOT / "dist" / f"webhook_incident_responder-{VERSION}.tar.gz"
    source_bundle = build()
    with tempfile.TemporaryDirectory(prefix="incident-distribution-") as directory:
        temp = Path(directory)
        assert not temp.is_relative_to(ROOT)
        repeated = build(temp / "repeat.tar.gz")
        assert repeated["sha256"] == source_bundle["sha256"]
        with ZipFile(wheel) as archive:
            assert (
                len(
                    [
                        name
                        for name in archive.namelist()
                        if name.startswith("incident_responder/sample_data/")
                        and name.endswith(".json")
                    ]
                )
                == 13
            )
        requirements = temp / "requirements.txt"
        requirements.write_text(
            run(
                [
                    UV,
                    "export",
                    "--locked",
                    "--extra",
                    "test",
                    "--no-dev",
                    "--no-emit-project",
                    "--format",
                    "requirements-txt",
                ],
                ROOT,
            ).stdout
        )
        environment = temp / "wheel-env"
        run([UV, "venv", "--python", sys.executable, environment], temp)
        python = environment / "bin/python"
        run([UV, "pip", "sync", "--python", python, requirements], temp)
        run([UV, "pip", "install", "--python", python, "--no-deps", wheel], temp)
        exercise(python, temp / "wheel-case")
        source = extract(sdist, temp / "sdist")
        assert all(
            (source / name).exists()
            for name in (
                "uv.lock",
                "data/index.json",
                "scripts/build_datasets.py",
                "tests/test_delivery_cli.py",
                ".github/workflows/check.yml",
                "COMPATIBILITY.md",
            )
        )
        run(
            [
                UV,
                "sync",
                "--locked",
                "--extra",
                "test",
                "--no-editable",
                "--project",
                source,
            ],
            temp,
        )
        exercise(source / ".venv/bin/python", temp / "sdist-case")
        run(
            [source / ".venv/bin/python", "scripts/build_datasets.py", "--check"],
            source,
        )
        complete = extract(source_bundle["path"], temp / "complete")
        manifest = verify_source(complete)
        run(
            [
                UV,
                "sync",
                "--locked",
                "--extra",
                "test",
                "--no-editable",
                "--project",
                complete,
            ],
            temp,
        )
        exercise(complete / ".venv/bin/python", temp / "complete-case")
        run(
            [complete / ".venv/bin/python", "scripts/build_datasets.py", "--check"],
            complete,
        )
        suite = run([complete / ".venv/bin/python", "-m", "pytest", "-q"], complete)
        assert "183 passed" in suite.stdout, suite.stdout
        # Every archived original file remains byte-identical after the full suite.
        for row in manifest["files"]:
            assert (
                hashlib.sha256((complete / row["path"]).read_bytes()).hexdigest()
                == row["sha256"]
            )
    report = {
        **source_bundle,
        "checks": "fresh locked wheel/sdist/complete source, all twelve private cases, native HTTP, controlled delivery, missing-key receipts, complete183 suite",
    }
    (ROOT / "dist/qualification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
