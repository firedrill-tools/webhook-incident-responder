"""Local webhook, worker, state and notification CLI."""

import argparse
import json
import os
import urllib.request
from pathlib import Path

import uvicorn

from .graph import run_once
from .schema import ServiceEvent
from .store import EventStore
from .web import create_app


def send_pending(store: EventStore, webhook_url: str) -> list[dict]:
    results = []
    for item in store.outbox():
        if item["status"] != "pending":
            continue
        body = json.dumps({"text": item["message"]}).encode()
        request = urllib.request.Request(webhook_url, data=body,
                                         headers={"Content-Type": "application/json"},
                                         method="POST")
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"Slack webhook returned {response.status}")
        store.mark_sent(item["notification_id"])
        results.append({"notification_id": item["notification_id"], "status": "sent"})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="LangGraph incident responder")
    parser.add_argument("--db", default="incidents.db", help="SQLite journal")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("submit").add_argument("file", type=Path)
    commands.add_parser("work").add_argument("--once", action="store_true")
    commands.add_parser("get").add_argument("event_id")
    commands.add_parser("retry").add_argument("event_id")
    commands.add_parser("state").add_argument("service")
    commands.add_parser("outbox")
    commands.add_parser("send").add_argument("--confirm", action="store_true", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    store = EventStore(args.db)
    if args.command == "submit":
        result = store.submit(ServiceEvent.model_validate_json(args.file.read_text()))
    elif args.command == "work":
        if args.once:
            result = run_once(store)
        else:
            results = []
            while (item := run_once(store)) is not None:
                results.append(item)
            result = results
    elif args.command == "get":
        result = store.get_event(args.event_id)
    elif args.command == "retry":
        result = store.retry(args.event_id)
    elif args.command == "state":
        result = store.state(args.service)
    elif args.command == "outbox":
        result = store.outbox()
    elif args.command == "send":
        webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        if not webhook_url:
            parser.error("SLACK_WEBHOOK_URL is required for send")
        result = send_pending(store, webhook_url)
    else:
        secret = os.environ.get("INCIDENT_WEBHOOK_SECRET")
        if not secret:
            parser.error("INCIDENT_WEBHOOK_SECRET is required for serve")
        uvicorn.run(create_app(store, secret), host=args.host, port=args.port)
        return
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
