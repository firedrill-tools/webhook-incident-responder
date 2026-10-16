"""Local webhook, worker, state and notification CLI."""

import argparse
import asyncio
import signal
import json
import os
import sys
import urllib.request
from pathlib import Path

import uvicorn

from .graph import run_once_async
from .schema import ServiceEvent
from .store import EventStore
from .web import create_app


def send_pending(store: EventStore, webhook_url: str) -> list[dict]:
    results = []
    for item in store.outbox():
        if item["status"] != "pending":
            continue
        body = json.dumps({"text": item["message"]}).encode()
        request = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"Slack webhook returned {response.status}")
        store.mark_sent(item["notification_id"])
        results.append({"notification_id": item["notification_id"], "status": "sent"})
    return results


async def work_queue(store, args):
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    seen = []
    previous = {}
    results = []

    def stop(number, frame):
        if not seen:
            seen.append(number)
            task.cancel(f"Received {signal.Signals(number).name}")
            loop.call_soon_threadsafe(lambda: None)

    for name in ("SIGINT", "SIGTERM", "SIGHUP"):
        if hasattr(signal, name):
            number = getattr(signal, name)
            previous[number] = signal.getsignal(number)
            signal.signal(number, stop)
    try:
        while True:
            result = await run_once_async(
                store,
                model=args.model,
                base_url=args.provider_base_url,
                timeout_seconds=args.timeout_seconds,
                output_root=args.output_root,
            )
            if result is None:
                break
            results.append(result)
            if args.once:
                break
        failed = any(item.get("run_status") == "failed" for item in results)
        return (
            results[0] if results else None
        ) if args.once else results, 1 if failed else 0
    except asyncio.CancelledError as error:
        number = seen[0] if seen else None
        return {
            "status": "interrupted",
            "signal": signal.Signals(number).name if number is not None else None,
            "run_status": "failed",
            "receipt_path": getattr(error, "receipt_path", None),
            "completed_results": results,
        }, 128 + number if number is not None else 130
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="LangGraph incident responder")
    parser.add_argument("--db", default="incidents.db", help="SQLite journal")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("submit").add_argument("file", type=Path)
    work = commands.add_parser("work")
    work.add_argument("--once", action="store_true")
    work.add_argument("--model")
    work.add_argument("--provider-base-url", default="https://api.anthropic.com")
    work.add_argument("--timeout-seconds", type=float, default=60)
    work.add_argument("--output-root", type=Path)
    commands.add_parser("get").add_argument("event_id")
    commands.add_parser("retry").add_argument("event_id")
    commands.add_parser("state").add_argument("service")
    commands.add_parser("outbox")
    commands.add_parser("send").add_argument(
        "--confirm", action="store_true", required=True
    )
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    store = EventStore(args.db)
    exit_code = 0
    if args.command == "submit":
        result = store.submit(ServiceEvent.model_validate_json(args.file.read_text()))
    elif args.command == "work":
        result, exit_code = asyncio.run(work_queue(store, args))
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
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(
            json.dumps(
                {
                    "error": {"name": type(error).__name__, "message": str(error)},
                    "receipt_path": getattr(error, "receipt_path", None),
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)
