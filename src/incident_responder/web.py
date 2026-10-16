"""Authenticated bounded webhook intake; model work runs separately."""

from __future__ import annotations
import hashlib
import hmac
import re
from fastapi import FastAPI, HTTPException, Request
from .schema import ServiceEvent
from .source import InvalidSource, strict_json
from .store import EventStore

MAX_EVENT_BYTES = 65536


def create_app(store: EventStore, secret: str) -> FastAPI:
    if not isinstance(secret, str) or not secret.strip():
        raise ValueError("a webhook secret is required")
    app = FastAPI(title="Synthetic incident intake")

    @app.post("/events", status_code=202)
    async def receive(request: Request) -> dict:
        supplied = request.headers.get("x-incident-signature", "")
        if not re.fullmatch(r"sha256=[0-9a-fA-F]{64}", supplied):
            raise HTTPException(status_code=401, detail="invalid signature")
        declared = request.headers.get("content-length")
        if declared is not None:
            if len(declared) > 20 or not declared.isascii() or not declared.isdigit():
                raise HTTPException(status_code=400, detail="invalid content length")
            if int(declared) > MAX_EVENT_BYTES:
                raise HTTPException(status_code=413, detail="event too large")
        payload = bytearray()
        async for chunk in request.stream():
            if len(payload) + len(chunk) > MAX_EVENT_BYTES:
                raise HTTPException(status_code=413, detail="event too large")
            payload.extend(chunk)
        body = bytes(payload)
        if not hmac.compare_digest(
            bytes.fromhex(supplied[7:]),
            hmac.new(secret.encode(), body, hashlib.sha256).digest(),
        ):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            event = ServiceEvent.model_validate(strict_json(body))
        except (ValueError, UnicodeError) as error:
            raise HTTPException(
                status_code=422, detail="invalid service event"
            ) from error
        try:
            stored = store.submit(event)
        except InvalidSource as error:
            raise HTTPException(
                status_code=503, detail="incident journal is invalid"
            ) from error
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"event_id": stored["event_id"], "status": stored["status"]}

    return app
