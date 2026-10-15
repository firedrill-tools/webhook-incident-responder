"""Authenticated webhook intake; investigation runs in the separate worker."""

from __future__ import annotations

import hashlib
import hmac

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError

from .schema import ServiceEvent
from .store import EventStore


def create_app(store: EventStore, secret: str) -> FastAPI:
    if not secret:
        raise ValueError("a webhook secret is required")
    app = FastAPI(title="Synthetic incident intake")

    @app.post("/events", status_code=202)
    async def receive(request: Request) -> dict:
        body = await request.body()
        if len(body) > 65536:
            raise HTTPException(status_code=413, detail="event too large")
        supplied = request.headers.get("x-incident-signature", "")
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            event = ServiceEvent.model_validate_json(body)
            stored = store.submit(event)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"event_id": stored["event_id"], "status": stored["status"]}

    return app
