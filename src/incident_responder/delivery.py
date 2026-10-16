"""Owned incoming-webhook delivery. Only HTTP 200 with body `ok` is acknowledged."""

import asyncio
import base64
import math
from urllib.parse import urlsplit
import httpx2
from .delivery_state import digest, payload
from .evidence import Receipt, now

MAX_ACK_BYTES = 4096


class DeliveryError(RuntimeError):
    def __init__(self, code, original_type=None):
        self.code = code
        self.original_type = original_type
        super().__init__(
            f"notification delivery failed ({code}); inspect the private receipt"
        )


def target(value):
    try:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or any(ord(c) <= 32 or ord(c) == 127 for c in value)
        ):
            raise ValueError()
        url = urlsplit(value)
        port = url.port
        if (
            not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or not (
                url.scheme == "https"
                or url.scheme == "http"
                and url.hostname in {"127.0.0.1", "localhost", "::1"}
            )
        ):
            raise ValueError()
        host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
        origin = f"{url.scheme}://{host}" + (f":{port}" if port is not None else "")
        return {"origin": origin, "url_sha256": digest(value)}
    except (ValueError, TypeError):
        raise ValueError(
            "webhook target requires HTTPS or loopback HTTP without credentials/query/fragment"
        ) from None


async def send_once_async(
    store,
    webhook_url,
    *,
    output_root=None,
    timeout_seconds=60,
    writer=None,
    transport=None,
):
    endpoint = target(webhook_url)
    if (
        type(timeout_seconds) not in (int, float)
        or not 0 < timeout_seconds <= 120
        or not math.isfinite(timeout_seconds)
    ):
        raise ValueError("delivery timeout must be finite and in (0,120]")
    receipt = Receipt(store, output_root, timeout_seconds, writer)
    receipt.data.update(
        execution_kind="incoming_webhook_delivery",
        target=endpoint,
        delivery=None,
        request=None,
        response=None,
        client_closed=False,
    )
    claimed = None
    result = None
    failure = None
    cancellation = None
    client = None
    try:
        receipt.save()
        receipt.guard()
        claimed = store.claim_notification(
            endpoint["url_sha256"],
            lease_seconds=timeout_seconds + 15,
            guard=receipt.guard,
        )
        if claimed is not None:
            token = claimed["attempt"]["attempt_id"]
            body = payload(claimed["notification"]["message"])
            receipt.data.update(
                status="delivery_claimed",
                delivery=claimed,
                request={
                    "method": "POST",
                    "body_utf8": body.decode(),
                    "body_sha256": digest(body),
                },
            )
            receipt.save()
            receipt.guard()
            client = httpx2.AsyncClient(
                timeout=httpx2.Timeout(
                    min(timeout_seconds, 30), connect=min(timeout_seconds, 5)
                ),
                trust_env=False,
                follow_redirects=False,
                transport=transport,
            )
            async with asyncio.timeout_at(receipt.deadline):
                # Persist dispatch intent first. A crash in this interval is conservatively unknown.
                receipt.data["status"] = "dispatch_prepared"
                receipt.save()
                receipt.guard()
                store.dispatch_notification(token, guard=receipt.guard)
                receipt.data["status"] = "dispatching"
                receipt.save()
                receipt.guard()
                async with client.stream(
                    "POST",
                    webhook_url,
                    content=body,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    original = {
                        "status": response.status_code,
                        "decoded_body_base64": "",
                        "body_sha256": None,
                        "complete": False,
                        "received_at": now(),
                    }
                    receipt.data["response"] = original
                    receipt.save()
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(raw) + len(chunk) > MAX_ACK_BYTES:
                            original.update(
                                decoded_body_base64=base64.b64encode(raw).decode(),
                                body_sha256=digest(bytes(raw)),
                                retention_limit=MAX_ACK_BYTES,
                            )
                            receipt.save()
                            raise DeliveryError("invalid_ack")
                        raw.extend(chunk)
                        original.update(
                            decoded_body_base64=base64.b64encode(raw).decode(),
                            body_sha256=digest(bytes(raw)),
                        )
                        receipt.save()
                        receipt.guard()
                    original["complete"] = True
                    receipt.save()
                    receipt.guard()
                    if response.status_code != 200 or bytes(raw) != b"ok":
                        raise DeliveryError("invalid_ack")
            await client.aclose()
            receipt.data["client_closed"] = True
            receipt.save()
            receipt.guard()
            receipt.data["status"] = "ack_commit_prepared"
            receipt.save()
            receipt.guard()
            store.acknowledge_notification(token, guard=receipt.guard)
            result = {
                "notification_id": claimed["notification"]["notification_id"],
                "attempt_id": token,
                "status": "sent",
                "delivery_basis": "http_200_ok",
            }
            receipt.data["result"] = result
            receipt.save()
    except BaseException as error:
        cancellation = error if not isinstance(error, Exception) else None
        code = (
            "recording_failed"
            if receipt.recording_error is not None
            else "cancelled"
            if isinstance(error, asyncio.CancelledError)
            else "deadline"
            if isinstance(error, TimeoutError)
            else error.code
            if isinstance(error, DeliveryError)
            else "operation_failed"
        )
        failure = DeliveryError(code, type(error).__name__)
        receipt.data["failure_type"] = type(error).__name__
        if claimed is not None:
            try:
                store.abandon_notification(claimed["attempt"]["attempt_id"], code)
                item = next(
                    row
                    for row in store.outbox()
                    if row["notification_id"]
                    == claimed["notification"]["notification_id"]
                )
                result = {
                    "notification_id": item["notification_id"],
                    "attempt_id": claimed["attempt"]["attempt_id"],
                    "status": item["status"],
                }
            except BaseException as journal_error:
                receipt.data["journal_error"] = {
                    "name": type(journal_error).__name__,
                    "message": "delivery journal cleanup failed; state requires inspection",
                }
    finally:
        if client is not None and not client.is_closed:
            try:
                await client.aclose()
                receipt.data["client_closed"] = True
            except BaseException as close_error:
                failure = failure or DeliveryError(
                    "operation_failed", type(close_error).__name__
                )
                receipt.data["close_error"] = {
                    "name": type(close_error).__name__,
                    "message": "owned HTTP client did not confirm closure",
                }
        try:
            receipt.finish(result, failure)
        except BaseException as error:
            receipt.data.update(
                status="failed",
                durable_status="failed",
                recording_error={
                    "name": type(error).__name__,
                    "message": "private delivery receipt could not be saved",
                },
            )
            try:
                receipt.save(final=True)
            except BaseException:
                receipt.data["durable_status"] = "unknown"
            failure = failure or DeliveryError("recording_failed", type(error).__name__)
    if cancellation is not None:
        cancellation.receipt_path = str(receipt.path)
        cancellation.evidence = receipt.data
        raise cancellation
    if result is not None:
        return {
            **result,
            "run_id": receipt.run_id,
            "receipt_path": str(receipt.path),
            "run_status": "failed" if failure else "completed",
            "receipt_durable_status": receipt.data.get("durable_status", "finalized"),
            "run_error": str(failure) if failure else None,
        }
    if failure:
        failure.receipt_path = str(receipt.path)
        failure.evidence = receipt.data
        raise failure
    return None


def send_once(store, webhook_url, **options):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(send_once_async(store, webhook_url, **options))
    raise RuntimeError("use send_once_async from an active event loop")
