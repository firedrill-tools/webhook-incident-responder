"""LangGraph path from original event/context to owned assessment and fenced commit."""

from __future__ import annotations
import asyncio
import inspect
import math
from typing import TypedDict
from .schema import IncidentAssessment
from .store import EventStore
from .native import investigate
from .evidence import Receipt, sha
from langgraph.graph import END, START, StateGraph


class ResponseState(TypedDict, total=False):
    event: dict
    context: dict
    stale: bool
    assessment: IncidentAssessment
    result: dict


async def anthropic_assessment(event, context, *, receipt, **options):
    return await investigate(event, context, receipt, **options)


def build_graph(
    store: EventStore,
    assessor=anthropic_assessment,
    *,
    receipt=None,
    native_options=None,
):
    def inspect_source(state):
        event = state["event"]
        context = store.context(event["service"], event["sequence"])
        current = context["state"]
        if receipt:
            receipt.data.update(
                event=event, context=context, context_sha256=sha(context)
            )
            receipt.save()
            receipt.guard()
        return {
            "context": context,
            "stale": bool(current and event["sequence"] <= current["last_sequence"]),
        }

    def route(state):
        return (
            "assess"
            if state["event"]["kind"] == "degraded" and not state["stale"]
            else "commit"
        )

    async def assess(state):
        if receipt:
            receipt.guard()
        if assessor is anthropic_assessment:
            result = await assessor(
                state["event"],
                state["context"],
                receipt=receipt,
                **(native_options or {}),
            )
        else:
            if receipt:
                receipt.data["execution_kind"] = "caller_injected_assessor"
                receipt.save()
            result = assessor(state["event"], state["context"])
            if inspect.isawaitable(result):
                result = await result
        parsed = IncidentAssessment.model_validate(result)
        allowed = {item["event_id"] for item in state["context"]["recent_events"]}
        if (
            state["event"]["event_id"] not in parsed.evidence_event_ids
            or not set(parsed.evidence_event_ids) <= allowed
        ):
            raise ValueError("assessment cites event IDs outside supplied context")
        if receipt:
            receipt.data["assessment"] = parsed.model_dump(mode="json")
            receipt.save()
            receipt.guard()
        return {"assessment": parsed}

    def commit(state):
        if receipt:
            receipt.guard()
            receipt.data["status"] = "commit_prepared"
            receipt.save()
            receipt.guard()
        event = state["event"]
        result = store.commit(
            event["event_id"],
            event["lease_token"],
            state.get("assessment"),
            guard=receipt.guard if receipt else None,
        )
        if receipt:
            receipt.data["result"] = result
            receipt.save()
        return {"result": result}

    graph = StateGraph(ResponseState)
    for name, node in [
        ("inspect", inspect_source),
        ("assess", assess),
        ("commit", commit),
    ]:
        graph.add_node(name, node)
    graph.add_edge(START, "inspect")
    graph.add_conditional_edges(
        "inspect", route, {"assess": "assess", "commit": "commit"}
    )
    graph.add_edge("assess", "commit")
    graph.add_edge("commit", END)
    return graph.compile()


def run_once(store, assessor=anthropic_assessment, **options):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_once_async(store, assessor, **options))
    raise RuntimeError("use run_once_async from an active event loop")


async def run_once_async(
    store,
    assessor=anthropic_assessment,
    *,
    output_root=None,
    timeout_seconds=60,
    writer=None,
    **native_options,
):
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 120
    ):
        raise ValueError("worker timeout must be finite and in (0,120]")
    receipt = Receipt(store, output_root, timeout_seconds, writer)
    claimed = None
    result = None
    failure = None
    try:
        receipt.save()
        receipt.guard()
        claimed = store.claim(guard=receipt.guard)
        if claimed is not None:
            receipt.data.update(status="claimed", event=claimed)
            receipt.save()
            receipt.guard()
            async with asyncio.timeout(timeout_seconds):
                result = (
                    await build_graph(
                        store, assessor, receipt=receipt, native_options=native_options
                    ).ainvoke({"event": claimed}, config={"recursion_limit": 8})
                )["result"]
            receipt.guard()
    except BaseException as error:
        failure = error
        if claimed is not None:
            try:
                store.fail(
                    claimed["event_id"],
                    claimed["lease_token"],
                    f"{type(error).__name__}: {error}",
                )
                result = store.get_event(claimed["event_id"])
            except BaseException as journal_error:
                receipt.data["journal_error"] = {
                    "name": type(journal_error).__name__,
                    "message": str(journal_error),
                }
    finally:
        try:
            receipt.finish(result, failure)
        except BaseException as error:
            failure = failure or error
            receipt.data.update(
                status="failed",
                evidence_error={"name": type(error).__name__, "message": str(error)},
                durable_status="unknown",
            )
            try:
                receipt.data["durable_status"] = "failed"
                receipt.save(final=True)
            except BaseException:
                receipt.data["durable_status"] = "unknown"
            error.evidence = receipt.data
            error.receipt_path = str(receipt.path)
            raise
    if failure is not None and not isinstance(failure, Exception):
        failure.evidence = receipt.data
        failure.receipt_path = str(receipt.path)
        raise failure
    if result is not None:
        return {
            **result,
            "run_id": receipt.run_id,
            "receipt_path": str(receipt.path),
            "run_status": "failed" if failure else "completed",
            "run_error": None
            if failure is None
            else {"name": type(failure).__name__, "message": str(failure)},
        }
    if failure is not None:
        failure.evidence = receipt.data
        failure.receipt_path = str(receipt.path)
        raise failure
    return None
