"""LangGraph path from event snapshot through investigation to fenced commit."""

from __future__ import annotations

import json
import os
from typing import TypedDict

from langchain_anthropic import ChatAnthropic
from langgraph.graph import END, START, StateGraph

from .schema import IncidentAssessment
from .store import EventStore


class ResponseState(TypedDict, total=False):
    event: dict
    context: dict
    stale: bool
    assessment: IncidentAssessment
    result: dict


def anthropic_assessment(event: dict, context: dict) -> IncidentAssessment:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is required for live investigation")
    model_name = os.environ.get("ANTHROPIC_MODEL")
    if not model_name:
        raise RuntimeError("ANTHROPIC_MODEL must name an enabled Claude model")
    model = ChatAnthropic(model=model_name, max_tokens=512)
    return model.with_structured_output(IncidentAssessment).invoke([
        ("system", "Investigate a synthetic service degradation. Use only the supplied event evidence. "
         "Keep claims cautious. Cite the current event ID and only event IDs in the context. "
         "Propose an operator action; do not claim that you sent alerts or changed service state."),
        ("human", json.dumps({"event": event, "context": context}, sort_keys=True)),
    ])


def build_graph(store: EventStore, assessor=anthropic_assessment):
    def inspect(state: ResponseState) -> dict:
        event = state["event"]
        context = store.context(event["service"], event["sequence"])
        current = context["state"]
        return {"context": context,
                "stale": bool(current and event["sequence"] <= current["last_sequence"])}

    def route(state: ResponseState) -> str:
        return "assess" if state["event"]["kind"] == "degraded" and not state["stale"] else "commit"

    def assess(state: ResponseState) -> dict:
        result = assessor(state["event"], state["context"])
        return {"assessment": IncidentAssessment.model_validate(result)}

    def commit(state: ResponseState) -> dict:
        event = state["event"]
        result = store.commit(event["event_id"], event["lease_token"], state.get("assessment"))
        return {"result": result}

    graph = StateGraph(ResponseState)
    graph.add_node("inspect", inspect)
    graph.add_node("assess", assess)
    graph.add_node("commit", commit)
    graph.add_edge(START, "inspect")
    graph.add_conditional_edges("inspect", route, {"assess": "assess", "commit": "commit"})
    graph.add_edge("assess", "commit")
    graph.add_edge("commit", END)
    return graph.compile()


def run_once(store: EventStore, assessor=anthropic_assessment) -> dict | None:
    claimed = store.claim()
    if claimed is None:
        return None
    try:
        return build_graph(store, assessor).invoke({"event": claimed})["result"]
    except Exception as exc:
        if store.fail(claimed["event_id"], claimed["lease_token"], str(exc)):
            return store.get_event(claimed["event_id"])
        return store.get_event(claimed["event_id"])
