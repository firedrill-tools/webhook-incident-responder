"""Validated event and model assessment contracts."""

from typing import Literal
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ServiceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    kind: Literal["degraded", "recovered"]
    observed_at: str = Field(description="ISO 8601 timestamp from source")
    details: dict[str, str | int | float | bool] = Field(default_factory=dict)

    @field_validator("observed_at")
    @classmethod
    def timestamp_has_timezone(cls, value: str) -> str:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return value


class IncidentAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: Literal["warning", "critical"]
    summary: str = Field(min_length=1, max_length=500)
    suspected_cause: str = Field(min_length=1, max_length=500)
    recommended_action: str = Field(min_length=1, max_length=500)
    evidence_event_ids: list[str] = Field(min_length=1)
