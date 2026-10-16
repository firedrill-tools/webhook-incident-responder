"""Strict bounded business input and assessment contracts."""

from datetime import datetime
import math
import re
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
SAFE_INTEGER = 2**53 - 1


class ServiceEvent(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, revalidate_instances="always", allow_inf_nan=False
    )
    event_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    service: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    sequence: int = Field(ge=1, le=SAFE_INTEGER)
    kind: Literal["degraded", "recovered"]
    observed_at: str = Field(
        max_length=64, description="ISO 8601 timestamp from source"
    )
    details: dict[str, str | int | float | bool] = Field(
        default_factory=dict, max_length=32
    )

    @field_validator("observed_at")
    @classmethod
    def timestamp_has_timezone(cls, value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return value

    @field_validator("details")
    @classmethod
    def bounded_details(cls, value):
        for key, item in value.items():
            if not key or len(key) > 80 or key.strip() != key:
                raise ValueError("detail names must contain 1-80 unpadded characters")
            if isinstance(item, str) and len(item) > 2000:
                raise ValueError("detail text exceeds 2000 characters")
            if type(item) in (int, float) and (
                abs(item) > SAFE_INTEGER or not math.isfinite(item)
            ):
                raise ValueError("detail numbers must be finite and safely bounded")
        return value

    @model_validator(mode="after")
    def bounded_event(self):
        if len(self.model_dump_json().encode()) > 65536:
            raise ValueError("event exceeds 65536 source bytes")
        return self


class IncidentAssessment(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, revalidate_instances="always"
    )
    severity: Literal["warning", "critical"]
    summary: str = Field(min_length=1, max_length=500)
    suspected_cause: str = Field(min_length=1, max_length=500)
    recommended_action: str = Field(min_length=1, max_length=500)
    evidence_event_ids: list[str] = Field(min_length=1, max_length=5)

    @field_validator("summary", "suspected_cause", "recommended_action")
    @classmethod
    def nonblank_text(cls, value):
        if not value.strip():
            raise ValueError("assessment text must not be blank")
        return value

    @field_validator("evidence_event_ids")
    @classmethod
    def exact_ids(cls, value):
        if len(set(value)) != len(value) or any(
            not re.fullmatch(ID_PATTERN, item) for item in value
        ):
            raise ValueError("evidence must contain unique exact event IDs")
        return value
