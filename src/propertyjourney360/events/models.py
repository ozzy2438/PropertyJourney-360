"""Versioned behavioural event contract."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EventType(StrEnum):
    SEARCH = "SEARCH"
    VIEW = "VIEW"
    SAVE = "SAVE"
    ENQUIRY = "ENQUIRY"
    IDENTIFY = "IDENTIFY"


class PropertyEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1, 2]
    event_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    occurred_at: datetime
    received_at: datetime
    session_id: str = Field(min_length=1, max_length=128)
    anonymous_id: str | None = None
    customer_id: str | None = None
    customer_source_system: str | None = None
    customer_source_id: str | None = None
    property_id: str | None = None
    device_type: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_version_and_identity(self) -> PropertyEvent:
        has_source_identity = bool(self.customer_source_system and self.customer_source_id)
        has_partial_source_identity = bool(self.customer_source_system) != bool(
            self.customer_source_id
        )
        if has_partial_source_identity:
            raise ValueError("customer source system and ID must be supplied together")
        if not self.anonymous_id and not self.customer_id and not has_source_identity:
            raise ValueError("event requires anonymous_id or customer_id")
        if self.event_type is EventType.IDENTIFY and not (self.customer_id or has_source_identity):
            raise ValueError("IDENTIFY requires a canonical or source customer identity")
        if self.schema_version == 1 and self.device_type is not None:
            raise ValueError("device_type was introduced in schema version 2")
        if self.received_at < self.occurred_at:
            raise ValueError("received_at cannot precede occurred_at")
        return self
