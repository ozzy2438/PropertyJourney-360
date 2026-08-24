"""Idempotent consumer and explicit anonymous-to-known identity linker."""

from __future__ import annotations

import json
from datetime import UTC, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.events.models import EventType, PropertyEvent


class EventDisposition(StrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class EventResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    disposition: EventDisposition
    linked_event_count: int = Field(ge=0)
    reason: str


class EventProcessor:
    def __init__(self, repository: CustomerRepository, *, history_window: timedelta | None = None):
        self.repository = repository
        self.history_window = history_window or timedelta(days=30)

    def process(self, event: PropertyEvent) -> EventResult:
        inserted = self.repository.insert_event(
            {
                "event_id": event.event_id,
                "schema_version": event.schema_version,
                "event_type": event.event_type.value,
                "occurred_at": event.occurred_at.astimezone(UTC).isoformat(),
                "received_at": event.received_at.astimezone(UTC).isoformat(),
                "session_id": event.session_id,
                "anonymous_id": event.anonymous_id,
                "customer_id": event.customer_id,
                "customer_source_system": event.customer_source_system,
                "customer_source_id": event.customer_source_id,
                "property_id": event.property_id,
                "device_type": event.device_type,
                "payload_json": json.dumps(event.attributes, sort_keys=True),
            }
        )
        if not inserted:
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.DUPLICATE,
                linked_event_count=0,
                reason="event_id was already accepted",
            )

        event_customer = event.customer_id
        if not event_customer and event.customer_source_system and event.customer_source_id:
            event_customer = self.repository.customer_for_source(
                source_system=event.customer_source_system,
                source_record_id=event.customer_source_id,
            )
        if event.event_type is EventType.IDENTIFY and not event_customer:
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.REVIEW_REQUIRED,
                linked_event_count=0,
                reason="explicit source identity did not resolve to one canonical customer",
            )

        existing_customer = self.repository.session_customer(event.session_id)
        if existing_customer and event_customer and event_customer != existing_customer:
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.REVIEW_REQUIRED,
                linked_event_count=0,
                reason="session contains competing authenticated customer IDs",
            )

        if event.event_type is EventType.IDENTIFY and event_customer:
            self.repository.identify_session(
                session_id=event.session_id,
                customer_id=event_customer,
                identified_at=event.occurred_at,
            )
            linked = self.repository.link_session_events(
                session_id=event.session_id,
                customer_id=event_customer,
                occurred_after=event.occurred_at - self.history_window,
                linked_at=event.received_at,
            )
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.ACCEPTED,
                linked_event_count=linked,
                reason="explicit identify event linked bounded session history",
            )

        if existing_customer:
            linked = self.repository.link_session_events(
                session_id=event.session_id,
                customer_id=existing_customer,
                occurred_after=event.occurred_at - self.history_window,
                linked_at=event.received_at,
            )
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.ACCEPTED,
                linked_event_count=linked,
                reason="event inherited an existing explicit session identity",
            )

        return EventResult(
            event_id=event.event_id,
            disposition=EventDisposition.ACCEPTED,
            linked_event_count=0,
            reason="anonymous event retained without inferred identity",
        )
