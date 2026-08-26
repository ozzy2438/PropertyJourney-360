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
    EVENT_ID_COLLISION = "EVENT_ID_COLLISION"


class EventResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    disposition: EventDisposition
    linked_event_count: int = Field(ge=0)
    reason: str


class EventProcessor:
    def __init__(self, repository: CustomerRepository, *, history_window: timedelta | None = None):
        self.repository = repository
        self.history_window = history_window or timedelta(hours=24)

    def process(self, event: PropertyEvent) -> EventResult:
        if event.customer_id and not self.repository.get_customer(event.customer_id):
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.REVIEW_REQUIRED,
                linked_event_count=0,
                reason="explicit canonical customer ID does not exist",
            )

        source_customer = None
        if event.customer_source_system and event.customer_source_id:
            source_customer = self.repository.customer_for_source(
                source_system=event.customer_source_system,
                source_record_id=event.customer_source_id,
            )
            if not source_customer:
                return EventResult(
                    event_id=event.event_id,
                    disposition=EventDisposition.REVIEW_REQUIRED,
                    linked_event_count=0,
                    reason="explicit source identity did not resolve to one canonical customer",
                )
        if event.customer_id and source_customer and event.customer_id != source_customer:
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.REVIEW_REQUIRED,
                linked_event_count=0,
                reason="event contains competing canonical and source customer IDs",
            )
        event_customer = event.customer_id or source_customer
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

        insert_status = self.repository.insert_event(
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
                "price_observed": event.price_observed,
                "device_type": event.device_type,
                "payload_json": json.dumps(event.attributes, sort_keys=True),
            }
        )
        if insert_status == "COLLISION":
            return EventResult(
                event_id=event.event_id,
                disposition=EventDisposition.EVENT_ID_COLLISION,
                linked_event_count=0,
                reason="event_id was previously accepted with a different canonical payload",
            )
        accepted_disposition = (
            EventDisposition.DUPLICATE
            if insert_status == "DUPLICATE"
            else EventDisposition.ACCEPTED
        )

        direct_linked = 0
        if event_customer and event.event_type is not EventType.IDENTIFY:
            direct_linked = self.repository.link_event_identity(
                event_id=event.event_id,
                customer_id=event_customer,
                link_reason="DIRECT_EVENT_SUBJECT",
                linked_at=event.received_at,
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
                occurred_before=event.occurred_at,
                linked_at=event.received_at,
            )
            return EventResult(
                event_id=event.event_id,
                disposition=accepted_disposition,
                linked_event_count=linked,
                reason="explicit identify event linked bounded session history",
            )

        if existing_customer:
            linked = self.repository.link_session_events(
                session_id=event.session_id,
                customer_id=existing_customer,
                occurred_after=event.occurred_at - self.history_window,
                occurred_before=event.occurred_at,
                linked_at=event.received_at,
            )
            return EventResult(
                event_id=event.event_id,
                disposition=accepted_disposition,
                linked_event_count=direct_linked + linked,
                reason="event inherited an existing explicit session identity",
            )

        if event_customer:
            return EventResult(
                event_id=event.event_id,
                disposition=accepted_disposition,
                linked_event_count=direct_linked,
                reason="explicit event subject linked directly",
            )

        return EventResult(
            event_id=event.event_id,
            disposition=accepted_disposition,
            linked_event_count=0,
            reason="anonymous event retained without inferred identity",
        )
