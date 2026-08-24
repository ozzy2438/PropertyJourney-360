from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.events.models import EventType, PropertyEvent
from propertyjourney360.events.processor import EventDisposition, EventProcessor

NOW = datetime(2026, 8, 24, 1, tzinfo=UTC)


def _event(
    event_id: str,
    event_type: EventType,
    *,
    occurred_at: datetime = NOW,
    customer_id: str | None = None,
    customer_source_system: str | None = None,
    customer_source_id: str | None = None,
) -> PropertyEvent:
    return PropertyEvent(
        schema_version=2,
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=1),
        session_id="session-1",
        anonymous_id="anonymous-1",
        customer_id=customer_id,
        customer_source_system=customer_source_system,
        customer_source_id=customer_source_id,
        property_id="property-1",
        device_type="mobile",
    )


def test_duplicate_event_is_idempotent(tmp_path: Path) -> None:
    processor = EventProcessor(CustomerRepository(tmp_path / "events.db"))
    event = _event("event-1", EventType.VIEW)
    assert processor.process(event).disposition is EventDisposition.ACCEPTED
    assert processor.process(event).disposition is EventDisposition.DUPLICATE


def test_explicit_identify_links_bounded_anonymous_history(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    processor = EventProcessor(repository, history_window=timedelta(days=1))
    processor.process(_event("old", EventType.VIEW, occurred_at=NOW - timedelta(days=2)))
    processor.process(_event("recent", EventType.SAVE, occurred_at=NOW - timedelta(hours=1)))

    result = processor.process(_event("identify", EventType.IDENTIFY, customer_id="customer-1"))

    assert result.disposition is EventDisposition.ACCEPTED
    # The recent event and identify event link; the older event remains anonymous.
    assert result.linked_event_count == 2
    linked_ids = {
        item["event_id"] for item in repository.list_journey("customer-1", limit=10, offset=0)
    }
    assert linked_ids == {
        "recent",
        "identify",
    }


def test_competing_customer_on_identified_session_requires_review(tmp_path: Path) -> None:
    processor = EventProcessor(CustomerRepository(tmp_path / "events.db"))
    processor.process(_event("identify-1", EventType.IDENTIFY, customer_id="customer-1"))

    result = processor.process(_event("identify-2", EventType.IDENTIFY, customer_id="customer-2"))

    assert result.disposition is EventDisposition.REVIEW_REQUIRED


def test_source_identity_resolves_to_canonical_customer(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    repository.upsert_customer(
        {
            "customer_id": "customer-1",
            "display_name": "Synthetic Customer",
            "preferred_suburbs": "[]",
            "observed_price_band": None,
            "identity_rule_version": "identity-v1.1.0",
            "as_of": NOW.isoformat(),
        }
    )
    repository.add_identity_source(
        {
            "customer_id": "customer-1",
            "source_system": "salesforce",
            "source_record_id": "003-test",
            "normalized_email": None,
            "normalized_phone": None,
            "match_score": 1.0,
            "match_decision": "AUTO_MATCH",
        }
    )
    result = EventProcessor(repository).process(
        _event(
            "identify-source",
            EventType.IDENTIFY,
            customer_source_system="salesforce",
            customer_source_id="003-test",
        )
    )
    assert result.disposition is EventDisposition.ACCEPTED
    assert repository.session_customer("session-1") == "customer-1"


def test_unknown_source_identity_requires_review(tmp_path: Path) -> None:
    result = EventProcessor(CustomerRepository(tmp_path / "events.db")).process(
        _event(
            "identify-unknown",
            EventType.IDENTIFY,
            customer_source_system="salesforce",
            customer_source_id="003-unknown",
        )
    )
    assert result.disposition is EventDisposition.REVIEW_REQUIRED


def test_partial_source_identity_is_rejected() -> None:
    with pytest.raises(ValidationError, match="system and ID"):
        _event(
            "identify-partial",
            EventType.IDENTIFY,
            customer_source_system="salesforce",
        )
