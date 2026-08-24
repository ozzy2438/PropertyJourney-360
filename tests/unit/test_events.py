from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.events.models import EventType, PropertyEvent
from propertyjourney360.events.processor import EventDisposition, EventProcessor

NOW = datetime(2026, 8, 24, 1, tzinfo=UTC)


def _upsert_customer(repository: CustomerRepository, customer_id: str) -> None:
    repository.upsert_customer(
        {
            "customer_id": customer_id,
            "display_name": f"Synthetic {customer_id}",
            "preferred_suburbs": "[]",
            "observed_price_band": None,
            "identity_rule_version": "identity-v1.1.0",
            "as_of": NOW.isoformat(),
        }
    )


def _event(
    event_id: str,
    event_type: EventType,
    *,
    occurred_at: datetime = NOW,
    customer_id: str | None = None,
    customer_source_system: str | None = None,
    customer_source_id: str | None = None,
    session_id: str = "session-1",
) -> PropertyEvent:
    return PropertyEvent(
        schema_version=2,
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=1),
        session_id=session_id,
        anonymous_id="anonymous-1",
        customer_id=customer_id,
        customer_source_system=customer_source_system,
        customer_source_id=customer_source_id,
        property_id="property-1",
        price_observed=875_000,
        device_type="mobile",
    )


def test_duplicate_event_is_idempotent(tmp_path: Path) -> None:
    processor = EventProcessor(CustomerRepository(tmp_path / "events.db"))
    event = _event("event-1", EventType.VIEW)
    assert processor.process(event).disposition is EventDisposition.ACCEPTED
    assert processor.process(event).disposition is EventDisposition.DUPLICATE

    collision = event.model_copy(update={"price_observed": 999_000})
    result = processor.process(collision)
    assert result.disposition is EventDisposition.EVENT_ID_COLLISION


def test_explicit_identify_links_bounded_anonymous_history(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    _upsert_customer(repository, "customer-1")
    processor = EventProcessor(repository, history_window=timedelta(days=1))
    processor.process(_event("old", EventType.VIEW, occurred_at=NOW - timedelta(days=2)))
    processor.process(_event("recent", EventType.SAVE, occurred_at=NOW - timedelta(hours=1)))
    processor.process(_event("future", EventType.VIEW, occurred_at=NOW + timedelta(hours=1)))

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


def test_default_identity_history_window_is_24_hours(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "default-window.db")
    _upsert_customer(repository, "c-1")
    processor = EventProcessor(repository)
    processor.process(_event("too-old", EventType.VIEW, occurred_at=NOW - timedelta(hours=25)))

    result = processor.process(_event("identify-default", EventType.IDENTIFY, customer_id="c-1"))

    assert result.linked_event_count == 1
    assert {item["event_id"] for item in repository.list_journey("c-1", limit=10, offset=0)} == {
        "identify-default"
    }


def test_competing_customer_on_identified_session_requires_review(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    _upsert_customer(repository, "customer-1")
    _upsert_customer(repository, "customer-2")
    processor = EventProcessor(repository)
    processor.process(_event("identify-1", EventType.IDENTIFY, customer_id="customer-1"))

    result = processor.process(_event("identify-2", EventType.IDENTIFY, customer_id="customer-2"))

    assert result.disposition is EventDisposition.REVIEW_REQUIRED


def test_source_identity_resolves_to_canonical_customer(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    _upsert_customer(repository, "customer-1")
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
    direct_source = EventProcessor(repository).process(
        _event(
            "direct-source",
            EventType.VIEW,
            customer_source_system="salesforce",
            customer_source_id="003-test",
            session_id="session-direct-source",
        )
    )
    direct_canonical = EventProcessor(repository).process(
        _event(
            "direct-canonical",
            EventType.VIEW,
            customer_id="customer-1",
            session_id="session-direct-canonical",
        )
    )
    assert direct_source.linked_event_count == 1
    assert direct_canonical.linked_event_count == 1
    assert {
        item["event_id"] for item in repository.list_journey("customer-1", limit=10, offset=0)
    } >= {
        "direct-source",
        "direct-canonical",
    }

    _upsert_customer(repository, "customer-2")
    repository.add_identity_source(
        {
            "customer_id": "customer-2",
            "source_system": "salesforce",
            "source_record_id": "003-conflict",
            "normalized_email": None,
            "normalized_phone": None,
            "match_score": 1.0,
            "match_decision": "AUTO_MATCH",
        }
    )
    conflict = EventProcessor(repository).process(
        _event(
            "identify-conflict",
            EventType.IDENTIFY,
            customer_id="customer-1",
            customer_source_system="salesforce",
            customer_source_id="003-conflict",
        )
    )
    assert conflict.disposition is EventDisposition.REVIEW_REQUIRED
    assert conflict.reason == "event contains competing canonical and source customer IDs"


def test_unknown_source_identity_requires_review(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    processor = EventProcessor(repository)
    result = processor.process(
        _event(
            "identify-unknown",
            EventType.IDENTIFY,
            customer_source_system="salesforce",
            customer_source_id="003-unknown",
        )
    )
    assert result.disposition is EventDisposition.REVIEW_REQUIRED
    unknown_canonical = processor.process(
        _event("identify-unknown-canonical", EventType.IDENTIFY, customer_id="missing")
    )
    assert unknown_canonical.disposition is EventDisposition.REVIEW_REQUIRED
    assert unknown_canonical.reason == "explicit canonical customer ID does not exist"

    _upsert_customer(repository, "missing")
    retried = processor.process(
        _event("identify-unknown-canonical", EventType.IDENTIFY, customer_id="missing")
    )
    assert retried.disposition is EventDisposition.ACCEPTED


def test_partial_source_identity_is_rejected() -> None:
    with pytest.raises(ValidationError, match="system and ID"):
        _event(
            "identify-partial",
            EventType.IDENTIFY,
            customer_source_system="salesforce",
        )


def test_naive_event_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        PropertyEvent(
            schema_version=2,
            event_id="naive-event",
            event_type=EventType.VIEW,
            occurred_at=datetime(2026, 8, 24, 1),  # noqa: DTZ001 - intentional contract probe
            received_at=datetime(2026, 8, 24, 1, 0, 1),  # noqa: DTZ001
            session_id="session-naive",
            anonymous_id="anonymous-naive",
        )


def test_exported_event_schema_contains_behavioural_invariants() -> None:
    schema = PropertyEvent.model_json_schema()
    invariants = schema["allOf"]

    assert any("anyOf" in invariant and "if" not in invariant for invariant in invariants)
    assert any(
        invariant.get("if", {}).get("properties", {}).get("event_type", {}).get("const")
        == "IDENTIFY"
        for invariant in invariants
    )
    assert any(
        invariant.get("if", {}).get("properties", {}).get("schema_version", {}).get("const") == 1
        for invariant in invariants
    )


def test_price_is_a_typed_top_level_warehouse_field() -> None:
    event = _event("priced-view", EventType.VIEW)
    assert event.price_observed == 875_000
    assert event.model_dump()["price_observed"] == 875_000
