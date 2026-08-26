from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.events.models import EventType, PropertyEvent
from propertyjourney360.events.processor import EventProcessor
from propertyjourney360.events.pubsub import (
    MessageDisposition,
    PubSubEventHandler,
    PubSubEventPublisher,
)


class FakeFuture:
    def __init__(self, value: str = "published-1", error: Exception | None = None) -> None:
        self.value = value
        self.error = error

    def result(self, timeout: float | None = None) -> str:
        assert timeout is not None
        if self.error:
            raise self.error
        return self.value


class FakePublisher:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.error = error

    def publish(self, topic: str, data: bytes, **attributes: str) -> FakeFuture:
        self.calls.append({"topic": topic, "data": data, "attributes": attributes})
        return FakeFuture(error=self.error)


class FakeMessage:
    def __init__(self, data: bytes, message_id: str = "message-1") -> None:
        self.data = data
        self.message_id = message_id
        self.acked = False
        self.nacked = False

    def ack(self) -> None:
        self.acked = True

    def nack(self) -> None:
        self.nacked = True


def _event(event_id: str = "event-1") -> PropertyEvent:
    occurred_at = datetime(2026, 8, 24, tzinfo=UTC)
    return PropertyEvent(
        schema_version=2,
        event_id=event_id,
        event_type=EventType.VIEW,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=1),
        session_id="session-1",
        anonymous_id="anonymous-1",
        property_id="property-1",
        device_type="mobile",
    )


def test_valid_event_is_processed_before_ack(tmp_path: Path) -> None:
    publisher = FakePublisher()
    handler = PubSubEventHandler(
        processor=EventProcessor(CustomerRepository(tmp_path / "events.db")),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=publisher,
    )
    message = FakeMessage(_event().model_dump_json().encode())

    result = handler.handle(message)

    assert result.disposition is MessageDisposition.ACCEPTED
    assert message.acked
    assert not message.nacked
    assert not publisher.calls


def test_duplicate_delivery_is_acknowledged_idempotently(tmp_path: Path) -> None:
    handler = PubSubEventHandler(
        processor=EventProcessor(CustomerRepository(tmp_path / "events.db")),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=FakePublisher(),
    )
    assert handler.handle(FakeMessage(_event().model_dump_json().encode())).disposition is (
        MessageDisposition.ACCEPTED
    )
    duplicate = FakeMessage(_event().model_dump_json().encode(), "message-2")
    assert handler.handle(duplicate).disposition is MessageDisposition.DUPLICATE
    assert duplicate.acked


def test_divergent_event_id_is_dead_lettered_without_overwrite(tmp_path: Path) -> None:
    publisher = FakePublisher()
    handler = PubSubEventHandler(
        processor=EventProcessor(CustomerRepository(tmp_path / "events.db")),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=publisher,
    )
    assert handler.handle(FakeMessage(_event().model_dump_json().encode())).disposition is (
        MessageDisposition.ACCEPTED
    )
    divergent = _event().model_copy(update={"price_observed": 123_000})
    message = FakeMessage(divergent.model_dump_json().encode(), "message-collision")

    result = handler.handle(message)

    assert result.disposition is MessageDisposition.DEAD_LETTERED
    assert message.acked
    assert publisher.calls[-1]["attributes"]["failure_reason"] == "EVENT_ID_COLLISION"


def test_identity_review_is_dead_lettered_then_can_be_reprocessed(tmp_path: Path) -> None:
    repository = CustomerRepository(tmp_path / "events.db")
    publisher = FakePublisher()
    handler = PubSubEventHandler(
        processor=EventProcessor(repository),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=publisher,
    )
    unresolved = _event("review-event").model_copy(
        update={"anonymous_id": None, "customer_id": "customer-later"}
    )
    first = FakeMessage(unresolved.model_dump_json().encode(), "message-review")

    result = handler.handle(first)

    assert result.disposition is MessageDisposition.REVIEW_REQUIRED
    assert first.acked
    assert publisher.calls[-1]["attributes"]["failure_reason"] == ("EVENT_IDENTITY_REVIEW_REQUIRED")
    with repository.connect() as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM property_event WHERE event_id = ?", (unresolved.event_id,)
            ).fetchone()
            is None
        )

    repository.upsert_customer(
        {
            "customer_id": "customer-later",
            "display_name": "Synthetic Later",
            "preferred_suburbs": "[]",
            "observed_price_band": None,
            "identity_rule_version": "identity-v1.1.0",
            "as_of": "2026-08-24T00:00:00+00:00",
        }
    )
    second = FakeMessage(unresolved.model_dump_json().encode(), "message-replay")
    assert handler.handle(second).disposition is MessageDisposition.ACCEPTED
    assert second.acked


def test_invalid_event_is_dead_lettered_before_ack(tmp_path: Path) -> None:
    publisher = FakePublisher()
    handler = PubSubEventHandler(
        processor=EventProcessor(CustomerRepository(tmp_path / "events.db")),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=publisher,
    )
    message = FakeMessage(b'{"schema_version":99,"event_id":"bad"}')

    result = handler.handle(message)

    assert result.disposition is MessageDisposition.DEAD_LETTERED
    assert message.acked
    assert not message.nacked
    assert publisher.calls[0]["attributes"]["failure_reason"] == ("EVENT_SCHEMA_VALIDATION_FAILED")


def test_failed_dead_letter_publish_nacks_for_retry(tmp_path: Path) -> None:
    handler = PubSubEventHandler(
        processor=EventProcessor(CustomerRepository(tmp_path / "events.db")),
        dead_letter_topic="projects/test/topics/dlq",
        publisher=FakePublisher(error=RuntimeError("publish failed")),
    )
    message = FakeMessage(b"not-json")

    with pytest.raises(RuntimeError, match="publish failed"):
        handler.handle(message)
    assert message.nacked
    assert not message.acked


def test_publisher_emits_contract_attributes() -> None:
    client = FakePublisher()
    publisher = PubSubEventPublisher(topic="projects/test/topics/events", publisher=client)

    assert publisher.publish(_event()) == "published-1"
    assert client.calls[0]["attributes"] == {
        "schema_version": "2",
        "event_type": "VIEW",
        "event_id": "event-1",
    }
