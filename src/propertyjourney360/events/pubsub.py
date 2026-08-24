"""Pub/Sub boundary adapters with schema validation and explicit acknowledgement rules."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, ValidationError

from propertyjourney360.events.models import PropertyEvent
from propertyjourney360.events.processor import EventDisposition, EventProcessor


class PubSubMessage(Protocol):
    data: bytes
    message_id: str

    def ack(self) -> None: ...

    def nack(self) -> None: ...


class PublishFuture(Protocol):
    def result(self, timeout: float | None = None) -> str: ...


class Publisher(Protocol):
    def publish(self, topic: str, data: bytes, **attributes: str) -> PublishFuture: ...


class MessageDisposition(StrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    DEAD_LETTERED = "DEAD_LETTERED"


class PubSubHandleResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    message_id: str
    event_id: str | None
    disposition: MessageDisposition
    reason: str


class PubSubEventPublisher:
    """Publish only validated event-contract payloads."""

    def __init__(self, *, topic: str, publisher: Publisher | None = None) -> None:
        if publisher is None:
            import google.cloud.pubsub_v1 as pubsub_v1  # type: ignore[import-untyped]

            publisher = pubsub_v1.PublisherClient()
        self.topic = topic
        self.publisher = publisher

    def publish(self, event: PropertyEvent, *, timeout_seconds: float = 30.0) -> str:
        future = self.publisher.publish(
            self.topic,
            event.model_dump_json().encode(),
            schema_version=str(event.schema_version),
            event_type=event.event_type.value,
            event_id=event.event_id,
        )
        return future.result(timeout=timeout_seconds)


class PubSubEventHandler:
    """Validate before processing; acknowledge only after a durable disposition."""

    def __init__(
        self,
        *,
        processor: EventProcessor,
        dead_letter_topic: str,
        publisher: Publisher | None = None,
        publish_timeout_seconds: float = 30.0,
    ) -> None:
        if publisher is None:
            import google.cloud.pubsub_v1 as pubsub_v1

            publisher = pubsub_v1.PublisherClient()
        self.processor = processor
        self.dead_letter_topic = dead_letter_topic
        self.publisher = publisher
        self.publish_timeout_seconds = publish_timeout_seconds

    def handle(self, message: PubSubMessage) -> PubSubHandleResult:
        try:
            event = PropertyEvent.model_validate_json(message.data)
        except (ValidationError, ValueError) as exc:
            try:
                future = self.publisher.publish(
                    self.dead_letter_topic,
                    message.data,
                    original_message_id=message.message_id,
                    failure_class=type(exc).__name__,
                    failure_reason="EVENT_SCHEMA_VALIDATION_FAILED",
                )
                future.result(timeout=self.publish_timeout_seconds)
            except Exception:
                message.nack()
                raise
            message.ack()
            return PubSubHandleResult(
                message_id=message.message_id,
                event_id=None,
                disposition=MessageDisposition.DEAD_LETTERED,
                reason="invalid event was durably forwarded to the dead-letter topic",
            )

        try:
            result = self.processor.process(event)
        except Exception:
            message.nack()
            raise
        message.ack()
        mapping = {
            EventDisposition.ACCEPTED: MessageDisposition.ACCEPTED,
            EventDisposition.DUPLICATE: MessageDisposition.DUPLICATE,
            EventDisposition.REVIEW_REQUIRED: MessageDisposition.REVIEW_REQUIRED,
        }
        return PubSubHandleResult(
            message_id=message.message_id,
            event_id=event.event_id,
            disposition=mapping[result.disposition],
            reason=result.reason,
        )


def google_subscriber_callback(handler: PubSubEventHandler) -> Any:
    """Return a callback suitable for SubscriberClient.subscribe."""

    def callback(message: PubSubMessage) -> None:
        handler.handle(message)

    return callback
