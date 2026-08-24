from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from google.cloud import bigquery, pubsub_v1, storage

from propertyjourney360.connectors.salesforce import SalesforceOAuthConfig
from propertyjourney360.events.models import EventType, PropertyEvent
from propertyjourney360.ingestion import operator as operator_module
from propertyjourney360.ingestion.landing import LocalRawLanding
from propertyjourney360.ingestion.operator import (
    BatchIngestionOperator,
    EventIngestionOperator,
    EventPullSource,
    FrozenWindow,
    OperatorState,
    PulledEvent,
    frozen_window_from_values,
)
from propertyjourney360.ingestion.raw_contracts import RAW_CONTRACTS, SOURCE_ASSETS
from propertyjourney360.ingestion.watermarks import WatermarkStore
from propertyjourney360.public_data.fetch import PublicFetchManifest


class FakeLoadResult:
    def __init__(self, *, job_id: str, output_rows: int) -> None:
        self.job_id = job_id
        self.output_rows = output_rows


class FakeBigQueryLoader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def load(self, manifest: Any, *, table: str) -> FakeLoadResult:
        self.calls.append((manifest.object_path, table))
        return FakeLoadResult(job_id=f"job-{len(self.calls)}", output_rows=manifest.record_count)


class FakePullSource(EventPullSource):
    def __init__(self, messages: tuple[PulledEvent, ...]) -> None:
        self.messages = messages
        self.dead_letters: list[str] = []
        self.acknowledgements: list[tuple[str, ...]] = []

    def pull(self, *, max_messages: int) -> tuple[PulledEvent, ...]:
        return self.messages[:max_messages]

    def dead_letter(self, message: PulledEvent, *, reason: str) -> None:
        self.dead_letters.append(f"{message.message_id}:{reason}")

    def acknowledge(self, ack_ids: tuple[str, ...] | list[str]) -> None:
        self.acknowledgements.append(tuple(ack_ids))


class FailingAckSource(FakePullSource):
    def __init__(self, messages: tuple[PulledEvent, ...]) -> None:
        super().__init__(messages)
        self.fail_valid_ack_once = True

    def acknowledge(self, ack_ids: tuple[str, ...] | list[str]) -> None:
        if ack_ids and self.fail_valid_ack_once:
            self.fail_valid_ack_once = False
            raise RuntimeError("simulated ACK interruption")
        super().acknowledge(ack_ids)


class SequencedAckRecoverySource(FakePullSource):
    def __init__(self, pulls: tuple[tuple[PulledEvent, ...], ...]) -> None:
        super().__init__(pulls[0])
        self.pulls = list(pulls)
        self.fail_valid_ack_once = True

    def pull(self, *, max_messages: int) -> tuple[PulledEvent, ...]:
        return self.pulls.pop(0)[:max_messages]

    def acknowledge(self, ack_ids: tuple[str, ...] | list[str]) -> None:
        if ack_ids and self.fail_valid_ack_once:
            self.fail_valid_ack_once = False
            raise RuntimeError("simulated ACK interruption")
        super().acknowledge(ack_ids)


class FailingMarkerState(OperatorState):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.fail_once = True

    def mark_event_acknowledged(self, *, window_id: str, manifest_sha256: str) -> None:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("simulated marker interruption")
        super().mark_event_acknowledged(
            window_id=window_id,
            manifest_sha256=manifest_sha256,
        )


class FailingInitialProgressState(OperatorState):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.fail_once = True

    def record_event_ack_progress(self, **kwargs: Any) -> Any:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("simulated progress interruption")
        return super().record_event_ack_progress(**kwargs)


def _window(*, mode: str = "INCREMENTAL", replay_id: str | None = None) -> FrozenWindow:
    return FrozenWindow(
        window_start=datetime(2026, 8, 23, tzinfo=UTC),
        window_end=datetime(2026, 8, 24, tzinfo=UTC),
        window_id="a" * 64 if replay_id is None else "b" * 64,
        mode=mode,
        replay_id=replay_id,
    )


def test_cloud_operator_builders_use_purpose_specific_impersonated_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ingest_credentials = object()
    event_credentials = object()
    requested_accounts: list[str] = []
    client_calls: list[tuple[str, object, str | None]] = []

    def fake_credentials(variable: str) -> object:
        requested_accounts.append(variable)
        return {
            "PJ360_INGEST_SERVICE_ACCOUNT": ingest_credentials,
            "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT": event_credentials,
        }[variable]

    def fake_storage_client(*, project: str | None, credentials: object) -> object:
        client_calls.append(("storage", credentials, project))
        return ("storage", credentials)

    def fake_bigquery_client(*, project: str, credentials: object) -> object:
        client_calls.append(("bigquery", credentials, project))
        return ("bigquery", credentials)

    def fake_subscriber_client(*, credentials: object) -> object:
        client_calls.append(("subscriber", credentials, None))
        return ("subscriber", credentials)

    def fake_publisher_client(*, credentials: object) -> object:
        client_calls.append(("publisher", credentials, None))
        return ("publisher", credentials)

    monkeypatch.setattr(
        operator_module, "impersonated_credentials_from_environment", fake_credentials
    )
    monkeypatch.setattr(storage, "Client", fake_storage_client)
    monkeypatch.setattr(bigquery, "Client", fake_bigquery_client)
    monkeypatch.setattr(pubsub_v1, "SubscriberClient", fake_subscriber_client)
    monkeypatch.setattr(pubsub_v1, "PublisherClient", fake_publisher_client)
    for name, value in {
        "PJ360_INGEST_STATE_DIR": str(tmp_path / "state"),
        "GCS_RAW_BUCKET": "raw-bucket",
        "BIGQUERY_PROJECT": "example-project",
        "BIGQUERY_RAW_DATASET": "raw",
        "PUBSUB_EVENT_SUBSCRIPTION": "projects/example-project/subscriptions/events",
        "PUBSUB_DEAD_LETTER_TOPIC": "projects/example-project/topics/events-dlq",
    }.items():
        monkeypatch.setenv(name, value)

    batch = operator_module.build_operator_from_environment()
    event = operator_module.build_event_operator_from_environment()

    assert requested_accounts == [
        "PJ360_INGEST_SERVICE_ACCOUNT",
        "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT",
    ]
    assert batch.landing.client == ("storage", ingest_credentials)
    assert batch.bigquery_loader is not None
    assert batch.bigquery_loader.client == ("bigquery", ingest_credentials)
    assert event.landing.client == ("storage", event_credentials)
    assert event.bigquery_loader is not None
    assert event.bigquery_loader.client == ("bigquery", event_credentials)
    assert isinstance(event.pull_source, operator_module.GooglePubSubPullSource)
    assert event.pull_source.subscriber == ("subscriber", event_credentials)
    assert event.pull_source.publisher == ("publisher", event_credentials)
    assert client_calls == [
        ("storage", ingest_credentials, "example-project"),
        ("bigquery", ingest_credentials, "example-project"),
        ("storage", event_credentials, "example-project"),
        ("subscriber", event_credentials, None),
        ("publisher", event_credentials, None),
        ("bigquery", event_credentials, "example-project"),
    ]


@pytest.mark.parametrize(
    ("builder", "configured_environment", "required_variable"),
    [
        (
            operator_module.build_operator_from_environment,
            {"GCS_RAW_BUCKET": "raw-bucket"},
            "PJ360_INGEST_SERVICE_ACCOUNT",
        ),
        (
            operator_module.build_event_operator_from_environment,
            {
                "PUBSUB_EVENT_SUBSCRIPTION": "projects/example/subscriptions/events",
                "PUBSUB_DEAD_LETTER_TOPIC": "projects/example/topics/dlq",
            },
            "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT",
        ),
    ],
)
def test_cloud_operator_builders_fail_closed_without_purpose_specific_account(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    builder: Any,
    configured_environment: dict[str, str],
    required_variable: str,
) -> None:
    for variable in (
        "GCS_RAW_BUCKET",
        "BIGQUERY_PROJECT",
        "GCP_PROJECT_ID",
        "BIGQUERY_RAW_DATASET",
        "DBT_BIGQUERY_RAW_DATASET",
        "PUBSUB_EVENT_SUBSCRIPTION",
        "PUBSUB_DEAD_LETTER_TOPIC",
        "PUBSUB_EMULATOR_HOST",
        "PJ360_INGEST_SERVICE_ACCOUNT",
        "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT",
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("PJ360_INGEST_STATE_DIR", str(tmp_path / "state"))
    for variable, value in configured_environment.items():
        monkeypatch.setenv(variable, value)

    with pytest.raises(RuntimeError, match=required_variable):
        builder()


def test_batch_operator_extract_load_and_commit_are_replay_safe(tmp_path: Path) -> None:
    state = OperatorState(tmp_path / "state")
    loader = FakeBigQueryLoader()

    def extract_legacy(_window: FrozenWindow) -> dict[str, tuple[dict[str, Any], ...]]:
        return {
            "raw_legacy_customers": (
                {
                    "customer_id": "LEG-1",
                    "first_name": "Synthetic",
                    "updated_at": "2026-08-24T00:00:00+00:00",
                },
            )
        }

    operator = BatchIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"legacy_crm": extract_legacy},
        bigquery_loader=loader,  # type: ignore[arg-type]
        bigquery_project="pj360-test",
        bigquery_raw_dataset="pj360_raw_test",
    )
    window = _window()
    first = operator.extract(source="legacy_crm", window=window)
    second = operator.extract(source="legacy_crm", window=window)
    assert first == second
    assert first.assets[0].manifest.record_count == 1

    loaded = operator.load_raw(source="legacy_crm", window=window)
    assert loaded.assets[0].status == "LOADED"
    assert len(loader.calls) == 1
    assert operator.load_raw(source="legacy_crm", window=window) == loaded
    assert len(loader.calls) == 1
    conflicting_window = window.model_copy(
        update={"window_start": window.window_start - timedelta(hours=1)}
    )
    with pytest.raises(RuntimeError, match="immutable extract window"):
        operator.load_raw(source="legacy_crm", window=conflicting_window)

    committed = operator.commit_watermarks(sources=["legacy_crm"], window=window)
    assert committed.committed_sources == ("legacy_crm",)
    watermark_before_retry = WatermarkStore(tmp_path / "state" / "watermarks.db").get(
        source="legacy_crm", stream="raw_legacy_customers"
    )
    assert operator.commit_watermarks(sources=["legacy_crm"], window=window) == committed
    assert (
        WatermarkStore(tmp_path / "state" / "watermarks.db").get(
            source="legacy_crm", stream="raw_legacy_customers"
        )
        == watermark_before_retry
    )
    assert (tmp_path / "state" / "watermarks.db").exists()


def test_incremental_extract_resumes_from_committed_multi_asset_watermark(tmp_path: Path) -> None:
    seen: list[FrozenWindow] = []

    def extract_property(window: FrozenWindow) -> dict[str, tuple[dict[str, Any], ...]]:
        seen.append(window)
        return {"raw_property_master": (), "raw_property_market": ()}

    operator = BatchIngestionOperator(
        state=OperatorState(tmp_path / "state"),
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"property": extract_property},
    )
    store = WatermarkStore(tmp_path / "state" / "watermarks.db")
    committed = "2026-08-20T00:00:00+00:00"
    for asset in SOURCE_ASSETS["property"]:
        store.advance(source="property", stream=asset, value=committed, run_id="prior")
    requested = FrozenWindow(
        window_start=datetime(2026, 8, 23, tzinfo=UTC),
        window_end=datetime(2026, 8, 24, tzinfo=UTC),
        window_id="c" * 64,
        mode="INCREMENTAL",
    )
    report = operator.extract(source="property", window=requested)
    assert seen[0].window_start == datetime(2026, 8, 20, tzinfo=UTC)
    assert report.window == requested
    assert report.effective_window_source == "COMMITTED_WATERMARK"


def test_incremental_extract_rejects_partial_multi_asset_watermark(tmp_path: Path) -> None:
    operator = BatchIngestionOperator(
        state=OperatorState(tmp_path / "state"),
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={
            "property": lambda _window: {"raw_property_master": (), "raw_property_market": ()}
        },
    )
    WatermarkStore(tmp_path / "state" / "watermarks.db").advance(
        source="property",
        stream="raw_property_master",
        value="2026-08-20T00:00:00+00:00",
        run_id="prior",
    )
    with pytest.raises(RuntimeError, match="partial committed watermark"):
        operator.extract(source="property", window=_window())


def test_batch_replay_reuses_original_manifest_without_reextracting(tmp_path: Path) -> None:
    calls = 0

    def extract_legacy(_window: FrozenWindow) -> dict[str, tuple[dict[str, Any], ...]]:
        nonlocal calls
        calls += 1
        return {
            "raw_legacy_customers": (
                {
                    "customer_id": "LEG-REPLAY",
                    "first_name": "Original",
                    "updated_at": "2026-08-24T00:00:00+00:00",
                },
            )
        }

    state = OperatorState(tmp_path / "state")
    operator = BatchIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"legacy_crm": extract_legacy},
    )
    original_window = _window()
    original = operator.extract(source="legacy_crm", window=original_window)
    replay_window = FrozenWindow(
        window_start=original_window.window_start,
        window_end=original_window.window_end,
        window_id="b" * 64,
        mode="REPLAY",
        replay_id=original_window.window_id,
    )

    replay = operator.extract(source="legacy_crm", window=replay_window)
    replay_again = operator.extract(source="legacy_crm", window=replay_window)

    assert calls == 1
    assert replay == replay_again
    assert replay.assets == original.assets
    assert replay.effective_window_source == "REPLAY_MANIFEST"
    assert replay.replayed_from_window_id == original_window.window_id


def test_every_source_contract_has_a_complete_asset_mapping() -> None:
    mapped_assets = {asset for assets in SOURCE_ASSETS.values() for asset in assets}
    mapped_assets.add("raw_property_events")
    assert mapped_assets == set(RAW_CONTRACTS)
    assert {"price_observed", "device_type", "attributes", "occurred_at", "received_at"} <= set(
        RAW_CONTRACTS["raw_property_events"].fields
    )


def test_event_operator_dead_letters_then_lands_before_ack_and_loads(tmp_path: Path) -> None:
    occurred_at = datetime(2026, 8, 23, 12, tzinfo=UTC)
    event = PropertyEvent(
        schema_version=2,
        event_id="event-1",
        event_type=EventType.VIEW,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=1),
        session_id="session-1",
        anonymous_id="anonymous-1",
        property_id="property-1",
        price_observed=910_000,
        device_type="mobile",
    )
    pull_source = FakePullSource(
        (
            PulledEvent(
                ack_id="ack-1", message_id="message-1", data=event.model_dump_json().encode()
            ),
            PulledEvent(
                ack_id="ack-2", message_id="message-2", data=event.model_dump_json().encode()
            ),
            PulledEvent(ack_id="ack-3", message_id="message-3", data=b'{"bad":true}'),
            PulledEvent(
                ack_id="ack-4",
                message_id="message-4",
                data=event.model_copy(
                    update={"event_id": "event-collision", "property_id": "property-1"}
                )
                .model_dump_json()
                .encode(),
            ),
            PulledEvent(
                ack_id="ack-5",
                message_id="message-5",
                data=event.model_copy(
                    update={"event_id": "event-collision", "property_id": "property-2"}
                )
                .model_dump_json()
                .encode(),
            ),
        )
    )
    loader = FakeBigQueryLoader()
    operator = EventIngestionOperator(
        state=OperatorState(tmp_path / "state"),
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=pull_source,
        bigquery_loader=loader,  # type: ignore[arg-type]
        bigquery_project="pj360-test",
        bigquery_raw_dataset="pj360_raw_test",
    )
    window = _window(mode="LIVE")

    consumed = operator.consume(window=window, max_messages=10)
    assert consumed.received_count == 5
    assert consumed.accepted_count == 1
    assert consumed.duplicate_count == 1
    assert consumed.dead_lettered_count == 3
    assert pull_source.dead_letters == [
        "message-3:EVENT_SCHEMA_VALIDATION_FAILED",
        "message-4:EVENT_ID_COLLISION",
        "message-5:EVENT_ID_COLLISION",
    ]
    assert pull_source.acknowledgements == [("ack-3", "ack-4", "ack-5"), ("ack-1", "ack-2")]
    assert consumed.accepted_event_fingerprints == {
        "event-1": EventIngestionOperator._event_fingerprint(event)
    }
    raw_record = json.loads((tmp_path / "landing" / consumed.manifest.object_path).read_text())
    assert raw_record["price_observed"] == 910_000
    assert raw_record["device_type"] == "mobile"
    assert raw_record["received_at"].endswith("+00:00")

    loaded = operator.load_validated(window=window)
    assert loaded.assets[0].record_count == 1
    conflicting_window = window.model_copy(
        update={"window_start": window.window_start - timedelta(minutes=5)}
    )
    with pytest.raises(RuntimeError, match="immutable consume window"):
        operator.load_validated(window=conflicting_window)
    checkpoint = operator.commit_checkpoint(window=window)
    assert checkpoint.load_status == "LOADED"
    assert operator.commit_checkpoint(window=window) == checkpoint


def test_live_event_ack_recovers_from_state_after_interruption(tmp_path: Path) -> None:
    event = _event("ack-recovery", datetime(2026, 8, 23, 12, tzinfo=UTC))
    pull_source = FailingAckSource(
        (
            PulledEvent(
                ack_id="ack-recovery",
                message_id="message-recovery",
                data=event.model_dump_json().encode(),
            ),
        )
    )
    state = OperatorState(tmp_path / "state")
    operator = EventIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=pull_source,
    )
    window = _window(mode="LIVE")
    with pytest.raises(RuntimeError, match="ACK interruption"):
        operator.consume(window=window, max_messages=10)
    assert state.read_event_consume(window_id=window.window_id).accepted_event_ids == (
        "ack-recovery",
    )
    progress = state.read_event_ack_progress(window_id=window.window_id)
    assert progress is None
    assert not state.event_acknowledged(window_id=window.window_id)

    recovered = operator.consume(window=window, max_messages=10)
    assert recovered.accepted_count == 1
    assert state.event_acknowledged(window_id=window.window_id)
    assert pull_source.acknowledgements[-1] == ("ack-recovery",)


def test_live_event_ack_recovery_persists_partial_progress(tmp_path: Path) -> None:
    event_a = _event("ack-a", datetime(2026, 8, 23, 12, tzinfo=UTC))
    event_b = _event("ack-b", datetime(2026, 8, 23, 13, tzinfo=UTC))
    message_a = PulledEvent(
        ack_id="ack-a", message_id="message-a", data=event_a.model_dump_json().encode()
    )
    message_b = PulledEvent(
        ack_id="ack-b", message_id="message-b", data=event_b.model_dump_json().encode()
    )
    source = SequencedAckRecoverySource(((message_a, message_b), (message_a,), (message_b,)))
    state = OperatorState(tmp_path / "state")
    operator = EventIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=source,
    )
    window = _window(mode="LIVE")

    with pytest.raises(RuntimeError, match="ACK interruption"):
        operator.consume(window=window, max_messages=10)
    assert state.read_event_ack_progress(window_id=window.window_id) is None
    recovered = operator.consume(window=window, max_messages=10)
    assert recovered.accepted_count == 2
    progress = state.read_event_ack_progress(window_id=window.window_id)
    assert progress is not None
    assert set(progress.recovered_event_fingerprints) == {"ack-a"}
    assert state.event_acknowledged(window_id=window.window_id)

    next_window = window.model_copy(update={"window_id": "c" * 64})
    later_duplicate = operator.consume(window=next_window, max_messages=10)
    assert later_duplicate.accepted_event_ids == ("ack-b",)
    assert [acknowledgement for acknowledgement in source.acknowledgements if acknowledgement][
        -2:
    ] == [("ack-a",), ("ack-b",)]


def test_live_event_ack_recovery_finalizes_when_ack_succeeded_before_progress_crash(
    tmp_path: Path,
) -> None:
    event = _event("ack-then-progress", datetime(2026, 8, 23, 12, tzinfo=UTC))
    message = PulledEvent(
        ack_id="ack-success", message_id="message-success", data=event.model_dump_json().encode()
    )
    source = SequencedAckRecoverySource(((message,), ()))
    source.fail_valid_ack_once = False
    state = FailingInitialProgressState(tmp_path / "state")
    operator = EventIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=source,
    )
    window = _window(mode="LIVE")

    with pytest.raises(RuntimeError, match="progress interruption"):
        operator.consume(window=window, max_messages=10)
    assert source.acknowledgements[-1] == ("ack-success",)
    assert state.read_event_ack_progress(window_id=window.window_id) is None
    assert not state.event_acknowledged(window_id=window.window_id)

    recovered = operator.consume(window=window, max_messages=10)
    assert recovered.accepted_event_ids == ("ack-then-progress",)
    assert state.event_acknowledged(window_id=window.window_id)


def test_live_event_ack_recovery_restores_marker_after_ack_succeeded(tmp_path: Path) -> None:
    event = _event("marker-recovery", datetime(2026, 8, 23, 12, tzinfo=UTC))
    message = PulledEvent(
        ack_id="marker-ack",
        message_id="marker-message",
        data=event.model_dump_json().encode(),
    )
    source = SequencedAckRecoverySource(((message,), ()))
    source.fail_valid_ack_once = False
    state = FailingMarkerState(tmp_path / "state")
    operator = EventIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=source,
    )
    window = _window(mode="LIVE")

    with pytest.raises(RuntimeError, match="marker interruption"):
        operator.consume(window=window, max_messages=10)
    assert source.acknowledgements[-1] == ("marker-ack",)
    assert not state.event_acknowledged(window_id=window.window_id)

    operator.consume(window=window, max_messages=10)
    assert state.event_acknowledged(window_id=window.window_id)


def test_frozen_window_validation_is_fail_closed() -> None:
    parsed = frozen_window_from_values(
        window_start="2026-08-23T00:00:00Z",
        window_end="2026-08-24T00:00:00+00:00",
        window_id="c" * 64,
        mode="incremental",
        replay_id=None,
    )
    assert parsed.mode == "INCREMENTAL"
    with pytest.raises(ValueError, match="ISO-8601"):
        frozen_window_from_values(
            window_start="bad",
            window_end="2026-08-24T00:00:00Z",
            window_id="c" * 64,
            mode="INCREMENTAL",
            replay_id=None,
        )
    with pytest.raises(ValueError, match="timezone"):
        frozen_window_from_values(
            window_start="2026-08-23T00:00:00",
            window_end="2026-08-24T00:00:00Z",
            window_id="c" * 64,
            mode="INCREMENTAL",
            replay_id=None,
        )
    with pytest.raises(ValueError, match="later"):
        FrozenWindow(
            window_start=datetime(2026, 8, 24, tzinfo=UTC),
            window_end=datetime(2026, 8, 23, tzinfo=UTC),
            window_id="c" * 64,
            mode="INCREMENTAL",
        )
    with pytest.raises(ValueError, match="requires replay_id"):
        _window(mode="REPLAY")
    with pytest.raises(ValueError, match="unsupported characters"):
        _window(mode="REPLAY", replay_id="bad value")
    with pytest.raises(ValueError, match="Input should be"):
        _window(mode="INCREMENTL")
    with pytest.raises(ValueError, match="unsupported batch mode"):
        BatchIngestionOperator(
            state=OperatorState(Path("unused")),
            landing=LocalRawLanding(Path("unused")),
            extractors={},
        ).extract(source="legacy_crm", window=_window(mode="LIVE"))
    with pytest.raises(ValueError, match="unsupported event mode"):
        EventIngestionOperator(
            state=OperatorState(Path("unused")),
            landing=LocalRawLanding(Path("unused")),
            pull_source=None,
        ).consume(window=_window(mode="INCREMENTAL"), max_messages=1)


def test_batch_operator_blocks_incomplete_or_unloadable_runs(tmp_path: Path) -> None:
    state = OperatorState(tmp_path / "state")
    operator = BatchIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"property": lambda _window: {"raw_property_master": ()}},
    )
    with pytest.raises(ValueError, match="unsupported"):
        operator.extract(source="unknown", window=_window())
    with pytest.raises(RuntimeError, match="complete RAW asset"):
        operator.extract(source="property", window=_window())
    with pytest.raises(FileNotFoundError, match="extract report"):
        operator.load_raw(source="legacy_crm", window=_window())

    valid = BatchIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"legacy_crm": lambda _window: {"raw_legacy_customers": ()}},
    )
    valid.extract(source="legacy_crm", window=_window())
    with pytest.raises(RuntimeError, match="BigQuery loader"):
        valid.load_raw(source="legacy_crm", window=_window())
    with pytest.raises(ValueError, match="unsupported watermark"):
        valid.commit_watermarks(sources=["unknown"], window=_window())


def test_batch_operator_empty_backfill_skips_watermark(tmp_path: Path) -> None:
    operator = BatchIngestionOperator(
        state=OperatorState(tmp_path / "state"),
        landing=LocalRawLanding(tmp_path / "landing"),
        extractors={"legacy_crm": lambda _window: {"raw_legacy_customers": ()}},
        bigquery_loader=FakeBigQueryLoader(),  # type: ignore[arg-type]
        bigquery_project="pj360-test",
        bigquery_raw_dataset="raw",
    )
    window = _window(mode="BACKFILL")
    operator.extract(source="legacy_crm", window=window)
    loaded = operator.load_raw(source="legacy_crm", window=window)
    assert loaded.assets[0].status == "EMPTY"
    report = operator.commit_watermarks(sources=["legacy_crm", "legacy_crm"], window=window)
    assert report.committed_sources == ()
    assert report.skipped_sources == ("legacy_crm",)


def test_operator_state_detects_immutable_report_conflicts(tmp_path: Path) -> None:
    state = OperatorState(tmp_path)
    state.write_report(window_id="a" * 64, source="source", phase="extract", payload="one")
    state.write_report(window_id="a" * 64, source="source", phase="extract", payload="one")
    with pytest.raises(RuntimeError, match="conflicts"):
        state.write_report(window_id="a" * 64, source="source", phase="extract", payload="two")
    with pytest.raises(ValueError, match="unsafe"):
        state.write_report(window_id="bad/path", source="source", phase="extract", payload="x")
    with pytest.raises(FileNotFoundError, match="load report"):
        state.read_load(window_id="a" * 64, source="source")
    with pytest.raises(FileNotFoundError, match="event consume"):
        state.read_event_consume(window_id="a" * 64)
    with pytest.raises(FileNotFoundError, match="watermark commit"):
        state.read_watermark_commit(window_id="a" * 64)
    with pytest.raises(FileNotFoundError, match="event checkpoint"):
        state.read_event_checkpoint(window_id="a" * 64)


def _event(event_id: str, occurred_at: datetime) -> PropertyEvent:
    return PropertyEvent(
        schema_version=2,
        event_id=event_id,
        event_type=EventType.VIEW,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=1),
        session_id=f"session-{event_id}",
        anonymous_id=f"anonymous-{event_id}",
        property_id="property-1",
        price_observed=800_000,
        device_type="mobile",
    )


def test_event_backfill_replay_and_precondition_failures(tmp_path: Path) -> None:
    backfill_path = tmp_path / "events.jsonl"
    backfill_path.write_text(
        "\n".join(
            (
                _event("inside", datetime(2026, 8, 23, 12, tzinfo=UTC)).model_dump_json(),
                _event("outside", datetime(2026, 8, 22, 12, tzinfo=UTC)).model_dump_json(),
                "",
            )
        )
    )
    state = OperatorState(tmp_path / "state")
    operator = EventIngestionOperator(
        state=state,
        landing=LocalRawLanding(tmp_path / "landing"),
        pull_source=None,
        bigquery_loader=FakeBigQueryLoader(),  # type: ignore[arg-type]
        bigquery_project="pj360-test",
        bigquery_raw_dataset="raw",
        backfill_path=backfill_path,
    )
    backfill = _window(mode="BACKFILL")
    consumed = operator.consume(window=backfill, max_messages=10)
    assert consumed.received_count == 1
    assert operator.load_validated(window=backfill).assets[0].status == "LOADED"
    assert operator.commit_checkpoint(window=backfill).load_status == "LOADED"

    with pytest.raises(ValueError, match="between 1 and 50000"):
        operator.consume(window=_window(mode="LIVE"), max_messages=0)
    live = EventIngestionOperator(
        state=OperatorState(tmp_path / "no-pull-state"),
        landing=LocalRawLanding(tmp_path / "no-pull-landing"),
        pull_source=None,
    )
    with pytest.raises(RuntimeError, match="Pub/Sub"):
        live.consume(window=_window(mode="LIVE"), max_messages=1)
    missing_backfill = EventIngestionOperator(
        state=OperatorState(tmp_path / "missing-state"),
        landing=LocalRawLanding(tmp_path / "missing-landing"),
        pull_source=None,
    )
    with pytest.raises(RuntimeError, match="BACKFILL"):
        missing_backfill.consume(window=backfill, max_messages=1)

    original_state = OperatorState(tmp_path / "replay-state")
    original = EventIngestionOperator(
        state=original_state,
        landing=LocalRawLanding(tmp_path / "replay-landing"),
        pull_source=FakePullSource(
            (
                PulledEvent(
                    ack_id="ack",
                    message_id="message",
                    data=_event("replay", datetime(2026, 8, 23, 12, tzinfo=UTC))
                    .model_dump_json()
                    .encode(),
                ),
            )
        ),
    )
    original.consume(window=_window(mode="LIVE"), max_messages=1)
    replay_window = FrozenWindow(
        window_start=datetime(2026, 8, 23, tzinfo=UTC),
        window_end=datetime(2026, 8, 24, tzinfo=UTC),
        window_id="b" * 64,
        mode="REPLAY",
        replay_id="a" * 64,
    )
    replayed = original.consume(window=replay_window, max_messages=1)
    assert replayed.accepted_count == 1


def _write_public_payload(
    root: Path, source_id: str, filename: str, content: str, retrieved_at: str
) -> Path:
    digest = hashlib.sha256(content.encode()).hexdigest()
    payload = root / source_id / "retrieved_date=2026-08-24" / f"{digest}{Path(filename).suffix}"
    payload.parent.mkdir(parents=True, exist_ok=True)
    payload.write_text(content)
    manifest = payload.with_suffix(".manifest.json")
    manifest.write_text(
        PublicFetchManifest(
            source_id=source_id,
            publisher="Test publisher",
            dataset_title="Test dataset",
            retrieved_at=datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")),
            requested_url="https://example.test/request",
            final_url="https://example.test/final",
            status_code=200,
            content_type="text/plain",
            etag=None,
            last_modified=None,
            byte_count=len(content.encode()),
            sha256=digest,
            object_path=str(payload.relative_to(root)),
            response_record_count=None,
            licence_name="Test licence",
            licence_url=None,
            attribution_text=None,
            redistribution_status="ALLOWED",
            repo_policy="COMMIT_METADATA_ONLY",
        ).model_dump_json(indent=2)
    )
    return payload


def _write_prepared_public_payload(
    root: Path,
    filename: str,
    content: str,
    *,
    retrieved_at: str,
    source_payload_sha256: str,
    source_payload_object: str,
) -> Path:
    payload = root / filename
    payload.write_text(content)
    payload.with_suffix(".manifest.json").write_text(
        operator_module.PreparedPublicArtifactManifest(
            retrieved_at=datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")),
            byte_count=len(content.encode()),
            sha256=hashlib.sha256(content.encode()).hexdigest(),
            source_payload_sha256=source_payload_sha256,
            source_payload_object=source_payload_object,
        ).model_dump_json(indent=2)
    )
    return payload


def test_latest_public_payload_is_time_ordered_and_fails_closed_on_tamper(
    tmp_path: Path,
) -> None:
    latest_root = tmp_path / "latest"
    _write_public_payload(latest_root, "source", "older.csv", "older", "2026-08-24T02:00:00Z")
    newer = _write_public_payload(
        latest_root, "source", "newer.csv", "newer", "2026-08-24T03:00:00Z"
    )
    selected = operator_module._latest_public_payload(latest_root, "source", ".csv")
    assert selected is not None
    assert selected.path == newer.resolve()
    assert selected.sha256 == hashlib.sha256(b"newer").hexdigest()
    assert selected.object_path == str(newer.relative_to(latest_root))

    tamper_root = tmp_path / "tamper"
    tampered = _write_public_payload(
        tamper_root, "source", "payload.csv", "original", "2026-08-24T03:00:00Z"
    )
    tampered.write_text("changed")
    with pytest.raises(RuntimeError, match=r"byte count mismatch|SHA-256 mismatch"):
        operator_module._latest_public_payload(tamper_root, "source", ".csv")

    escape_root = tmp_path / "escape"
    escaped_payload = _write_public_payload(
        escape_root, "source", "payload.csv", "safe", "2026-08-24T03:00:00Z"
    )
    manifest_path = escaped_payload.with_suffix(".manifest.json")
    manifest = PublicFetchManifest.model_validate_json(manifest_path.read_text())
    manifest_path.write_text(
        manifest.model_copy(update={"object_path": "../outside.csv"}).model_dump_json(indent=2)
    )
    with pytest.raises(RuntimeError, match="escaped its source root"):
        operator_module._latest_public_payload(escape_root, "source", ".csv")


def test_public_extractors_map_bounded_payloads(tmp_path: Path) -> None:
    public_root = tmp_path / "public"
    prepared_root = tmp_path / "prepared"
    prepared_root.mkdir()
    _write_public_payload(
        public_root,
        "vicmap_address",
        "address.json",
        json.dumps(
            {
                "features": [
                    {
                        "attributes": {
                            "property_pfi": "PROP-1",
                            "pfi": "ADDR-1",
                            "ezi_address": "1 TEST STREET TESTVILLE VIC 3000",
                            "locality_name": "TESTVILLE",
                            "postcode": "3000",
                        }
                    }
                ]
            }
        ),
        "2026-08-23T12:00:00Z",
    )
    _write_prepared_public_payload(
        prepared_root,
        "raw_property_market.jsonl",
        json.dumps(
            {
                "geography_code": "TESTVILLE",
                "geography_type": "SUBURB",
                "period_start": "2026-01-01",
                "property_type": "HOUSE",
                "median_price": 900000,
                "sales_volume": 10,
                "source_dataset": "VGV",
            }
        )
        + "\n",
        retrieved_at="2026-08-24T00:00:00Z",
        source_payload_sha256=hashlib.sha256(b"official VGV source").hexdigest(),
        source_payload_object="vgv/example.csv",
    )
    vgv_source = public_root / "vgv" / "example.csv"
    vgv_source.parent.mkdir(parents=True)
    vgv_source.write_bytes(b"official VGV source")
    property_rows = operator_module._property_extractor(public_root, prepared_root)(_window())
    assert property_rows["raw_property_master"][0]["property_id"] == "PROP-1"
    assert len(property_rows["raw_property_master"][0]["source_payload_sha256"]) == 64
    assert len(property_rows["raw_property_market"]) == 1

    rba_rows = [
        ["metadata"],
        ["Title", "Cash Rate", "ASX decoy"],
        ["metadata"],
        ["metadata"],
        ["metadata"],
        ["Units", "Per cent", "Index"],
        ["metadata"],
        ["Source", "RBA", "ASX"],
        ["Publication", "F1", "Decoy"],
        ["Series ID", "FIRMMCRTD", "ASX200"],
        ["23-Aug-2026", "4.35", "8100"],
    ]
    _write_public_payload(
        public_root,
        "rba_f1_interest_rates",
        "rba.csv",
        "\n".join(",".join(row) for row in rba_rows),
        "2026-08-24T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g01_sa2_vic",
        "g01.csv",
        "STRUCTURE_ID,SEXP,PCHAR,REGION,REGION_TYPE,STATE,TIME_PERIOD,OBS_VALUE\n"
        "ABS:C21_G01_SA2(1.0.0),3,P_1,206041122,SA2,2,2021,1000\n",
        "2026-08-24T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g02_sa2_vic",
        "g02.csv",
        "STRUCTURE_ID,MEDAVG,REGION,REGION_TYPE,STATE,TIME_PERIOD,OBS_VALUE\n"
        "ABS:C21_G02_SA2(1.0.0),4,206041122,SA2,2,2021,2000\n",
        "2026-08-24T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g37_sa2_vic",
        "g37.csv",
        "STRUCTURE_ID,TENLLD,REGION,REGION_TYPE,STATE,TIME_PERIOD,OBS_VALUE\n"
        "ABS:C21_G37_SA2(1.0.0),1,206041122,SA2,2,2021,100\n"
        "ABS:C21_G37_SA2(1.0.0),2,206041122,SA2,2,2021,200\n"
        "ABS:C21_G37_SA2(1.0.0),R_T,206041122,SA2,2,2021,200\n",
        "2026-08-24T00:00:00Z",
    )
    (prepared_root / "raw_abs_demographics.jsonl").write_text(
        json.dumps(
            {
                "geography_code": "206041122",
                "geography_type": "SA2",
                "reference_period": "2021",
                "population": 1000,
                "median_household_income_weekly": 2000,
                "owner_occupied_dwellings": 300,
                "rented_dwellings": 200,
                "source_dataset": "ABS",
            }
        )
        + "\n"
    )
    economic_window = FrozenWindow(
        window_start=datetime(2026, 8, 22, tzinfo=UTC),
        window_end=datetime(2026, 8, 24, tzinfo=UTC),
        window_id="d" * 64,
        mode="INCREMENTAL",
    )
    economic_rows = operator_module._economic_extractor(public_root, prepared_root)(economic_window)
    assert economic_rows["raw_rba_indicators"][0]["series_id"] == "FIRMMCRTD"
    assert len(economic_rows["raw_rba_indicators"][0]["source_payload_sha256"]) == 64
    assert {row["series_id"] for row in economic_rows["raw_rba_indicators"]} == {"FIRMMCRTD"}
    assert len(economic_rows["raw_abs_demographics"]) == 1
    assert economic_rows["raw_abs_demographics"][0]["owner_occupied_dwellings"] == 300
    assert len(economic_rows["raw_abs_demographics"][0]["source_payload_sha256"]) == 64


def test_public_extractors_fail_closed_on_schema_drift(tmp_path: Path) -> None:
    public_root = tmp_path / "public"
    prepared_root = tmp_path / "prepared"
    prepared_root.mkdir()
    _write_public_payload(
        public_root,
        "vicmap_address",
        "address.json",
        json.dumps({"unexpected": []}),
        "2026-08-23T12:00:00Z",
    )
    with pytest.raises(RuntimeError, match="features array"):
        operator_module._property_extractor(public_root, prepared_root)(_window())

    _write_public_payload(
        public_root,
        "rba_f1_interest_rates",
        "rba.csv",
        "Title,Cash Rate\n23-Aug-2026,4.35\n",
        "2026-08-24T00:00:00Z",
    )
    with pytest.raises(RuntimeError, match="required labels"):
        operator_module._economic_extractor(public_root, prepared_root)(_window())


def test_vicmap_missing_identifier_remains_null_for_quarantine(tmp_path: Path) -> None:
    public_root = tmp_path / "public"
    prepared_root = tmp_path / "prepared"
    prepared_root.mkdir()
    _write_public_payload(
        public_root,
        "vicmap_address",
        "address.json",
        json.dumps({"features": [{"attributes": {"ezi_address": "UNKNOWN"}}]}),
        "2026-08-23T12:00:00Z",
    )
    rows = operator_module._property_extractor(public_root, prepared_root)(_window())
    assert rows["raw_property_master"][0]["property_id"] is None
    assert rows["raw_property_master"][0]["address_id"] is None


def test_public_snapshots_use_retrieval_window_and_keep_old_rba_revisions(
    tmp_path: Path,
) -> None:
    public_root = tmp_path / "public"
    prepared_root = tmp_path / "prepared"
    prepared_root.mkdir()
    rba_rows = [
        ["Title", "Cash Rate"],
        ["Units", "Per cent"],
        ["Source", "RBA"],
        ["Series ID", "FIRMMCRTD"],
        ["01-Jan-2020", "4.10"],
    ]
    _write_public_payload(
        public_root,
        "rba_f1_interest_rates",
        "rba.csv",
        "\n".join(",".join(row) for row in rba_rows),
        "2026-08-24T00:00:00Z",
    )
    in_window = FrozenWindow(
        window_start=datetime(2026, 8, 23, tzinfo=UTC),
        window_end=datetime(2026, 8, 24, 1, tzinfo=UTC),
        window_id="e" * 64,
        mode="INCREMENTAL",
    )
    rows = operator_module._economic_extractor(public_root, prepared_root)(in_window)
    assert rows["raw_rba_indicators"][0]["observation_date"] == "2020-01-01"

    later_window = FrozenWindow(
        window_start=datetime(2026, 8, 24, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 25, tzinfo=UTC),
        window_id="f" * 64,
        mode="INCREMENTAL",
    )
    assert not operator_module._economic_extractor(public_root, prepared_root)(later_window)[
        "raw_rba_indicators"
    ]


def test_prepared_public_artifact_requires_valid_sidecar(tmp_path: Path) -> None:
    prepared = tmp_path / "prepared.jsonl"
    public_root = tmp_path / "public"
    prepared.write_text('{"value": 1}\n')
    with pytest.raises(RuntimeError, match="manifest is missing"):
        operator_module._load_prepared_public_jsonl(
            prepared, window=_window(), public_root=public_root
        )

    _write_prepared_public_payload(
        tmp_path,
        "prepared.jsonl",
        '{"value": 1}\n',
        retrieved_at="2026-08-23T12:00:00Z",
        source_payload_sha256="f" * 64,
        source_payload_object="official/source.csv",
    )
    prepared.write_text('{"value": 2}\n')
    with pytest.raises(RuntimeError, match=r"byte count mismatch|SHA-256 mismatch"):
        operator_module._load_prepared_public_jsonl(
            prepared, window=_window(), public_root=public_root
        )


def test_prepared_public_artifact_rehashes_its_claimed_source(tmp_path: Path) -> None:
    prepared_root = tmp_path / "prepared"
    prepared_root.mkdir()
    public_root = tmp_path / "public"
    source = public_root / "official" / "source.csv"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"official source")
    prepared = _write_prepared_public_payload(
        prepared_root,
        "prepared.jsonl",
        '{"value": 1}\n',
        retrieved_at="2026-08-23T12:00:00Z",
        source_payload_sha256=hashlib.sha256(b"official source").hexdigest(),
        source_payload_object="official/source.csv",
    )
    source.write_bytes(b"tampered source")

    with pytest.raises(RuntimeError, match="source payload SHA-256 mismatch"):
        operator_module._load_prepared_public_jsonl(
            prepared, window=_window(), public_root=public_root
        )


def test_abs_composite_snapshot_accepts_one_flow_revision(tmp_path: Path) -> None:
    public_root = tmp_path / "public"
    common = "REGION,REGION_TYPE,STATE,TIME_PERIOD,OBS_VALUE"
    _write_public_payload(
        public_root,
        "abs_c21_g01_sa2_vic",
        "g01.csv",
        f"STRUCTURE_ID,PCHAR,{common}\nABS:C21_G01_SA2(1.0.0),P_1,206041122,SA2,2,2021,900\n",
        "2026-08-22T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g02_sa2_vic",
        "g02.csv",
        f"STRUCTURE_ID,MEDAVG,{common}\nABS:C21_G02_SA2(1.0.0),4,206041122,SA2,2,2021,2000\n",
        "2026-08-22T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g37_sa2_vic",
        "g37.csv",
        f"STRUCTURE_ID,TENLLD,{common}\n"
        "ABS:C21_G37_SA2(1.0.0),1,206041122,SA2,2,2021,100\n"
        "ABS:C21_G37_SA2(1.0.0),2,206041122,SA2,2,2021,200\n"
        "ABS:C21_G37_SA2(1.0.0),R_T,206041122,SA2,2,2021,200\n",
        "2026-08-22T00:00:00Z",
    )
    _write_public_payload(
        public_root,
        "abs_c21_g01_sa2_vic",
        "g01-revision.csv",
        f"STRUCTURE_ID,PCHAR,{common}\nABS:C21_G01_SA2(1.0.0),P_1,206041122,SA2,2,2021,1000\n",
        "2026-08-23T12:00:00Z",
    )

    rows = operator_module._load_abs_demographics(public_root, window=_window())
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["population"] == 1000


@pytest.mark.parametrize(
    ("source", "expected_table"),
    [
        ("legacy_crm", "raw_legacy_customers"),
        ("support", "raw_support_cases"),
        ("consent", "raw_marketing_consents"),
    ],
)
def test_postgres_extractors_use_frozen_window_and_map_source(
    monkeypatch: pytest.MonkeyPatch, source: str, expected_table: str
) -> None:
    calls: list[tuple[str, tuple[datetime, datetime]]] = []

    class FakeCursor:
        def fetchall(self) -> list[dict[str, str]]:
            return [{"id": "row-1"}]

    class FakeConnection:
        def __enter__(self) -> FakeConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[datetime, datetime]) -> FakeCursor:
            calls.append((sql, params))
            return FakeCursor()

    import psycopg

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: FakeConnection())
    rows = operator_module._postgres_extractor("postgresql://test", source)(_window())
    assert rows[expected_table] == ({"id": "row-1"},)
    assert calls[0][1] == (_window().window_start, _window().window_end)


def test_salesforce_extractor_maps_all_raw_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    records = {
        "Account": {"Id": "A1", "Name": "Account", "SystemModstamp": "2026-08-24"},
        "Contact": {
            "Id": "C1",
            "Email": "synthetic@example.invalid",
            "SystemModstamp": "2026-08-24",
        },
        "Case": {"Id": "CASE1", "Status": "New", "SystemModstamp": "2026-08-24"},
    }

    class FakeConnector:
        def __init__(self, _oauth: SalesforceOAuthConfig) -> None:
            pass

        def __enter__(self) -> FakeConnector:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def extract_incremental(self, *, object_name: str, **_kwargs: Any) -> Any:
            return SimpleNamespace(records=(records[object_name],))

    monkeypatch.setattr(operator_module, "SalesforceConnector", FakeConnector)
    extractor = operator_module._salesforce_extractor(
        SalesforceOAuthConfig(client_id="client", client_secret="secret")
    )
    mapped = extractor(_window())
    assert set(mapped) == {
        "raw_salesforce_accounts",
        "raw_salesforce_contacts",
        "raw_salesforce_cases",
    }
    assert mapped["raw_salesforce_accounts"][0]["abn"] is None
    assert mapped["raw_salesforce_contacts"][0]["email_verified"] is False
