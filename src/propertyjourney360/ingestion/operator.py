"""Executable source-to-RAW operators used by the thin Airflow DAGs."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from propertyjourney360.connectors.salesforce import SalesforceConnector, SalesforceOAuthConfig
from propertyjourney360.events.models import PropertyEvent
from propertyjourney360.google_auth import impersonated_credentials_from_environment
from propertyjourney360.ingestion.bigquery import BigQueryJsonLoader
from propertyjourney360.ingestion.gcs import GCSRawLanding
from propertyjourney360.ingestion.landing import LandingManifest, LocalRawLanding
from propertyjourney360.ingestion.raw_contracts import RAW_CONTRACTS, SOURCE_ASSETS
from propertyjourney360.ingestion.watermarks import WatermarkStore
from propertyjourney360.public_data.fetch import PublicFetchManifest

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_TABLE_COMPONENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WindowMode = Literal["INCREMENTAL", "LIVE", "BACKFILL", "REPLAY"]


def _parse_timestamp(value: str, *, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


class FrozenWindow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window_start: datetime
    window_end: datetime
    window_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    mode: WindowMode
    replay_id: str | None = None

    @model_validator(mode="after")
    def validate_window(self) -> FrozenWindow:
        if self.window_start.tzinfo is None or self.window_end.tzinfo is None:
            raise ValueError("window timestamps must include a timezone")
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be later than window_start")
        if self.mode.upper() == "REPLAY" and not self.replay_id:
            raise ValueError("REPLAY mode requires replay_id")
        if self.replay_id and not _SAFE_ID.fullmatch(self.replay_id):
            raise ValueError("replay_id contains unsupported characters")
        return self


class ExtractedAsset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    table_name: str
    manifest: LandingManifest


class BatchExtractReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    window: FrozenWindow
    effective_window_start: datetime
    effective_window_source: Literal["REQUESTED", "COMMITTED_WATERMARK", "REPLAY_MANIFEST"]
    assets: tuple[ExtractedAsset, ...]
    extracted_at: datetime
    replayed_from_window_id: str | None = None


class LoadedAsset(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    table_name: str
    status: Literal["LOADED", "EMPTY"]
    record_count: int = Field(ge=0)
    job_id: str | None = None

    @model_validator(mode="after")
    def validate_status_envelope(self) -> LoadedAsset:
        if self.status == "EMPTY" and (self.record_count != 0 or self.job_id is not None):
            raise ValueError("EMPTY load assets require zero rows and no job_id")
        if self.status == "LOADED" and (self.record_count == 0 or not self.job_id):
            raise ValueError("LOADED assets require non-zero rows and a job_id")
        return self


class BatchLoadReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    window_id: str
    assets: tuple[LoadedAsset, ...]
    loaded_at: datetime


class WatermarkCommitReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window_id: str
    mode: str
    committed_sources: tuple[str, ...]
    skipped_sources: tuple[str, ...]
    committed_at: datetime


class EventConsumeReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window: FrozenWindow
    manifest: LandingManifest
    received_count: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    dead_lettered_count: int = Field(ge=0)
    duplicate_count: int = Field(ge=0)
    accepted_event_ids: tuple[str, ...] = ()
    accepted_event_fingerprints: dict[str, str] = Field(default_factory=dict)
    consumed_at: datetime


class EventCheckpointReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window_id: str
    mode: str
    load_status: Literal["LOADED", "EMPTY"]
    checkpointed_at: datetime


class EventAckRecoveryProgress(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    window_id: str
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    recovered_event_fingerprints: dict[str, str] = Field(default_factory=dict)


class PulledEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    ack_id: str
    message_id: str
    data: bytes


class EventPullSource:
    """Small Pub/Sub pull boundary that can be replaced by deterministic tests."""

    def pull(self, *, max_messages: int) -> Sequence[PulledEvent]:
        raise NotImplementedError

    def dead_letter(self, message: PulledEvent, *, reason: str) -> None:
        raise NotImplementedError

    def acknowledge(self, ack_ids: Sequence[str]) -> None:
        raise NotImplementedError


class GooglePubSubPullSource(EventPullSource):
    def __init__(
        self,
        *,
        subscription: str,
        dead_letter_topic: str,
        subscriber: Any | None = None,
        publisher: Any | None = None,
    ) -> None:
        if subscriber is None or publisher is None:
            import google.cloud.pubsub_v1 as pubsub_v1  # type: ignore[import-untyped]

            subscriber = subscriber or pubsub_v1.SubscriberClient()
            publisher = publisher or pubsub_v1.PublisherClient()
        self.subscription = subscription
        self.dead_letter_topic = dead_letter_topic
        self.subscriber = subscriber
        self.publisher = publisher

    def pull(self, *, max_messages: int) -> Sequence[PulledEvent]:
        response = self.subscriber.pull(
            request={"subscription": self.subscription, "max_messages": max_messages},
            timeout=30,
        )
        return tuple(
            PulledEvent(
                ack_id=received.ack_id,
                message_id=received.message.message_id,
                data=bytes(received.message.data),
            )
            for received in response.received_messages
        )

    def dead_letter(self, message: PulledEvent, *, reason: str) -> None:
        future = self.publisher.publish(
            self.dead_letter_topic,
            message.data,
            original_message_id=message.message_id,
            failure_reason=reason,
        )
        future.result(timeout=30)

    def acknowledge(self, ack_ids: Sequence[str]) -> None:
        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self.subscription, "ack_ids": list(ack_ids)}
            )


class OperatorState:
    """Shared durable task state; each source/window owns a separate immutable report."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, *, window_id: str, source: str, phase: str) -> Path:
        if not _SAFE_ID.fullmatch(window_id) or not _SAFE_ID.fullmatch(source):
            raise ValueError("unsafe state key")
        return self.root / "runs" / window_id / source / f"{phase}.json"

    def write_report(self, *, window_id: str, source: str, phase: str, payload: str) -> None:
        path = self._path(window_id=window_id, source=source, phase=phase)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_text() != payload:
                raise RuntimeError(
                    f"immutable operator state conflicts with existing {phase} report"
                )
            return
        temporary = path.with_suffix(".tmp")
        temporary.write_text(payload)
        temporary.replace(path)

    def read_extract(self, *, window_id: str, source: str) -> BatchExtractReport:
        path = self._path(window_id=window_id, source=source, phase="extract")
        if not path.exists():
            raise FileNotFoundError(f"extract report is missing for {source}/{window_id}")
        return BatchExtractReport.model_validate_json(path.read_text())

    def read_load(self, *, window_id: str, source: str) -> BatchLoadReport:
        path = self._path(window_id=window_id, source=source, phase="load")
        if not path.exists():
            raise FileNotFoundError(f"load report is missing for {source}/{window_id}")
        return BatchLoadReport.model_validate_json(path.read_text())

    def read_event_consume(self, *, window_id: str) -> EventConsumeReport:
        path = self._path(window_id=window_id, source="events", phase="consume")
        if not path.exists():
            raise FileNotFoundError(f"event consume report is missing for {window_id}")
        return EventConsumeReport.model_validate_json(path.read_text())

    def read_watermark_commit(self, *, window_id: str) -> WatermarkCommitReport:
        path = self._path(window_id=window_id, source="batch", phase="commit")
        if not path.exists():
            raise FileNotFoundError(f"watermark commit report is missing for {window_id}")
        return WatermarkCommitReport.model_validate_json(path.read_text())

    def read_event_checkpoint(self, *, window_id: str) -> EventCheckpointReport:
        path = self._path(window_id=window_id, source="events", phase="checkpoint")
        if not path.exists():
            raise FileNotFoundError(f"event checkpoint report is missing for {window_id}")
        return EventCheckpointReport.model_validate_json(path.read_text())

    def event_acknowledged(self, *, window_id: str) -> bool:
        return self._path(window_id=window_id, source="events", phase="ack").exists()

    def read_event_ack_progress(self, *, window_id: str) -> EventAckRecoveryProgress | None:
        path = self._path(window_id=window_id, source="events", phase="ack-progress")
        if not path.exists():
            return None
        return EventAckRecoveryProgress.model_validate_json(path.read_text())

    def record_event_ack_progress(
        self,
        *,
        window_id: str,
        manifest_sha256: str,
        recovered_event_fingerprints: Mapping[str, str],
    ) -> EventAckRecoveryProgress:
        previous = self.read_event_ack_progress(window_id=window_id)
        if previous is not None and previous.manifest_sha256 != manifest_sha256:
            raise RuntimeError("event ACK progress conflicts with the immutable landing manifest")
        merged = dict(previous.recovered_event_fingerprints if previous is not None else {})
        for event_id, fingerprint in recovered_event_fingerprints.items():
            if event_id in merged and merged[event_id] != fingerprint:
                raise RuntimeError("event ACK progress contains a conflicting payload fingerprint")
            merged[event_id] = fingerprint
        progress = EventAckRecoveryProgress(
            window_id=window_id,
            manifest_sha256=manifest_sha256,
            recovered_event_fingerprints=dict(sorted(merged.items())),
        )
        path = self._path(window_id=window_id, source="events", phase="ack-progress")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(progress.model_dump_json(indent=2))
        temporary.replace(path)
        return progress

    def mark_event_acknowledged(self, *, window_id: str, manifest_sha256: str) -> None:
        self.write_report(
            window_id=window_id,
            source="events",
            phase="ack",
            payload=json.dumps({"manifest_sha256": manifest_sha256}, sort_keys=True),
        )


ExtractFunction = Callable[[FrozenWindow], Mapping[str, Sequence[Mapping[str, Any]]]]


def _validate_prior_load_report(
    report: BatchLoadReport,
    *,
    source: str,
    window: FrozenWindow,
    manifests: Mapping[str, LandingManifest],
) -> None:
    expected_tables = tuple(manifests)
    if (
        report.source != source
        or report.window_id != window.window_id
        or tuple(asset.table_name for asset in report.assets) != expected_tables
    ):
        raise RuntimeError("prior load report does not match the current source/window contract")
    for loaded in report.assets:
        manifest = manifests[loaded.table_name]
        contract = RAW_CONTRACTS[loaded.table_name]
        expected_status = "EMPTY" if manifest.record_count == 0 else "LOADED"
        if (
            manifest.source != loaded.table_name
            or manifest.schema_version != contract.schema_version
            or loaded.status != expected_status
            or loaded.record_count != manifest.record_count
        ):
            raise RuntimeError("prior load report does not match its immutable extract manifest")


class BatchIngestionOperator:
    def __init__(
        self,
        *,
        state: OperatorState,
        landing: LocalRawLanding | GCSRawLanding,
        extractors: Mapping[str, ExtractFunction],
        bigquery_loader: BigQueryJsonLoader | None = None,
        bigquery_project: str | None = None,
        bigquery_raw_dataset: str | None = None,
    ) -> None:
        self.state = state
        self.landing = landing
        self.extractors = extractors
        self.bigquery_loader = bigquery_loader
        self.bigquery_project = bigquery_project
        self.bigquery_raw_dataset = bigquery_raw_dataset

    def _resolve_extract_window(
        self, *, source: str, requested: FrozenWindow
    ) -> tuple[FrozenWindow, Literal["REQUESTED", "COMMITTED_WATERMARK"]]:
        if requested.mode.upper() != "INCREMENTAL":
            return requested, "REQUESTED"
        store = WatermarkStore(self.state.root / "watermarks.db")
        watermarks = [store.get(source=source, stream=asset) for asset in SOURCE_ASSETS[source]]
        present = [watermark for watermark in watermarks if watermark is not None]
        if not present:
            return requested, "REQUESTED"
        if len(present) != len(watermarks):
            raise RuntimeError(f"partial committed watermark state for {source}")
        values = {watermark.value for watermark in present}
        if len(values) != 1:
            raise RuntimeError(f"inconsistent committed watermark state for {source}")
        committed_start = _parse_timestamp(values.pop(), name=f"{source} committed watermark")
        if committed_start >= requested.window_end:
            raise RuntimeError(f"committed watermark is not earlier than window end for {source}")
        return requested.model_copy(update={"window_start": committed_start}), "COMMITTED_WATERMARK"

    def extract(self, *, source: str, window: FrozenWindow) -> BatchExtractReport:
        if window.mode not in {"INCREMENTAL", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported batch mode: {window.mode}")
        if source not in SOURCE_ASSETS:
            raise ValueError(f"unsupported batch source: {source}")
        try:
            previous = self.state.read_extract(window_id=window.window_id, source=source)
        except FileNotFoundError:
            previous = None
        if previous is not None:
            if previous.window != window:
                raise RuntimeError("extract arguments conflict with immutable prior run state")
            return previous
        if window.mode.upper() == "REPLAY":
            original = self.state.read_extract(
                window_id=window.replay_id or "",
                source=source,
            )
            if original.source != source or {asset.table_name for asset in original.assets} != set(
                SOURCE_ASSETS[source]
            ):
                raise RuntimeError("replay manifest does not match the requested source contract")
            report = BatchExtractReport(
                source=source,
                window=window,
                effective_window_start=original.effective_window_start,
                effective_window_source="REPLAY_MANIFEST",
                assets=original.assets,
                extracted_at=datetime.now(UTC),
                replayed_from_window_id=window.replay_id,
            )
            self.state.write_report(
                window_id=window.window_id,
                source=source,
                phase="extract",
                payload=report.model_dump_json(indent=2),
            )
            return report
        if source not in self.extractors:
            raise ValueError(f"unsupported batch source: {source}")
        extracted_at = datetime.now(UTC)
        effective_window, effective_window_source = self._resolve_extract_window(
            source=source, requested=window
        )
        raw_assets = self.extractors[source](effective_window)
        if set(raw_assets) != set(SOURCE_ASSETS[source]):
            raise RuntimeError(f"{source} extractor did not return its complete RAW asset set")
        assets: list[ExtractedAsset] = []
        for table_name in SOURCE_ASSETS[source]:
            contract = RAW_CONTRACTS[table_name]
            metadata = {
                "_ingested_at": extracted_at.isoformat(),
                "_window_id": window.window_id,
                "_source_run_id": window.replay_id or window.window_id,
                "_schema_version": contract.schema_version,
            }
            prepared = [
                contract.prepare(dict(record), metadata=metadata)
                for record in raw_assets[table_name]
            ]
            manifest = self.landing.land(
                source=table_name,
                run_id=window.window_id,
                schema_version=contract.schema_version,
                records=prepared,
                extracted_at=extracted_at,
                watermark=window.window_end.astimezone(UTC).isoformat(),
            )
            assets.append(ExtractedAsset(table_name=table_name, manifest=manifest))
        report = BatchExtractReport(
            source=source,
            window=window,
            effective_window_start=effective_window.window_start,
            effective_window_source=effective_window_source,
            assets=tuple(assets),
            extracted_at=extracted_at,
        )
        self.state.write_report(
            window_id=window.window_id,
            source=source,
            phase="extract",
            payload=report.model_dump_json(indent=2),
        )
        return report

    def load_raw(self, *, source: str, window: FrozenWindow) -> BatchLoadReport:
        if window.mode not in {"INCREMENTAL", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported batch mode: {window.mode}")
        try:
            previous_load = self.state.read_load(window_id=window.window_id, source=source)
        except FileNotFoundError:
            previous_load = None
        if previous_load is not None:
            previous_extract = self.state.read_extract(window_id=window.window_id, source=source)
            if previous_extract.window != window:
                raise RuntimeError("load arguments do not match the immutable extract window")
            _validate_prior_load_report(
                previous_load,
                source=source,
                window=window,
                manifests={asset.table_name: asset.manifest for asset in previous_extract.assets},
            )
            return previous_load
        report = self.state.read_extract(window_id=window.window_id, source=source)
        if report.window != window:
            raise RuntimeError("load arguments do not match the immutable extract window")
        if (
            self.bigquery_loader is None
            or not self.bigquery_project
            or not self.bigquery_raw_dataset
        ):
            raise RuntimeError("BigQuery loader, project and RAW dataset are required for load-raw")
        if not _TABLE_COMPONENT.fullmatch(self.bigquery_raw_dataset):
            raise ValueError("BIGQUERY_RAW_DATASET is not a valid dataset identifier")
        loaded_assets: list[LoadedAsset] = []
        for asset in report.assets:
            if asset.manifest.record_count == 0:
                loaded_assets.append(
                    LoadedAsset(table_name=asset.table_name, status="EMPTY", record_count=0)
                )
                continue
            result = self.bigquery_loader.load(
                asset.manifest,
                table=f"{self.bigquery_project}.{self.bigquery_raw_dataset}.{asset.table_name}",
            )
            if result.output_rows != asset.manifest.record_count:
                raise RuntimeError(f"BigQuery row count mismatch for {asset.table_name}")
            loaded_assets.append(
                LoadedAsset(
                    table_name=asset.table_name,
                    status="LOADED",
                    record_count=result.output_rows,
                    job_id=result.job_id,
                )
            )
        load_report = BatchLoadReport(
            source=source,
            window_id=window.window_id,
            assets=tuple(loaded_assets),
            loaded_at=datetime.now(UTC),
        )
        self.state.write_report(
            window_id=window.window_id,
            source=source,
            phase="load",
            payload=load_report.model_dump_json(indent=2),
        )
        return load_report

    def commit_watermarks(
        self, *, sources: Sequence[str], window: FrozenWindow
    ) -> WatermarkCommitReport:
        if window.mode not in {"INCREMENTAL", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported batch mode: {window.mode}")
        unique_sources = tuple(dict.fromkeys(sources))
        unknown = sorted(set(unique_sources) - set(SOURCE_ASSETS))
        if unknown:
            raise ValueError(f"unsupported watermark sources: {unknown}")
        for source in unique_sources:
            load_report = self.state.read_load(window_id=window.window_id, source=source)
            expected_assets = tuple(SOURCE_ASSETS[source])
            if (
                load_report.source != source
                or load_report.window_id != window.window_id
                or tuple(asset.table_name for asset in load_report.assets) != expected_assets
            ):
                raise RuntimeError(f"incomplete load evidence for {source}")

        mode = window.mode.upper()
        expected_committed = unique_sources if mode == "INCREMENTAL" else ()
        expected_skipped = () if mode == "INCREMENTAL" else unique_sources
        try:
            previous = self.state.read_watermark_commit(window_id=window.window_id)
        except FileNotFoundError:
            previous = None
        if previous is not None:
            if (
                previous.window_id != window.window_id
                or previous.mode != mode
                or previous.committed_sources != expected_committed
                or previous.skipped_sources != expected_skipped
            ):
                raise RuntimeError("watermark commit arguments conflict with immutable prior state")
            return previous

        if mode == "INCREMENTAL":
            store = WatermarkStore(self.state.root / "watermarks.db")
            value = window.window_end.astimezone(UTC).isoformat()
            store.advance_many(
                tuple(
                    (source, asset, value, window.window_id)
                    for source in unique_sources
                    for asset in SOURCE_ASSETS[source]
                )
            )
        commit_report = WatermarkCommitReport(
            window_id=window.window_id,
            mode=mode,
            committed_sources=expected_committed,
            skipped_sources=expected_skipped,
            committed_at=datetime.now(UTC),
        )
        self.state.write_report(
            window_id=window.window_id,
            source="batch",
            phase="commit",
            payload=commit_report.model_dump_json(indent=2),
        )
        return commit_report


class EventIngestionOperator:
    """Pub/Sub-to-immutable-landing operator; ACK follows a durable disposition only."""

    def __init__(
        self,
        *,
        state: OperatorState,
        landing: LocalRawLanding | GCSRawLanding,
        pull_source: EventPullSource | None,
        bigquery_loader: BigQueryJsonLoader | None = None,
        bigquery_project: str | None = None,
        bigquery_raw_dataset: str | None = None,
        backfill_path: Path | None = None,
    ) -> None:
        self.state = state
        self.landing = landing
        self.pull_source = pull_source
        self.bigquery_loader = bigquery_loader
        self.bigquery_project = bigquery_project
        self.bigquery_raw_dataset = bigquery_raw_dataset
        self.backfill_path = backfill_path

    @staticmethod
    def _warehouse_record(event: PropertyEvent) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "session_id": event.session_id,
            "anonymous_id": event.anonymous_id,
            "customer_id": event.customer_id,
            "customer_source_system": event.customer_source_system,
            "customer_source_id": event.customer_source_id,
            "property_id": event.property_id,
            "price_observed": event.price_observed,
            "device_type": event.device_type,
            "attributes": event.attributes,
            "schema_version": event.schema_version,
            "occurred_at": event.occurred_at.astimezone(UTC).isoformat(),
            "received_at": event.received_at.astimezone(UTC).isoformat(),
        }

    @staticmethod
    def _event_fingerprint(event: PropertyEvent) -> str:
        canonical = json.dumps(
            event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    def _read_backfill(self, window: FrozenWindow) -> tuple[PropertyEvent, ...]:
        if self.backfill_path is None or not self.backfill_path.exists():
            raise RuntimeError("BACKFILL requires PJ360_EVENT_BACKFILL_JSONL")
        events: list[PropertyEvent] = []
        for line in self.backfill_path.read_text().splitlines():
            if not line.strip():
                continue
            event = PropertyEvent.model_validate_json(line)
            occurred_at = event.occurred_at.astimezone(UTC)
            if window.window_start < occurred_at <= window.window_end:
                events.append(event)
        return tuple(events)

    def _recover_live_ack(self, report: EventConsumeReport, *, max_messages: int) -> None:
        if self.pull_source is None:
            raise RuntimeError("LIVE event ACK recovery requires a Pub/Sub subscription")
        accepted_ids = set(report.accepted_event_ids)
        if accepted_ids != set(report.accepted_event_fingerprints):
            raise RuntimeError("LIVE event ACK recovery is missing accepted payload fingerprints")
        progress = self.state.read_event_ack_progress(window_id=report.window.window_id)
        recovered_fingerprints = (
            dict(progress.recovered_event_fingerprints) if progress is not None else {}
        )
        if progress is not None and progress.manifest_sha256 != report.manifest.sha256:
            raise RuntimeError("LIVE event ACK progress has a conflicting landing manifest")
        if not set(recovered_fingerprints) <= accepted_ids or any(
            report.accepted_event_fingerprints[event_id] != fingerprint
            for event_id, fingerprint in recovered_fingerprints.items()
        ):
            raise RuntimeError("LIVE event ACK progress has conflicting accepted fingerprints")
        remaining_ids = accepted_ids - set(recovered_fingerprints)
        recovered_ack_ids: list[str] = []
        newly_recovered: dict[str, str] = {}
        rejected_ack_ids: list[str] = []
        for message in self.pull_source.pull(max_messages=max_messages):
            try:
                event = PropertyEvent.model_validate_json(message.data)
            except ValueError:
                self.pull_source.dead_letter(message, reason="EVENT_SCHEMA_VALIDATION_FAILED")
                rejected_ack_ids.append(message.ack_id)
                continue
            if event.event_id not in accepted_ids:
                continue
            fingerprint = self._event_fingerprint(event)
            if fingerprint != report.accepted_event_fingerprints[event.event_id]:
                self.pull_source.dead_letter(message, reason="EVENT_ID_COLLISION")
                rejected_ack_ids.append(message.ack_id)
            else:
                recovered_ack_ids.append(message.ack_id)
                if event.event_id in remaining_ids:
                    newly_recovered[event.event_id] = fingerprint
        self.pull_source.acknowledge((*rejected_ack_ids, *recovered_ack_ids))
        progress = self.state.record_event_ack_progress(
            window_id=report.window.window_id,
            manifest_sha256=report.manifest.sha256,
            recovered_event_fingerprints=newly_recovered,
        )
        if not set(progress.recovered_event_fingerprints) <= accepted_ids:
            raise RuntimeError("LIVE event ACK recovery recorded an unknown accepted event")
        self.state.mark_event_acknowledged(
            window_id=report.window.window_id,
            manifest_sha256=report.manifest.sha256,
        )

    def consume(self, *, window: FrozenWindow, max_messages: int) -> EventConsumeReport:
        if window.mode not in {"LIVE", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported event mode: {window.mode}")
        if not 1 <= max_messages <= 50_000:
            raise ValueError("max_messages must be between 1 and 50000")
        try:
            previous = self.state.read_event_consume(window_id=window.window_id)
        except FileNotFoundError:
            previous = None
        if previous is not None:
            if previous.window != window:
                raise RuntimeError("consume arguments conflict with immutable prior run state")
            if window.mode.upper() == "LIVE" and not self.state.event_acknowledged(
                window_id=window.window_id
            ):
                self._recover_live_ack(previous, max_messages=max_messages)
            return previous

        if window.mode.upper() == "REPLAY":
            original = self.state.read_event_consume(window_id=window.replay_id or "")
            report = EventConsumeReport(
                window=window,
                manifest=original.manifest,
                received_count=original.received_count,
                accepted_count=original.accepted_count,
                dead_lettered_count=original.dead_lettered_count,
                duplicate_count=original.duplicate_count,
                accepted_event_ids=original.accepted_event_ids,
                accepted_event_fingerprints=original.accepted_event_fingerprints,
                consumed_at=datetime.now(UTC),
            )
            self.state.write_report(
                window_id=window.window_id,
                source="events",
                phase="consume",
                payload=report.model_dump_json(indent=2),
            )
            return report

        pulled: Sequence[PulledEvent] = ()
        accepted_ack_ids: list[str] = []
        dead_lettered_ack_ids: list[str] = []
        if window.mode.upper() == "BACKFILL":
            events = self._read_backfill(window)
            messages_by_event_id: dict[str, list[PulledEvent]] = {}
        else:
            if self.pull_source is None:
                raise RuntimeError("LIVE event consumption requires a Pub/Sub subscription")
            pulled = self.pull_source.pull(max_messages=max_messages)
            valid_events: list[PropertyEvent] = []
            messages_by_event_id = {}
            for message in pulled:
                try:
                    event = PropertyEvent.model_validate_json(message.data)
                except ValueError:
                    self.pull_source.dead_letter(message, reason="EVENT_SCHEMA_VALIDATION_FAILED")
                    dead_lettered_ack_ids.append(message.ack_id)
                    continue
                valid_events.append(event)
                messages_by_event_id.setdefault(event.event_id, []).append(message)
            events = tuple(valid_events)

        grouped_events: dict[str, list[PropertyEvent]] = {}
        for event in events:
            grouped_events.setdefault(event.event_id, []).append(event)
        deduplicated: dict[str, PropertyEvent] = {}
        accepted_fingerprints: dict[str, str] = {}
        duplicate_count = 0
        for event_id, variants in grouped_events.items():
            fingerprints = {self._event_fingerprint(event) for event in variants}
            if len(fingerprints) > 1:
                if not pulled:
                    raise RuntimeError(
                        f"backfill contains conflicting payloads for event_id {event_id}"
                    )
                for message in messages_by_event_id[event_id]:
                    if self.pull_source is not None:
                        self.pull_source.dead_letter(message, reason="EVENT_ID_COLLISION")
                    dead_lettered_ack_ids.append(message.ack_id)
                continue
            deduplicated[event_id] = variants[0]
            accepted_fingerprints[event_id] = next(iter(fingerprints))
            duplicate_count += len(variants) - 1
            accepted_ack_ids.extend(
                message.ack_id for message in messages_by_event_id.get(event_id, [])
            )
        if pulled and self.pull_source is not None:
            self.pull_source.acknowledge(dead_lettered_ack_ids)
        contract = RAW_CONTRACTS["raw_property_events"]
        consumed_at = datetime.now(UTC)
        metadata = {
            "_ingested_at": consumed_at.isoformat(),
            "_window_id": window.window_id,
            "_source_run_id": window.replay_id or window.window_id,
            "_schema_version": contract.schema_version,
        }
        prepared = [
            contract.prepare(self._warehouse_record(event), metadata=metadata)
            for event in deduplicated.values()
        ]
        manifest = self.landing.land(
            source="raw_property_events",
            run_id=window.window_id,
            schema_version=contract.schema_version,
            records=prepared,
            extracted_at=consumed_at,
            watermark=window.window_end.astimezone(UTC).isoformat(),
        )
        report = EventConsumeReport(
            window=window,
            manifest=manifest,
            received_count=len(pulled) if pulled else len(events),
            accepted_count=len(deduplicated),
            dead_lettered_count=len(dead_lettered_ack_ids),
            duplicate_count=duplicate_count,
            accepted_event_ids=tuple(deduplicated),
            accepted_event_fingerprints=accepted_fingerprints,
            consumed_at=consumed_at,
        )
        self.state.write_report(
            window_id=window.window_id,
            source="events",
            phase="consume",
            payload=report.model_dump_json(indent=2),
        )
        if pulled and self.pull_source is not None:
            self.pull_source.acknowledge(accepted_ack_ids)
            self.state.record_event_ack_progress(
                window_id=window.window_id,
                manifest_sha256=manifest.sha256,
                recovered_event_fingerprints=accepted_fingerprints,
            )
        if window.mode.upper() == "LIVE":
            self.state.mark_event_acknowledged(
                window_id=window.window_id,
                manifest_sha256=manifest.sha256,
            )
        return report

    def load_validated(self, *, window: FrozenWindow) -> BatchLoadReport:
        if window.mode not in {"LIVE", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported event mode: {window.mode}")
        try:
            previous = self.state.read_load(window_id=window.window_id, source="events")
        except FileNotFoundError:
            previous = None
        if previous is not None:
            previous_consume = self.state.read_event_consume(window_id=window.window_id)
            if previous_consume.window != window:
                raise RuntimeError("event load arguments do not match the immutable consume window")
            _validate_prior_load_report(
                previous,
                source="events",
                window=window,
                manifests={"raw_property_events": previous_consume.manifest},
            )
            return previous
        consumed = self.state.read_event_consume(window_id=window.window_id)
        if consumed.window != window:
            raise RuntimeError("event load arguments do not match the immutable consume window")
        if (
            self.bigquery_loader is None
            or not self.bigquery_project
            or not self.bigquery_raw_dataset
        ):
            raise RuntimeError("BigQuery loader, project and RAW dataset are required")
        if consumed.manifest.record_count == 0:
            loaded = LoadedAsset(table_name="raw_property_events", status="EMPTY", record_count=0)
        else:
            result = self.bigquery_loader.load(
                consumed.manifest,
                table=(f"{self.bigquery_project}.{self.bigquery_raw_dataset}.raw_property_events"),
            )
            if result.output_rows != consumed.manifest.record_count:
                raise RuntimeError("BigQuery row count mismatch for raw_property_events")
            loaded = LoadedAsset(
                table_name="raw_property_events",
                status="LOADED",
                record_count=result.output_rows,
                job_id=result.job_id,
            )
        report = BatchLoadReport(
            source="events",
            window_id=window.window_id,
            assets=(loaded,),
            loaded_at=datetime.now(UTC),
        )
        self.state.write_report(
            window_id=window.window_id,
            source="events",
            phase="load",
            payload=report.model_dump_json(indent=2),
        )
        return report

    def commit_checkpoint(self, *, window: FrozenWindow) -> EventCheckpointReport:
        if window.mode not in {"LIVE", "BACKFILL", "REPLAY"}:
            raise ValueError(f"unsupported event mode: {window.mode}")
        load_report = self.state.read_load(window_id=window.window_id, source="events")
        if (
            load_report.source != "events"
            or load_report.window_id != window.window_id
            or len(load_report.assets) != 1
            or load_report.assets[0].table_name != "raw_property_events"
        ):
            raise RuntimeError("event load evidence is incomplete")
        mode = window.mode.upper()
        try:
            previous = self.state.read_event_checkpoint(window_id=window.window_id)
        except FileNotFoundError:
            previous = None
        if previous is not None:
            if (
                previous.window_id != window.window_id
                or previous.mode != mode
                or previous.load_status != load_report.assets[0].status
            ):
                raise RuntimeError("event checkpoint arguments conflict with immutable prior state")
            return previous
        checkpoint = EventCheckpointReport(
            window_id=window.window_id,
            mode=mode,
            load_status=load_report.assets[0].status,
            checkpointed_at=datetime.now(UTC),
        )
        self.state.write_report(
            window_id=window.window_id,
            source="events",
            phase="checkpoint",
            payload=checkpoint.model_dump_json(indent=2),
        )
        return checkpoint


def _postgres_extractor(dsn: str, source: str) -> ExtractFunction:
    def extract(window: FrozenWindow) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        import psycopg
        from psycopg.rows import dict_row

        if source == "legacy_crm":
            sql = """
                SELECT source_record_id AS customer_id, NULL::text AS account_id,
                       first_name, last_name, email, false AS email_verified, phone,
                       address_line AS address_line_1, suburb, 'VIC'::text AS state, postcode,
                       source_updated_at AS updated_at
                FROM legacy.customer
                WHERE source_updated_at > %s AND source_updated_at <= %s
                ORDER BY source_updated_at, source_record_id
            """
            table = "raw_legacy_customers"
        elif source == "support":
            sql = """
                SELECT cases.case_id, cases.customer_source_record_id AS customer_id,
                       customers.first_name AS customer_first_name,
                       customers.last_name AS customer_last_name,
                       customers.email AS customer_email,
                       false AS customer_email_verified,
                       customers.phone AS customer_phone, cases.status,
                       'MEDIUM'::text AS priority, cases.subject, cases.opened_at,
                       NULL::timestamptz AS closed_at, cases.source_updated_at AS updated_at
                FROM support.case_record AS cases
                LEFT JOIN support.customer AS customers
                  ON cases.customer_source_record_id = customers.source_record_id
                WHERE cases.source_updated_at > %s AND cases.source_updated_at <= %s
                ORDER BY cases.source_updated_at, cases.case_id
            """
            table = "raw_support_cases"
        elif source == "consent":
            sql = """
                SELECT consent_event_id, 'legacy_crm'::text AS subject_source_system,
                       customer_source_record_id AS subject_source_id, channel,
                       CASE WHEN consent_status = 'WITHDRAWN' THEN 'REVOKED'
                            ELSE consent_status END AS consent_status,
                       'synthetic-v1'::text AS policy_version,
                       'legacy_crm_seed'::text AS capture_source,
                       effective_at AS occurred_at
                FROM marketing.consent_history
                WHERE source_updated_at > %s AND source_updated_at <= %s
                ORDER BY source_updated_at, consent_event_id
            """
            table = "raw_marketing_consents"
        else:
            raise ValueError(f"unsupported PostgreSQL source: {source}")
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            rows = connection.execute(sql, (window.window_start, window.window_end)).fetchall()
        return {table: tuple(dict(row) for row in rows)}

    return extract


_SALESFORCE_ASSETS: dict[str, tuple[str, tuple[str, ...]]] = {
    "raw_salesforce_accounts": (
        "Account",
        (
            "Name",
            "Industry",
            "BillingStreet",
            "BillingCity",
            "BillingState",
            "BillingPostalCode",
        ),
    ),
    "raw_salesforce_contacts": (
        "Contact",
        (
            "AccountId",
            "FirstName",
            "LastName",
            "Email",
            "MobilePhone",
            "MailingStreet",
            "MailingCity",
            "MailingState",
            "MailingPostalCode",
        ),
    ),
    "raw_salesforce_cases": (
        "Case",
        ("ContactId", "AccountId", "Status", "Priority", "Subject", "CreatedDate", "ClosedDate"),
    ),
}


def _salesforce_extractor(oauth: SalesforceOAuthConfig) -> ExtractFunction:
    key_maps = {
        "raw_salesforce_accounts": {
            "Id": "id",
            "Name": "name",
            "Industry": "industry",
            "BillingStreet": "billing_street",
            "BillingCity": "billing_city",
            "BillingState": "billing_state",
            "BillingPostalCode": "billing_postal_code",
            "SystemModstamp": "system_modstamp",
        },
        "raw_salesforce_contacts": {
            "Id": "id",
            "AccountId": "account_id",
            "FirstName": "first_name",
            "LastName": "last_name",
            "Email": "email",
            "MobilePhone": "mobile_phone",
            "MailingStreet": "mailing_street",
            "MailingCity": "mailing_city",
            "MailingState": "mailing_state",
            "MailingPostalCode": "mailing_postal_code",
            "SystemModstamp": "system_modstamp",
        },
        "raw_salesforce_cases": {
            "Id": "id",
            "ContactId": "contact_id",
            "AccountId": "account_id",
            "Status": "status",
            "Priority": "priority",
            "Subject": "subject",
            "CreatedDate": "created_date",
            "ClosedDate": "closed_date",
            "SystemModstamp": "system_modstamp",
        },
    }

    def extract(window: FrozenWindow) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        output: dict[str, Sequence[Mapping[str, Any]]] = {}
        with SalesforceConnector(oauth) as connector:
            for table, (object_name, fields) in _SALESFORCE_ASSETS.items():
                result = connector.extract_incremental(
                    object_name=object_name,
                    fields=fields,
                    low_watermark=window.window_start,
                    high_watermark=window.window_end,
                )
                mapped: list[dict[str, Any]] = []
                for record in result.records:
                    row = {target: record.get(source) for source, target in key_maps[table].items()}
                    if table == "raw_salesforce_accounts":
                        row["abn"] = None
                    if table == "raw_salesforce_contacts":
                        row["email_verified"] = False
                    mapped.append(row)
                output[table] = tuple(mapped)
        return output

    return extract


@dataclass(frozen=True, slots=True)
class VerifiedPublicPayload:
    path: Path
    retrieved_at: datetime
    first_seen_at: datetime
    sha256: str
    object_path: str


def _latest_public_payload(
    root: Path,
    source_id: str,
    suffix: str,
    *,
    as_of: datetime | None = None,
) -> VerifiedPublicPayload | None:
    source_root = (root / source_id).resolve()
    manifests = sorted(source_root.rglob("*.manifest.json"))
    if not manifests:
        return None
    candidates: list[tuple[datetime, str, Path, PublicFetchManifest]] = []
    for manifest_path in manifests:
        resolved_manifest = manifest_path.resolve()
        if not resolved_manifest.is_relative_to(source_root):
            raise RuntimeError(f"public manifest escaped its source root: {manifest_path}")
        manifest = PublicFetchManifest.model_validate_json(manifest_path.read_text())
        if manifest.source_id != source_id:
            raise RuntimeError(f"public manifest source mismatch: {manifest_path}")
        object_path = Path(manifest.object_path)
        if object_path.is_absolute():
            raise RuntimeError(f"public manifest object path must be relative: {manifest_path}")
        payload = (root / object_path).resolve()
        if not payload.is_relative_to(source_root):
            raise RuntimeError(f"public manifest payload escaped its source root: {manifest_path}")
        if not payload.is_file() or payload.suffix != suffix:
            raise RuntimeError(f"public manifest payload is missing or has wrong format: {payload}")
        payload_bytes = payload.read_bytes()
        if len(payload_bytes) != manifest.byte_count:
            raise RuntimeError(f"public manifest payload byte count mismatch: {payload}")
        if hashlib.sha256(payload_bytes).hexdigest() != manifest.sha256:
            raise RuntimeError(f"public manifest payload SHA-256 mismatch: {payload}")
        retrieved_at = _parse_timestamp(manifest.retrieved_at.isoformat(), name="retrieved_at")
        candidates.append((retrieved_at, manifest_path.as_posix(), payload, manifest))
    eligible = [candidate for candidate in candidates if as_of is None or candidate[0] <= as_of]
    if not eligible:
        return None
    retrieved_at, _, payload, manifest = max(eligible, key=lambda candidate: candidate[:2])
    first_seen_at = min(
        candidate[0] for candidate in eligible if candidate[3].sha256 == manifest.sha256
    )
    return VerifiedPublicPayload(
        path=payload,
        retrieved_at=retrieved_at,
        first_seen_at=first_seen_at,
        sha256=manifest.sha256,
        object_path=manifest.object_path,
    )


def _public_provenance(payload: VerifiedPublicPayload) -> dict[str, Any]:
    return {
        "source_retrieved_at": payload.retrieved_at.isoformat(),
        "source_payload_sha256": payload.sha256,
        "source_payload_object": payload.object_path,
    }


def _load_prepared_jsonl(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        document = json.loads(line)
        if not isinstance(document, dict):
            raise ValueError(f"{path}:{line_number} must contain a JSON object")
        rows.append(document)
    return tuple(rows)


class PreparedPublicArtifactManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    retrieved_at: datetime
    byte_count: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_payload_object: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_retrieved_at(self) -> PreparedPublicArtifactManifest:
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("prepared public artifact retrieved_at must include a timezone")
        return self


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_prepared_source_payload(
    *, public_root: Path, object_claim: str, expected_sha256: str
) -> None:
    root = public_root.resolve()

    def verify_one(object_path: str, sha256: str) -> None:
        relative = Path(object_path)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise RuntimeError("prepared source payload object must be a normalized relative path")
        try:
            payload_path = (root / relative).resolve(strict=True)
        except FileNotFoundError as exc:
            raise RuntimeError("prepared source payload object does not exist") from exc
        if not payload_path.is_relative_to(root) or not payload_path.is_file():
            raise RuntimeError("prepared source payload object escaped the public raw root")
        if _hash_file(payload_path) != sha256:
            raise RuntimeError("prepared source payload SHA-256 mismatch")

    if object_claim.lstrip().startswith("["):
        try:
            provenance = json.loads(object_claim)
        except json.JSONDecodeError as exc:
            raise RuntimeError("prepared composite source provenance is invalid JSON") from exc
        if not isinstance(provenance, list) or not provenance:
            raise RuntimeError("prepared composite source provenance must be a non-empty array")
        canonical = json.dumps(provenance, sort_keys=True, separators=(",", ":"))
        if hashlib.sha256(canonical.encode()).hexdigest() != expected_sha256:
            raise RuntimeError("prepared composite source provenance SHA-256 mismatch")
        for item in provenance:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("object_path"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", str(item.get("sha256", "")))
            ):
                raise RuntimeError("prepared composite source provenance entry is invalid")
            verify_one(item["object_path"], item["sha256"])
        return
    verify_one(object_claim, expected_sha256)


def _load_prepared_public_jsonl(
    path: Path, *, window: FrozenWindow, public_root: Path
) -> tuple[dict[str, Any], ...]:
    manifest_path = path.with_suffix(".manifest.json")
    if not path.exists() and not manifest_path.exists():
        return ()
    if not path.is_file() or not manifest_path.is_file():
        raise RuntimeError(f"prepared public artifact or manifest is missing: {path}")
    manifest = PreparedPublicArtifactManifest.model_validate_json(manifest_path.read_text())
    retrieved_at = manifest.retrieved_at.astimezone(UTC)
    payload = path.read_bytes()
    if len(payload) != manifest.byte_count:
        raise RuntimeError(f"prepared public artifact byte count mismatch: {path}")
    if hashlib.sha256(payload).hexdigest() != manifest.sha256:
        raise RuntimeError(f"prepared public artifact SHA-256 mismatch: {path}")
    _verify_prepared_source_payload(
        public_root=public_root,
        object_claim=manifest.source_payload_object,
        expected_sha256=manifest.source_payload_sha256,
    )
    if not window.window_start < retrieved_at <= window.window_end:
        return ()
    provenance = {
        "source_retrieved_at": retrieved_at.isoformat(),
        "source_payload_sha256": manifest.source_payload_sha256,
        "source_payload_object": manifest.source_payload_object,
    }
    rows = _load_prepared_jsonl(path)
    for row in rows:
        for field, expected in provenance.items():
            actual = row.get(field)
            if actual is not None and actual != expected:
                raise RuntimeError(f"prepared public artifact provenance mismatch: {field}")
            row[field] = expected
    return rows


def _load_abs_demographics(
    public_root: Path, *, window: FrozenWindow
) -> tuple[dict[str, Any], ...] | None:
    source_flows = {
        "abs_c21_g01_sa2_vic": "C21_G01_SA2",
        "abs_c21_g02_sa2_vic": "C21_G02_SA2",
        "abs_c21_g37_sa2_vic": "C21_G37_SA2",
    }
    payloads = {
        flow: _latest_public_payload(public_root, source_id, ".csv", as_of=window.window_end)
        for source_id, flow in source_flows.items()
    }
    if not any(payloads.values()):
        return None
    missing = sorted(flow for flow, payload in payloads.items() if payload is None)
    if missing:
        raise RuntimeError(f"incomplete ABS demographic source set: {missing}")
    selected = tuple(payload for payload in payloads.values() if payload is not None)
    composite_first_seen_at = max(payload.first_seen_at for payload in selected)
    if not window.window_start < composite_first_seen_at <= window.window_end:
        return ()

    provenance_rows = [
        {
            "source_id": source_id,
            "retrieved_at": payload.retrieved_at.isoformat(),
            "sha256": payload.sha256,
            "object_path": payload.object_path,
        }
        for source_id, flow in source_flows.items()
        if (payload := payloads[flow]) is not None
    ]
    provenance_json = json.dumps(provenance_rows, sort_keys=True, separators=(",", ":"))
    provenance = {
        "source_retrieved_at": max(row["retrieved_at"] for row in provenance_rows),
        "source_payload_sha256": hashlib.sha256(provenance_json.encode()).hexdigest(),
        "source_payload_object": provenance_json,
    }
    by_geography: dict[str, dict[str, Any]] = {}
    matched_metrics = dict.fromkeys(source_flows.values(), 0)
    for expected_flow, payload in payloads.items():
        if payload is None:  # pragma: no cover - guarded by the complete-set check above.
            continue
        with payload.path.open(encoding="utf-8-sig", newline="") as handle:
            rows = csv.DictReader(handle)
            required = {
                "STRUCTURE_ID",
                "REGION",
                "REGION_TYPE",
                "STATE",
                "TIME_PERIOD",
                "OBS_VALUE",
            }
            discriminator = {
                "C21_G01_SA2": "PCHAR",
                "C21_G02_SA2": "MEDAVG",
                "C21_G37_SA2": "TENLLD",
            }[expected_flow]
            missing_headers = sorted((required | {discriminator}) - set(rows.fieldnames or ()))
            if missing_headers:
                raise RuntimeError(
                    f"ABS {expected_flow} payload missing required headers: {missing_headers}"
                )
            for row in rows:
                structure_id = str(row.get("STRUCTURE_ID") or "")
                if expected_flow not in structure_id:
                    raise RuntimeError(f"ABS payload did not match expected flow {expected_flow}")
                if row.get("REGION_TYPE") != "SA2" or row.get("STATE") != "2":
                    raise RuntimeError("ABS payload escaped the declared Victorian SA2 slice")
                geography_code = str(row.get("REGION") or "").strip()
                period = str(row.get("TIME_PERIOD") or "").strip()
                if not geography_code or period != "2021":
                    continue
                record = by_geography.setdefault(
                    geography_code,
                    {
                        "geography_code": geography_code,
                        "geography_type": "SA2",
                        "reference_period": "2021-01-01",
                        "population": None,
                        "median_household_income_weekly": None,
                        "owner_occupied_dwellings": 0,
                        "rented_dwellings": None,
                        "source_dataset": "ABS Census 2021 G01/G02/G37 v1.0.0",
                        **provenance,
                    },
                )
                try:
                    value = int(float(str(row.get("OBS_VALUE") or "")))
                except ValueError as exc:
                    raise RuntimeError("ABS observation was not numeric") from exc
                if expected_flow == "C21_G01_SA2" and row.get("PCHAR") == "P_1":
                    record["population"] = value
                    matched_metrics[expected_flow] += 1
                elif expected_flow == "C21_G02_SA2" and row.get("MEDAVG") == "4":
                    record["median_household_income_weekly"] = value
                    matched_metrics[expected_flow] += 1
                elif expected_flow == "C21_G37_SA2" and row.get("TENLLD") in {"1", "2"}:
                    record["owner_occupied_dwellings"] += value
                    matched_metrics[expected_flow] += 1
                elif expected_flow == "C21_G37_SA2" and row.get("TENLLD") == "R_T":
                    record["rented_dwellings"] = value
                    matched_metrics[expected_flow] += 1

    missing_metrics = sorted(flow for flow, count in matched_metrics.items() if count == 0)
    if missing_metrics:
        raise RuntimeError(f"ABS payloads contained no required observations: {missing_metrics}")

    return tuple(by_geography[key] for key in sorted(by_geography))


def _property_extractor(public_root: Path, prepared_root: Path) -> ExtractFunction:
    def extract(window: FrozenWindow) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        master: list[dict[str, Any]] = []
        payload = _latest_public_payload(
            public_root, "vicmap_address", ".json", as_of=window.window_end
        )
        if payload and window.window_start < payload.first_seen_at <= window.window_end:
            document = json.loads(payload.path.read_text())
            if not isinstance(document, dict) or not isinstance(document.get("features"), list):
                raise RuntimeError("Vicmap payload must contain a features array")
            for feature in document["features"]:
                if not isinstance(feature, dict) or not isinstance(feature.get("attributes"), dict):
                    raise RuntimeError("Vicmap feature must contain an attributes object")
                attributes = feature["attributes"]
                property_id = attributes.get("property_pfi") or attributes.get("pfi")
                address_id = attributes.get("pfi") or attributes.get("ufi")
                master.append(
                    {
                        "property_id": str(property_id) if property_id is not None else None,
                        "address_id": str(address_id) if address_id is not None else None,
                        "address_line_1": attributes.get("ezi_address"),
                        "suburb": attributes.get("locality_name"),
                        "state": "VIC",
                        "postcode": attributes.get("postcode"),
                        "property_type": None,
                        "latitude": None,
                        "longitude": None,
                        "geometry_wkt": None,
                        "source_dataset": "Vicmap Address",
                        "source_updated_at": payload.retrieved_at.isoformat(),
                        **_public_provenance(payload),
                    }
                )
        return {
            "raw_property_master": tuple(master),
            "raw_property_market": _load_prepared_public_jsonl(
                prepared_root / "raw_property_market.jsonl",
                window=window,
                public_root=public_root,
            ),
        }

    return extract


def _economic_extractor(public_root: Path, prepared_root: Path) -> ExtractFunction:
    def extract(window: FrozenWindow) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        indicators: list[dict[str, Any]] = []
        payload = _latest_public_payload(
            public_root, "rba_f1_interest_rates", ".csv", as_of=window.window_end
        )
        if payload and window.window_start < payload.first_seen_at <= window.window_end:
            with payload.path.open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.reader(handle))
            labelled_rows = {
                row[0].strip().casefold(): (index, row)
                for index, row in enumerate(rows[:20])
                if row and row[0].strip()
            }
            required_labels = {"title", "units", "source", "series id"}
            missing_labels = sorted(required_labels - set(labelled_rows))
            if missing_labels:
                raise RuntimeError(f"RBA payload missing required labels: {missing_labels}")
            titles = labelled_rows["title"][1]
            units = labelled_rows["units"][1]
            sources = labelled_rows["source"][1]
            series_index, series_ids = labelled_rows["series id"]
            rba_series = [
                index
                for index, series_id in enumerate(series_ids[1:], start=1)
                if series_id and index < len(sources) and sources[index].strip().casefold() == "rba"
            ]
            if not rba_series:
                raise RuntimeError("RBA payload contained no declared RBA series")
            parseable_observation_rows = 0
            parseable_rba_values = 0
            for raw_row in rows[series_index + 1 :]:
                try:
                    observation_date = (
                        datetime.strptime(raw_row[0], "%d-%b-%Y").replace(tzinfo=UTC).date()
                    )
                except (ValueError, IndexError):
                    continue
                parseable_observation_rows += 1
                for index, series_id in enumerate(series_ids[1:], start=1):
                    value = raw_row[index].strip() if index < len(raw_row) else ""
                    source_name = sources[index].strip().casefold() if index < len(sources) else ""
                    if not series_id or not value or source_name != "rba":
                        continue
                    try:
                        numeric_value = float(value)
                    except ValueError:
                        continue
                    parseable_rba_values += 1
                    indicators.append(
                        {
                            "series_id": series_id,
                            "series_name": titles[index] if index < len(titles) else None,
                            "observation_date": observation_date.isoformat(),
                            "observation_value": numeric_value,
                            "unit": units[index] if index < len(units) else None,
                            **_public_provenance(payload),
                        }
                    )
            if parseable_observation_rows == 0 or parseable_rba_values == 0:
                raise RuntimeError("RBA payload contained no parseable RBA observations")
        abs_rows = _load_abs_demographics(public_root, window=window)
        if abs_rows is None:
            abs_rows = _load_prepared_public_jsonl(
                prepared_root / "raw_abs_demographics.jsonl",
                window=window,
                public_root=public_root,
            )
        return {
            "raw_abs_demographics": abs_rows,
            "raw_rba_indicators": tuple(indicators),
        }

    return extract


def _storage_client(*, credentials: Any, project: str | None) -> Any:
    import google.cloud.storage as storage  # type: ignore[import-untyped]

    return storage.Client(project=project, credentials=credentials)


def _bigquery_client(*, credentials: Any, project: str) -> Any:
    import google.cloud.bigquery as bigquery

    return bigquery.Client(project=project, credentials=credentials)


def _pubsub_clients(*, credentials: Any) -> tuple[Any, Any]:
    import google.cloud.pubsub_v1 as pubsub_v1

    return (
        pubsub_v1.SubscriberClient(credentials=credentials),
        pubsub_v1.PublisherClient(credentials=credentials),
    )


def build_operator_from_environment() -> BatchIngestionOperator:
    state_root = Path(os.getenv("PJ360_INGEST_STATE_DIR", "data/ingestion-state"))
    state = OperatorState(state_root)
    project = os.getenv("BIGQUERY_PROJECT", os.getenv("GCP_PROJECT_ID", "")).strip() or None
    raw_dataset = (
        os.getenv("BIGQUERY_RAW_DATASET", os.getenv("DBT_BIGQUERY_RAW_DATASET", "")).strip() or None
    )
    credentials = impersonated_credentials_from_environment("PJ360_INGEST_SERVICE_ACCOUNT")
    bucket = os.getenv("GCS_RAW_BUCKET", "").strip()
    if credentials is None and (bucket or (project and raw_dataset)):
        raise RuntimeError(
            "PJ360_INGEST_SERVICE_ACCOUNT is required when batch GCP resources are configured"
        )
    landing: LocalRawLanding | GCSRawLanding
    if bucket:
        storage_client = None
        if credentials is not None:
            storage_client = _storage_client(project=project, credentials=credentials)
        landing = GCSRawLanding(bucket_name=bucket, client=storage_client)
    else:
        landing = LocalRawLanding(state_root / "landing")

    extractors: dict[str, ExtractFunction] = {}
    dsn = os.getenv("PJ360_POSTGRES_DSN", "").strip()
    if dsn:
        for source in ("legacy_crm", "support", "consent"):
            extractors[source] = _postgres_extractor(dsn, source)

    sf_client_id = os.getenv("SALESFORCE_CLIENT_ID", "").strip()
    sf_client_secret = os.getenv("SALESFORCE_CLIENT_SECRET", "").strip()
    sf_login_url = os.getenv("SALESFORCE_LOGIN_URL", "").strip()
    sf_settings = (sf_client_id, sf_client_secret, sf_login_url)
    if any(sf_settings) and not all(sf_settings):
        raise RuntimeError(
            "SALESFORCE_CLIENT_ID, SALESFORCE_CLIENT_SECRET and SALESFORCE_LOGIN_URL "
            "must be configured together"
        )
    if all(sf_settings):
        extractors["salesforce"] = _salesforce_extractor(
            SalesforceOAuthConfig(
                client_id=sf_client_id,
                client_secret=sf_client_secret,
                auth_base_url=sf_login_url,
                api_version=os.getenv("SALESFORCE_API_VERSION", "65.0"),
            )
        )

    public_root = Path(os.getenv("PJ360_PUBLIC_RAW_DIR", "data/raw/public"))
    prepared_root = Path(os.getenv("PJ360_PREPARED_RAW_DIR", "data/raw/normalized"))
    extractors["property"] = _property_extractor(public_root, prepared_root)
    extractors["economic"] = _economic_extractor(public_root, prepared_root)

    location = os.getenv("BIGQUERY_LOCATION", "australia-southeast1").strip()
    loader = None
    if project and raw_dataset:
        bigquery_client = None
        if credentials is not None:
            bigquery_client = _bigquery_client(project=project, credentials=credentials)
        loader = BigQueryJsonLoader(client=bigquery_client, location=location)
    return BatchIngestionOperator(
        state=state,
        landing=landing,
        extractors=extractors,
        bigquery_loader=loader,
        bigquery_project=project,
        bigquery_raw_dataset=raw_dataset,
    )


def build_event_operator_from_environment() -> EventIngestionOperator:
    state_root = Path(os.getenv("PJ360_INGEST_STATE_DIR", "data/ingestion-state"))
    state = OperatorState(state_root)
    project = os.getenv("BIGQUERY_PROJECT", os.getenv("GCP_PROJECT_ID", "")).strip() or None
    raw_dataset = (
        os.getenv("BIGQUERY_RAW_DATASET", os.getenv("DBT_BIGQUERY_RAW_DATASET", "")).strip() or None
    )
    subscription = os.getenv("PUBSUB_EVENT_SUBSCRIPTION", "").strip()
    dead_letter_topic = os.getenv("PUBSUB_DEAD_LETTER_TOPIC", "").strip()
    if bool(subscription) != bool(dead_letter_topic):
        raise RuntimeError(
            "PUBSUB_EVENT_SUBSCRIPTION and PUBSUB_DEAD_LETTER_TOPIC must be configured together"
        )
    credentials = impersonated_credentials_from_environment("PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT")
    bucket = os.getenv("GCS_RAW_BUCKET", "").strip()
    real_pubsub = bool(subscription and not os.getenv("PUBSUB_EMULATOR_HOST", "").strip())
    if credentials is None and (bucket or (project and raw_dataset) or real_pubsub):
        raise RuntimeError(
            "PJ360_EVENT_CONSUMER_SERVICE_ACCOUNT is required when event GCP resources "
            "are configured"
        )
    landing: LocalRawLanding | GCSRawLanding
    if bucket:
        storage_client = None
        if credentials is not None:
            storage_client = _storage_client(project=project, credentials=credentials)
        landing = GCSRawLanding(bucket_name=bucket, client=storage_client)
    else:
        landing = LocalRawLanding(state_root / "landing")

    pull_source: EventPullSource | None = None
    if subscription and dead_letter_topic:
        subscriber = None
        publisher = None
        if credentials is not None:
            subscriber, publisher = _pubsub_clients(credentials=credentials)
        pull_source = GooglePubSubPullSource(
            subscription=subscription,
            dead_letter_topic=dead_letter_topic,
            subscriber=subscriber,
            publisher=publisher,
        )

    location = os.getenv("BIGQUERY_LOCATION", "australia-southeast1").strip()
    loader = None
    if project and raw_dataset:
        bigquery_client = None
        if credentials is not None:
            bigquery_client = _bigquery_client(project=project, credentials=credentials)
        loader = BigQueryJsonLoader(client=bigquery_client, location=location)
    raw_backfill = os.getenv("PJ360_EVENT_BACKFILL_JSONL", "").strip()
    return EventIngestionOperator(
        state=state,
        landing=landing,
        pull_source=pull_source,
        bigquery_loader=loader,
        bigquery_project=project,
        bigquery_raw_dataset=raw_dataset,
        backfill_path=Path(raw_backfill) if raw_backfill else None,
    )


def frozen_window_from_values(
    *, window_start: str, window_end: str, window_id: str, mode: str, replay_id: str | None
) -> FrozenWindow:
    return FrozenWindow(
        window_start=_parse_timestamp(window_start, name="window_start"),
        window_end=_parse_timestamp(window_end, name="window_end"),
        window_id=window_id,
        mode=cast(WindowMode, mode.upper()),
        replay_id=replay_id or None,
    )
