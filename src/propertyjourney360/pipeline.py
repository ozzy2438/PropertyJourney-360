"""Deterministic local vertical slice used for integration evidence and reviewer onboarding."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from propertyjourney360.api.repository import CustomerRepository
from propertyjourney360.domain.models import MatchDecision
from propertyjourney360.events.models import EventType, PropertyEvent
from propertyjourney360.events.processor import EventProcessor
from propertyjourney360.evidence import hash_source_tree
from propertyjourney360.governance.reconciliation import ReconciliationResult, reconcile
from propertyjourney360.governance.remediation import open_case
from propertyjourney360.identity.evaluation import (
    ClusterMetrics,
    IdentityMetrics,
    evaluate_clusters,
    evaluate_matches,
)
from propertyjourney360.identity.matcher import IdentityResolver
from propertyjourney360.identity.normalization import normalize_identity
from propertyjourney360.ingestion.landing import LandingManifest, LocalRawLanding
from propertyjourney360.synthetic import (
    DatasetSplit,
    SyntheticDefect,
    SyntheticIdentityConfig,
    generate_identity_dataset,
)


class LocalPipelineReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    source_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    seed: int
    synthetic_customer_count: int = Field(ge=1)
    source_identity_count: int = Field(ge=1)
    canonical_customer_count: int = Field(ge=1)
    held_out_pair_metrics: IdentityMetrics
    held_out_cluster_metrics: ClusterMetrics
    cluster_conflict_count: int = Field(ge=0)
    reconciliation: ReconciliationResult
    landing_manifest: LandingManifest
    customer_journey_event_count: int = Field(ge=0)
    generated_at: datetime
    evidence_scope: str


def _record_disposition(defects: tuple[SyntheticDefect, ...]) -> str:
    critical = {SyntheticDefect.MALFORMED_EMAIL, SyntheticDefect.MALFORMED_POSTCODE}
    return "QUARANTINED" if critical.intersection(defects) else "ACCEPTED"


def run_local_pipeline(
    *,
    output_root: Path,
    database_path: Path,
    seed: int = 20260824,
    customer_count: int = 100,
) -> LocalPipelineReport:
    """Run a real local slice without implying Salesforce or GCP execution."""

    dataset = generate_identity_dataset(
        SyntheticIdentityConfig(customer_count=customer_count, seed=seed)
    )
    run_id = f"local-{seed}-{customer_count}"
    extracted_at = datetime(2026, 8, 24, tzinfo=UTC)
    landing = LocalRawLanding(output_root / "raw")
    manifest = landing.land(
        source="synthetic_identities",
        run_id=run_id,
        schema_version="identity-record-v1",
        records=(record.model_dump(mode="json") for record in dataset.records),
        extracted_at=extracted_at,
        watermark=max(record.source_updated_at for record in dataset.records).isoformat(),
    )

    resolver = IdentityResolver()
    all_matches = resolver.compare_all(dataset.records)
    cluster_resolution = resolver.resolve_clusters(dataset.records, all_matches)
    held_out_records = dataset.records_for_split(DatasetSplit.HELD_OUT)
    held_out_matches = resolver.compare_all(held_out_records)
    pair_metrics = evaluate_matches(held_out_records, held_out_matches)
    held_out_assignments = resolver.resolve_clusters(
        held_out_records,
        held_out_matches,
    ).assignments
    cluster_metrics = evaluate_clusters(held_out_records, held_out_assignments)

    labels_by_key = {label.source_key: label for label in dataset.labels}
    dispositions = {
        source_key: _record_disposition(label.defects)
        for source_key, label in labels_by_key.items()
    }
    quarantined = sum(value == "QUARANTINED" for value in dispositions.values())
    reconciliation = reconcile(
        run_id=run_id,
        dataset="synthetic_identities",
        source=len(dataset.records),
        accepted=len(dataset.records) - quarantined,
        quarantined=quarantined,
        rejected=0,
        explained_duplicates=0,
    )

    repository = CustomerRepository(database_path)
    records_by_customer: defaultdict[str, list[Any]] = defaultdict(list)
    for record in dataset.records:
        customer_id = cluster_resolution.assignments[record.source_key]
        records_by_customer[customer_id].append(record)

    match_rank = {
        MatchDecision.NO_MATCH: 0,
        MatchDecision.REVIEW_REQUIRED: 1,
        MatchDecision.AUTO_MATCH: 2,
    }
    best_match_by_source: dict[str, tuple[MatchDecision, float]] = {}
    for match in all_matches:
        for source_key in (match.left_source_key, match.right_source_key):
            previous = best_match_by_source.get(source_key)
            if previous is None or (match_rank[match.decision], match.score) > (
                match_rank[previous[0]],
                previous[1],
            ):
                best_match_by_source[source_key] = (match.decision, match.score)

    for customer_id, records in records_by_customer.items():
        preferred = next(
            (record for record in records if record.source_system.value == "salesforce"),
            records[0],
        )
        display_name = " ".join(
            value for value in (preferred.first_name, preferred.last_name) if value
        )
        suburbs = sorted({record.suburb for record in records if record.suburb})
        repository.upsert_customer(
            {
                "customer_id": customer_id,
                "display_name": display_name or "Synthetic customer",
                "preferred_suburbs": json.dumps(suburbs),
                "observed_price_band": None,
                "identity_rule_version": resolver.config.rule_version,
                "as_of": max(record.source_updated_at for record in records).isoformat(),
            }
        )
        for record in records:
            normalized = normalize_identity(record)
            decision, score = best_match_by_source.get(
                record.source_key,
                (MatchDecision.NO_MATCH, 0.0),
            )
            repository.add_identity_source(
                {
                    "customer_id": customer_id,
                    "source_system": record.source_system.value,
                    "source_record_id": record.source_record_id,
                    "normalized_email": normalized.email,
                    "normalized_phone": normalized.phone,
                    "match_score": score,
                    "match_decision": decision.value,
                }
            )

    for record in dataset.records:
        label = labels_by_key[record.source_key]
        if dispositions[record.source_key] != "QUARANTINED":
            continue
        issue_type = (
            "MALFORMED_EMAIL"
            if SyntheticDefect.MALFORMED_EMAIL in label.defects
            else "MALFORMED_POSTCODE"
        )
        case = open_case(
            source_system=record.source_system.value,
            source_record_id=record.source_record_id,
            issue_type=issue_type,
            actor="local-dq-worker",
            note="Synthetic defect quarantined by the local contract gate.",
        )
        repository.upsert_remediation(
            {
                "case_id": str(case.case_id),
                "source_system": case.source_system,
                "source_record_id": case.source_record_id,
                "issue_type": case.issue_type,
                "status": case.status.value,
                "opened_at": case.opened_at.isoformat(),
                "payload_json": case.model_dump_json(),
            }
        )

    journey_customer = min(records_by_customer)
    processor = EventProcessor(repository, history_window=timedelta(days=30))
    base_event = extracted_at - timedelta(hours=2)
    events = (
        PropertyEvent(
            schema_version=1,
            event_id=f"{run_id}-view",
            event_type=EventType.VIEW,
            occurred_at=base_event,
            received_at=base_event + timedelta(seconds=3),
            session_id=f"{run_id}-session",
            anonymous_id=f"{run_id}-anonymous",
            property_id="vicmap-property-fixture-001",
        ),
        PropertyEvent(
            schema_version=2,
            event_id=f"{run_id}-save",
            event_type=EventType.SAVE,
            occurred_at=base_event + timedelta(minutes=5),
            received_at=base_event + timedelta(minutes=5, seconds=2),
            session_id=f"{run_id}-session",
            anonymous_id=f"{run_id}-anonymous",
            property_id="vicmap-property-fixture-001",
            device_type="mobile",
        ),
        PropertyEvent(
            schema_version=2,
            event_id=f"{run_id}-identify",
            event_type=EventType.IDENTIFY,
            occurred_at=base_event + timedelta(minutes=10),
            received_at=base_event + timedelta(minutes=10, seconds=1),
            session_id=f"{run_id}-session",
            anonymous_id=f"{run_id}-anonymous",
            customer_id=journey_customer,
            property_id="vicmap-property-fixture-001",
            device_type="mobile",
        ),
    )
    for event in events:
        processor.process(event)
    journey_count = len(repository.list_journey(journey_customer, limit=100, offset=0))

    report = LocalPipelineReport(
        run_id=run_id,
        source_tree_sha256=hash_source_tree(Path(__file__).resolve().parents[2]),
        seed=seed,
        synthetic_customer_count=customer_count,
        source_identity_count=len(dataset.records),
        canonical_customer_count=len(records_by_customer),
        held_out_pair_metrics=pair_metrics,
        held_out_cluster_metrics=cluster_metrics,
        cluster_conflict_count=len(cluster_resolution.conflicts),
        reconciliation=reconciliation,
        landing_manifest=manifest,
        customer_journey_event_count=journey_count,
        generated_at=datetime.now(UTC),
        evidence_scope=(
            "Deterministic local synthetic execution only; "
            "not Salesforce, BigQuery or GCP evidence."
        ),
    )
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "local_pipeline_report.json").write_text(report.model_dump_json(indent=2))
    return report
