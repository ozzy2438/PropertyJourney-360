"""Validated public-source register; access policy is checked before network ingestion."""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, HttpUrl, model_validator


class RedistributionStatus(StrEnum):
    ALLOWED = "allowed"
    RESTRICTED = "restricted"
    UNVERIFIED = "unverified"


class RepoPolicy(StrEnum):
    RAW_OUT_OF_GIT = "raw_out_of_git"
    FIXTURE_ONLY = "fixture_only"
    DERIVED_ALLOWED = "derived_allowed"
    PROHIBITED = "prohibited"


class SourceDefinition(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    enabled: bool
    publisher: str
    dataset_title: str
    metadata_url: HttpUrl
    access_url: HttpUrl
    access_method: str
    format: str
    update_frequency: str
    record_grain: str
    geographic_coverage: str
    query_or_filter: str
    licence_name: str
    licence_url: HttpUrl | None
    attribution_text: str | None
    redistribution_status: RedistributionStatus
    third_party_material_policy: str
    repo_policy: RepoPolicy
    suppression_policy: str | None = None
    quality_notes: str

    @model_validator(mode="after")
    def enforce_licence_gate(self) -> SourceDefinition:
        if self.redistribution_status is RedistributionStatus.UNVERIFIED and self.enabled:
            raise ValueError(f"unverified source {self.source_id} cannot be enabled")
        if self.repo_policy is RepoPolicy.PROHIBITED and self.enabled:
            raise ValueError(f"prohibited source {self.source_id} cannot be enabled")
        if self.redistribution_status is RedistributionStatus.ALLOWED and not self.licence_url:
            raise ValueError(f"allowed source {self.source_id} requires a licence URL")
        return self


class SourceCatalog(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    catalog_version: int
    terms_checked_at: date
    sources: tuple[SourceDefinition, ...]

    @model_validator(mode="after")
    def require_unique_ids(self) -> SourceCatalog:
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("source_id values must be unique")
        return self

    def require_enabled(self, source_id: str) -> SourceDefinition:
        source = next((item for item in self.sources if item.source_id == source_id), None)
        if source is None:
            raise KeyError(f"unknown public source: {source_id}")
        if not source.enabled:
            raise PermissionError(f"public source is disabled by catalogue policy: {source_id}")
        return source


def load_catalog(path: Path) -> SourceCatalog:
    raw = yaml.safe_load(path.read_text())
    return SourceCatalog.model_validate(raw)
