"""Strict preparation of pinned Valuer-General Victoria annual workbooks."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from zipfile import BadZipFile, ZipFile

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
from pydantic import BaseModel, ConfigDict, Field, model_validator

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_CELL_REFERENCE = re.compile(r"^([A-Z]+)[1-9][0-9]*$")
_SOURCE_ID = "vgv_property_sales_timeseries"
_DATASET_NAME = "Valuer-General Victoria annual property sales 2025"
_YEARS = {
    2015: ("B", "C"),
    2016: ("D", "E"),
    2017: ("F", "G"),
    2018: ("H", "I"),
    2019: ("J", "K"),
    2020: ("L", "M"),
    2021: ("N", "O"),
    2022: ("P", "Q"),
    2023: ("R", "S"),
    2024: ("T", "U"),
    2025: ("V", "W"),
}
_MISSING_PRICE = {"", "-", "NA"}
_QUALITY_MARKERS = {"", "^", "*"}

PropertyType = Literal["HOUSE", "UNIT", "LAND"]


class VGVWorkbookSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    property_type: PropertyType
    filename: str
    official_url: str
    expected_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_byte_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_official_url(self) -> VGVWorkbookSpec:
        if not self.official_url.startswith("https://www.land.vic.gov.au/"):
            raise ValueError("VGV official_url must use the land.vic.gov.au HTTPS origin")
        return self


VGV_ANNUAL_2025_SPECS: tuple[VGVWorkbookSpec, ...] = (
    VGVWorkbookSpec(
        property_type="HOUSE",
        filename="houses-by-suburb-2015-2025.xlsx",
        official_url=(
            "https://www.land.vic.gov.au/__data/assets/excel_doc/0033/775617/"
            "houses-by-suburb-2015-2025.xlsx"
        ),
        expected_sha256="b6220620f5ac4a586625d0d2adfbab4e82925531b34abfbefb67f926986e6249",
        expected_byte_count=137_891,
    ),
    VGVWorkbookSpec(
        property_type="UNIT",
        filename="units-by-suburb-2015-2025.xlsx",
        official_url=(
            "https://www.land.vic.gov.au/__data/assets/excel_doc/0034/775618/"
            "units-by-suburb-2015-2025.xlsx"
        ),
        expected_sha256="94bfff96f5845d3a811d0ac03fe638dae4c170a347d4b365b3652cc287b576ee",
        expected_byte_count=83_164,
    ),
    VGVWorkbookSpec(
        property_type="LAND",
        filename="land-by-suburb-2015-2025.xlsx",
        official_url=(
            "https://www.land.vic.gov.au/__data/assets/excel_doc/0035/775619/"
            "land-by-suburb-2015-2025.xlsx"
        ),
        expected_sha256="0dccc5e1543812d239971ccc4d270a2e9d4ab9f1ae0ca0316e16055987273136",
        expected_byte_count=46_134,
    ),
)


class VGVWorkbookObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    property_type: PropertyType
    filename: str
    official_url: str
    byte_count: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    raw_object_path: str
    locality_count: int = Field(gt=0)
    prepared_row_count: int = Field(gt=0)
    unavailable_price_count: int = Field(ge=0)
    quality_marker_counts: dict[str, int]


class VGVPreparationReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_version: int = 1
    release_id: str = "vgv-annual-2025"
    generated_at: datetime
    acquisition_method: str = "owner_supplied_browser_download"
    observations: tuple[VGVWorkbookObservation, ...]
    prepared_row_count: int = Field(gt=0)
    prepared_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    prepared_byte_count: int = Field(gt=0)
    prepared_filename: str
    source_payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_payload_object: str
    limitation: str


class _ManualImportManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str = _SOURCE_ID
    publisher: str = "Valuer-General Victoria"
    dataset_title: str = _DATASET_NAME
    acquired_at: datetime
    acquisition_method: str = "owner_supplied_browser_download"
    original_filename: str
    official_url: str
    byte_count: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    object_path: str
    licence_name: str = "Creative Commons Attribution 4.0 International"
    licence_url: str = "https://creativecommons.org/licenses/by/4.0/"
    limitation: str = (
        "The owner downloaded the workbook in a browser; HTTP status and response headers were "
        "not independently captured by the pipeline. File identity and workbook structure were "
        "verified locally."
    )


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _shared_strings(archive: ZipFile) -> tuple[str, ...]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return ()
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    return tuple(
        "".join(text.text or "" for text in item.iter(f"{{{_MAIN_NS}}}t"))
        for item in root.findall(f"{{{_MAIN_NS}}}si")
    )


def _single_sheet_path(archive: ZipFile) -> str:
    workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    sheets = workbook.findall(f".//{{{_MAIN_NS}}}sheet")
    if len(sheets) != 1 or sheets[0].attrib.get("name") != "Sheet1":
        raise ValueError("VGV workbook must contain exactly the expected Sheet1 worksheet")
    relationships = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relationship_id = sheets[0].attrib[f"{{{_OFFICE_REL_NS}}}id"]
    targets: dict[str, str] = {item.attrib["Id"]: item.attrib["Target"] for item in relationships}
    target = targets.get(relationship_id)
    if not target:
        raise ValueError("VGV workbook worksheet relationship is missing")
    normalized = target.lstrip("/")
    if not normalized.startswith("xl/"):
        normalized = f"xl/{normalized}"
    if normalized not in archive.namelist():
        raise ValueError("VGV workbook worksheet payload is missing")
    return normalized


def _read_cells(path: Path) -> dict[int, dict[str, str]]:
    try:
        with ZipFile(path) as archive:
            shared = _shared_strings(archive)
            sheet = ElementTree.fromstring(archive.read(_single_sheet_path(archive)))
    except (BadZipFile, ElementTree.ParseError, DefusedXmlException, KeyError) as exc:
        raise ValueError(f"invalid VGV XLSX package: {path.name}") from exc

    rows: dict[int, dict[str, str]] = {}
    for row in sheet.findall(f".//{{{_MAIN_NS}}}sheetData/{{{_MAIN_NS}}}row"):
        row_number = int(row.attrib["r"])
        values: dict[str, str] = {}
        for cell in row.findall(f"{{{_MAIN_NS}}}c"):
            reference = cell.attrib.get("r", "")
            match = _CELL_REFERENCE.fullmatch(reference)
            if match is None:
                raise ValueError(f"invalid VGV cell reference: {reference!r}")
            if cell.find(f"{{{_MAIN_NS}}}f") is not None:
                raise ValueError(f"VGV source values must not depend on formulas: {reference}")
            value_node = cell.find(f"{{{_MAIN_NS}}}v")
            value = "" if value_node is None or value_node.text is None else value_node.text
            cell_type = cell.attrib.get("t")
            if cell_type == "s" and value:
                try:
                    value = shared[int(value)]
                except (IndexError, ValueError) as exc:
                    raise ValueError(f"invalid shared string at {reference}") from exc
            elif cell_type == "inlineStr":
                value = "".join(text.text or "" for text in cell.iter(f"{{{_MAIN_NS}}}t"))
            values[match.group(1)] = value.strip()
        rows[row_number] = values
    return rows


def _parse_workbook(
    path: Path, spec: VGVWorkbookSpec
) -> tuple[list[dict[str, object]], int, int, dict[str, int]]:
    if path.name != spec.filename:
        raise ValueError(f"expected {spec.filename}, received {path.name}")
    if not path.is_file():
        raise ValueError(f"VGV workbook does not exist: {path}")
    if path.stat().st_size != spec.expected_byte_count:
        raise ValueError(f"VGV workbook byte count changed: {path.name}")
    if _hash_file(path) != spec.expected_sha256:
        raise ValueError(f"VGV workbook SHA-256 changed: {path.name}")

    rows = _read_cells(path)
    if rows.get(2, {}).get("A") != "Locality":
        raise ValueError(f"VGV workbook locality header changed: {path.name}")
    for year, (price_column, _marker_column) in _YEARS.items():
        if rows.get(2, {}).get(price_column) != str(year):
            raise ValueError(f"VGV workbook year header changed for {year}: {path.name}")
    if rows.get(2, {}).get("X") != "Prelim" or rows.get(3, {}).get("X") != "2026":
        raise ValueError(f"VGV preliminary column contract changed: {path.name}")

    records: list[dict[str, object]] = []
    localities: set[str] = set()
    unavailable = 0
    marker_counts: Counter[str] = Counter()
    for row_number in sorted(number for number in rows if number >= 6):
        cells = rows[row_number]
        locality = cells.get("A", "").strip()
        if not locality:
            continue
        if locality != locality.upper():
            raise ValueError(f"VGV locality must be uppercase at row {row_number}: {path.name}")
        if locality in localities:
            raise ValueError(f"duplicate VGV locality {locality!r}: {path.name}")
        localities.add(locality)
        for year, (price_column, marker_column) in _YEARS.items():
            price_text = cells.get(price_column, "").strip()
            marker = cells.get(marker_column, "").strip()
            if marker not in _QUALITY_MARKERS:
                raise ValueError(
                    f"unexpected VGV quality marker {marker!r} at {marker_column}{row_number}"
                )
            marker_counts[marker or "NONE"] += 1
            if price_text in _MISSING_PRICE:
                unavailable += 1
                continue
            if not price_text.isdecimal():
                raise ValueError(
                    f"invalid VGV median price {price_text!r} at {price_column}{row_number}"
                )
            median_price = int(price_text)
            if median_price <= 0:
                raise ValueError(f"VGV median price must be positive at row {row_number}")
            records.append(
                {
                    "geography_code": locality,
                    "geography_type": "SUBURB",
                    "period_start": f"{year}-01-01",
                    "property_type": spec.property_type,
                    "median_price": median_price,
                    "sales_volume": None,
                    "quality_marker": marker or None,
                    "source_dataset": _DATASET_NAME,
                }
            )
    if not localities or not records:
        raise ValueError(f"VGV workbook contained no usable annual records: {path.name}")
    return records, len(localities), unavailable, dict(sorted(marker_counts.items()))


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _existing_import(
    raw_root: Path, spec: VGVWorkbookSpec
) -> tuple[Path, _ManualImportManifest] | None:
    property_root = raw_root / _SOURCE_ID / f"property_type={spec.property_type.casefold()}"
    candidates = sorted(property_root.rglob(f"{spec.expected_sha256}.xlsx"))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise RuntimeError(
            f"multiple imports exist for the pinned {spec.property_type} VGV workbook"
        )
    raw_path = candidates[0]
    if _hash_file(raw_path) != spec.expected_sha256:
        raise RuntimeError(f"content-addressed VGV source was modified: {raw_path}")
    manifest_path = raw_path.with_suffix(".browser-import.json")
    if not manifest_path.is_file():
        raise RuntimeError(f"VGV browser import manifest is missing: {manifest_path}")
    try:
        manifest = _ManualImportManifest.model_validate_json(manifest_path.read_text())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"VGV browser import manifest is invalid: {manifest_path}") from exc
    expected_object = raw_path.relative_to(raw_root).as_posix()
    if (
        manifest.original_filename != spec.filename
        or manifest.official_url != spec.official_url
        or manifest.byte_count != spec.expected_byte_count
        or manifest.sha256 != spec.expected_sha256
        or manifest.object_path != expected_object
    ):
        raise RuntimeError(f"VGV browser import manifest changed: {manifest_path}")
    return raw_path, manifest


def prepare_vgv_market(
    *,
    workbook_paths: Mapping[str, Path],
    raw_root: Path,
    prepared_path: Path,
    evidence_path: Path,
    acquired_at: datetime | None = None,
) -> VGVPreparationReport:
    """Validate, content-address, normalize and evidence the pinned 2025 VGV release."""

    if acquired_at is not None and acquired_at.utcoffset() is None:
        raise ValueError("acquired_at must be timezone-aware")
    expected_types = {spec.property_type for spec in VGV_ANNUAL_2025_SPECS}
    if set(workbook_paths) != expected_types:
        raise ValueError("VGV preparation requires HOUSE, UNIT and LAND workbooks exactly once")

    existing_imports = {
        spec.property_type: existing
        for spec in VGV_ANNUAL_2025_SPECS
        if (existing := _existing_import(raw_root, spec)) is not None
    }
    existing_timestamps = {
        manifest.acquired_at.astimezone(UTC) for _path, manifest in existing_imports.values()
    }
    if len(existing_timestamps) > 1:
        raise RuntimeError("pinned VGV workbooks have inconsistent acquisition timestamps")
    existing_timestamp = next(iter(existing_timestamps), None)
    if acquired_at is not None:
        requested_timestamp = acquired_at.astimezone(UTC)
        if existing_timestamp is not None and requested_timestamp != existing_timestamp:
            raise RuntimeError(
                "pinned VGV release was already imported with a different acquisition timestamp"
            )
        timestamp = requested_timestamp
    else:
        timestamp = existing_timestamp or datetime.now(UTC)

    all_records: list[dict[str, object]] = []
    observations: list[VGVWorkbookObservation] = []
    provenance: list[dict[str, str]] = []
    for spec in VGV_ANNUAL_2025_SPECS:
        source_path = workbook_paths[spec.property_type]
        records, locality_count, unavailable, marker_counts = _parse_workbook(source_path, spec)
        existing = existing_imports.get(spec.property_type)
        if existing is None:
            partition = timestamp.strftime("retrieved_date=%Y-%m-%d")
            relative_path = (
                Path(_SOURCE_ID)
                / f"property_type={spec.property_type.casefold()}"
                / partition
                / f"{spec.expected_sha256}.xlsx"
            )
            raw_path = raw_root / relative_path
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, raw_path)
        else:
            raw_path, _existing_manifest = existing
            relative_path = raw_path.relative_to(raw_root)
        import_manifest = _ManualImportManifest(
            acquired_at=timestamp,
            original_filename=spec.filename,
            official_url=spec.official_url,
            byte_count=spec.expected_byte_count,
            sha256=spec.expected_sha256,
            object_path=relative_path.as_posix(),
        )
        import_manifest_path = raw_path.with_suffix(".browser-import.json")
        if existing is None:
            _atomic_write(
                import_manifest_path,
                import_manifest.model_dump_json(indent=2).encode(),
            )
        elif import_manifest != _existing_manifest:
            raise RuntimeError(f"VGV browser import manifest changed: {import_manifest_path}")
        all_records.extend(records)
        provenance.append({"object_path": relative_path.as_posix(), "sha256": spec.expected_sha256})
        observations.append(
            VGVWorkbookObservation(
                property_type=spec.property_type,
                filename=spec.filename,
                official_url=spec.official_url,
                byte_count=spec.expected_byte_count,
                sha256=spec.expected_sha256,
                raw_object_path=relative_path.as_posix(),
                locality_count=locality_count,
                prepared_row_count=len(records),
                unavailable_price_count=unavailable,
                quality_marker_counts=marker_counts,
            )
        )

    prepared_payload = (
        "\n".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) for record in all_records
        )
        + "\n"
    ).encode()
    prepared_sha256 = hashlib.sha256(prepared_payload).hexdigest()
    canonical_provenance = json.dumps(provenance, sort_keys=True, separators=(",", ":"))
    source_payload_sha256 = hashlib.sha256(canonical_provenance.encode()).hexdigest()
    _atomic_write(prepared_path, prepared_payload)
    prepared_manifest = {
        "retrieved_at": timestamp.isoformat().replace("+00:00", "Z"),
        "byte_count": len(prepared_payload),
        "sha256": prepared_sha256,
        "source_payload_sha256": source_payload_sha256,
        "source_payload_object": canonical_provenance,
    }
    _atomic_write(
        prepared_path.with_suffix(".manifest.json"),
        json.dumps(prepared_manifest, indent=2, sort_keys=True).encode(),
    )
    report = VGVPreparationReport(
        generated_at=timestamp,
        observations=tuple(observations),
        prepared_row_count=len(all_records),
        prepared_sha256=prepared_sha256,
        prepared_byte_count=len(prepared_payload),
        prepared_filename=prepared_path.name,
        source_payload_sha256=source_payload_sha256,
        source_payload_object=canonical_provenance,
        limitation=(
            "Annual suburb medians for 2015-2025 only. The workbook does not provide sales "
            "volumes, so sales_volume remains null. Preliminary 2026 values are deliberately "
            "excluded. Source quality markers are preserved without inferring their meaning. "
            "The owner downloaded the workbooks in a browser; HTTP status and response headers "
            "were not independently captured by the pipeline."
        ),
    )
    _atomic_write(evidence_path, report.model_dump_json(indent=2).encode())
    return report


__all__ = [
    "VGV_ANNUAL_2025_SPECS",
    "VGVPreparationReport",
    "VGVWorkbookObservation",
    "prepare_vgv_market",
]
