from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from propertyjourney360.ingestion.operator import (
    PreparedPublicArtifactManifest,
    _load_prepared_public_jsonl,
    frozen_window_from_values,
)
from propertyjourney360.public_data import vgv


def _cell(reference: str, value: str, shared: dict[str, int]) -> str:
    if value.isdecimal():
        return f'<c r="{reference}"><v>{value}</v></c>'
    return f'<c r="{reference}" t="s"><v>{shared[value]}</v></c>'


def _write_workbook(path: Path, *, include_formula: bool = False) -> None:
    strings = [
        "Sheet1",
        "Locality",
        *(str(year) for year in range(2015, 2026)),
        "Prelim",
        "2026",
        "ABBOTSFORD",
        "TESTVILLE",
        "^",
    ]
    shared = {value: index for index, value in enumerate(strings)}
    shared_xml = "".join(f"<si><t>{value}</t></si>" for value in strings)
    year_columns = {
        2015: "B",
        2016: "D",
        2017: "F",
        2018: "H",
        2019: "J",
        2020: "L",
        2021: "N",
        2022: "P",
        2023: "R",
        2024: "T",
        2025: "V",
    }
    header = _cell("A2", "Locality", shared) + "".join(
        _cell(f"{column}2", str(year), shared) for year, column in year_columns.items()
    )
    header += _cell("X2", "Prelim", shared)
    formula = (
        '<c r="B6"><f>50+50</f><v>100</v></c>' if include_formula else _cell("B6", "100", shared)
    )
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<dimension ref="A1:AB7"/><sheetData>'
        f'<row r="2">{header}</row>'
        f'<row r="3">{_cell("X3", "2026", shared)}</row>'
        f'<row r="6">{_cell("A6", "ABBOTSFORD", shared)}{formula}'
        f"{_cell('V6', '200', shared)}{_cell('W6', '^', shared)}</row>"
        f'<row r="7">{_cell("A7", "TESTVILLE", shared)}{_cell("V7", "300", shared)}</row>'
        "</sheetData></worksheet>"
    )
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Target="worksheets/sheet1.xml" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"/>'
            "</Relationships>",
        )
        archive.writestr(
            "xl/sharedStrings.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"{shared_xml}</sst>",
        )
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def _spec(property_type: vgv.PropertyType, path: Path) -> vgv.VGVWorkbookSpec:
    return vgv.VGVWorkbookSpec(
        property_type=property_type,
        filename=path.name,
        official_url=f"https://www.land.vic.gov.au/test/{path.name}",
        expected_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        expected_byte_count=path.stat().st_size,
    )


def test_prepare_vgv_market_is_strict_reproducible_and_lineage_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workbooks: dict[str, Path] = {}
    specs: list[vgv.VGVWorkbookSpec] = []
    for property_type in ("HOUSE", "UNIT", "LAND"):
        path = tmp_path / f"{property_type.casefold()}.xlsx"
        _write_workbook(path)
        workbooks[property_type] = path
        specs.append(_spec(property_type, path))  # type: ignore[arg-type]
    monkeypatch.setattr(vgv, "VGV_ANNUAL_2025_SPECS", tuple(specs))

    raw_root = tmp_path / "raw"
    prepared_path = tmp_path / "prepared" / "raw_property_market.jsonl"
    evidence_path = tmp_path / "evidence.json"
    acquired_at = datetime(2026, 8, 25, 8, tzinfo=UTC)
    report = vgv.prepare_vgv_market(
        workbook_paths=workbooks,
        raw_root=raw_root,
        prepared_path=prepared_path,
        evidence_path=evidence_path,
        acquired_at=acquired_at,
    )

    assert report.prepared_row_count == 9
    assert [item.prepared_row_count for item in report.observations] == [3, 3, 3]
    assert all(item.unavailable_price_count == 19 for item in report.observations)
    assert report == vgv.VGVPreparationReport.model_validate_json(evidence_path.read_text())
    prepared_manifest = PreparedPublicArtifactManifest.model_validate_json(
        prepared_path.with_suffix(".manifest.json").read_text()
    )
    assert prepared_manifest.source_payload_sha256 == report.source_payload_sha256
    assert len(json.loads(prepared_manifest.source_payload_object)) == 3

    window = frozen_window_from_values(
        window_start="2026-08-25T00:00:00Z",
        window_end="2026-08-26T00:00:00Z",
        window_id="a" * 64,
        mode="BACKFILL",
        replay_id=None,
    )
    loaded = _load_prepared_public_jsonl(
        prepared_path,
        window=window,
        public_root=raw_root,
    )
    assert len(loaded) == 9
    assert loaded[0]["geography_code"] == "ABBOTSFORD"
    assert loaded[0]["sales_volume"] is None
    assert loaded[1]["quality_marker"] == "^"
    assert loaded[0]["source_payload_sha256"] == report.source_payload_sha256

    evidence_bytes = evidence_path.read_bytes()
    second = vgv.prepare_vgv_market(
        workbook_paths=workbooks,
        raw_root=raw_root,
        prepared_path=prepared_path,
        evidence_path=evidence_path,
    )
    assert second == report
    assert evidence_path.read_bytes() == evidence_bytes
    assert len(list(raw_root.rglob("*.xlsx"))) == 3

    with pytest.raises(RuntimeError, match="different acquisition timestamp"):
        vgv.prepare_vgv_market(
            workbook_paths=workbooks,
            raw_root=raw_root,
            prepared_path=prepared_path,
            evidence_path=evidence_path,
            acquired_at=datetime(2026, 8, 26, 8, tzinfo=UTC),
        )


def test_prepare_vgv_market_rejects_changed_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths: dict[str, Path] = {}
    specs: list[vgv.VGVWorkbookSpec] = []
    for property_type in ("HOUSE", "UNIT", "LAND"):
        path = tmp_path / f"{property_type.casefold()}.xlsx"
        _write_workbook(path)
        paths[property_type] = path
        specs.append(_spec(property_type, path))  # type: ignore[arg-type]
    monkeypatch.setattr(vgv, "VGV_ANNUAL_2025_SPECS", tuple(specs))
    paths["HOUSE"].write_bytes(paths["HOUSE"].read_bytes() + b"tampered")

    with pytest.raises(ValueError, match="byte count changed"):
        vgv.prepare_vgv_market(
            workbook_paths=paths,
            raw_root=tmp_path / "raw",
            prepared_path=tmp_path / "prepared.jsonl",
            evidence_path=tmp_path / "evidence.json",
        )


def test_vgv_parser_rejects_formula_derived_source_values(tmp_path: Path) -> None:
    path = tmp_path / "house.xlsx"
    _write_workbook(path, include_formula=True)
    spec = _spec("HOUSE", path)
    with pytest.raises(ValueError, match="must not depend on formulas"):
        vgv._parse_workbook(path, spec)


def test_prepare_vgv_market_rejects_naive_acquisition_timestamp(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be timezone-aware"):
        vgv.prepare_vgv_market(
            workbook_paths={},
            raw_root=tmp_path / "raw",
            prepared_path=tmp_path / "prepared.jsonl",
            evidence_path=tmp_path / "evidence.json",
            acquired_at=datetime(2026, 8, 25, 8),  # noqa: DTZ001 - deliberately naive
        )
