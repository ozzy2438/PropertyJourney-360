from pathlib import Path

import pytest

from propertyjourney360.public_data.catalog import RedistributionStatus, load_catalog

CATALOG = Path(__file__).parents[2] / "config" / "sources.yml"


def test_source_catalog_is_valid_and_unique() -> None:
    catalog = load_catalog(CATALOG)
    assert catalog.catalog_version == 1
    assert len(catalog.sources) == len({source.source_id for source in catalog.sources})
    assert catalog.require_enabled("vicmap_address").redistribution_status is (
        RedistributionStatus.ALLOWED
    )


def test_unverified_transaction_sales_source_is_hard_disabled() -> None:
    catalog = load_catalog(CATALOG)
    with pytest.raises(PermissionError, match="disabled by catalogue policy"):
        catalog.require_enabled("vips_transaction_level_sales")
