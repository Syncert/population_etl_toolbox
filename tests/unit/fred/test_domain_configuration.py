from __future__ import annotations

import pytest

from data_ingestion_toolbox.fred.config import CONFIG, FredConfig

pytestmark = pytest.mark.unit


def _config(**overrides) -> FredConfig:
    values = {
        "domains": ["labor", "prices"],
        "curated_series_ids": ["PAYEMS", "CPIAUCSL"],
        "curated_by_domain": {
            "labor": ["PAYEMS"],
            "prices": ["CPIAUCSL"],
        },
    }
    values.update(overrides)
    return FredConfig(**values)


def test_configured_series_by_domain_returns_configured_order() -> None:
    """Covers: ETL-015 — domain mapping preserves configured order."""
    config = _config()

    assert config.configured_series_by_domain() == {
        "labor": ["PAYEMS"],
        "prices": ["CPIAUCSL"],
    }


def test_default_configuration_has_one_owner_for_every_curated_series() -> None:
    """Covers: ETL-015 — defaults give each curated series one owner."""
    classified = CONFIG.configured_series_by_domain()

    assert list(classified) == CONFIG.domains
    assert {
        series_id for series_ids in classified.values() for series_id in series_ids
    } == set(CONFIG.curated_series_ids)


def test_recommended_platform_series_remain_curated() -> None:
    """Covers: ETL-034 — recommended FRED series remain curated."""
    assert {
        "ICSA",
        "INDPRO",
        "MSACSR",
        "MSPUS",
        "PCEPI",
        "PCEPILFE",
        "T10Y2Y",
        "T10YIE",
        "NFCI",
        "PCEC96",
        "DSPIC96",
        "PSAVERT",
        "RSAFS",
    } <= set(CONFIG.curated_series_ids)


def test_configured_series_by_domain_rejects_conflicting_owners() -> None:
    """Covers: ETL-015, ETL-030 — conflicting owners are rejected."""
    config = _config(
        curated_by_domain={
            "labor": ["PAYEMS", "CPIAUCSL"],
            "prices": ["CPIAUCSL"],
        }
    )

    with pytest.raises(ValueError, match="exactly one domain"):
        config.configured_series_by_domain()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"curated_by_domain": {"labor": ["PAYEMS"]}},
            "Invalid FRED domain classification",
        ),
        (
            {
                "curated_by_domain": {
                    "labor": ["PAYEMS"],
                    "prices": ["CPIAUCSL"],
                    "rates": ["DGS10"],
                }
            },
            "Invalid FRED domain classification",
        ),
        (
            {"curated_series_ids": ["PAYEMS", "CPIAUCSL", "DGS10"]},
            "unclassified",
        ),
    ],
)
def test_configured_series_by_domain_rejects_incomplete_classification(
    overrides: dict,
    message: str,
) -> None:
    """Covers: ETL-015, ETL-030 — incomplete classification is rejected."""
    with pytest.raises(ValueError, match=message):
        _config(**overrides).configured_series_by_domain()
