"""API unit tests: historical timeseries contract.

Migrated from apps/api/tests/test_historical_contract.py.
Covers: API-012 (historical response durability — timeseries view uses
        durable source fact views, not the rolling dashboard table).
"""

import pytest
from pathlib import Path


@pytest.mark.unit
@pytest.mark.api
def test_timeseries_contract_uses_durable_source_facts() -> None:
    """API-012: cross-source history reads durable source reporting views."""
    root = Path(__file__).resolve().parents[3]
    contract_sql = (
        root / "sql" / "gold_contract" / "001_gold_contract_views.sql"
    ).read_text(encoding="utf-8")

    timeseries_sql = contract_sql.split(
        "CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS",
        maxsplit=1,
    )[1]

    assert "FROM gold_census.v_metric_timeseries_by_geo" in timeseries_sql
    assert "FROM gold_bls.v_metric_timeseries_by_geo" in timeseries_sql
    assert "FROM gold_fred.v_metric_timeseries_by_geo" in timeseries_sql
    assert "FROM gold.rpt_observation_dashboard" not in timeseries_sql
