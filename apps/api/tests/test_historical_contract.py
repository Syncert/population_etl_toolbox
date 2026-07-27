from pathlib import Path


def test_timeseries_contract_uses_durable_source_facts() -> None:
    root = Path(__file__).resolve().parents[3]
    contract_sql = (root / "sql" / "gold_contract" / "001_gold_contract_views.sql").read_text(
        encoding="utf-8"
    )
    timeseries_sql = contract_sql.split(
        "CREATE OR REPLACE VIEW gold.v_metric_timeseries_by_geo AS",
        maxsplit=1,
    )[1]

    assert "FROM gold.fact_acs_observation" in timeseries_sql
    assert "FROM gold.fact_bls_observation" in timeseries_sql
    assert "FROM gold.fact_fred_observation" in timeseries_sql
    assert "FROM gold.rpt_observation_dashboard" not in timeseries_sql
