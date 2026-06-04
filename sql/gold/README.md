# Gold Contract (First Pass)

This folder defines the app-facing gold contract consumed by FastAPI and the frontend.

## Contract objects

- `gold.dim_metric`
- `gold.dim_geography`
- `gold.fact_observation`
- `gold.v_metric_latest_by_geo`
- `gold.v_metric_timeseries_by_geo`
- `gold.v_metric_distribution`
- `gold.v_metric_comparison`
- `gold.v_county_choropleth_latest`

## Notes

- Raw ingestion remains source-specific in `raw_*` schemas.
- Business comparison logic is modeled in gold views, not raw ingestion tables.
- Predictive/model outputs should be added downstream in separate modeled views/tables.
- Compatibility views below use existing `gold.rpt_observation_dashboard` and `silver_ref.dim_geo` where available.
