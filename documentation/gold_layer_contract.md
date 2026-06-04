# Gold Layer Contract

Contract definitions are under:

- `sql/gold/`
- `sql/materialized_views/`
- `sql/indexes/`

Primary app-facing contract objects:

- `gold.dim_metric`
- `gold.dim_geography`
- `gold.fact_observation`
- `gold.v_metric_latest_by_geo`
- `gold.v_metric_timeseries_by_geo`
- `gold.v_metric_distribution`
- `gold.v_metric_comparison`
- `gold.v_county_choropleth_latest`

These are compatibility-first views over existing gold/silver structures and are intended to stabilize API/frontend contracts.
