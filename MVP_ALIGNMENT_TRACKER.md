# MVP Alignment Tracker

This tracker follows the MVP next steps discussed for the Population ETL Toolbox architecture. Keep it current as each step moves from implementation to verification.

## Current Status

| Step | Status | Notes |
|---|---|---|
| 1. Make local dev/test reproducible | Complete | Fresh `population-etl-api` conda env with Python 3.11 installed `.[local]` and passed targeted API/utility tests: 19 passed, 1 warning. `.[local]` is API/dev only and does not install Airflow. Airflow remains available through `.[airflow]` or `.[airflow-dev]` for isolated Airflow environments and through the Docker compose setup. |
| 2. Make the gold contract match the MVP response shape | Complete | Observation responses now expose MVP-friendly fields alongside existing warehouse fields: `period`, `unit`, `source`, `dataset`, `vintage`, `release_date`, `geo_name`, `margin_of_error`, and `margin_of_error_pct`. Gold compatibility views, SQL query builders, Pydantic models, and observation API tests were updated. Targeted API/utility tests pass: 19 passed, 1 warning. |
| 3. Confirm Martin serves actual county geometry | Complete | Live external MVP validation passed. `gold.dim_geo_latest` has 3,234 county rows, 3,222 county geometry rows, 3,222 joinable county geometry rows, and 0 invalid geometries. Martin `counties` exposes `geo_id` and returns non-empty protobuf tiles. API county observations for canonical population metric `ACS:acs5:B01003_001` joined to county geometry at 100/100 sampled rows. Note: friendly `metric_code=population` currently returns zero rows; smoke falls back to the canonical ACS5 population metric until an alias is added. |
| 4. Switch Compose web from static smoke dashboard to Next.js | Not started | Keep smoke dashboard if useful, but public `web` should run the Next.js MVP app. |
| 5. Finish frontend MVP interactions | Not started | Add county hover tooltip, clicked-county side panel, and timeseries fetch. |
| 6. Use distribution-backed legend bins | Not started | Drive choropleth breaks from `/api/distribution/bins` instead of client-only equal-width bins. |
| 7. Add exact MVP end-to-end smoke checks | Not started | Verify health, catalog, population county latest observations, tile layer, join-key compatibility, and web route. |
| 8. Add public-demo hardening | Not started | Read-only API DB user, Redis caching, reverse proxy, private internal services, and documented env flow. |

## Steering Rule

Work through the list in order unless a later step blocks an earlier one. Verification counts as part of each step, not a separate afterthought.
