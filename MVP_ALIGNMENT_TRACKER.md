# MVP Alignment Tracker

This tracker follows the MVP next steps discussed for the Population ETL Toolbox architecture. Keep it current as each step moves from implementation to verification.

## Current Status

| Step | Status | Notes |
|---|---|---|
| 1. Make local dev/test reproducible | Complete | Fresh `population-etl-api` conda env with Python 3.11 installed `.[local]` and passed targeted API/utility tests: 19 passed, 1 warning. `.[local]` is API/dev only and does not install Airflow. Airflow remains available through `.[airflow]` or `.[airflow-dev]` for isolated Airflow environments and through the Docker compose setup. |
| 2. Make the gold contract match the MVP response shape | Complete | Observation responses now expose MVP-friendly fields alongside existing warehouse fields: `period`, `unit`, `source`, `dataset`, `vintage`, `release_date`, `geo_name`, `margin_of_error`, and `margin_of_error_pct`. Gold compatibility views, SQL query builders, Pydantic models, and observation API tests were updated. Targeted API/utility tests pass: 19 passed, 1 warning. |
| 3. Confirm Martin serves actual county geometry | Complete | Live external MVP validation passed. `gold.dim_geo_latest` has 3,234 county rows, 3,222 county geometry rows, 3,222 joinable county geometry rows, and 0 invalid geometries. Martin `counties` exposes `geo_id` and returns non-empty protobuf tiles. API county observations for canonical population metric `ACS:acs5:B01003_001` joined to county geometry at 100/100 sampled rows. Note: friendly `metric_code=population` currently returns zero rows; smoke falls back to the canonical ACS5 population metric until an alias is added. |
| 4. Switch Compose web from static smoke dashboard to Next.js | Complete | Internal and external Compose modes now build the Next.js app with a multi-stage, non-root production image and proxy same-origin API/tile routes over Docker DNS. Live external-stack verification passed at `http://localhost:3001`: Next.js assets rendered, API and Martin checks were healthy, the `counties` layer joined on `geo_id`, and observations loaded. Frontend production build and targeted API/utility tests pass: 19 passed, 1 warning. |
| 5. Finish frontend MVP interactions | Complete | County polygons expose hover details, clicks pin/outline a county, and the side panel fetches history. The control panel now loads all 3,176 active public ACS metrics via pagination, defaults nationwide county maps to canonical `ACS:acs5:B01003_001`, provides an explicit ACS1/ACS5 selector, and adds cascading state/county selectors. ACS1 partial coverage is labeled as “not published” instead of a generic missing-data failure. Live browser verification covered ACS5 national county coverage, Alabama filtering, Autauga selection, and the equivalent ACS1 partial-coverage state. |
| 6. Use distribution-backed legend bins | Complete | The frontend fetches five server-calculated ranges from `/api/distribution/bins`, uses them for county colors, and labels the legend with per-bin counts. The first/last ranges are open-ended and a no-observation swatch accounts for every map color; local equal-width breaks remain an explicit error fallback. Live verification returned five bins across 861 records, including an empty interval. |
| 7. Add exact MVP end-to-end smoke checks | Complete | `smoke_external_mvp.ps1` now asserts Next.js identity/assets, health payload, nonempty metric catalog, population county observations with canonical fallback, Martin health, API-to-geometry join compatibility, and browser-rendered MapLibre/observation/distribution state. The live external stack passes the complete suite. |
| 8. Add public-demo hardening | Not started | Read-only API DB user, Redis caching, reverse proxy, private internal services, and documented env flow. |

## Steering Rule

Work through the list in order unless a later step blocks an earlier one. Verification counts as part of each step, not a separate afterthought.
