**Population ETL Toolbox**

Proposed Architecture for a Public Geospatial Analytics Platform

*Architecture design document*

<img src="media/image1.png"
style="width:7.2in;height:4.21168in" />

*Figure 1. Proposed end-to-end platform architecture.*

Prepared for: Nicholas Kreuziger  
Scope: first-pass homelab public demo architecture with clean path to
cloud scale

# 1. Executive Summary

The current repository already has the most valuable foundation:
ingestion logic, Airflow-oriented scheduling, and a Postgres-backed
analytical database. The next architecture move should not be another BI
dashboard layer. It should be a small but serious analytical web
application that treats the database as the source of truth and serves a
clean product experience over ACS, Census, BLS, and FRED metrics.

The recommended design is a multi-service Docker Compose platform for
the homelab demo. It should be organized like a production system even
while running on one machine: Airflow prepares data, Postgres/PostGIS
stores the truth, FastAPI exposes analytical contracts, Martin serves
map tiles, Redis caches common requests, and Next.js renders the
public-facing interface.

This architecture deliberately avoids forcing Apache Superset to behave
like Tableau. Superset can remain useful for internal QA and quick SQL
exploration, but the public product should be a custom frontend because
the needed experience involves interactive maps, metric selectors,
linked charts, distribution views, custom tooltips, and eventually
prediction or scenario tools.

- Do a light Python packaging pass now so Airflow and the API can import
  stable shared code.

- Keep Airflow as orchestration only; do not put business logic directly
  inside DAG files.

- Expose only product-friendly API concepts: Metric, Geography,
  Observation, Distribution, Comparison, Forecast, and ScenarioResult.

- Separate geometry from metric values: Martin serves vector tiles;
  FastAPI serves JSON values keyed by geo_id.

- Use Docker Compose as the homelab deployment unit, not a single
  mega-container.

## Source Alignment

The uploaded business strategy document frames the platform around
interactive dashboards, predictive risk models, scenario simulators, API
access, custom consulting reports, and a consulting-SaaS hybrid model.
That product direction requires a real application architecture rather
than a pure BI-dashboard architecture. The ingestion strategy also calls
for deterministic variable registries, reproducible pipelines,
national/state/county support, and metadata such as source, series or
variable ID, geography, time period, vintage, and release date. Those
requirements are reflected directly in the gold-layer and API-contract
recommendations below.

# 2. Target Architecture Overview

<img src="media/image1.png"
style="width:7.2in;height:4.21168in" />

*Figure 2. End-to-end relationship among ingestion, storage, API, tiles,
and frontend.*

The platform should be treated as an analytical data product. The
pipeline side and the product side are related, but they are not the
same application. Airflow and ETL code should focus on freshness,
correctness, lineage, and durable gold outputs. The API and frontend
should focus on stable user-facing concepts, performance, and
interpretability.

| **Component**    | **Primary responsibility**                                                               | **Design note**                                                            |
|------------------|------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| Airflow          | Schedules ACS, BLS, FRED, boundary, and gold-refresh workflows.                          | Private/internal only. DAGs call packaged pipeline functions.              |
| Postgres/PostGIS | Stores raw, silver, gold, and spatial data.                                              | Source of truth. Use indexes, materialized views, and read-only API roles. |
| FastAPI          | Serves metric catalog, observations, time series, distributions, comparisons, forecasts. | Stateless analytical API. Do not expose raw table complexity.              |
| Martin           | Serves vector tiles from PostGIS geometry tables/views.                                  | Keeps large geometry payloads out of FastAPI and the browser.              |
| Next.js          | Renders product UX: maps, selectors, tooltips, charts, comparison panels.                | Public-facing web application.                                             |
| Redis            | Caches common API responses and supports future background queues/rate limiting.         | Optional at first but worth adding early as a pattern.                     |
| Superset         | Internal QA and quick SQL dashboarding.                                                  | Not the core geospatial product UI.                                        |

# 3. Homelab Docker Compose Topology

<img src="media/image2.png"
style="width:7.2in;height:4.21168in" />

*Figure 3. Public and private services in the homelab deployment.*

The correct deployment model is one Docker Compose stack made of several
focused containers. One container should not run Airflow, Postgres,
FastAPI, Martin, and the frontend together. That approach would be
harder to debug, harder to secure, and harder to migrate to AWS later.

Implementation note: support two compose modes from the same repo. A
self-contained internal mode runs all core services locally for fast
bring-up. An external-existing mode supports a service-only local MVP
(api, martin, web, redis) against existing infrastructure, with optional
local Airflow services enabled via profile for testing. Airflow metadata
and analytics warehouse connections remain environment-driven to point at
existing homelab hosts.

## Public services

- web: Next.js application exposed through the reverse proxy.

- api: FastAPI service exposed through /api or a subdomain.

- martin: tile server exposed through /tiles or a tile subdomain.

## Private/internal services

- analytics_postgres: Postgres/PostGIS data warehouse for raw, silver,
  gold, and spatial data.

- service_postgres: Airflow metadata and future low-volume app metadata.

- redis: cache, future queue, and rate-limiting support.

- airflow-webserver and airflow-scheduler: internal orchestration
  services.

- optional Superset or pgAdmin: internal QA/admin only, never public
  without strong authentication.

# 4. Data Architecture and Gold-Layer Contract

<img src="media/image3.png"
style="width:7.2in;height:4.16196in" />

*Figure 4. Data flow from source-specific ingestion to app-facing metric
contracts.*

The database should remain source-aware in raw and silver layers but
become product-aware in the gold layer. Raw ACS, BLS, and FRED tables
can preserve their source quirks. The frontend should not. The frontend
should request a metric, geography, and time period and receive clean
values plus interpretability metadata.

## Recommended layer responsibilities

| **Layer** | **Purpose**                       | **Key rule**                                                                                |
|-----------|-----------------------------------|---------------------------------------------------------------------------------------------|
| Raw       | Source-faithful ingestion output. | Keep original series IDs, variables, payload metadata, load batches, and source timestamps. |
| Silver    | Cleaned and conformed data.       | Normalize data types, clean sentinels, standardize geographies, and join lookup tables.     |
| Gold      | App-facing analytical contract.   | Expose metric-centric and geography-centric tables/views used by API and frontend.          |
| Model     | Prediction and risk outputs.      | Store forecasts, risk scores, model versions, assumptions, and scenario results.            |

## Gold objects to build toward

| **Object**                      | **Purpose**                                      | **Important fields / usage**                                                                    |
|---------------------------------|--------------------------------------------------|-------------------------------------------------------------------------------------------------|
| gold.dim_metric                 | Metric catalog. One row per app-facing metric.   | metric_code, display_name, source, dataset, unit, frequency, supports_moe, description          |
| gold.dim_geography              | Geography catalog. One row per public geography. | geo_id, geo_level, geo_name, state_fips, county_fips, state_name, geometry availability         |
| gold.fact_observation           | Long metric observation table.                   | metric_code, geo_id, period, value, unit, source, dataset, vintage, release_date, margin_of_error |
| gold.v_metric_latest_by_geo     | Latest value per metric/geography.               | Primary API source for choropleth maps.                                                         |
| gold.v_metric_timeseries_by_geo | Time series by metric/geography.                 | Primary API source for side-panel trend charts.                                                 |
| gold.v_metric_distribution      | Distribution stats and bin inputs.               | Supports quantile/log/equal interval/color scale decisions.                                     |
| gold.v_metric_comparison        | Metric A vs Metric B records.                    | Supports scatterplots, correlation panels, rankings, and side-by-side comparisons.              |

## Key design rule

The app should never need to know that population came from B01003, that
LAUS series IDs are generated from area and measure codes, or that ACS1
and ACS5 have different coverage. Those details belong in metadata, gold
views, and API services. The app should know metric_code = population,
geo_level = county, period = latest.

# 5. API Architecture

FastAPI should act as the analytical contract between the database and
the frontend. The API should be stateless, easy to run locally, easy to
put behind a reverse proxy, and safe to scale horizontally later. It
should use environment-based configuration and read-only credentials for
the analytical database.

| **Endpoint**                     | **Purpose**                              | **Frontend usage**                             |
|----------------------------------|------------------------------------------|------------------------------------------------|
| GET /health                      | Basic service health.                    | Used by Docker, reverse proxy, and monitoring. |
| GET /api/catalog/sources         | Available data sources.                  | ACS, Census, BLS, FRED, derived/model outputs. |
| GET /api/catalog/metrics         | Metric catalog.                          | Drives frontend selectors.                     |
| GET /api/catalog/geographies     | Supported geographies.                   | county, state, cbsa, tract later.              |
| GET /api/observations/latest     | Latest metric values by geography.       | Primary choropleth endpoint.                   |
| GET /api/observations/timeseries | Metric history for a selected geography. | Side-panel line charts.                        |
| GET /api/distribution/bins       | Distribution and suggested breakpoints.  | Legend, color scale, and outlier context.      |
| GET /api/comparison              | Metric A vs Metric B.                    | Scatterplots and cross-metric analysis.        |
| GET /api/models/...              | Forecast or risk score outputs.          | Later phase after gold contracts are stable.   |

Implementation note: canonical query name is `metric_code`; `metric_id`
is accepted as a backward-compatible alias on observations, distribution,
and comparison endpoints.

## API response design

API responses should be boring, typed, and predictable. Avoid
source-specific response shapes. Every observation should include the
metric, geography, period, value, unit, source, dataset, vintage/release
metadata, and uncertainty fields when available. This makes charts and
maps consistent even when the underlying source changes.

# 6. Frontend and Map Runtime Flow

<img src="media/image4.png"
style="width:7.2in;height:3.87715in" />

*Figure 5. Runtime flow for loading a choropleth and linked side-panel
chart.*

The frontend should be built around a measure explorer pattern. The user
selects a source or metric, geography level, period, transform, and
possibly a comparison metric. The app then loads vector tiles for the
selected geography and separately fetches metric values from the API.
The map joins values to features by geo_id.

## Recommended frontend modules

| **Frontend component** | **Responsibility**                                                                      |
|------------------------|-----------------------------------------------------------------------------------------|
| MetricControlPanel     | Metric selector, source filter, geography selector, period selector, transform options. |
| GeoMap                 | MapLibre/deck.gl component that renders county/state geometry and color layers.         |
| MapLegend              | Displays color scale, binning method, min/max, and interpretation notes.                |
| CountyTooltip          | Shows state, county, value, unit, source, period, and margin-of-error context.          |
| ObservationSidePanel   | Selected geography details, rankings, and metadata.                                     |
| TimeSeriesChart        | Selected metric trend for clicked geography.                                            |
| ComparisonPanel        | Metric A vs Metric B, scatterplot, ranks, and caveat notes.                             |

## Why geometry and metric values should stay separate

A full county GeoJSON response with metric values attached is acceptable
for a tiny prototype, but it is the wrong long-term boundary. Geometry
changes rarely and can be tiled/cached aggressively. Metric values
change more often and should travel as compact JSON records. Keeping
those payloads separate enables faster maps, reusable tiles, and easier
metric switching.

# 7. Repository and Packaging Architecture

<img src="media/image5.png"
style="width:7.2in;height:4.21168in" />

*Figure 6. Recommended monorepo boundaries.*

A light packaging pass should happen before serious webapp work. The
goal is not to create a perfect reusable library. The goal is to make
pipeline, domain, and database helper code importable by both Airflow
and FastAPI. This prevents the app from importing random scripts and
prevents DAG files from becoming giant logic dumps.

| **Path**                   | **Purpose**                                                                          |
|----------------------------|--------------------------------------------------------------------------------------|
| src/data_ingestion_toolbox | Shared Python package: connectors, pipelines, domain objects, DB helpers, utilities. |
| dags                       | Thin Airflow orchestration wrappers that call packaged pipeline functions.           |
| apps/api                   | FastAPI service with routers, schemas, services, and database query layer.           |
| apps/web                   | Next.js frontend with map, metric catalog, observation, and comparison features.     |
| sql                        | Durable database schemas, views, materialized views, indexes, and migrations.        |
| infra/docker               | Docker Compose stack and environment examples.                                       |
| infra/martin               | Tile server configuration.                                                           |
| documentation              | Architecture, API contract, gold-layer contract, and operating notes.                |

## Recommended root structure

data_ingestion_toolbox/  
├── apps/  
│ ├── api/ \# FastAPI analytical service  
│ └── web/ \# Next.js public application  
├── src/  
│ └── data_ingestion_toolbox/ \# shared Python package  
├── dags/ \# Airflow DAGs, thin orchestrators only  
├── sql/ \# schemas, views, materialized views, indexes  
├── infra/  
│ ├── docker/ \# Docker Compose deployment  
│ ├── airflow/ \# Airflow image  
│ └── martin/ \# vector tile config  
├── tests/  
└── documentation/

# 8. Security and Exposure Boundaries

For a public homelab demo, the most important security decision is
simple: expose the product, not the infrastructure. The web app, API,
and tile routes can be public. Airflow, Postgres, Redis, Superset, and
admin tools should stay private. Airflow in particular should not be
public without strong authentication and network controls.

| **Boundary**          | **Recommended treatment**                                | **Reason**                                                             |
|-----------------------|----------------------------------------------------------|------------------------------------------------------------------------|
| Public                | Next.js app, FastAPI read endpoints, Martin tile routes. | Use reverse proxy, TLS, rate limits, and cache headers.                |
| Private               | Postgres, Redis, Airflow, Superset, pgAdmin.             | Use VPN, private network, or strict authenticated access.              |
| Database credentials  | Separate API read-only role from Airflow ETL write role. | The API should not be able to mutate raw/silver/gold ingestion tables. |
| Configuration         | .env.example in repo; real secrets outside Git.          | Use environment variables across API, Airflow, Martin, and Compose.    |
| Caching/rate limiting | Redis and reverse proxy rules.                           | Protects Postgres from repeated expensive public queries.              |

# 9. Scaling Path

<img src="media/image6.png"
style="width:7.2in;height:3.92188in" />

*Figure 7. Homelab-to-cloud scaling path using the same logical
services.*

FastAPI, Next.js, PostGIS, Martin, and Redis do not cap the product at
small scale. The limiting factor will be the data-serving architecture:
query shape, materialized views, caching, tile serving, frontend
rendering, and whether heavy modeling runs synchronously. The homelab
demo should use the same logical boundaries that would later map to AWS
services.

| **Stage**            | **Typical runtime**                                                                              | **Expectation**                                                |
|----------------------|--------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| Homelab demo         | Docker Compose, local volumes, reverse proxy, internal networks.                                 | Good for public demo, portfolio, and early feedback.           |
| Small AWS production | CloudFront, ALB, ECS/Fargate, RDS Postgres/PostGIS, ElastiCache.                                 | Supports serious traffic if endpoints and tiles are cacheable. |
| SaaS scale           | Autoscaled services, read replicas, stronger cache strategy, async modeling, WAF, observability. | Needed only once traffic and paying usage justify it.          |

# 10. First-Pass Implementation Plan

The first pass should prove the architecture vertically instead of
building every feature horizontally. The milestone that matters is a
working population choropleth outside Superset, fed by gold data through
FastAPI and using tiled geometry.

| **Step** | **Milestone**         | **First-pass deliverable**                                                                         |
|----------|-----------------------|----------------------------------------------------------------------------------------------------|
| 1        | Package skeleton      | Add pyproject.toml and src/data_ingestion_toolbox; make one importable module work.                |
| 2        | Thin DAGs             | Refactor one existing DAG to call packaged functions instead of inline logic.                      |
| 3        | Gold contract         | Create or document dim_metric, dim_geography, fact_observation, latest and time-series views.      |
| 4        | FastAPI MVP           | Implement health, metrics catalog, latest observations, and time-series endpoints.                 |
| 5        | Docker Compose        | Add web, api, analytics_postgres, service_postgres, redis, martin, airflow services.               |
| 6        | Martin tiles          | Serve county/state geometry from PostGIS or add TODO-backed config if geometry names need cleanup. |
| 7        | Next.js MVP           | Metric selector, map shell, legend, tooltip, and side panel.                                       |
| 8        | Public demo hardening | Reverse proxy routing, TLS, read-only API user, Redis cache, and private internal services.        |

Implementation note (June 2026): initial gold contract compatibility views now
live under `sql/gold_contract` (`001_gold_contract_views.sql`), and the FastAPI
query layer points to named contract views (`gold.dim_metric`,
`gold.dim_geography`, `gold.v_metric_latest_by_geo`,
`gold.v_metric_timeseries_by_geo`) rather than source object names.

## First vertical slice

- Metric: population.

- Geography: county.

- Period: latest.

- Database: gold.v_metric_latest_by_geo or equivalent compatibility
  view.

- API: GET
  /api/observations/latest?metric_code=population&geo_level=county.

- Tiles: /tiles/counties/{z}/{x}/{y}.pbf through Martin.

- Frontend: MapLibre map, county hover tooltip, legend, and
  clicked-county side panel.

# 11. Architectural Decisions and Tradeoffs

| **Decision**                        | **Reason**                                                                                                                          |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| FastAPI over Django initially       | The first problem is analytical API design, not a full business-admin app. Django can be added later for accounts/admin if needed.  |
| Python over Java initially          | The project is data-heavy and already Python-oriented through Airflow, ETL, Polars/pandas, and forecasting. Java adds friction now. |
| Martin for tiles                    | Do not use FastAPI to serve giant GeoJSON forever. Tiled geometry is the scalable pattern.                                          |
| Next.js over Superset as product UI | Superset is useful for QA, but not flexible enough for the intended map/chart/product UX.                                           |
| Docker Compose over one container   | One reproducible stack with multiple services preserves production-like boundaries without Kubernetes.                              |
| Gold contract before UI complexity  | Metric/geography/time/source semantics must be stable before the frontend grows.                                                    |

# 12. Risks and Controls

| **Risk**                                         | **Failure mode**                                                                 | **Control**                                                                       |
|--------------------------------------------------|----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| Frontend leaks source complexity                 | React components start hardcoding ACS/BLS/FRED details.                          | Force all metric semantics into dim_metric, gold views, and API response schemas. |
| Postgres becomes bottleneck                      | Every user interaction triggers expensive uncached queries.                      | Materialized views, indexes, Redis caching, query timeouts, and compact payloads. |
| Geometry payloads get too large                  | Large GeoJSON responses make maps sluggish.                                      | Use Martin vector tiles and simplified geometry columns.                          |
| Airflow grows into business logic dumping ground | DAG files become huge and hard to test.                                          | Keep DAGs thin; package pipeline functions under src/data_ingestion_toolbox.      |
| Security exposure                                | Internal services exposed on public internet.                                    | Only expose web/API/tiles; keep databases, Redis, and Airflow private.            |
| Scope creep                                      | Attempt to build SaaS, auth, forecasting, every metric, and polished UI at once. | Build one vertical slice first; expand metric by metric.                          |

# Appendix A: Example API Contract

Example response shape for latest observations:

{  
"metric_code": "population",  
"geo_level": "county",  
"period": "latest",  
"count": 3143,  
"observations": \[  
{  
"metric_code": "population",  
"geo_id": "55025",  
"geo_level": "county",  
"geo_name": "Dane County",  
"state_name": "Wisconsin",  
"period": "2023",  
"value": 575000,  
"unit": "count",  
"source": "ACS",  
"dataset": "acs5",  
"vintage": "2023",  
"release_date": "2024-12-01",  
"margin_of_error": 1234,  
"margin_of_error_pct": 0.21  
}  
\]  
}
