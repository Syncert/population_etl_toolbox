# data_ingestion_toolbox

A production-grade ETL system for ingesting, transforming, and serving economic and demographic data from authoritative US government sources. Built with Airflow, PostgreSQL, and Polars for real-time access to Census, BLS, and FRED data in a structured dimensional warehouse.

Active architecture changes are tracked in
[`docs/plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md`](docs/plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md).
The implemented test contract and longer-term product design live under
[`docs/reference`](docs/reference/TESTING_CONTRACT.md) and
[`docs/product`](docs/product/ECONOMIC_DATA_STUDIO_MANIFESTO.md), respectively.

## Project Vision

**Goal:** Build a self-service data warehouse that provides reliable, current economic and demographic statistics to support analysis, dashboards, and reporting without requiring manual data collection or API expertise.

**Scope:**
- **Census Bureau ACS** (American Community Survey): detailed demographic tables (1-year and 5-year) by geography (US, state, county)
- **BLS** (Bureau of Labor Statistics): labor statistics including employment, unemployment, and wage data
- **FRED** (Federal Reserve Economic Data): macroeconomic time series (employment, inflation, interest rates, etc.)

**Target architecture:** The layer contract is defined by
[`ADR-0001`](docs/decisions/0001-data-layer-boundaries.md): immutable lossless raw
captures, separate mutable control state, conformed silver data, deterministic
data-derived gold products, and independently owned semantic/serving policy.
The current source pipelines predate that decision and are being migrated under
the linked remediation tickets.

## Current State (May 2026)

### ✅ Completed
- **Raw Layer Ingestion:** Census ACS (1yr/5yr), BLS (9 programs), FRED (48 domains) with hash-based change detection
- **Geographic Master Data:** 14-year Census Gazetteer history (2012-2025) with first/last-seen tracking
- **Silver Transformations:** Dimension-matched fact tables with comprehensive metrics logging
- **Data Quality Monitoring:** TransformMetrics instrumentation logs pre/per-chunk/upsert/summary statistics
- **Idempotent Updates:** All ingestion and transforms support safe re-runs via unique constraints and ON CONFLICT logic
- **API Rate Limiting:** Airflow pools enforce safe concurrency (Census API, BLS API, FRED API)

### 🔄 In Progress
- Analytical layer queries and performance optimization
- Data quality SLA monitoring and alerting

### 📋 Roadmap
- Fact table aggregations (monthly/quarterly/annual by geography)
- Public API for warehouse access
- Historical trend analysis and anomaly detection
- Data lineage and audit trail enhancements

---

## Technical Architecture

### Data Models

#### Legacy Parsed Staging and Control Tables

The following current relations are parsed, typed, and replaceable; they are not
the target immutable raw boundary. The ingestion ledgers are legacy control-plane
state that will move or be exposed through compatibility views under ARCH-004.

| Dataset | Raw Fact Table | Series Metadata | Ingestion Ledger |
|---------|----------------|-----------------|------------------|
| Census  | `raw_census.acs_long` | `raw_census.acs_datasets`, `raw_census.acs_variables` | `raw_census.acs_ingestion_slices` |
| BLS     | `raw_bls.bls_long` | `raw_bls.bls_datasets`, `raw_bls.bls_series` | `raw_bls.bls_ingestion_slices` |
| FRED    | `raw_fred.fred_long` | `raw_fred.fred_series` | `raw_fred.fred_ingestion_slices` |

#### Silver Layer (Dimensional)
All silver schemas use standard dimension keys for consistent joins:

```
silver_ref.dim_time          — Daily calendar (gregorian dates)
silver_ref.dim_geo           — Geographic dimension (14-year Gazetteer history)
├─ state_fips, county_fips, geo_level, first_seen_year, last_seen_year
├─ geography coverage: US + 50 states + 3,000+ counties

silver_census.fact_demographics — Census ACS facts
├─ (time_sk, geo_sk, demographic_category, value)

silver_bls.fact_labor_statistics — BLS employment/wage facts
├─ (time_sk, geo_sk, series_id, value)

silver_fred.fact_economic_indicators — FRED macro series
├─ (time_sk, series_id, value) — no geography dimension
```

### Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `silver_ref` | Monthly (1st @ 05:00 UTC) | Sync geographic gazetteer (14 years) and time dimension |
| `acs_ingest_dag` | Daily @ 02:00 UTC | Ingest Census ACS (all years available, varies by geography) |
| `bls_ingest_dag` | Weekly Sundays @ 07:00 UTC | Ingest BLS series (100+ programs, national + state + county) |
| `fred_ingest_dag` | Daily @ 02:45 UTC | Ingest FRED economic indicators (48+ domains) |

### Key Design Decisions

**Hash-Based Change Detection:**
- Each ingestion slice computes a hash of available series/variables for the time period
- If hash matches a previous successful slice, skip re-ingestion (API-efficient)
- If hash changes (new variables added), mark old slices as stale and re-ingest

**Dimension Matching with Metrics:**
- All dimensions (time, geography) are loaded upfront into memory
- Failed joins are tracked and logged per chunk
- Missing dimension entries are flagged as warnings (don't drop rows — allows debugging)

**Idempotent Upserts:**
- All fact tables use `INSERT ... ON CONFLICT` with natural primary keys
- Safe to replay any time period without data duplication
- Ingestion ledger supports replay of any slice by external job

**Rate Limiting:**
- Airflow pools limit concurrent API calls (Census, BLS, FRED each have own pool)
- Default: 4 concurrent requests per API (configurable per environment)

**Incremental Gold Serving Refresh:**
- `gold_glossary.serving_refresh_state` stores a silver `ingested_at` watermark per source.
- Scheduled refreshes split changed history into calendar-year chunks. Each report/latest chunk and its row in `gold_glossary.serving_refresh_chunk_state` commit together.
- A retry skips completed annual chunks and resumes at the first incomplete year; a failed year is rolled back without undoing earlier years.
- Each chunk recomputes latest rows only for affected natural keys.
- Geography synchronization commits in a separate short task, so ACS, BLS, and FRED cannot hold a shared geography lock throughout a report rebuild.
- A bounded `lock_timeout` and source-specific `statement_timeout` prevent refreshes from waiting indefinitely.
- Airflow logs the planned window and every chunk start, skip, completion, failure, duration, target watermark, and resulting report-row count. PostgreSQL procedures also emit row-count and duration notices.

To force a source reconciliation manually, use the three-argument procedure and set `p_force_full` to `TRUE`:

```sql
CALL gold_census.refresh_dashboard_serving_layer_acs(NULL, NULL, TRUE);
CALL gold_bls.refresh_dashboard_serving_layer_bls(NULL, NULL, TRUE);
CALL gold_fred.refresh_dashboard_serving_layer_fred(NULL, NULL, TRUE);
```

Supplying dates with `p_force_full = TRUE` rebuilds only that date range and intentionally leaves the normal incremental watermark unchanged.
Manual calls to the outer procedures remain single transactions. Scheduled DAG refreshes use the resumable annual checkpoint path.

Chunk status can be inspected with:

```sql
SELECT source_code, chunk_start, chunk_end, status, attempt_count,
       target_silver_ingested_at, completed_silver_ingested_at, last_error
FROM gold_glossary.serving_refresh_chunk_state
ORDER BY source_code, chunk_start;
```

---

## Setup & Configuration

### Prerequisites

- **Airflow 2.7+** (tested with 2.8)
- **PostgreSQL 14+** (tested with 15)
- **Python 3.10+**
- **Python packages:** managed via `pyproject.toml` extras

### Python Environment and Install

Standard local development install path for API, tests, and shared package work:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e .[local]
```

This installs runtime dependencies plus API and dev tooling. Airflow is intentionally not included in `local` because Airflow pins a large dependency set and should run in Docker, WSL2, or a dedicated isolated environment.

Smoke test imports (no PYTHONPATH/path hacks required):

```bash
python -c "import data_ingestion_toolbox, data_ingestion_toolbox.bls, data_ingestion_toolbox.census_acs, data_ingestion_toolbox.fred, data_ingestion_toolbox.silver_ref, data_ingestion_toolbox.utility; print('imports ok')"
```

Optional targeted installs:

```bash
# API/analytics web layer only
pip install -e .[api]

# Lint/test tooling only
pip install -e .[dev]

# Airflow installs require Python 3.11 and the official constraints.
# Use the exact two-step sequence in the Testing section below.
```

### API MVP (Vertical Slice)

Run the API locally:

```bash
pip install -e .[api]
uvicorn apps.api.main:app --reload
```

Default environment variables (override as needed):
- `DB_HOST` (default `localhost`)
- `DB_PORT` (default `5432`)
- `DB_USER` (default `postgres`)
- `DB_PASSWORD` (default empty)
- `DB_NAME` (default `population_etl`)

Available endpoints:
- `GET /health`
- `GET /api/catalog/sources`
- `GET /api/catalog/metrics`
- `GET /api/catalog/geographies`
- `GET /api/observations/latest`
- `GET /api/observations/timeseries`
- `GET /api/distribution/bins`
- `GET /api/comparison`
- `GET /api/models/status`

Observation endpoint parameter note:
- `GET /api/observations/latest` accepts `metric_code` and `metric_id` as equivalent aliases.
- `GET /api/observations/timeseries` accepts `metric_code` and `metric_id` as equivalent aliases.

### Next.js Web App (Local Iteration)

Run the new web app scaffold:

```bash
cd apps/web
copy .env.local.example .env.local
npm install
npm run dev
```

Open `http://localhost:3100`.

The Next.js app proxies local service traffic using same-origin rewrites:
- `/api/*` -> API origin (default `http://localhost:8000`)
- `/tiles/*` -> tile server origin (default `http://localhost:3000`)

Override targets in `apps/web/.env.local`:
- `NEXT_PUBLIC_API_ORIGIN`
- `NEXT_PUBLIC_TILES_ORIGIN`

### API-to-Map and Compose Contract Smoke

Run the centralized disposable service checks end-to-end:

```bash
powershell -ExecutionPolicy Bypass -File tests/run.ps1 compose-smoke
```

```bash
curl http://localhost:3001/
curl http://localhost:3001/api/health
curl http://localhost:3001/api/catalog/metrics?limit=5
curl "http://localhost:3001/api/observations/latest?metric_code=population&geo_level=county&limit=5"
curl http://localhost:3001/tiles/health
# if /tiles/health is unavailable:
curl http://localhost:3001/tiles/
```

Web smoke dashboard:
- `http://localhost:3001`
- The web container proxies same-origin routes to backend services:
    - `/api/*` -> API service (`api:8000`)
    - `/tiles/*` -> Martin service (`martin:3000`)

Expected contract alignment:
- API observation responses expose `geo_id`.
- Martin layers/catalog entries should expose geography identifiers that map to the same county/state `geo_id` values.

Security note:
- Keep credentials only in local env files (for example `infra/docker/stack.external.env`) or host environment variables, never committed docs/examples.

### 1. Database Setup

#### Create Schemas
```sql
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS raw_census;
CREATE SCHEMA IF NOT EXISTS raw_bls;
CREATE SCHEMA IF NOT EXISTS raw_fred;
CREATE SCHEMA IF NOT EXISTS silver_ref;
CREATE SCHEMA IF NOT EXISTS silver_census;
CREATE SCHEMA IF NOT EXISTS silver_bls;
CREATE SCHEMA IF NOT EXISTS silver_fred;
```

#### Run DDL
Execute all DDL scripts in order:
```bash
# Shared raw-capture and control foundation
psql -v ON_ERROR_STOP=1 -U postgres -d population_etl < sql/migrations/001_raw_capture_control_foundation.sql

# Reference dimensions (required first)
psql -U postgres -d population_etl < src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql

# Raw schemas (data ingestion tables)
psql -U postgres -d population_etl < src/data_ingestion_toolbox/census_acs/DDL/raw_census.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/bls/DDL/raw_bls.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/fred/DDL/raw_fred.sql

# Silver schemas (transformed fact tables)
psql -U postgres -d population_etl < src/data_ingestion_toolbox/census_acs/DDL/silver_census.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/bls/DDL/silver_bls.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/fred/DDL/silver_fred.sql

# Shared gold glossary objects (run once before source-specific gold DDL)
psql -U postgres -d population_etl < sql/gold_contract/002_gold_glossary_schema.sql

# Source-specific gold tables, indexes, and refresh procedures
psql -U postgres -d population_etl < src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql
psql -U postgres -d population_etl < src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql

# API-facing compatibility views (run after the source gold objects exist)
psql -U postgres -d population_etl < sql/gold_contract/001_gold_contract_views.sql
```

### 2. Airflow Setup

#### DAG Runtime Compatibility (MVP + Legacy Admin Layout)

The DAGs in [dags/acs_ingest_dag.py](dags/acs_ingest_dag.py), [dags/bls_ingest_dag.py](dags/bls_ingest_dag.py), [dags/fred_ingest_dag.py](dags/fred_ingest_dag.py), and [dags/silver_ref_dag.py](dags/silver_ref_dag.py) support two runtime layouts:

- MVP/package layout (preferred for this repository):
    - Imports resolve through `data_ingestion_toolbox.*`.
    - DDL files resolve under `src/data_ingestion_toolbox/.../DDL`.
- Legacy admin layout (copy/paste compatibility):
    - Imports fall back to sibling folders next to `dags` (`census_acs`, `bls`, `fred`, `silver_ref`).
    - DDL files fall back to sibling paths like `../census_acs/DDL/...`.

This preserves backward compatibility with existing Airflow administrative deployments that run from a folder tree and periodically receive copied DAG/module updates.

For legacy copy/paste deployment, keep these folders together under the same Airflow project root:

- `dags`
- `census_acs`
- `bls`
- `fred`
- `silver_ref`
- `utility`

Minimum smoke validation after copy:

```bash
airflow dags list
airflow dags test silver_ref
airflow dags test acs_ingest
airflow dags test bls_ingest
airflow dags test fred_ingest
```

#### Runtime Paths and Infra Files

Airflow runtime paths are hard-wired via `infra/airflow/airflow.env.example`:

```bash
AIRFLOW__CORE__DAGS_FOLDER=/opt/data_ingestion_toolbox/dags
PYTHONPATH=/opt/data_ingestion_toolbox/src:/opt/data_ingestion_toolbox
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Use the Airflow-only compose stack at `infra/docker/docker-compose.airflow.yml` when you just need DAG orchestration + metadata DB.

Use the full platform compose stack at `infra/docker/docker-compose.yml` when you need API + Martin + analytics PostGIS + Airflow together.

The full platform now supports two deployment modes: internal self-contained (`docker-compose.yml`) and external integration (`docker-compose.external.yml`) where analytics Postgres and (optionally) Airflow metadata can point at existing infrastructure via environment variables.

External mode can run as service-only local MVP (`redis`, `api`, `martin`, `web`) without local Airflow and is the recommended path when you already have an Airflow deployment and populated warehouse.

```bash
docker compose -f infra/docker/docker-compose.airflow.yml up airflow-init
docker compose -f infra/docker/docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Full stack startup:

```bash
cp infra/docker/stack.env.example infra/docker/stack.env
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up airflow-init
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up -d
```

Optional helper script for internal/external stack lifecycle:

```powershell
# Defaults: -Mode internal -Action all
./scripts/deploy_stack.ps1

# External mode examples
./scripts/deploy_stack.ps1 -Mode external -Action init
./scripts/deploy_stack.ps1 -Mode external -Action up
./scripts/deploy_stack.ps1 -Mode external -Action down

# Optional: include local Airflow services in external mode
./scripts/deploy_stack.ps1 -Mode external -WithLocalAirflow -Action init
./scripts/deploy_stack.ps1 -Mode external -WithLocalAirflow -Action up
```

Deployment verification is owned by the cataloged test suite; run
`./tests/run.ps1 compose-smoke` rather than embedding assertions in the
lifecycle script.

In the compose environment, `airflow-init` automatically seeds the `public_data` Airflow connection:

- Airflow-only compose seeds `public_data` -> host `postgres`, schema `airflow` (metadata DB).
- Full compose seeds `public_data` -> host `analytics_postgres`, schema `population_etl` (analytics DB).

For production/real runs, set `public_data` to your target analytics warehouse.

#### Create Database Connection
Only needed if you are not using compose init seeding.

```bash
# In Airflow UI (Admin > Connections) or via CLI:
airflow connections add \
  --conn-id public_data \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-login postgres \
  --conn-password YOUR_PASSWORD \
  --conn-schema population_etl
```

#### Create Pools
Airflow pools limit concurrent API requests (prevents rate-limiting):

```bash
# In Airflow UI (Admin > Pools) or via CLI:
airflow pools create census_api 4 "Census Bureau API limit"
airflow pools create bls_api 4 "BLS API limit"
airflow pools create fred_api 4 "FRED API limit"
```

Sizing guidance:
- **Census API:** Start at 2-4 concurrent requests (Census is rate-limited; 120 req/min per IP)
- **BLS API:** Start at 4 concurrent (BLS is generous with concurrency)
- **FRED API:** Start at 4 concurrent (FRED allows high concurrency)
- Adjust upward if you see pool exhaustion; downward if API returns 429 (rate limit)

#### Environment Variables

```bash
# For FRED API (required)
export FRED_API_KEY="your_fred_api_key_here"

# For Census API (optional; Census has high default limits)
export CENSUS_API_KEY="your_census_api_key_here"

# For BLS API (usually not required; BLS allows public access)
export BLS_API_KEY="your_bls_api_key_here"
```

### 3. Configuration Files

Each module has a `config.py` file:

For a configuration-agnostic overview (contract vs selected scope), see `documentation/CONFIGURATION.md`.

**src/data_ingestion_toolbox/census_acs/config.py:**
```python
CONFIG.postgres_conn_id = "public_data"
CONFIG.datasets = ["acs1", "acs5"]  # which ACS datasets to ingest
CONFIG.geo_levels = ["us", "state", "county"]  # geographic levels
CONFIG.curated_tables = [
    "B01003",  # Total population table
    "B19013",  # Median household income table
    # ... selected table scope is optional and environment-specific
]
```

**src/data_ingestion_toolbox/bls/config.py:**
```python
CONFIG.postgres_conn_id = "public_data"
CONFIG.programs = ["la", "cu", "ce"]  # LAUS, CPI, CES program codes
CONFIG.curated_by_program = {
    "la": ["03", "04", "05"],  # LAUS series codes
    "cu": ["sa0"],  # CPI series codes
}
```

**src/data_ingestion_toolbox/fred/config.py:**
```python
CONFIG.postgres_conn_id = "public_data"
CONFIG.domains = ["labor_cycle", "employment", "prices", ...]  # FRED domains
```

### 4. Silver Reference Initialization

Before first run, sync dimension tables:

```bash
# Option A: Run via Airflow UI (trigger silver_ref DAG manually)

# Option B: Run directly
cd /path/to/data_ingestion_toolbox
python -c "
from data_ingestion_toolbox.silver_ref.geography import sync_geo_dim
from data_ingestion_toolbox.silver_ref.time_dim import sync_time_dim
sync_geo_dim()  # Loads 14 years of Gazetteer data
sync_time_dim()  # Loads daily calendar (1970 - 2100)
"
```

This step:
- Downloads Census Gazetteer files (2012-2025), merges multi-year history
- Creates `dim_geo` with ~270K unique geography entries (with first/last seen years)
- Creates `dim_time` with daily calendar for date-to-time_sk joins

### 5. First Run

#### Trigger Ingestion DAGs Manually

```bash
# In Airflow UI, manually trigger or use CLI:
airflow dags test silver_ref
airflow dags test acs_ingest
airflow dags test bls_ingest
airflow dags test fred_ingest
```

Monitor logs to ensure:
- ✅ No API authentication errors
- ✅ Dimension tables populated (check `raw_census.acs_ingestion_slices`)
- ✅ Row counts reasonable (e.g., Census ACS ~2-5M rows, BLS ~1M rows, FRED ~50K rows)

#### Transform Raw → Silver (When Ready)

```bash
# Manual execution of transformation functions:
python -c "
from data_ingestion_toolbox.census_acs.silver_census.transform import transform_census_to_silver
from data_ingestion_toolbox.bls.silver_bls.transform import transform_bls_to_silver
from data_ingestion_toolbox.fred.silver_fred.transform import transform_fred_to_silver

# Transform Census
transform_census_to_silver()

# Transform BLS by program
for program in ['la', 'cu', 'ce', ...]:
    transform_bls_to_silver(program)

# Transform FRED by domain
for domain in ['labor_cycle', 'employment', ...]:
    transform_fred_to_silver(domain)
"
```

Or integrate into your automated silver refresh orchestration.

---

## Troubleshooting

### "CheckViolation: started_at before finished_at"
Occurs when ingestion ledger has NULL timestamps. Fixed in v2.1.

**Workaround:**
```sql
— Clean invalid ingestion ledger rows
UPDATE raw_fred.fred_ingestion_slices 
SET started_at = finished_at 
WHERE finished_at IS NOT NULL AND started_at IS NULL;

UPDATE raw_bls.bls_ingestion_slices 
SET started_at = finished_at 
WHERE finished_at IS NOT NULL AND started_at IS NULL;

UPDATE raw_census.acs_ingestion_slices 
SET started_at = finished_at 
WHERE finished_at IS NOT NULL AND started_at IS NULL;
```

### "Missing geo_sk" Warnings
Indicates Census/BLS data contains geographic codes not in `silver_ref.dim_geo`.

**Diagnosis:**
```sql
— Find missing geographies
SELECT DISTINCT geo_level, geo_id 
FROM raw_census.acs_long ac
LEFT JOIN silver_ref.dim_geo sg ON ac.geo_id = sg.geo_id
WHERE sg.geo_sk IS NULL
ORDER BY geo_level, geo_id;
```

**Resolution:**
- Check if geographies are historical (retired counties, annexed areas)
- Re-run `silver_ref` DAG to load multi-year Gazetteer history
- Manually add missing geographies to `silver_ref.dim_geo` if not in Gazetteer

### "Pool exhausted" Errors
API request pool is full; Airflow task queues instead of execute.

**Resolution:**
1. Increase pool size in Airflow UI (Admin > Pools)
2. Check if upstream tasks are stuck (see task logs)
3. Monitor API response times; may indicate upstream overload

### Transform Metrics Show High Null Counts
Indicates missing dimension entries or data quality issues.

**Resolution:**
1. Check transform logs: `[DATASET CHUNK] Rows filtered: missing_time=X, missing_geo=Y`
2. For time: verify `dim_time` covers all observation dates
3. For geography: verify `dim_geo` has all required geographic codes

---

## Data Quality & Monitoring

### Transform Metrics

All silver transforms log comprehensive metrics at four phases:

1. **Pre-transform:** Raw row counts by temporal grouping
2. **Per-chunk:** Input→output retention %, dimension join success rates
3. **Upsert:** Duration, row count
4. **Post-transform:** Total processed/inserted, error summary

Example log output:
```
[CENSUS PRE-TRANSFORM] Starting transform: 2025 (500K rows), 2026 (520K rows)
[CENSUS CHUNK] year=2025: input=500K, output=498K (99.6%), time_hits=498K, geo_hits=497K, geo_misses=3
[CENSUS UPSERT] Inserted 498K rows in 12s
[CENSUS TRANSFORM-SUMMARY] Total: 1.02M processed, 1.01M inserted (99.0%)
```

### Monitoring Recommendations

**Ingestion Ledger:**
```sql
— Check for failed or stale slices
SELECT domain, status, COUNT(*) 
FROM raw_census.acs_ingestion_slices 
GROUP BY dataset, status;
```

**Geographic Coverage:**
```sql
— Verify coverage against expected geographies
SELECT 
    MAX(first_seen_year) as oldest_year,
    MIN(last_seen_year) as newest_year,
    COUNT(*) as total_geos
FROM silver_ref.dim_geo;

— Expected: oldest_year ≥ 2012, newest_year =2025, total_geos ≥ 270K
```

**Fact Table Growth:**
```sql
— Monitor ingestion volume
SELECT 
    DATE(ingested_at) as date,
    COUNT(*) as row_count
FROM silver_census.fact_demographics
GROUP BY DATE(ingested_at)
ORDER BY date DESC LIMIT 30;
```

---

## Development

### Testing

All automated tests and test-owned assets live under `tests/`. Two separate
Python 3.11 environments are required because the API and Airflow layers have
conflicting SQLAlchemy requirements. Plain `pytest` is deterministic and does
not collect the Airflow, integration, external, E2E, or performance tiers.
See the concise [test-suite user guide](docs/user-guides/RUNNING_TESTS.md) for
setup, tier commands, disposable services, credentials, and result handling.

#### API + ETL unit tests (Python 3.11, `.[api,dev]`)

```bash
# Install
pip install -e ".[api,dev]"

# Run all deterministic unit tests
make test-unit
# Windows equivalent
./tests/run.ps1 unit

# ETL unit tests only (Census, BLS, FRED, shared)
make test-etl
# Windows equivalent: ./tests/run.ps1 etl

# API unit tests only
make test-api
# Windows equivalent: ./tests/run.ps1 api

# Full unit suite with coverage
pytest --cov=src --cov=apps --cov-report=term-missing tests/unit/
```

#### DAG structural tests (Python 3.11, `.[airflow-dev]`)

```bash
pip install apache-airflow==2.9.3 \
  apache-airflow-providers-postgres==5.11.2 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
pip install -e ".[airflow-dev]"

make test-dags
# Windows equivalent: ./tests/run.ps1 dags
```

The remaining tier commands are `make test-integration`, `make test-external`,
`make test-e2e`, `make test-martin-unit`, `make test-martin-integration`,
`make test-performance`, `make test-resilience`, `make test-web-unit`,
`make test-web-browser`, `make test-web-build`, and `make test-compose-smoke`;
pass the same tier name to `./tests/run.ps1` on Windows. Infrastructure tiers
remain opt-in and use disposable services and explicit environment guards.

The deterministic Martin contracts run with `make test-martin-unit`. The live
TileJSON, decoded MVT, read-only role, proxy, failure-mode, and API join suite
runs through `make test-martin-integration`; its runner always removes the
disposable Compose project and volumes. Install the decoder only in the
API/Martin environment with `pip install -e ".[api,dev,martin-test]"`; it is
intentionally excluded from `airflow-dev` because the two environments require
incompatible protobuf versions.

Frontend lint/build/unit/browser checks use the `test-web-*` targets. Frontend
test code and reviewed browser MVT fixture live under `tests/frontend/`.

#### PostgreSQL integration tests

The database integration suite requires the pinned
`postgis/postgis:16-3.5-alpine@sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd`
image and refuses to connect unless all `TEST_POSTGRES_*` variables are set and
the database name ends in `_test`. The suite bootstraps reference, raw, silver,
source-specific gold, shared glossary, and API contract schemas in dependency
order, then verifies that the complete DDL is safely rerunnable.

```bash
export TEST_POSTGRES_HOST=127.0.0.1
export TEST_POSTGRES_PORT=5432
export TEST_POSTGRES_USER=population_test
export TEST_POSTGRES_PASSWORD=population_test
export TEST_POSTGRES_DATABASE=population_etl_test

make test-integration
# Windows equivalent: ./tests/run.ps1 integration
```

The `postgres-integration` workflow provisions the disposable service and
validates clean bootstrap, DDL reruns, raw natural keys, ledger checks, and
transaction rollback automatically. It also exercises representative silver
foreign keys; raw status, range, measure, period, and row-count constraints;
and real Census, BLS, and FRED raw-loader replay and revision replacement.

#### Redis integration tests

The API cache integration suite requires the pinned
`redis:7.4.9-alpine@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99`
image. It accepts only an explicit loopback `TEST_REDIS_URL` using disposable
database 15, without credentials, and clears that database around every test.

```bash
export TEST_REDIS_URL=redis://127.0.0.1:6379/15

make test-integration
# Windows equivalent: ./tests/run.ps1 integration
```

The `redis-integration` workflow validates cache miss/hit behavior, cache-key
separation, TTL expiry, response bypass rules, Redis 7 compatibility, cleanup,
and graceful fallback when Redis is unavailable.

The external runner includes both the small source contracts and EXT-007 through
EXT-010 legacy ingestion/metadata contracts. It therefore requires the same
disposable `TEST_POSTGRES_*` settings plus explicit network access. Census and
FRED Data API calls require their respective keys; those secrets are optional
for a partial local run, where missing-key skips are named. The scheduled
workflow reports every skip separately. BLS continues to support its bounded
anonymous contract, with a registration key used when configured.

#### Marker reference

| Marker        | Description                                              |
|---------------|----------------------------------------------------------|
| `unit`        | Deterministic, process-local logic; no network/infra     |
| `dag`         | Airflow DAG import and structure tests                   |
| `api`         | FastAPI router/service/schema/middleware tests           |
| `integration` | Multi-component tests requiring running services         |
| `database`    | Requires a disposable Postgres 16 container              |
| `redis`       | Requires a disposable Redis 7 service                    |
| `external`    | Live external-source contract tests (scheduled only)     |
| `e2e`         | Raw-to-API deterministic end-to-end fixture flow         |
| `performance` | Load, volume, or benchmark scenarios                     |
| `slow`        | Expected duration exceeds 30 seconds                     |
| `frontend`    | JavaScript unit, component, and browser contracts        |
| `deployment`  | Container, proxy, and composed-service contracts         |

### Project Structure
```
data_ingestion_toolbox/
├── apps/
│   └── api/              — FastAPI service
├── src/
│   └── data_ingestion_toolbox/
│       ├── bls/          — BLS ingestion and transforms
│       ├── census_acs/   — Census ACS ingestion and transforms
│       ├── fred/         — FRED ingestion and transforms
│       ├── silver_ref/   — Shared dimensions
│       ├── sql/          — Shared SQL helpers
│       ├── utility/      — Shared utilities
│       └── models.py, db.py, config.py
├── dags/                 — Airflow DAGs
├── documentation/        — Architecture and operational docs
├── scripts/              — Deployment, provisioning, and production diagnostics
├── infra/
│   ├── airflow/
│   ├── docker/
│   └── martin/
```

### Adding a New Data Source

New source onboarding is gated until the shared glossary, policy separation, and
raw-capture foundation are ready. Use the contract-driven
[`Adding a data source` checklist](docs/reference/ADDING_A_DATA_SOURCE.md); do not
copy the current sources' legacy raw or shared-gold ownership patterns.

---

## Contributing

- All ingestion functions must support idempotent re-runs (ON CONFLICT logic)
- All transforms must use `TransformMetrics` for comprehensive logging
- Add tests in `tests/` directories
- Update this README if adding major features or configuration changes

---

## License

Personal use. Adapt for your own data warehouse needs.

## Contact & Support

For issues, questions, or contributions, see individual module READMEs or contact the maintainer.

---

**Last Updated:** May 2026
**Status:** Production (v2.1, FRED ledger fix applied)
**Data Currency:** Daily ingestion updates (Gazetteer quarterly) 
