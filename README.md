# data_ingestion_toolbox

A production-grade ETL system for ingesting, transforming, and serving economic and demographic data from authoritative US government sources. Built with Airflow, PostgreSQL, and Polars for real-time access to Census, BLS, and FRED data in a structured dimensional warehouse.

Repository architecture reference: `data_ingestion_toolbox_proposed_architecture.md`.

## Project Vision

**Goal:** Build a self-service data warehouse that provides reliable, current economic and demographic statistics to support analysis, dashboards, and reporting without requiring manual data collection or API expertise.

**Scope:**
- **Census Bureau ACS** (American Community Survey): detailed demographic tables (1-year and 5-year) by geography (US, state, county)
- **BLS** (Bureau of Labor Statistics): labor statistics including employment, unemployment, and wage data
- **FRED** (Federal Reserve Economic Data): macroeconomic time series (employment, inflation, interest rates, etc.)

**Architecture:** Three-layer medallion pattern:
1. **Raw Layer** (`raw_*` schemas): Unmodified data from source APIs, with ingestion ledgers for replay and idempotency
2. **Silver Layer** (`silver_*` schemas): Dimension-matched, deduplicated, and validated fact tables with comprehensive data quality logging
3. **Analytical Layer** (future): Aggregated star schemas and domain-specific views for reporting and dashboards

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

#### Raw Layer Tables
All raw schemas contain immutable source data plus an ingestion ledger for tracking:

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
| `bls_ingest_dag` | Daily @ 02:30 UTC | Ingest BLS series (100+ programs, national + state + county) |
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

---

## Setup & Configuration

### Prerequisites

- **Airflow 2.7+** (tested with 2.8)
- **PostgreSQL 14+** (tested with 15)
- **Python 3.10+**
- **Python packages:** managed via `pyproject.toml` extras

### Python Environment and Install

Standard local development install path:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e .[local]
```

This single command installs runtime dependencies and optional groups for Airflow, API/web development, and dev tooling.

Smoke test imports (no PYTHONPATH/path hacks required):

```bash
python -c "import data_ingestion_toolbox, data_ingestion_toolbox.bls, data_ingestion_toolbox.census_acs, data_ingestion_toolbox.fred, data_ingestion_toolbox.silver_ref, data_ingestion_toolbox.utility; print('imports ok')"
```

Optional targeted installs:

```bash
# ETL orchestration only
pip install -e .[airflow]

# API/analytics web layer only
pip install -e .[api]

# Lint/test tooling only
pip install -e .[dev]
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
```

### 2. Airflow Setup

#### Runtime Paths and Infra Files

Airflow runtime paths are hard-wired via `infra/airflow/airflow.env.example`:

```bash
AIRFLOW__CORE__DAGS_FOLDER=/opt/data_ingestion_toolbox/dags
PYTHONPATH=/opt/data_ingestion_toolbox/src:/opt/data_ingestion_toolbox
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Use the Airflow-only compose stack at `infra/docker/docker-compose.airflow.yml` when you just need DAG orchestration + metadata DB.

Use the full platform compose stack at `infra/docker/docker-compose.yml` when you need API + Martin + analytics PostGIS + Airflow together.

The full platform now supports two deployment modes: internal self-contained (`docker-compose.yml`) and external integration (`docker-compose.external.yml`) where Airflow metadata and analytics Postgres can point at existing infrastructure via environment variables.

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
├── scripts/              — Tooling and validation scripts
├── infra/
│   ├── airflow/
│   ├── docker/
│   └── martin/
```

### Adding a New Data Source

1. Create `new_source/` directory with `config.py`, `metadata.py`, `ingest.py`
2. Create `new_source/DDL/raw_new_source.sql` and `silver_new_source/DDL/silver_new_source.sql`
3. Implement ingestion functions following BLS/FRED/Census patterns
4. Create `dags/new_source_ingest_dag.py` following existing DAG structure
5. Define transformation logic in `new_source/silver_new_source/transform.py` using `TransformMetrics` class

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
