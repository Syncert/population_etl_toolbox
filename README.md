# data_ingestion_toolbox

An analytics platform over seven public-data sources, built on a dimensional
warehouse it also owns. Census ACS, Census PEP, BLS, FRED, CDC, FBI UCR, and
USDA NASS are captured losslessly, conformed in silver, published as
deterministic gold products, served by a versioned FastAPI contract, and
explored through a Next.js application with catalog, map, chart, comparison,
workbench, and evidence-composition surfaces.

The warehouse is the foundation, not the product. What it exists to support is
described in
[`docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md`](docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md),
which also states the guardrails every packaged product inherits — among them
that cross-source association is never presented as causation, and that unlike
measures are never collapsed into an unexplained score.

The implemented test contract is
[`docs/reference/TESTING_CONTRACT.md`](docs/reference/TESTING_CONTRACT.md).
The data-layer migration that produced the current boundaries is recorded in
[`docs/plans/completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md`](docs/plans/completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md).

## Project Vision

**Goal:** Give analysts, journalists, planners, and residents reliable current
public-data statistics — and the tools to compose an argument from them —
without manual data collection, API expertise, or a spreadsheet that nobody
else can reproduce.

**Sources:**
- **Census Bureau ACS** (American Community Survey): detailed demographic tables (1-year and 5-year) by geography (US, state, county, place)
- **Census Bureau PEP** (Population Estimates Program): annual national, state, county, and incorporated-place estimates with immutable release vintages
- **BLS** (Bureau of Labor Statistics): labor statistics including employment, unemployment, and wage data
- **FRED** (Federal Reserve Economic Data): macroeconomic time series (employment, inflation, interest rates, etc.)
- **CDC**: disease and illness surveillance, with suppression, provisional status, and case definitions preserved
- **FBI UCR**: reported offense and arrest data by agency, with reporting participation preserved
- **USDA NASS**: agricultural production and operations from the Quick Stats program

**Architecture order.** `AGENTS.md` sets the dependency order this repository
builds in, and the README does not restate it differently:

```text
stable warehouse objects -> stable API contracts -> frontend analytics/social features
```

Warehouse first, API only on stable warehouse contracts, web only on stable API
contracts. When a downstream requirement exposes a missing upstream foundation,
the upstream contract is fixed or planned first.

**Layer contract.** Defined by
[`ADR-0001`](docs/decisions/0001-data-layer-boundaries.md): immutable lossless
raw captures, separate mutable control state, conformed silver data,
deterministic data-derived gold products, and independently owned
semantic/serving policy. Subsequent decisions are recorded beside it in
[`docs/decisions/`](docs/decisions/0002-api-versioning-and-deprecation.md).

## Current State (as of 2026-09-16)

This section is dated because it goes stale. If the date is old, trust
`docs/plans/` and `docs/reference/` over this list.

### Delivered

- **Seven source pipelines**, each with an Airflow DAG, capture-first raw
  ingestion, silver conformance, gold publication, operator documentation, and
  end-to-end coverage.
- **Geographic master data:** capture-first, versioned Census
  nation/state/county/place identities, attributes, boundaries, and
  relationships.
- **Warehouse quality controls:** transform metrics, declared data-quality
  rules run against the warehouse, and idempotent re-runnable ingestion and
  transforms.
- **A versioned public API** (FastAPI) serving catalog, observations,
  distribution, comparison, and evidence-packet resources, with response
  caching, per-client rate limiting by cost class, request telemetry, and a
  content-aware health resource.
- **Vector tiles** through Martin, joined to API-served measures.
- **A web application** (Next.js, ~21,000 lines of TypeScript) with catalog,
  per-source explorers, map and chart surfaces, a comparison workspace, a
  multi-series workbench, saved analyses, an evidence-packet builder, and a
  composed-article reader.
- **Account-owned storage** for saved analyses and evidence packets, behind an
  operator-provisioned bearer credential (ADR-0003, ADR-0004).
- **CI across every tier:** units, integration against disposable PostGIS and
  Redis, Martin contracts, DAG parsing, frontend lint/typecheck/unit/browser,
  a live-stack smoke tier, package build, and coverage gates.

### Not yet delivered

Tracked as plans in [`docs/plans/to_do/`](docs/plans/README.md):

- **Self-service accounts.** Every write path still requires an
  operator-minted credential, so a visitor cannot own saved work. The identity
  contract is proposed in
  [`ADR-0005`](docs/decisions/0005-self-service-accounts.md) and held at a
  human review gate.
- **Publishing.** An evidence packet can be composed but not made public;
  there is no approval path from private to published.
- **The second wave of packaged products.** Twenty use cases are described;
  the first wave of surfaces is built, the rest are not.
- **A portable deployment path.** The only deployment entrypoint is a
  PowerShell script, and no deployment origin is configured for the
  live-deployment smoke job to grade.

---

## Technical Architecture

### Data Models

#### Raw capture and control

Exact provider response bytes are committed under `raw_capture` before parsing.
Runs, requests, retries, watermarks, slice ledgers, and quarantine are maintained
separately under `control`. Parsed observation revisions live in source silver
schemas; the retired `raw_*.{source}_long` tables are not deployed.

#### Silver Layer (Dimensional)
All silver schemas use standard dimension keys for consistent joins:

```
silver_ref.dim_time                   — Daily Gregorian calendar
silver_ref.dim_geo_entity             — Stable nation/state/county/place identities
silver_ref.dim_geo_entity_version     — Immutable vintage-specific attributes
silver_ref.dim_geo_geometry_version   — Immutable boundary geometries
silver_ref.bridge_geo_relationship_version — Versioned containment/intersection evidence
silver_ref.dim_geo_current            — Approved current projection
silver_ref.dim_geo                    — Read-only compatibility projection

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
| `silver_ref` | Monthly (1st @ 05:00 UTC) | Capture/replay the latest complete Census geography snapshot and sync time |
| `acs_ingest` | Monthly (1st @ 06:00 UTC) | Ingest configured ACS history after shared geography is ready |
| `census_pep_ingest` | Monthly (1st @ 06:00 UTC) | Capture current registered PEP releases after production-scale geography checks |
| `bls_ingest` | Monthly (1st @ 07:00 UTC) | Ingest configured BLS history after shared geography is ready |
| `fred_ingest` | Monthly (1st @ 08:00 UTC) | Ingest configured FRED history |

### Key Design Decisions

**Hash-Based Change Detection:**
- Each ingestion slice computes a hash of available series/variables for the time period
- If hash matches a previous successful slice, skip re-ingestion (API-efficient)
- If hash changes (new variables added), mark old slices as stale and re-ingest

**Dimension Matching with Metrics:**
- Observation transforms resolve exact provider codes against shared current geography
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
- The independent `silver_ref` DAG owns geography; ACS and BLS fail early when its required snapshot is incomplete.
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

On Linux and macOS, one command installs everything the checks in this repository are graded by — the `local` Python extra and the frontend's locked dependency tree:

```bash
make bootstrap
source .venv/bin/activate    # bootstrap prints this line when it creates .venv
```


The Python runtime set is locked. `pyproject.toml` declares the ranges the API
must satisfy; `requirements/api.lock.txt` records one hashed resolution of
them, and it is what `infra/docker/Dockerfile.api` and the `package-api` job
install. Regenerate it with `make lock-api` (which needs
[`uv`](https://docs.astral.sh/uv/)) and review the diff: it is the file that
decides what a deployed image contains. `api-lock-refresh` re-resolves it
weekly and proposes a diff; it never merges one, and it is not a required
check. The Airflow extra is deliberately outside this lock -- it pins
SQLAlchemy 1.4 against the API's 2.x, and one resolution cannot hold both.

`make bootstrap` is idempotent and safe to re-run, which is how to pick up a dependency change after a pull. It installs into an already-active virtual environment, creates `.venv` when none is active, installs the API's runtime packages from `requirements/api.lock.txt` by hash so a local checkout holds the versions the deployed image holds, and installs the web tree from `apps/web/package-lock.json` with `npm ci` so a local checkout resolves the same tree CI grades. It deliberately never installs into a bare system interpreter: on Ubuntu 24.04 `/usr/lib/python3/dist-packages` sits on the supported Python 3.11 interpreter's `sys.path` carrying C extensions built for 3.12, and importing one of those aborts collection with a panic instead of the `ImportError` that callers guard for.

Windows PowerShell has no `make`; install the same set directly:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e .[local]
npm ci --prefix apps/web
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

Consumers should start with the
[API consumer guide](docs/reference/API_CONSUMER_GUIDE.md), which is the stable
contract for routes, semantics, errors, caching, limits, and version policy.

Every resource is served under `/api/v1`, and only there. The unversioned
`/api` aliases that carried the original MVP paths were retired once the API
had no downstream dependants; an unversioned data path now answers `404`. See
[ADR-0002](docs/decisions/0002-api-versioning-and-deprecation.md) and its
2026-09-01 amendment.

Provider-neutral endpoints:
- `GET /api/v1/catalog/sources`
- `GET /api/v1/catalog/metrics`
- `GET /api/v1/catalog/metrics/{metric_code}` — one metric's published
  semantics plus the versioned routes that can serve it
- `GET /api/v1/catalog/geographies`
- `GET /api/v1/catalog/capabilities` — machine-readable per-source capability
  metadata: route segment, neutral-route reachability, registered datasets,
  and per-route filter names for every completed source
- `GET /api/v1/catalog/freshness` — per-source publication and freshness
  state rolled up from the harvested glossary
- `GET /api/v1/observations` — observations for any completed source's metric,
  dispatched through the reviewed registry to the owning source's serving
  relations. `scope=latest` (default) serves the source's own latest
  publication; `scope=as_released` serves every published release, optionally
  pinned with `release=`. Filters beyond the universal parameters are
  per-source and declared by `/api/v1/catalog/capabilities`; an unsupported
  filter is rejected with an explanation
- `GET /api/v1/observations/releases` — the published release identities
  holding a metric's observations, newest first
- `GET /api/v1/observations/latest` — legacy shape; serves the three sources
  published into the cross-source union views (Census ACS, BLS, FRED)
- `GET /api/v1/observations/timeseries` — legacy shape; same three sources
- `GET /api/v1/comparison/preflight` — whether two metrics can be compared,
  and why: the declared compatibility rules (units, time grains, geography
  grains, aggregation, source analysis readiness) evaluated three-valued,
  with unpublished semantics reported as caveats rather than assumed
- `GET /api/v1/comparison` — aligned comparison of two compatible metrics,
  one newest value per geography per side, with both inputs' periods and
  identities on every API-derived difference/ratio; an incompatible pair is
  rejected with the failed rules
- `GET /api/v1/comparison/correlation` — API-derived Pearson and Spearman
  coefficients over exactly the pairs `/comparison` would page, with the
  pair count, each side's coverage, how many pairs were contemporaneous, an
  optional same-year pin, and caveats led by association-not-causation; a
  coefficient the pairs cannot support is `null` with its reason, never `0`
- `GET /api/v1/comparison/matrix` — two to eight measures aligned on
  geography: a compatibility verdict and, where it allows one, a correlation
  per unordered pair, plus wide rows over the union of the geographies the
  measures published, each cell carrying its own period and release. A
  declined pair is a cell; a declined source refuses the whole request
- `GET /api/v1/distribution/bins` — API-derived equal-width bins over one
  metric's latest values, dispatched to the owning source; stratified
  sources are declined with their declared restriction
- `GET /api/v1/health`

Saved analysis configurations (API-owned, authenticated — see
[ADR-0003](docs/decisions/0003-saved-analysis-authentication-and-persistence.md)):
- `GET|POST /api/v1/analysis-configurations`
- `GET|PUT|DELETE /api/v1/analysis-configurations/{configuration_id}`

These require `Authorization: Bearer <token>`, are scoped to the authenticated
owner, and are never publicly cached. Tokens are operator-provisioned with
`python scripts/provision_app_api.py --apply-schema --issue-token "<label>"`,
which prints the token exactly once and stores only its SHA-256 digest;
`--revoke-token-label "<label>"` revokes it. Storage lives in the `app_api`
schema under the separate `api_app_writer` role — the warehouse role stays
read-only — and is configured with `APP_API_DATABASE_URL`. With that unset the
routes answer an explicit 503 and the rest of the API is unaffected.

Source-scoped endpoints:
- `GET /api/v1/{bls,census,fred,pep}/observations/latest`
- `GET /api/v1/{bls,census,fred,pep}/observations/timeseries`
- `GET /api/v1/cdc/observations`
- `GET /api/v1/usda-nass/{observations,series,measures,source-notes}`

Every observation route pages with `limit`/`offset` over a total order, so
consecutive pages neither repeat a row nor skip one; the orders are listed in
[the consumer guide](docs/reference/API_CONSUMER_GUIDE.md).

`GET /health` — without the `/api` prefix — is the container and load-balancer
liveness probe; `GET /health/ready` is the readiness probe (503 while the
database is unreachable; Redis never gates readiness). Both sit outside the
version policy and are not deprecated.

Operational contract (API-006): responses are cached under a key carrying the
served-contract fingerprint and the warehouse publication epoch, so a
republication is served within `API_CACHE_FRESHNESS_SECONDS` regardless of the
TTL; the API engine runs with declared pool, connect, and statement-timeout
budgets (`API_DB_*`); optional per-client rate limits split catalog from
analytical cost (`API_RATE_LIMIT_*`, off by default); and every response
carries an `X-Request-ID` logged with a structured completion line.

The limiter's client is the address the request arrived from, so a deployment
that fronts the API with a proxy — every topology here does — must declare
that proxy in `API_TRUSTED_PROXY_IPS` (addresses or CIDR blocks) or the
per-client budgets become one budget for the whole deployment. A forwarded
address is read only from a declared hop; from anywhere else the header is
ignored, so it can never be used to claim a second budget.

Metric identity: `metric_code` is required wherever a metric is named, and is
its only spelling. The `metric_id` alias and the `population` convenience
mapping were retired with the route aliases — one spelling means a request
cannot silently resolve to something the caller did not name.

### Next.js Web App (Local Iteration)

Run the web application locally:

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

The analytical pages:

- `/catalog` — the published measures and what each one declares.
- `/explore` — one measure at a time: its map, its history, its table, its
  distribution, and every field qualifying its values.
- `/compare` — two measures checked against the declared compatibility rules
  before any data moves, then aligned on geography.
- `/workbench` — build your own: any published measures on one chart, as a
  line, bars, a scatter, a ranking, a geography × period heatmap, or an
  API-derived correlation. Each series is one measure at one geography;
  nothing is rolled up from a finer grain, normalised, or rescaled to share an
  axis. `/builder` is the evidence-packet composer and keeps its name, so
  "build" means compose a document and "workbench" means compose a chart.
- `/profiles`, `/articles`, `/quality`, `/saved` — the product surfaces over
  the same contracts.

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

Create the PostgreSQL database first; DAGs do not create databases. Apply every
checked-in schema and migration through the ordered bootstrap manifest:

```bash
export WAREHOUSE_URL='postgresql://postgres:REDACTED@HOST:5432/population_etl'
jq -r '.assets[].path' sql/bootstrap/warehouse_manifest.json |
while IFS= read -r asset; do
    psql "$WAREHOUSE_URL" -X -v ON_ERROR_STOP=1 -f "$asset" || exit 1
done
```

For the destructive beta cutover, container-based `psql` alternative, validation
queries, and exact DAG order, use the
[beta reset and re-ingestion runbook](docs/reference/BETA_RESET_REINGESTION.md).

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

Use the Airflow-only compose stack at `infra/docker/docker-compose.airflow.yml` when you just need DAG orchestration.
It runs one PostGIS cluster with Airflow's metadata database and a separate warehouse database
(`PUBLIC_DATA_DB_NAME`, default `population_etl`) that the `public_data` connection points at.

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

The stack lifecycle has one set of rules and two entrypoints over it. Which
compose and env file a mode uses, which services it starts, and the refusal to
run `airflow db migrate` against the warehouse all live in
[`tools/deployment.py`](tools/deployment.py); neither entrypoint carries a rule
of its own, so neither can drift from the other (DEPLOY-008).

On Linux or macOS:

```bash
# Defaults: --mode internal --action all
make deploy-up                                          # or: python scripts/deploy_stack.py --action up
make deploy-init
make deploy-down

# External mode
make deploy-up MODE=external
make deploy-init MODE=external DEPLOY_ARGS=--with-local-airflow

# Print the resolved compose invocation and the guard's verdict, run nothing
make deploy-plan MODE=external
```

On Windows PowerShell, unchanged:

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

Both refuse to start when `AIRFLOW_METADATA_DB_*` and the warehouse resolve to
the same database: `airflow-init` runs `airflow db migrate`, which would create
Airflow's metadata schema inside the warehouse and then reset the `public_data`
connection and every API pool. The refusal names both targets and how to fix
it, and `--allow-airflow-metadata-in-warehouse` (PowerShell:
`-AllowAirflowMetadataInWarehouse`) permits it deliberately.

Deployment verification is owned by the cataloged test suite; run
`./tests/run.ps1 compose-smoke` rather than embedding assertions in the
lifecycle script.

In the compose environment, `airflow-init` automatically seeds the `public_data` Airflow connection:

- Airflow-only compose seeds `public_data` -> host `postgres`, schema
  `${PUBLIC_DATA_DB_NAME:-population_etl}` (the warehouse database beside the
  metadata one, created by `infra/docker/initdb/create_warehouse_database.sh`).
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
airflow pools create cdc_api 2 "CDC Open Data API limit"
airflow pools create fbi_cde_api 2 "FBI Crime Data Explorer API limit"
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

# Optional CDC Socrata public-read app token; anonymous reads are supported.
export CDC_SOCRATA_APP_TOKEN="your_cdc_socrata_app_token_here"

# Required api.data.gov key for the FBI Crime Data Explorer API.
export FBI_CDE_API_KEY="your_fbi_cde_api_key_here"

# For USDA NASS Quick Stats (required); inject it into every Airflow container
# from the deployment secret store, never from a tracked file or an image.
export USDA_NASS_API_KEY="your_usda_nass_api_key_here"
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

# Option B: Trigger from the Airflow CLI
airflow dags trigger silver_ref
```

On its first successful run, this capture-first load publishes the 1990 and
2000 decennial county Gazetteers, backfills every available annual county
Gazetteer from 2013 onward, then publishes the newest complete
nation/state/county/place Gazetteer and boundary snapshot last. Legacy
Gazetteers retain names, land/water area, and internal-point coordinates; their
raw captures also retain source population and housing fields. Later monthly
runs skip completed historical county vintages and refresh the newest complete
snapshot. This keeps retired county identities resolvable for historical ACS
and BLS observations while preserving current retirement state. ACS and BLS
should run only after this DAG succeeds; the ACS silver transform fails its
preflight instead of silently dropping observations when any captured
geography identity is unresolved.

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
SELECT provider_source, provider_dataset, source_geo_type, source_code,
       source_vintage, status, reason_code
FROM silver_ref.geography_resolution
WHERE status <> 'resolved'
ORDER BY resolved_at DESC;
```

**Resolution:**
- Check whether the exact source code/type and vintage are supported.
- Re-run `silver_ref` to replay a complete captured reference snapshot.
- Do not insert guessed name matches; unresolved observations remain reported until an exact-code or evidence-backed crosswalk exists.

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
SELECT geo_type, COUNT(*) AS active_entities,
       MIN(first_seen_version) AS first_vintage,
       MAX(last_seen_version) AS last_vintage
FROM silver_ref.dim_geo_entity
JOIN silver_ref.dim_geo_current USING (geo_sk)
WHERE is_active
GROUP BY geo_type ORDER BY geo_type;
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
# Install (see Setup & Configuration; `make bootstrap` installs the superset)
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

**Last Updated:** August 2026
**Status:** Beta; clean database reset/re-ingestion is supported
**Data Currency:** Source schedules plus monthly Census reference discovery
