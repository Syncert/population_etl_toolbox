# Docker Infrastructure

Containerization artifacts and runtime definitions.

## Compose Stacks

- `docker-compose.airflow.yml`: Airflow-focused stack for scheduler/webserver + a single Postgres metadata/service DB.
- `docker-compose.yml`: Internal self-contained stack with analytics PostGIS DB, service Postgres, Redis, API, Martin, the Next.js MVP app, and Airflow services.
- `docker-compose.external.yml`: External integration stack targeting existing analytics and Airflow metadata Postgres hosts. Supports service-only local MVP (`redis`, `api`, `martin`, `web`) by default, with optional local Airflow services under profile `airflow-local`.

## Modes

### Internal Mode (Self-Contained)

```bash
cp infra/docker/stack.env.example infra/docker/stack.env
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up airflow-init
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up -d
```

### External Mode (Existing Airflow and Postgres)

```bash
cp infra/docker/stack.external.env.example infra/docker/stack.external.env
python scripts/provision_api_readonly.py --env-file infra/docker/stack.external.env --write-env
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml up -d redis api martin web
```

### External MVP (Service-Only)

This is the recommended local workflow when an existing Airflow deployment and populated warehouse already exist.

```bash
cp infra/docker/stack.external.env.example infra/docker/stack.external.env
# fill secrets/host values in infra/docker/stack.external.env
python scripts/provision_api_readonly.py --env-file infra/docker/stack.external.env --write-env
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml up -d redis api martin web
```

### External + Local Airflow Profile (Optional)

Use this only when you explicitly want local Airflow services for testing.

```bash
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml --profile airflow-local up airflow-init
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml --profile airflow-local up -d redis api martin web airflow-webserver airflow-scheduler
```

Secrets guidance:
- Put credentials in `infra/docker/stack.external.env` (local, gitignored) or provide them via host environment variables.
- Do not put real secrets in tracked example files.

## Smoke Checks

### Internal Mode

```bash
# API health
curl http://localhost:8000/health

# Airflow DAG visibility
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml exec airflow-webserver airflow dags list
```

### External Mode

```bash
# Next.js MVP app and proxied API health
curl http://localhost:3001/
curl http://localhost:3001/api/health

# Martin health/root via same-origin web proxy
curl http://localhost:3001/tiles/health
# if /tiles/health is unavailable:
curl http://localhost:3001/tiles/

# Optional Airflow DAG visibility (only when using --profile airflow-local)
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml --profile airflow-local exec airflow-webserver airflow dags list
```

One-command disposable Compose smoke:

```bash
powershell -ExecutionPolicy Bypass -File tests/run.ps1 compose-smoke
```

`ANALYTICS_DB_*` credentials remain the ETL/owner connection. The public API and Martin use the separate `ANALYTICS_API_DB_*` role. `provision_api_readonly.py` creates that role, grants `SELECT` across `gold`, removes write/schema privileges, enables read-only transactions by default, and stores a generated password in the local gitignored env file.

Assertions live under `tests/`: Compose health/dependency contracts are in
`tests/integration/deployment/`, Martin runtime and MVT checks are in
`tests/integration/martin/`, the API/tile join is in `tests/e2e/`, and browser
MapLibre/catalog/observation/failure flows are in `tests/frontend/`, and the
PowerShell tier runner is `tests/run.ps1`. The `scripts/` directory contains
only deployment, provisioning, and production-diagnostic utilities.

## API-to-Map Contract Smoke

Use these commands to verify the first API-to-map contract in external MVP mode:

```bash
curl http://localhost:3001/
curl http://localhost:3001/api/health
curl http://localhost:3001/api/catalog/metrics?limit=5
curl "http://localhost:3001/api/observations/latest?metric_code=population&geo_level=county&limit=5"
curl http://localhost:3001/tiles/health
# if /tiles/health is unavailable:
curl http://localhost:3001/tiles/
```

Next.js MVP app:
- `http://localhost:3001`
- Analytics routes: `/catalog`, `/explore`, `/profiles`, `/articles`, and `/builder`
- Same-origin routes proxied by the Next.js server:
	- `/api/*` -> API service (`api:8000`)
	- `/tiles/*` -> Martin service (`martin:3000`)

Expected alignment key:
- `geo_id` from API observation rows should align with geographic identifiers used by Martin-exposed map layers.
- The explicitly published Martin layer is `counties`, sourced from
  `gold.dim_geo_latest.geo_geom`; automatic table publication is disabled.

Centralized API-to-map contract check (with the disposable services running):

```bash
powershell -ExecutionPolicy Bypass -File tests/run.ps1 martin-integration
```

The product-friendly `metric_code=population` alias resolves centrally to canonical county-capable metric `ACS:acs5:B01003_001`; returned rows retain the canonical source code for traceability.

Security reminder:
- Keep real credentials in local untracked env files (for example `infra/docker/stack.external.env`), not in tracked examples.
- Only the Next.js gateway binds broadly by default. API, Martin, Postgres, Redis, and Airflow are internal or loopback-bound.
- Public analytical GET responses use Redis with a bounded TTL and fall back to the database if Redis is unavailable.
- Production base and service images use readable tags plus immutable manifest digests. Martin is pinned to version 1.11.0, and application/Martin containers run with read-only root filesystems and `no-new-privileges`.
