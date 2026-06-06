# Docker Infrastructure

Containerization artifacts and runtime definitions.

## Compose Stacks

- `docker-compose.airflow.yml`: Airflow-focused stack for scheduler/webserver + a single Postgres metadata/service DB.
- `docker-compose.yml`: Internal self-contained stack with analytics PostGIS DB, service Postgres, Redis, API, Martin, web MVP shell, and Airflow services.
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
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml up -d redis api martin web
```

### External MVP (Service-Only)

This is the recommended local workflow when an existing Airflow deployment and populated warehouse already exist.

```bash
cp infra/docker/stack.external.env.example infra/docker/stack.external.env
# fill secrets/host values in infra/docker/stack.external.env
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
# Web smoke dashboard and proxied API health
curl http://localhost:3001/
curl http://localhost:3001/api/health

# Martin health/root via same-origin web proxy
curl http://localhost:3001/tiles/health
# if /tiles/health is unavailable:
curl http://localhost:3001/tiles/

# Optional Airflow DAG visibility (only when using --profile airflow-local)
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml --profile airflow-local exec airflow-webserver airflow dags list
```

Note: `ANALYTICS_DB_*` values in external mode power both API database connectivity and Martin database connectivity.

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

Web smoke dashboard:
- `http://localhost:3001`
- Proxy routes from the web container:
	- `/api/*` -> API service (`api:8000`)
	- `/tiles/*` -> Martin service (`martin:3000`)

Expected alignment key:
- `geo_id` from API observation rows should align with geographic identifiers used by Martin-exposed map layers.

Security reminder:
- Keep real credentials in local untracked env files (for example `infra/docker/stack.external.env`), not in tracked examples.