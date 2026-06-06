# Docker Infrastructure

Containerization artifacts and runtime definitions.

## Compose Stacks

- `docker-compose.airflow.yml`: Airflow-focused stack for scheduler/webserver + a single Postgres metadata/service DB.
- `docker-compose.yml`: Internal self-contained stack with analytics PostGIS DB, service Postgres, Redis, API, Martin, web placeholder, and Airflow services.
- `docker-compose.external.yml`: External integration stack with Redis, API, and Airflow services only, targeting existing Airflow metadata and analytics Postgres hosts.

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
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml up airflow-init
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml up -d
```

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
# API health
curl http://localhost:8000/health

# Airflow DAG visibility
docker compose --env-file infra/docker/stack.external.env -f infra/docker/docker-compose.external.yml exec airflow-webserver airflow dags list
```