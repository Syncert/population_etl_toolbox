# Docker Infrastructure

Containerization artifacts and runtime definitions.

## Compose Stacks

- `docker-compose.airflow.yml`: Airflow-focused stack for scheduler/webserver + a single Postgres metadata/service DB.
- `docker-compose.yml`: Full local platform stack including analytics PostGIS DB, service Postgres, Redis, API, Martin, web placeholder, and Airflow services.

## Run Full Stack

```bash
cp infra/docker/stack.env.example infra/docker/stack.env
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up airflow-init
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up -d
```

## Smoke Checks

```bash
# API health
curl http://localhost:8000/health

# Airflow DAG visibility
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml exec airflow-webserver airflow dags list

# Airflow DAG smoke run
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml exec airflow-webserver airflow dags test silver_ref 2026-01-01
```