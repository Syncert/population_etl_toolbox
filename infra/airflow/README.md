# Airflow Infrastructure

Airflow runtime artifacts live under this folder.

## Source of truth

- DAG definitions live at repository root: `dags/`
- Runtime should use:
	- `AIRFLOW__CORE__DAGS_FOLDER=/opt/data_ingestion_toolbox/dags`
	- `PYTHONPATH=/opt/data_ingestion_toolbox/src:/opt/data_ingestion_toolbox`

## Runtime files

- Env example: `infra/airflow/airflow.env.example`
- Compose stack: `infra/docker/docker-compose.airflow.yml`

## Smoke checks

From repository root after environment is configured:

```bash
airflow dags list
airflow dags test silver_ref
```

Using Docker Compose:

```bash
docker compose -f infra/docker/docker-compose.airflow.yml up airflow-init
docker compose -f infra/docker/docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
docker compose -f infra/docker/docker-compose.airflow.yml exec airflow-scheduler airflow dags list
docker compose -f infra/docker/docker-compose.airflow.yml exec airflow-scheduler airflow dags test silver_ref
```