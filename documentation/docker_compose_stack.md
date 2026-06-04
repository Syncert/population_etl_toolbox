# Docker Compose Stack

Compose files live in `infra/docker/`.

Core services:

- web (3000)
- api (8000)
- martin (3001)
- analytics_postgres (internal)
- service_postgres (internal)
- redis (internal)
- airflow-webserver (8080)
- airflow-scheduler
- airflow-triggerer

Security notes:

- Do not expose Airflow publicly without authentication.
- Do not expose Postgres publicly.
- Do not expose Redis publicly.
- Use read-only DB credentials for API and write credentials for ETL/orchestration.
