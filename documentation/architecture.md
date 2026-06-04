# First-Pass Architecture

- Airflow prepares data.
- Postgres/PostGIS stores data.
- Gold tables/views define app contracts.
- FastAPI serves analytical JSON.
- Martin serves vector tiles.
- Next.js renders the UI.
- Redis caches common responses.

This first pass is additive and keeps existing ingestion DAGs and source logic intact.
