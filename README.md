# population_etl_toolbox

First-pass geospatial analytics platform foundation built on existing ACS/BLS/FRED ETL pipelines.

## Architecture

- Airflow prepares data.
- Postgres/PostGIS stores raw, silver, and gold data.
- Gold tables/views define app contracts.
- FastAPI serves analytical JSON.
- Martin serves vector tiles.
- Next.js renders the UI.
- Redis caches common responses.

## Repository highlights

- Existing ingestion logic remains in `census_acs/`, `bls/`, `fred/`, `silver_ref/`.
- New package namespace and wrappers in `src/population_etl_toolbox/`.
- New API app in `apps/api/`.
- New frontend shell in `apps/web/`.
- New compose stack in `infra/docker/`.
- Gold contract SQL in `sql/gold`, `sql/materialized_views`, `sql/indexes`.

## Quick start

```bash
pip install -e .
python -c "import population_etl_toolbox"
```

```bash
docker compose -f infra/docker/compose.yml config
docker compose -f infra/docker/compose.yml up -d
```

```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/catalog/metrics
```

## Frontend local commands

```bash
cd apps/web
npm install
npm run lint
npm run build
```

## Notes

- Existing DAGs are preserved; additive thin DAGs are provided in `dags/census_acs_ingestion.py`, `dags/bls_ingestion.py`, `dags/fred_ingestion.py`, and `dags/gold_refresh.py`.
- `USE_MOCK_DATA=true` can be used for API fallback where DB gold views are not yet populated.
- This is a first pass, not a production-complete SaaS implementation.
