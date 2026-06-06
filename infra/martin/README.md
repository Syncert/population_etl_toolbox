# Martin Infrastructure

Infrastructure artifacts for Martin service deployment.

The initial Martin runtime config is in `martin.yml` and is mounted by the full compose stack (`infra/docker/docker-compose.yml`).

Expected source database:
- Host: `analytics_postgres`
- Port: `5432`
- Database: `population_etl`
- Source table/layer seed: `gold.dim_geo_latest` (published as `counties`)