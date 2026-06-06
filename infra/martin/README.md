# Martin Infrastructure

Infrastructure artifacts for Martin service deployment.

The Martin runtime config is in `martin.yml` and is mounted by compose stacks. It only contains listen/table mapping configuration.

Database connectivity is injected at runtime by docker compose using `ANALYTICS_DB_*` environment variables.

Expected source database:
- Host: `analytics_postgres`
- Port: `5432`
- Database: `population_etl`
- Source table/layer seed: `gold.dim_geo_latest` (published as `counties`)