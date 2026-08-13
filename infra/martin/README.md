# Martin Infrastructure

Infrastructure artifacts for Martin service deployment.

The Martin runtime config is in `martin.yml` and is mounted by compose stacks. It only contains listen/table mapping configuration.

Database connectivity is injected at runtime by docker compose using `ANALYTICS_DB_*` environment variables.

The runtime is pinned to `ghcr.io/maplibre/martin:1.11.0@sha256:0650e9025f5fcffdc686358114679421b5e6b0ca37b374ad8a66f14709d59d2b`. The API/Martin database role is read-only, and the container root filesystem is read-only apart from a temporary-memory `/tmp` mount.

Expected source database:
- Host: `analytics_postgres`
- Port: `5432`
- Database: `population_etl`
- Source table/layer seed: `gold.dim_geo_latest` (published as `counties`)
