# CI evidence map

This register assigns each delivery contract to one authoritative GitHub Actions job. Required pull-request checks use the stable job names below; scheduled checks are release-freshness evidence and do not masquerade as commit checks.

| Contract | Authoritative workflow / job | Trigger tier | Owning paths |
| --- | --- | --- | --- |
| Formatting and repository evidence | `lint` / `Lint (ruff)` | Required PR/push | Python, tests, SQL manifests, docs contracts |
| ETL pure behavior and layer boundaries | `etl-unit` / `ETL unit tests (Python 3.11)` | Required PR/push | `src`, ETL tests, migrations |
| API contract, models, and query behavior | `api-unit` / `API unit tests (Python 3.11)` | Required PR/push | `apps/api` (routers, schemas, serving/discovery/observation-dispatch registries, the neutral observation resource, the analysis compatibility policy, versioning), SQL query builders, and the reviewed OpenAPI contract snapshot |
| Overall and changed executable coverage | `coverage` / `Unit + database coverage gates (Python 3.11)` | Required PR/push | Application Python and database replay tests |
| Runtime package contents and clean install | `package-api` / `Package build and install smoke (Python 3.11)` | Required PR/push | package metadata, runtime SQL, manifests |
| Ordered empty warehouse bootstrap, rerun, replay | `postgres-integration` / `Warehouse integration (PostGIS 16 + Python 3.11)` | Required PR/push | source DDL, migrations, bootstrap manifest, database tests |
| Airflow parsing and task topology | `dag-parse` / `DAG parse tests (Airflow 2.9.3 + Python 3.11)` | Required PR/push | DAGs, orchestration code, bootstrap manifest |
| Linux scheduler compatibility | `scheduler-image` / `DAG suite in scheduler image` | Required PR/push | scheduler image, DAGs, ETL package, SQL |
| Browser consumer contract | `frontend` / `Frontend lint, unit, build, and browser` | Required PR/push | web, `apps/api` response schemas, gold contracts, semantic migrations |
| Redis cache isolation | `redis-integration` / `API cache integration (Redis 7 + Python 3.11)` | Required PR/push | API/cache behavior |
| Spatial configuration | `martin-unit` / `Martin deterministic contracts (Python 3.11)` | Required PR/push | Martin configuration and contracts |
| Spatial database/proxy behavior | `martin-integration` / `Martin/PostGIS/API/proxy contracts` | Required PR/push | geography, gold serving, Martin, proxy |
| Deployment startup/readiness/teardown | `deployment-smoke` / `Deployment compose smoke` | Required PR/push | application, SQL, Docker, deployment config |
| Live provider compatibility | `external-contract` / `Census, PEP, BLS, FRED, CDC, FBI UCR, and USDA NASS live contracts` | Scheduled/manual; fresh within 7 days for release | source adapters and provider fixtures |
| Bounded E2E and performance | `e2e-performance` / `Bounded E2E and performance evidence` | Scheduled/manual; fresh within 7 days for release | full source-to-serving behavior |
| Data-product E2E coverage (E2E-008–E2E-014) | `e2e-performance` / `Bounded E2E and performance evidence`, plus `coverage` for the inventory unit test | Inventory unit test required PR/push; product run scheduled/manual | `tests/support/product_coverage.py`, `tests/support/warehouse_scope.py`, `tests/e2e`, source publishers and API routers |
| Warehouse data-quality contracts (DQ-001–DQ-007) | ride `etl-unit`, `coverage`, `postgres-integration`, `dag-parse`, `scheduler-image` above | Required PR/push | `src/data_ingestion_toolbox/quality`, `sql/migrations/013_data_quality_evidence.sql`, quality tests, `dags/warehouse_data_quality_dag.py` |

Branch protection should require the thirteen PR/push jobs above by their displayed job names. A release candidate additionally records successful live-provider and bounded E2E runs no older than seven days. The million-row profile remains opt-in and is required only when a release changes bulk loading, chunking, or serving-query plans.

The executable mirror is `tests/support/ci_evidence_manifest.json`; unit validation fails when a named workflow/job disappears or an architecture-sensitive owning path loses its trigger.
