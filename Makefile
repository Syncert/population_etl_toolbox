.PHONY: test-unit test-etl test-api test-dags test-dag-pipeline test-integration test-external test-e2e test-martin-unit test-martin-integration test-performance test-resilience test-web-unit test-web-browser test-web-build test-compose-smoke test-linux test-linux-build

test-unit:
	pytest tests/unit

test-etl:
	pytest -m "unit and not api" tests/unit/census tests/unit/bls tests/unit/fred tests/unit/cdc tests/unit/fbi_ucr tests/unit/usda_nass tests/unit/shared

test-api:
	pytest -m "unit and api" tests/unit/api

test-dags:
	RUN_DAG_TESTS=1 pytest -m dag tests/dags

test-dag-pipeline:
	@set -e; \
	  trap 'docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans' EXIT; \
	  docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres; \
	  RUN_DAG_TESTS=1 RUN_INTEGRATION_TESTS=1 \
	  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
	  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
	  TEST_POSTGRES_DATABASE=population_etl_test \
	  pytest -m "dag and integration and database" tests/dags/test_dag_pipeline_execution.py

test-integration:
	RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration

test-external:
	RUN_EXTERNAL_TESTS=1 RUN_INTEGRATION_TESTS=1 pytest -m external tests/external tests/integration/database/legacy

test-e2e:
	RUN_E2E_TESTS=1 pytest -m e2e tests/e2e

test-martin-unit:
	pytest -m unit tests/unit/martin

test-martin-integration:
	@set -e; \
	  trap 'docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans' EXIT; \
	  docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis martin proxy; \
	  RUN_INTEGRATION_TESTS=1 RUN_E2E_TESTS=1 RUN_MARTIN_TESTS=1 \
	  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
	  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
	  TEST_POSTGRES_DATABASE=population_etl_test \
	  pytest -m martin tests/integration/martin tests/e2e/test_martin_api_join.py

test-performance:
	RUN_PERFORMANCE_TESTS=1 pytest -m performance tests/performance

test-resilience:
	RUN_INTEGRATION_TESTS=1 RUN_E2E_TESTS=1 RUN_PERFORMANCE_TESTS=1 pytest -m "integration or e2e or performance" tests/resilience tests/integration/database/test_production_resilience.py tests/integration/api/test_connection_capacity.py

test-web-unit:
	npm --prefix apps/web run test:unit

test-web-browser:
	npm --prefix apps/web run test:browser

test-web-build:
	npm --prefix apps/web run lint
	npm --prefix apps/web run typecheck
	npm --prefix apps/web run build

test-compose-smoke:
	@set -e; \
	  trap 'docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans' EXIT; \
	  docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis martin proxy; \
	  RUN_INTEGRATION_TESTS=1 RUN_COMPOSE_TESTS=1 RUN_MARTIN_TESTS=1 \
	  TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
	  TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
	  TEST_POSTGRES_DATABASE=population_etl_test \
	  TEST_REDIS_URL=redis://127.0.0.1:56379/15 \
	  pytest -m "integration and deployment" tests/integration/deployment

# Airflow refuses to initialize outside a POSIX-compliant OS, so on a Windows
# checkout every `dag` test - and every module importing airflow at collection
# time, such as tests/integration/database/test_usda_nass_dag_tasks.py - dies
# before reaching an assertion. These targets run the suite inside the pinned
# Airflow 2.9.3 + Python 3.11 image against the disposable PostGIS service, so
# the CI result is reproducible locally. Sources are mounted read-only, so an
# edit needs no rebuild: Compose builds the image on first use and reuses it
# after. Run test-linux-build after a dependency change, and only then - the
# image Dockerfile copies the whole checkout, so every rebuild reinstalls.
#
# TEST_ARGS replaces the pytest arguments; the default is the `dag` suite:
#   make test-linux
#   make test-linux TEST_ARGS='-m "integration and database" tests/integration/database'
#
# The database is torn down per invocation on purpose. CI grades these suites
# in separate jobs against separate databases, and a suite run against another
# suite's residue reports failures CI will not reproduce.
COMPOSE_PYTEST = docker compose -f infra/docker/docker-compose.test.yml -f infra/docker/docker-compose.pytest.yml

test-linux-build:
	$(COMPOSE_PYTEST) build pytest

test-linux:
	@set -e; \
	  trap '$(COMPOSE_PYTEST) down --volumes --remove-orphans' EXIT; \
	  $(COMPOSE_PYTEST) run --rm pytest $(TEST_ARGS)
