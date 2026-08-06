.PHONY: test-unit test-etl test-api test-dags test-integration test-external test-e2e test-performance

test-unit:
	pytest tests/unit

test-etl:
	pytest -m "unit and not api" tests/unit/census tests/unit/bls tests/unit/fred tests/unit/shared

test-api:
	pytest -m "unit and api" tests/unit/api

test-dags:
	RUN_DAG_TESTS=1 pytest -m dag tests/dags

test-integration:
	RUN_INTEGRATION_TESTS=1 pytest -m "integration and not e2e" tests/integration

test-external:
	RUN_EXTERNAL_TESTS=1 pytest -m external tests/external

test-e2e:
	RUN_E2E_TESTS=1 pytest -m e2e tests/e2e

test-performance:
	RUN_PERFORMANCE_TESTS=1 pytest -m performance tests/performance
