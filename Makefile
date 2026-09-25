.PHONY: bootstrap bootstrap-python bootstrap-web deploy-init deploy-up deploy-down deploy-plan test-unit test-etl test-api test-dags test-dag-pipeline test-integration test-external test-e2e test-martin-unit test-martin-integration test-performance test-resilience test-web-unit test-web-browser test-web-build test-web-smoke test-web-maps test-compose-smoke test-linux test-linux-build

# The one command that turns a fresh clone into a checkout that can run the
# checks it is graded by. `pyproject.toml`'s `local` extra is already exactly
# the right Python set and `apps/web/package-lock.json` is already exactly the
# right web set; bootstrap's only job is to make both reachable without
# rediscovering them from the manifests.
#
# `npm ci` rather than `npm install`, because the lockfile is the committed
# definition of the web environment and CI installs from it. A lockfile that
# no longer matches `package.json` is a defect to fix, not a reason to resolve
# a different tree locally than the one CI grades.
#
# Re-running is cheap on purpose: a SessionStart hook calls this on every
# session start, and `npm ci` deletes and rebuilds `node_modules` every time
# it is invoked. The stamp records the lockfile the installed tree was built
# from, and lives inside `node_modules` so that removing the tree also removes
# the claim that the tree is current. Hashing runs through Python because
# `sha256sum` and `shasum` are not the same command on Linux and macOS.
BOOTSTRAP_WEB_STAMP = apps/web/node_modules/.bootstrap-lockfile-sha256

bootstrap: bootstrap-python bootstrap-web

# Bootstrap installs into a virtual environment and never into a bare system
# interpreter, and that is not a style preference. The web container's
# `python` is 3.11 while `/usr/lib/python3/dist-packages` holds Ubuntu 24.04's
# packages built for 3.12 -- every C extension there is wrong-ABI for the
# running interpreter, and the directory is still on its `sys.path`. On
# 2026-09-15 that one condition produced two unrelated-looking failures on the
# same clone: pip could not replace Debian's PyYAML 6.0.1 with `dev`'s 6.0.3,
# because the distro installed it with no RECORD file to uninstall from, and
# importing `apps.api.main` aborted collection of 25 API test modules with a
# `pyo3_runtime.PanicException` out of Debian's `cryptography`, whose bindings
# want a `_cffi_backend` compiled for 3.12. PyJWT guards that import with
# `except ImportError`, and a panic is not one.
#
# A venv built without `--system-site-packages` drops
# `/usr/lib/python3/dist-packages` from `sys.path` altogether, which retires
# the class rather than the two instances of it that happened to surface.
#
# `--timeout`/`--retries` because a bootstrap that dies on one slow read from
# files.pythonhosted.org reads as a broken repository rather than a slow
# network, and pip's default single retry does not separate those.
BOOTSTRAP_VENV = .venv
PIP_BOOTSTRAP = -m pip install --timeout 60 --retries 5
# The reviewed dependency set the API image installs (ENV-022). Installed
# first, by hash, so a contributor's .venv holds the versions the image holds;
# `.[local]` then adds the development tools, which are exact-pinned in
# pyproject.toml and are not part of the image. It is applied as a first
# install rather than as a constraints file because a constraints file
# carrying hashes puts pip into hash-requiring mode for the whole run, and
# `-e .` has no hash to offer.
API_LOCK = requirements/api.lock.txt

bootstrap-python:
	@set -e; \
	  if python -c 'import sys; sys.exit(0 if sys.prefix != sys.base_prefix else 1)'; then \
	    bootstrap_python=python; \
	    echo "bootstrap: installing into the active virtual environment."; \
	  else \
	    if [ -x '$(BOOTSTRAP_VENV)/bin/python' ]; then \
	      echo "bootstrap: no active virtual environment; reusing $(BOOTSTRAP_VENV)."; \
	    else \
	      echo "bootstrap: no active virtual environment; creating $(BOOTSTRAP_VENV)."; \
	      python -m venv '$(BOOTSTRAP_VENV)'; \
	    fi; \
	    bootstrap_python='$(BOOTSTRAP_VENV)/bin/python'; \
	  fi; \
	  $$bootstrap_python $(PIP_BOOTSTRAP) --upgrade pip; \
	  if [ -f '$(API_LOCK)' ]; then \
	    echo "bootstrap: installing the API dependency set from $(API_LOCK)."; \
	    $$bootstrap_python $(PIP_BOOTSTRAP) --require-hashes -r '$(API_LOCK)'; \
	  fi; \
	  $$bootstrap_python $(PIP_BOOTSTRAP) -e ".[local]"; \
	  if [ "$$bootstrap_python" != python ]; then \
	    echo ""; \
	    echo "bootstrap: activate it before running the checks:"; \
	    echo "    source $(BOOTSTRAP_VENV)/bin/activate"; \
	  fi

# Re-resolve the API dependency set. Review the diff: this is the file that
# decides what the deployed image contains.
lock-api:
	@set -e; \
	  command -v uv >/dev/null 2>&1 || { \
	    echo "lock-api needs uv (https://docs.astral.sh/uv/); install it first."; \
	    exit 1; \
	  }; \
	  python tools/lock/refresh_api_lock.py


bootstrap-web:
	@set -e; \
	  lockfile_sha="$$(python -c "import hashlib, pathlib; print(hashlib.sha256(pathlib.Path('apps/web/package-lock.json').read_bytes()).hexdigest())")"; \
	  if [ -f '$(BOOTSTRAP_WEB_STAMP)' ] && [ "$$(cat '$(BOOTSTRAP_WEB_STAMP)')" = "$$lockfile_sha" ]; then \
	    echo "apps/web/node_modules already matches package-lock.json; skipping npm ci"; \
	  else \
	    npm ci --prefix apps/web; \
	    printf '%s\n' "$$lockfile_sha" > '$(BOOTSTRAP_WEB_STAMP)'; \
	  fi

# The deployment lifecycle, on the host a deployment will actually run on.
# Every rule these share -- which compose and env file a mode uses, which
# services it starts, and the refusal to run `airflow db migrate` against the
# warehouse -- lives in `tools/deployment.py`, which `deploy_stack.ps1` reads
# too. See DEPLOY-008.
#
# MODE selects internal (default) or external; DEPLOY_ARGS passes anything
# else through, so the escape hatches stay available without a target each:
#   make deploy-up
#   make deploy-up MODE=external
#   make deploy-init MODE=external DEPLOY_ARGS=--with-local-airflow
#
# deploy-plan executes nothing. It prints the resolved compose invocation and
# the guard's verdict as JSON, which is the honest way to see what a mode
# would do before doing it.
MODE ?= internal
DEPLOY = python scripts/deploy_stack.py --mode $(MODE) $(DEPLOY_ARGS)

deploy-init:
	$(DEPLOY) --action init

deploy-up:
	$(DEPLOY) --action up

deploy-down:
	$(DEPLOY) --action down

deploy-plan:
	$(DEPLOY) --action all --emit-plan

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

# The live-stack smoke tier: the frontend's own discovery and request
# building against a deployed API, Martin, and proxy, with nothing stubbed.
#
# `--build` is not optional. Compose reuses an image by name, and on
# 2026-09-12 a four-day-old `population-etl-api:smoke` served ACS
# observations under the pre-ARC-005 identity and failed two WEB-027 tests
# against code that had been correct for days.
test-web-smoke:
	@set -e; \
	  trap 'docker compose -f infra/docker/docker-compose.test.yml -f infra/docker/docker-compose.smoke.yml down --volumes --remove-orphans' EXIT; \
	  docker compose -f infra/docker/docker-compose.test.yml -f infra/docker/docker-compose.smoke.yml up --detach --wait --build postgres martin api proxy; \
	  SMOKE_BASE_URL=http://127.0.0.1:33001 SMOKE_REQUIRED=1 \
	  npm --prefix apps/web run test:smoke

# The explorer map checks (WEB-118) against a stack already running with the
# web app, API, and tiles on one origin; the local development stack is
# http://localhost:3001. The composed web-smoke stack serves no web app.
test-web-maps:
	SMOKE_BASE_URL=$${SMOKE_BASE_URL:-http://localhost:3001} SMOKE_REQUIRED=1 	  npm --prefix apps/web run test:smoke -- ../../tests/frontend/smoke/map-display.smoke.test.js
	SMOKE_BASE_URL=$${SMOKE_BASE_URL:-http://localhost:3001} 	  npm --prefix apps/web run test:maps

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
