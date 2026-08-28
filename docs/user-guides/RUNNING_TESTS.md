# Running the Test Suite

Run commands from the repository root. The authoritative catalog, current
results, markers, and CI ownership are in
[`docs/reference/TESTING_CONTRACT.md`](../reference/TESTING_CONTRACT.md).

## Quick Start

Use Python 3.11 for supported local and CI-compatible results:

```bash
python -m venv .venv-test
# Linux/macOS: source .venv-test/bin/activate
# Windows PowerShell: .\.venv-test\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[api,dev]"
python -m pip check
python -m pytest
```

Plain `pytest` runs the deterministic suite only. It requires no Docker,
database, Redis, credentials, or external network access. To run one file or
test, pass its node directly:

```bash
python -m pytest tests/unit/bls/test_national_routing.py -q
python -m pytest tests/unit/bls/test_national_routing.py::test_laus_national_area_is_rejected -q
```

## Tier Commands

Linux/macOS uses `make`; Windows PowerShell uses the equivalent tier name with
`tests/run.ps1`.

| Test tier | Linux/macOS | Windows PowerShell |
|---|---|---|
| All deterministic units | `make test-unit` | `.\tests\run.ps1 unit` |
| ETL units | `make test-etl` | `.\tests\run.ps1 etl` |
| API units | `make test-api` | `.\tests\run.ps1 api` |
| Airflow DAGs | `make test-dags` | `.\tests\run.ps1 dags` |
| Orchestrated DAG execution | `make test-dag-pipeline` | `.\tests\run.ps1 dag-pipeline` |
| Integration | `make test-integration` | `.\tests\run.ps1 integration` |
| External contracts | `make test-external` | `.\tests\run.ps1 external` |
| End-to-end | `make test-e2e` | `.\tests\run.ps1 e2e` |
| Martin units | `make test-martin-unit` | `.\tests\run.ps1 martin-unit` |
| Martin live stack | `make test-martin-integration` | `.\tests\run.ps1 martin-integration` |
| Performance | `make test-performance` | `.\tests\run.ps1 performance` |
| Resilience | `make test-resilience` | `.\tests\run.ps1 resilience` |
| Frontend units | `make test-web-unit` | `.\tests\run.ps1 web-unit` |
| Frontend browser | `make test-web-browser` | `.\tests\run.ps1 web-browser` |
| Frontend lint/build | `make test-web-build` | `.\tests\run.ps1 web-build` |
| Compose smoke | `make test-compose-smoke` | `.\tests\run.ps1 compose-smoke` |

`martin-integration` and `compose-smoke` start the pinned disposable Compose
services and remove their containers, network, and test-only volumes when the
run finishes, including after a failure.

## Airflow DAG Tests

Airflow has dependencies incompatible with the API environment. Use a separate
Python 3.11 virtual environment:

```bash
python -m venv .venv-airflow-test
# Activate the environment, then:
python -m pip install -e ".[airflow-dev]"
python -m pip check
make test-dags
```

On native Windows, use the pinned scheduler image or WSL2 for the authoritative
Airflow run. The scheduler workflow runs the same DAG tier in Linux.

## Orchestrated DAG Execution

`make test-dags` proves DAG *shape* — task ordering, pools, import side effects,
parse budget. It does not run anything.

`make test-dag-pipeline` runs every DAG in `dags/` as a real Airflow `DagRun`
against the disposable PostGIS warehouse, driving a bounded reviewed provider
sample from capture through replay to publication:

```bash
make test-dag-pipeline
```

```powershell
.\tests\run.ps1 dag-pipeline
```

Only the provider HTTP boundary is replaced. Airflow, the operators, the
`public_data` connection, the provider pools, the capture-control plane, and
every warehouse write are real, so a failure here is a genuine orchestration
defect rather than a mocking artifact. This is the closest automated equivalent
of a first production Airflow run, and it is the required evidence for the
four-source review gate in `docs/plans/gates/`.

The suite also asserts that the set of DAGs it executes equals the set in the
DagBag, so a newly added pipeline cannot be silently left uncovered.

## Database and Redis Tests

General integration, E2E, performance, and resilience tiers expect explicitly
configured disposable services. The checked-in stack publishes PostGIS on
`55432` and Redis database 15 on `56379`:

```powershell
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres redis

$env:TEST_POSTGRES_HOST = "127.0.0.1"
$env:TEST_POSTGRES_PORT = "55432"
$env:TEST_POSTGRES_USER = "population_test"
$env:TEST_POSTGRES_PASSWORD = "population_test"
$env:TEST_POSTGRES_DATABASE = "population_etl_test"
$env:TEST_REDIS_URL = "redis://127.0.0.1:56379/15"

.\tests\run.ps1 integration

docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
```

Safety guards reject a non-loopback Redis URL, Redis credentials, the default
Redis database, incomplete PostgreSQL settings, or a database name that does
not end in `_test`.

## External and Frontend Tests

External tests make bounded live requests. Census and FRED require
`CENSUS_API_KEY` and `FRED_API_KEY`; BLS uses `BLS_API_KEY` when available.
Missing required keys are reported as named skips only in runners that permit a
partial local run. The scheduled GitHub Actions runner requires all three keys
before it collects live contract evidence. Never commit credentials.

For frontend tests:

```bash
npm --prefix apps/web ci
npm --prefix apps/web exec -- playwright install chromium
make test-web-unit
make test-web-browser
make test-web-build
```

## Reading Results

- A pass means every selected test completed successfully.
- A skip is acceptable only when it is explicitly named in the testing plan,
  such as the opt-in million-row performance profile.
- An unexpected skip or xfail should be treated as incomplete validation.
- Host warnings from native-Windows Airflow, the installed Starlette test
  client, or a pre-existing unwritable `.pytest_cache` do not represent
  application test failures; application-owned deprecation and resource
  warnings are configured as errors.

For failures, rerun the failing node with `-vv -s`, correct the behavior, and
then rerun its complete tier followed by plain `python -m pytest`.
