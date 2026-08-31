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
| Linux container runner | `make test-linux` | `.\tests\run.ps1 linux` |

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

### Native Windows

Airflow refuses to initialize outside a POSIX-compliant OS, so on a Windows
checkout the `dags` and `dag-pipeline` tiers fail before reaching an assertion,
as does any other module that imports `airflow` while collecting - notably
`tests/integration/database/test_usda_nass_dag_tasks.py`, which drops out of a
host-run `tests/integration/database` selection as a collection error.

The `linux` tier runs pytest inside the pinned Airflow 2.9.3 + Python 3.11
scheduler image against the disposable PostGIS service, which is what CI
grades. It defaults to the DAG tier and takes pytest arguments for anything
else:

```powershell
.\tests\run.ps1 linux
.\tests\run.ps1 linux -m "integration and database and not slow" tests/integration/database
```

```bash
make test-linux
make test-linux TEST_ARGS='-m "integration and database and not slow" tests/integration/database'
```

Sources are mounted read-only into the container, so an edit needs no rebuild.
Compose builds the image on first use; after a dependency change rebuild it
with `make test-linux-build`, or:

```bash
docker compose -f infra/docker/docker-compose.test.yml -f infra/docker/docker-compose.pytest.yml build pytest
```

Each invocation tears the database down. CI grades these suites in separate
jobs against separate databases, so reusing one database across two suites
reports failures CI will not reproduce: the second suite reads the first one's
committed rows and, for example, sees an already-ingested release as
`unchanged`.

WSL2 remains a working alternative for an interactive Airflow session.

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
of a first production Airflow run. It was the required evidence for the
four-source review gate, retired on 2026-08-28 and archived at
[`docs/plans/completed/FOUR_SOURCE_REVIEW_GATE.md`](../plans/completed/FOUR_SOURCE_REVIEW_GATE.md).
The `dag-parse` job selects the same module as part of the DAG tier, so this
evidence is produced on every push.

The suite also asserts that the set of DAGs it executes equals the set in the
DagBag, so a newly added pipeline cannot be silently left uncovered.

## Data-Product End-to-End Coverage

`tests/support/product_coverage.py` is the executable inventory of every
implemented data product and the single test node that owns its end-to-end
evidence. `tests/unit/shared/test_data_product_e2e_coverage.py` derives what
exists from the warehouse quality inventory and the served OpenAPI paths, so a
source that lands a publisher schema or an API router without an owner fails a
deterministic unit test rather than passing under broad marker selection.

List the owning nodes, for a targeted run or a CI selection:

```powershell
python -m tests.support.product_coverage
```

Set `E2E_REQUIRE_ALL_PRODUCTS=1` to grade a run against that inventory. The
session then fails unless every registered product ran and passed, so an
unexpected skip or deselection is a failure rather than a quietly shorter run.
The scheduled `e2e-performance` workflow sets it and publishes
`junit-e2e-products.xml`, whose test cases name the products individually.

Every product node commits real warehouse rows and removes them itself; a
session guard in `tests/e2e/conftest.py` independently reconciles the capture,
control, silver, reference, publisher, and glossary relations before and after
the run. Start each run against a freshly created database anyway -- that is
what CI grades, and it is the only way a count assertion means what it says:

```powershell
docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres
.	estsun.ps1 e2e
```

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

## External Source Contract Tests

External tests make bounded live requests against the real providers. Every
source that needs a credential owns one, and every source is covered: a source
missing from this tier drops out of live coverage silently, which is why
`tests/support/external.py::REQUIRED_SCHEDULED_CREDENTIALS` fails a scheduled
run that is missing any of them rather than skipping it.

### Source credentials

| Source | Variable | Where to register | Provider requires it? |
|---|---|---|---|
| Census ACS | `CENSUS_API_KEY` | <https://api.census.gov/data/key_signup.html> | Yes |
| Census PEP | none | — | No; PEP uses credential-free bulk transport |
| BLS | `BLS_API_KEY` | <https://data.bls.gov/registrationEngine/> | No; sent as `registrationkey` when present, raising the quota |
| FRED | `FRED_API_KEY` | <https://fredaccount.stlouisfed.org/apikeys> | Yes |
| CDC | `CDC_SOCRATA_APP_TOKEN` | <https://data.cdc.gov/profile/edit/developer_settings> | No; anonymous reads work, the token raises Socrata rate limits |
| FBI UCR | `FBI_CDE_API_KEY` | <https://api.data.gov/signup/> — the CDE API is served through api.data.gov | Yes |
| USDA NASS | `USDA_NASS_API_KEY` | <https://quickstats.nass.usda.gov/api/> | Yes |

"Provider requires it" and "the scheduled run requires it" are different
questions. `REQUIRED_SCHEDULED_CREDENTIALS` holds `CENSUS_API_KEY`,
`BLS_API_KEY`, `FRED_API_KEY`, `FBI_CDE_API_KEY`, and `USDA_NASS_API_KEY`: BLS
is optional to the provider but required of a scheduled run, because an
unkeyed BLS run collects weaker evidence than the tier claims to produce.
`CDC_SOCRATA_APP_TOKEN` is not on that list, since CDC coverage is complete
without it.

Never commit a credential. Only an empty placeholder belongs in a tracked
environment example; the real value belongs in an ignored local environment or
a deployment secret store.

Both provider-required keys are validated for shape at request execution and
rejected if they carry surrounding whitespace, so a trailing newline from a
copy-paste fails as `invalid_api_key` rather than as a network error.

### Running one source

Two flags are required and neither is optional:

- `RUN_EXTERNAL_TESTS=1`, because `tests/conftest.py` adds `external` to
  `collect_ignore` otherwise and the directory is never collected.
- `-m external` on the command line, because the `addopts` marker filter in
  `pyproject.toml` deselects `external` and `slow` by default. A command-line
  `-m` replaces that filter rather than intersecting with it.

```powershell
$env:FBI_CDE_API_KEY   = "your-api-data-gov-key"
$env:USDA_NASS_API_KEY = "your-quickstats-key"
$env:RUN_EXTERNAL_TESTS = "1"

python -m pytest -m external `
    tests/external/test_fbi_source_contracts.py `
    tests/external/test_nass_source_contracts.py `
    -v -ra

Remove-Item Env:RUN_EXTERNAL_TESTS
```

```bash
RUN_EXTERNAL_TESTS=1 python -m pytest -m external \
    tests/external/test_fbi_source_contracts.py \
    tests/external/test_nass_source_contracts.py \
    -v -ra
```

Each module mixes live and deterministic tests. The deterministic ones — outage
classification, missing-credential refusal, and credential non-leakage — pass
with no key at all, so a run with no credentials configured is not a smoke test
of the live contract.

Prefer the direct `pytest` invocation above when checking one source. The
`external` tier also runs `tests/integration/database/legacy`, which needs the
disposable PostGIS container from *Database and Redis Tests* to be up.

### Reading an external result

- **Skipped, naming the variable.** The credential did not reach the process.
  Most often the variable was set in a different shell than the one running
  pytest.
- **`failure_class=upstream-unavailable` in the log line.** A provider 429,
  5xx, or timeout. Not a code failure; re-run later. Every live request is
  wrapped in `observe_external_call`, which logs `source`, `status`,
  `latency_seconds`, and `failure_class` without ever emitting a credential.
- **`failure_class=contract-regression`, or an assertion failure.** This is the
  real signal: something the registry froze no longer matches what the provider
  publishes. Each assertion names the exact value that drifted — a retired ORI,
  a period window the provider no longer covers, a classification selection
  that vanished from the provider's own domain, or a registered partition that
  outgrew the provider's record ceiling.

### The scheduled run

`external-contract` runs daily and on manual dispatch, and is never a
pull-request gate. To dispatch it, add each variable above as a repository
secret under **Settings -> Secrets and variables -> Actions**, using the exact
variable name, then use **Actions -> external-contract -> Run workflow**.

Its first step is `python -m tests.support.external`, which fails the run when
a required credential is absent and names the missing variables without
printing values.

## Frontend Tests

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
