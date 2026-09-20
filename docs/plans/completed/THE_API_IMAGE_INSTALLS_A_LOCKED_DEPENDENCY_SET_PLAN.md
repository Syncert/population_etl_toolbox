---
id: api-dependency-lock
branch: claude/api-dependency-lock
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/tooling tests/unit/deployment -q
  - python -m pip install --dry-run --require-hashes -r requirements/api.lock.txt
  - ruff format --check . ; ruff check .
---

# The API image installs a locked dependency set

## Plan status

- **Status:** Ready for review. The lock exists, installs by hash, is guarded
  three ways, and on 2026-09-18 the image was built from it and the wheel
  installed against it on a machine with a Docker daemon. See "The machine
  run".
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

Everything about the API image is pinned except the Python it runs. The base
image, Redis and PostGIS are pinned by sha256 digest in every Compose file
and `infra/docker/Dockerfile.api:1`; the development tools in
`pyproject.toml` are exact-pinned. The runtime is not:

```toml
api = [
  "fastapi>=0.111,<1.0",
  "redis>=5.0,<6.0",
  "uvicorn[standard]>=0.30,<1.0",
  ...
```

and the core group (`httpx`, `polars`, `psycopg2-binary`, `pydantic`,
`requests`, `tenacity`) is range-pinned the same way; Starlette,
`pydantic-core` and `anyio` are unconstrained transitively. There is no
lock, constraints or requirements file in the repository.
`Dockerfile.api:7` runs `pip install --no-cache-dir -e .[api]` at build time,
and `.github/workflows/package-api.yml` installs the built wheel the same way,
so two builds a week apart can differ in every one of those packages, and a
green CI run does not describe the image an operator builds tomorrow.

This plan makes no claim about vulnerabilities; the audit could not read
advisory feeds. It is about reproducibility of the artifact the deployment
path ships.

## Deliverables

### 1. A hashed lock for the runtime

`requirements/api.lock.txt` (or the equivalent `uv` lock) generated from
`pyproject.toml`'s core and `api` groups on Python 3.11, with hashes. The
Airflow group stays separate: it pins SQLAlchemy 1.4 against the API's 2.x
(`TESTING_CONTRACT.md`, "Airflow and ETL Environment"), and one lock cannot
hold both.

### 2. The image and the package job install from it

`Dockerfile.api` and `package-api.yml` install with the lock as a
constraints file and `--require-hashes`; `pyproject.toml` keeps its ranges
as the contract the lock must satisfy.

### 3. The lock cannot drift silently

A unit test asserts every distribution in the `api` and core groups appears
in the lock at a version inside its declared range, and a scheduled workflow
regenerates the lock and opens a diff for review (never merges it).

### 4. The bootstrap uses it

`make bootstrap-python` installs from the lock when it exists so a
contributor's `.venv` matches the image; `README.md` says so.

## Acceptance criteria

- [x] `pip install --require-hashes -r requirements/api.lock.txt` succeeds on
      Python 3.11 and `pip check` is clean.
- [x] The API image builds from the lock, and `package-api` installs the
      wheel under the same constraints. Both run 2026-09-18; see "The machine
      run". The plan named `deployment-smoke`; that job does not build the API
      image. See below.
- [x] Removing a package from the lock fails the unit test naming it -- and
      so does pinning one outside its declared range, and so does stripping
      one pin's hashes. All three proven.
- [x] The scheduled refresh workflow exists and is named in
      `CI_EVIDENCE_MAP.md` as release-freshness evidence, not a required
      check.
- [x] `TESTING_CONTRACT.md` gains ENV-022.

## Implementation evidence

### The lock records what is already tested

`requirements/api.lock.txt` holds 35 distributions with hashes, resolved by
`uv pip compile` from the core dependencies and the `api` extra on Python
3.11. It is not an upgrade: every version in it matches what this checkout's
`.venv` already has, checked package by package, with one exception --
`async-timeout`, a conditional dependency of `redis` that is not installed
here. So the lock records the set the test suites have been passing against,
which is what "records what resolves today" has to mean if it is to be worth
anything.

### `--require-hashes` is all-or-nothing

pip refuses an unpinned requirement in any run that requires hashes, and an
editable install has no file to hash. That shapes two things:

- The Dockerfile installs in two steps: the locked set by hash, then
  `--no-deps -e .` for the project itself, by which point everything it
  declares is present at the locked version.
- `make bootstrap-python` installs the lock first and then `.[local]`, rather
  than passing the lock as a constraints file. A constraints file carrying
  hashes puts the whole run into hash-requiring mode, which was verified here:
  `pip install -c requirements/api.lock.txt -e ".[local]"` fails with "the
  editable requirement ... cannot be installed when requiring hashes".

### The plan named the wrong job again

Deliverable 2 and the second criterion say the image builds from the lock "in
`deployment-smoke`". It does not build there at all: that job's `api` service
is an nginx stub (`infra/docker/docker-compose.test.yml`). The only job that
builds `Dockerfile.api` is `frontend-smoke`, through
`docker-compose.smoke.yml`. This is the second plan in this session to name
`deployment-smoke` for work that happens in `frontend-smoke`; the name is
misleading and a later plan may want to say so.

The Dockerfile and `package-api.yml` are changed regardless -- both now
install from the lock -- and `package-api` no longer re-resolves every range
after building the wheel, which meant it graded a package against whatever the
index answered that morning rather than against the set the image ships.

### The refresh proposes and never merges

`api-lock-refresh` runs weekly, re-resolves with the same `uv` invocation the
lock's own header names, prints the diff and uploads the proposal as an
artifact, then fails. It opens no pull request: this repository uses
first-party actions only, and adding a third-party one to save a `git apply`
is not a trade this job is worth. `tools/lock/refresh_api_lock.py --check` is
the same comparison a person can run.

`refresh_api_lock.py` exists rather than a bare `uv` line because `uv pip
compile` writes the resolution and nothing else, and the committed lock
carries a header explaining what it is and why Airflow is not in it -- exactly
the part a regeneration would silently drop.

### The machine run

Run on 2026-09-18 on a Windows 11 machine with a Docker daemon.

**The image half.** The build installs
`pip install --no-cache-dir --require-hashes -r requirements/api.lock.txt`
and resolved nothing: every distribution came from the lock and matched its
recorded hash, or the build would have stopped there.

```text
docker build -f infra/docker/Dockerfile.api -t population-etl-api:lock-check .   -> built
docker run --rm population-etl-api:lock-check pip check                          -> No broken requirements found.
docker run --rm population-etl-api:lock-check python -c "import apps.api.main"   -> import ok
```

The same lock also built the `frontend-smoke` API image in this session, which
then served real requests against a real database -- so the locked set is
known to start and answer, not merely to install.

**The packaging half.** `package-api`'s install steps were run against a built
wheel in a clean virtualenv, in `python:3.11-slim` rather than on the Windows
host: the lock pins manylinux-only distributions (`uvloop`, `polars-runtime-32`,
`psycopg2-binary`), so a Windows venv cannot install it and a run there would
have graded a different dependency set than the one CI and the image use.

```text
python -m build --outdir dist/                                   -> data_ingestion_toolbox-0.1.0-py3-none-any.whl
python -m tests.support.package_artifacts --wheel ... --sdist ... -> passed
pip install --require-hashes -r requirements/api.lock.txt         -> installed
pip install --no-deps <wheel>; pip check                          -> No broken requirements found.
python -c "import data_ingestion_toolbox; import apps.api.main"   -> import ok
```

`--no-deps` on the wheel is what makes this a test of the lock: the wheel is
installed *against* the locked set rather than re-resolving `pyproject.toml`'s
ranges, which is the distinction the job's own comment records.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/tooling tests/unit/deployment -q` | 149 passed (was 145) |
| `python -m pip install --dry-run --require-hashes -r requirements/api.lock.txt` | resolves; only `async-timeout` would be added |
| `python tools/lock/refresh_api_lock.py --check` | the committed lock is a current resolution |
| `ruff format --check .` / `ruff check .` | clean, 482 files |
| `python -m pytest tests/unit -q` | 1822 passed (was 1818) |

## Definition of done

Two builds of the API image from the same commit contain the same Python
packages, and the set they contain is a reviewed file in the repository.

## What this plan deliberately does not do

- It does not lock the Airflow environment; that is a separate lock with a
  separate resolver and a separate plan if wanted.
- It does not upgrade any dependency; the first lock records what resolves
  today, and that turned out to be exactly what the suites already run
  against.
- It does not lock the development tools. They are exact-pinned in
  `pyproject.toml` already, they are not in the image, and putting them in
  this lock would make every `ruff` bump a change to the deployment's
  dependency file.
