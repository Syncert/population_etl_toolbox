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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] `pip install --require-hashes -r requirements/api.lock.txt` succeeds
      on Python 3.11 and `pip check` is clean.
- [ ] The API image builds from the lock in `deployment-smoke`, and
      `package-api` installs the wheel under the same constraints.
- [ ] Removing a package from the lock fails the unit test naming it.
- [ ] The scheduled refresh workflow exists and is named in
      `CI_EVIDENCE_MAP.md` as release-freshness evidence, not a required
      check.
- [ ] `TESTING_CONTRACT.md` ENV-001/ENV-005 are extended or an `ENV-` row is
      added.

## Definition of done

Two builds of the API image from the same commit contain the same Python
packages, and the set they contain is a reviewed file in the repository.

## What this plan deliberately does not do

- It does not lock the Airflow environment; that is a separate lock with a
  separate resolver and a separate plan if wanted.
- It does not upgrade any dependency; the first lock records what resolves
  today.
