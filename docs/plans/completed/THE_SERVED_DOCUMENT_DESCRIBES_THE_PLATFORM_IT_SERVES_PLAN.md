---
id: served-document-describes-the-platform
branch: claude/served-document-describes-the-platform
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_consumer_guide.py tests/unit/api/test_operational_hardening.py -q
  - ruff format --check . ; ruff check .
---

# The served document describes the platform it serves

## Plan status

- **Status:** Ready for review. All three deliverables are done, and the `ok`
  case was run on 2026-09-18 against a real application database provisioned
  by `scripts/provision_app_api.py --apply-schema`, reached by the built API
  image over `apps.api.appdb`. All four storage states were observed on that
  stack; see "The machine run".
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

Three small self-descriptions have fallen behind the platform, and the tests
that guard the surface look in only one direction.

**The OpenAPI description names three sources.**
`src/data_ingestion_toolbox/config.py:19-22` defaults `API_DESCRIPTION` to
"REST API for Census ACS, BLS, and FRED population data.", which
`apps/api/main.py` serves as `info.description` on `/openapi.json` and
`/docs`; `pyproject.toml:7` says "ETL toolbox for Census ACS, BLS, and FRED
data pipelines". Seven sources are registered in `apps/api/registry.py`. The
completed `front-door-describes-the-platform` plan fixed `README.md` and
`AGENTS.md` and did not reach these two strings, and the contract digest
deliberately drops descriptions (`tests/support/openapi_contract.py`), so no
test can catch it.

**Readiness ignores application storage and reports the cache from
settings.** `apps/api/routers/health.py:66-89` runs `SELECT 1` on the
warehouse engine and sets `cache = "configured" if get_settings().redis_url
else "disabled"` without contacting Redis; `APP_API_DATABASE_URL` is not
probed. With application storage unreachable the private routes answer
`503` while `/health/ready` says `ready`.

**The route inventory test is one-directional.**
`tests/unit/api/test_consumer_guide.py::test_every_documented_route_is_actually_served`
asserts documented ⊆ served; nothing asserts served ⊆ documented. Today all
42 operations are documented, so this is cheap insurance: the self-service
accounts plan adds routers, and one could ship undocumented without a red
test.

## Deliverables

### 1. The description is derived, not typed

`info.description` is built from the registry's source display names (one
source of truth), with `API_DESCRIPTION` kept as an operator override;
`pyproject.toml`'s description names the platform rather than three sources.
A unit test asserts `app.openapi()["info"]["description"]` names every
registered source's display name.

### 2. Readiness reports storage

An additive `storage: "ok" | "unavailable" | "unconfigured"` on
`ReadinessResponse` (`apps/api/schemas/health.py`), probed through
`apps.api.appdb` when configured. Per ADR-0003 storage is optional, so it is
reported and never gates `status`. Optionally `cache` gains a `ping` result
beside `configured`; if it does, it also never gates. The reviewed OpenAPI
snapshot is regenerated once for the new field, and the guide's readiness
paragraph names it.

### 3. The inventory test runs both ways

A second assertion in `test_consumer_guide.py`: every served path under
`/api/v1` appears in the guide's route inventory.

## Acceptance criteria

- [x] The served description names all seven sources and updates when a
      source is added to the registry (the test adds a fake registry entry
      and reads the description change).
- [x] `/health/ready` reports `storage` from a real probe: `unavailable` when
      the URL points at a closed port, `unconfigured` when unset, `status`
      unchanged in all three -- all asserted. `ok` was run on 2026-09-18
      against a real application database rather than a stand-in engine; see
      "The machine run".
- [x] Adding an undocumented route to the app fails the guide test naming the
      path (proven failing-first with a throwaway route).
- [x] `TESTING_CONTRACT.md` gains API-144, API-145 and API-146.

## Implementation evidence

### The description is derived

`registry.platform_description()` builds `info.description` from
`SOURCE_DISCOVERY`'s display names and their count, and `create_app` uses it
unless an operator set `API_DESCRIPTION` -- whose default is now empty, so the
sentence exists once. The test that guards it adds a source to the registry
and reads the description change, rather than comparing the string to a second
copy of itself, which is what a test of a typed default would amount to.

The layering runs the right way: the setting stays in
`src/data_ingestion_toolbox/config.py` and knows nothing about the API, while
`apps/api` -- which already imports the registry -- composes the sentence.
`pyproject.toml`'s description now names the platform instead of three
sources.

### Readiness reports storage

`storage` is additive on `ReadinessResponse` and defaults to `unconfigured`,
so the reviewed snapshot's diff is one line and no existing client breaks. It
is probed: `app_storage_state()` opens a connection and runs `SELECT 1`, and
a test asserts the connection is actually attempted rather than the setting
merely read -- a configured URL pointing at a closed port is precisely the
state that used to answer `ready` while every private route answered `503`.

It never gates `status`. Storage is optional by ADR-0003: without it the
public API is complete and the saved-analysis routes answer an explicit 503,
so failing readiness here would take a deployment out of rotation over a
feature it was never configured to offer. `cache` was left alone: the plan
offered a `ping` as optional, and a readiness probe that opens a Redis
connection on every call is a cost paid on the hot path of orchestration for
a field nothing gates on.

### The inventory test found two routes on its first run

The plan expected this to pass on arrival: "today all 42 operations are
documented". They were not.

- **`/api/v1/health`** was named in neither document. The guide described the
  unversioned probes and `/api/v1/health/content` and skipped the versioned
  liveness resource entirely.
- **`/api/v1/observations/timeseries`** was written as
  "`GET /api/v1/observations/latest` and `/observations/timeseries`" -- so
  not only did the extraction miss it, a reader searching the guide for the
  path they were about to call would not have found it either.

Both are documented now, and the readiness paragraph the plan asks for names
`storage` while it is there. The assertion was then proven failing-first with
a throwaway route, which it names in the failure message.

### The machine run

Run on 2026-09-18 on a Windows 11 machine with a Docker daemon, against the
stack `frontend-smoke.yml` composes -- the built API image, not the nginx
stub -- with an application database provisioned into the disposable test
database, which `sql/bootstrap/002_app_api.sql` explicitly supports:

```bash
ANALYTICS_DB_HOST=127.0.0.1 ANALYTICS_DB_PORT=55432 \
  ANALYTICS_DB_USER=population_test ANALYTICS_DB_PASSWORD=population_test \
  ANALYTICS_DB_NAME=population_etl_test APP_API_DB_PASSWORD=... \
  python scripts/provision_app_api.py --apply-schema
```

`APP_API_DATABASE_URL` was then varied on the `api` service through throwaway
override files, and `/health/ready` read after each recreate. All four states,
on one running deployment:

| `APP_API_DATABASE_URL` | `storage` | `status` | `database` |
|---|---|---|---|
| unset (the smoke file's own default) | `unconfigured` | `ready` | `ok` |
| the provisioned database | `ok` | `ready` | `ok` |
| right host, closed port `5599` | `unavailable` | `ready` | `ok` |
| right host and port, wrong password | `unavailable` | `ready` | `ok` |

The last row is the one worth keeping. A closed port only proves the probe
attempts a connection; a reachable server that *refuses the credential* proves
`ok` means an authenticated session that ran its `SELECT 1`, which is the
claim the unit tier could only make against a stand-in engine. And `status`
stays `ready` in all four, so storage is reported and never gates -- ADR-0003's
rule, observed on a deployment rather than asserted in a fixture.

**One deviation from the plan's instructions, deliberate.** It says "stop the
application database" for the `unavailable` case. On this stack the
application database and the warehouse are the same container, so stopping it
would have taken `database` down with it and proved nothing about whether
`storage` gates `status`. Pointing the URL at a closed port and then at a
refused credential isolates the two, which is what the criterion is actually
about.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api/test_consumer_guide.py tests/unit/api/test_operational_hardening.py -q` | 70 passed (was 61) |
| `python -m pytest tests/unit/api -q` | 620 passed |
| `ruff format --check .` / `ruff check .` | clean, 480 files |
| `python -m pytest tests/unit -q` | 1818 passed |

The reviewed OpenAPI snapshot was regenerated once, for one line: `storage`
added to `ReadinessResponse`'s properties and not to its `required` list.

Re-run on the machine session of 2026-09-18:

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api/test_consumer_guide.py tests/unit/api/test_operational_hardening.py -q` | 70 passed |
| `ruff format --check .` / `ruff check .` | clean, 493 files |

## Definition of done

The API's own front door, its readiness answer and its route inventory each
describe what is actually served, and a regression in any of them is a
failing unit test.

## What this plan deliberately does not do

- It does not gate readiness on storage or Redis; the stated policy that the
  API survives without both stands.
- It does not add schema examples to the OpenAPI document.
- It does not add the optional Redis `ping` to readiness. Orchestration calls
  this route on a schedule, and opening a Redis connection each time is a
  standing cost for a field that gates nothing.
