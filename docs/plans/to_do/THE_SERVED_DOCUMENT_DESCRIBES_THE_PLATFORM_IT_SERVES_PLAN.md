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

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

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

- [ ] The served description names all seven sources and updates when a
      source is added to the registry (test adds a fake registry entry).
- [ ] `/health/ready` reports `storage` from a real probe: `ok` on the
      integration stack, `unavailable` when `APP_API_DATABASE_URL` points at
      a closed port, `unconfigured` when unset, and `status` is unchanged in
      all three.
- [ ] Adding an undocumented route to the app fails the guide test naming
      the path (proven failing-first with a throwaway route).
- [ ] `TESTING_CONTRACT.md` gains `API-` rows for the three behaviours.

## Definition of done

The API's own front door, its readiness answer and its route inventory each
describe what is actually served, and a regression in any of them is a
failing unit test.

## What this plan deliberately does not do

- It does not gate readiness on storage or Redis; the stated policy that the
  API survives without both stands.
- It does not add schema examples to the OpenAPI document.
