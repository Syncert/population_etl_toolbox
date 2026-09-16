---
id: legacy-observation-pair-status
branch: claude/legacy-observation-pair-status
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - ruff format --check . ; ruff check .
---

# The legacy observation pair is a permanent v1 resource

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit.
  **Decision taken 2026-09-16 by the repository owner: promote the pair
  and the source-scoped routes to permanent v1 resources.** The retire
  option is recorded below as the path not taken, so a later reader knows
  it was considered.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`GET /api/v1/observations/latest` and `/observations/timeseries` are described
three different ways:

- `docs/reference/API_CONSUMER_GUIDE.md`, "Legacy observation routes": "They
  retire with the unversioned aliases." The unversioned aliases were retired
  in API-008 (`apps/api/versioning.py`), so by this sentence the pair is
  already gone. It is not.
- `apps/api/registry.py` (around the cross-source union views): "which still
  back the legacy latest/timeseries pair until API-008 retires it".
- `apps/api/services/observations_service.py`: "API-008 retired the
  unversioned prefix aliases, not these resources".

ADR-0002 defines the retirement mechanism: a retiring response carries
`Deprecation: true`, a `Sunset` date and a `Link: ...; rel="successor-version"`
header (RFC 8594), so "a consumer learns it is on a retiring path from a
response it was already reading". That code was removed with the aliases;
`Deprecation` and `Sunset` now appear in `apps/api` only in a comment, and
`tests/unit/api/test_consumer_guide.py` (`test_guide_describes_one_versioned_surface`)
forbids the words in the guide. The API therefore has no way to retire any
route, which is the opposite of what the ADR promises.

The web still depends on the pair and on the generated source-scoped pair
(`apps/web/lib/api/client.ts`, `apps/web/lib/observationAccess.ts`). Those
ten operations serve `ObservationDashboard`
(`apps/api/schemas/observations.py`), a row of 32 all-optional fields with
four duplicate pairs (`source`/`source_code`, `units`/`unit`,
`dataset`/`dataset_code`, `vintage`/`vintage_year`), while the neutral
`NeutralObservation` is typed and structured. The guide's "Reading a row
honestly" section and the nullability plan describe the neutral row; the
dashboard row gets one line.

## Deliverables

### 1. The decision, recorded

An amendment to ADR-0002 stating that `/observations/{latest,timeseries}`
and the source-scoped `/{bls,census,fred,pep}/observations/{latest,timeseries}`
routes are permanent v1 resources: they are the routes the web consumes,
the neutral resource is additive beside them, and nothing retires with the
aliases. The amendment also notes that the RFC 8594 header mechanism is
not currently implemented and is to be reinstated by whichever plan first
retires a route, not before.

### 2. One description

Rewrite the guide's "Legacy observation routes" section (drop "They retire
with the unversioned aliases"; call them the MVP-shaped routes and say what
they answer), the registry comment near the cross-source union views, and
the `observations_service.py` docstring, so all three say the same thing. A
guide test asserts no route is described as retiring, deprecated or
sunsetting anywhere in the guide, so the contradiction cannot return
without a reviewed change to the test.

### 3. The dashboard row is documented

Document `ObservationDashboard` in the guide beside "Reading a row
honestly": its four duplicate pairs (`source`/`source_code`, `units`/`unit`,
`dataset`/`dataset_code`, `vintage`/`vintage_year`), which member of each
pair a client should read, and which fields are typed null per source (the
registry already knows through `ServingContract.publishes_*`, so derive the
table from it in a test rather than typing it). Mark the secondary member
of each pair `deprecated: true` in the Pydantic schema so the OpenAPI
document carries the same guidance; regenerate the reviewed snapshot once
for those markers.

### The path not taken

Retiring the pair would reinstate the RFC 8594 headers per ADR-0002 as a
router-level dependency keyed on a `DEPRECATED_PATHS` constant, publish a
sunset date, and migrate `apps/web` to `/observations` with
`newest_per_geography` and `scope=as_released`. It was not chosen because
the web is the only consumer and the neutral resource serves the same
rows; the cost was a client migration and fixture regeneration with no
consumer asking for it.

## Acceptance criteria

- [ ] ADR-0002 carries the amendment.
- [ ] The guide, the registry comment and the service docstring describe
      the routes the same way, and the guide test forbids "retire",
      "deprecated" and "sunset" as descriptions of any served route.
- [ ] The guide documents `ObservationDashboard`'s duplicate pairs and the
      per-source null-typed fields, and a unit test derives the per-source
      table from `ServingContract.publishes_*` so it cannot drift.
- [ ] The OpenAPI snapshot is regenerated once with the `deprecated`
      markers and the digest test passes.
- [ ] `TESTING_CONTRACT.md` gains an `API-` row for each of the two tests.

## Definition of done

A client reading the guide or the OpenAPI document learns that the pair is
permanent, which field of each duplicate pair to read, and which fields a
given source leaves null, and none of the three places that describe the
routes can disagree again.

## What this plan deliberately does not do

- It does not change the neutral `/observations` resource or any response
  body; the `deprecated` markers are documentation in the schema, not a
  removal.
- It does not reinstate the RFC 8594 headers; nothing is retiring.
