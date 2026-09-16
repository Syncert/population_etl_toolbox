---
id: legacy-observation-pair-status
branch: claude/legacy-observation-pair-status
depends_on: []
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - ruff format --check . ; ruff check .
---

# The legacy observation pair has one status, and the API can retire a route

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit. The
  first deliverable is a decision; the rest depends on which way it goes.
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

### 1. The decision

Choose one, record it in ADR-0002 as an amendment, and in this plan:

- **(a) Promote.** The pair and the source-scoped routes are permanent v1
  resources. Rewrite the guide section, the registry comment and the
  service docstring to say so; document `ObservationDashboard`'s duplicate
  pairs and which fields are typed null per source (the registry already
  knows through `ServingContract.publishes_*`); mark the duplicate members
  `deprecated: true` in the schema so the OpenAPI document says which name
  to read.
- **(b) Retire.** Reinstate the RFC 8594 mechanism as a small router-level
  dependency keyed on a `DEPRECATED_PATHS` constant in `versioning.py`,
  emit the three headers on exactly those routes outside the response
  cache (per ADR-0002), publish the sunset date, and migrate `apps/web` to
  `/observations` with `newest_per_geography` and `scope=as_released`,
  which the neutral resource already answers.

### 2. Under either: one description

A guide test asserts that no route is described as retiring unless it is
in `DEPRECATED_PATHS`, and that every path in `DEPRECATED_PATHS` carries the
headers (so the guide, the constant and the response cannot disagree).

### 3. Under (b): the web reads the successor

`client.ts` loses the four legacy helpers, `observationAccess.ts` builds its
requests against the neutral resource, and the served-contract fixtures
under `tests/fixtures/api` are regenerated from the neutral shape.

## Acceptance criteria

- [ ] ADR-0002 records the decision.
- [ ] The three descriptions agree with each other and with the response
      headers, and a test proves it.
- [ ] Under (a): the OpenAPI snapshot is regenerated once with the
      `deprecated` field markers and the guide documents the dashboard row.
- [ ] Under (b): the pair answers with `Deprecation`, `Sunset` and `Link`
      headers, no other route does, the headers are absent from the cache
      digest, and the web browser suite passes on the successor resource.
- [ ] `TESTING_CONTRACT.md` gains the `API-` rows the choice needs.

## Definition of done

A client reading the guide, the OpenAPI document or a response gets the
same answer about whether the legacy pair will still exist next year, and
the API has a working way to say so about any route.

## What this plan deliberately does not do

- It does not change the neutral `/observations` resource.
- It does not remove the source-scoped routes under (b) in the same change;
  they get the same headers and the same sunset, and their removal is the
  follow-on the sunset date names.
