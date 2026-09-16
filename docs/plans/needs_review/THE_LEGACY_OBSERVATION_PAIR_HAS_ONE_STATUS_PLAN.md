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

- **Status:** Ready for review. Authored 2026-09-16 from the codebase audit.
  **Decision taken 2026-09-16 by the repository owner: promote the pair
  and the source-scoped routes to permanent v1 resources.** The retire
  option is recorded below as the path not taken, so a later reader knows
  it was considered.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.

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

- [x] ADR-0002 carries the amendment (`## Amendment (2026-09-16): the
      MVP-shaped observation routes are permanent`), including the note that
      the RFC 8594 mechanism is not implemented and is reinstated by whichever
      plan first retires a route.
- [x] The guide, the registry comment and the service docstring describe
      the routes the same way, and the guide test forbids "retire",
      "deprecated" and "sunset" as descriptions of any served route
      (`test_no_served_route_is_described_as_retiring`, API-141).
- [x] The guide documents `ObservationDashboard`'s duplicate pairs and the
      per-source null-typed fields, and a unit test derives the per-source
      table from `ServingContract.publishes_*` so it cannot drift
      (`test_guide_documents_the_dashboard_row_the_registry_serves`, API-142).
- [x] The OpenAPI snapshot is regenerated once with the `deprecated`
      markers and the digest test passes.
- [x] `TESTING_CONTRACT.md` gains an `API-` row for each of the two tests:
      API-141 and API-142.

## Implementation evidence

### What changed

- `docs/decisions/0002-api-versioning-and-deprecation.md`: the 2026-09-16
  amendment. It records the permanence decision and its reason (the dependant
  already exists and is served correctly, which is the opposite of the
  situation that justified retiring the aliases), states that the RFC 8594
  headers are deliberately not implemented while nothing is retiring, and
  separates documentation from retirement: a `deprecated` marker removes
  nothing and starts no clock.
- The three descriptions now agree. The guide's section is retitled "The
  MVP-shaped observation routes" and says they are permanent; the comment
  above `UNION_NEUTRAL_PATHS` in `apps/api/registry.py` and the
  `CROSS_SOURCE_*_RELATION` docstring in `observations_service.py` both point
  at the amendment and say the same thing.
- `docs/reference/API_CONSUMER_GUIDE.md` gains "Reading an MVP-shaped row"
  beside "Reading a row honestly": the four duplicate pairs with which member
  to read and why, the fifth repetition (`release_date` is `as_of_date`, left
  unmarked because neither spelling is the odd one out), and the per-source
  always-null table.
- `apps/api/schemas/observations.py`: `DASHBOARD_DUPLICATE_FIELDS` states the
  four pairs once, and `source`, `unit`, `dataset` and `vintage` carry
  `deprecated=True` with a description naming the field to read instead.
- `tests/support/openapi_contract.py`: the reviewed digest now carries a
  schema's deprecated property names. Without it, "regenerate the snapshot
  with the markers" was a no-op — the digest reduced a schema to its required
  set and property types, so a marker added or withdrawn would never appear in
  a reviewed diff. The key is emitted only when a schema has one, so the
  regenerated snapshot's diff is six lines, all of them `ObservationDashboard`.

### The two tests

`test_no_served_route_is_described_as_retiring` checks every guide sentence
that names a path the application serves. The first rule tried — the sentence
must also name something that genuinely retires — **passed the original
defective sentence**, because "They retire with the unversioned aliases" names
the aliases, which really did retire. Verified by restoring the sentence and
watching the test stay green. The rule that ships asks what the verb is
attached to: a retirement word must have a retiring noun within two words,
outside a code span. That fails the restored sentence
(`...retire... in \`GET /api/v1/observations/latest\` and
\`/observations/timeseries\` retire with the unversioned aliases`) and passes
the guide's three legitimate uses — a retired geography, retired catalog rows,
and `freshness_state: "retired"` as a value.

`test_guide_documents_the_dashboard_row_the_registry_serves` rebuilds the
always-null set per source from `publishes_seasonal_adjustment` and
`publishes_vintage_and_error` — the same two flags `_source_select_sql`
projects the typed `NULL`s from — and compares it against the guide's table
rows, and asserts each duplicate member is `deprecated` in the schema and each
canonical member is not.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api -q` | 609 passed |
| `python -m pytest tests/unit -q` | 1802 passed |
| `python -m tests.support.regenerate_openapi_contract` | 42 operations, 69 schemas; six-line diff, all `ObservationDashboard` |
| `npm --prefix apps/web run test:unit` | 557 passed, 35 files |
| `ruff format --check . ; ruff check .` | 477 files formatted; all checks passed |

### Scope notes

- The guide's table is scoped to the source-scoped routes, and the one place
  the cross-source pair differs is stated beside it: its union views give a BLS
  row the program code and a FRED row the literal `fred` for
  `dataset_code`/`dataset` rather than `null`
  (`sql/gold_contract/001_gold_contract_views.sql`). That is a fact the
  `publishes_*` flags do not carry, so it is prose rather than a derived row.
- No response body changed, and `apps/web` was not migrated off either
  spelling; its suite passes unchanged.

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
