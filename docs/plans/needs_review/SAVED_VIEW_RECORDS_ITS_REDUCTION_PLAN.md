---
id: saved-view-records-its-reduction
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - newest-release-per-period
parallel_safe: true
complexity: low
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# A saved view records the reduction it was viewed with

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of what a saved configuration can and cannot express.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/schemas/saved_analysis.py`,
  `apps/api/services/saved_analysis_service.py`
- **Depends on:** API-081, whose parameter this makes storable. Satisfied on
  this branch.
- **Next pickup:** none. The client half is filed separately.

## Context

`saved_analysis.py` states what a configuration is for:

> A configuration is the user's own analysis intent — which resource, which
> metric(s), which filters … It is deliberately not a copy of observation
> data: the configuration is replayed against live warehouse publications.

`AnalysisDocument` can record the kind, the metric codes, the scope, a pinned
release, filters, a bin count, and an opaque visualization block. It forbids
extra keys. What it cannot record is either reduction the observations
resource serves:

- `newest_per_geography=true` (API-066), which is how the explorer's map
  colours one value per geography;
- `newest_release_per_period=true` (API-081), which is how a geography's
  settled history is read.

So a saved explorer map view does not replay as the view. Census PEP's latest
publication is every estimated year of the current vintage — 3,144 counties
times six years — and the map asks the resource to reduce it. The saved
document replays as `scope=latest` with filters only, which answers all
18,864 rows; a map drawn from them colours whichever row arrived last per
polygon. That is precisely the failure API-066 exists to prevent, reaching
the screen again through the save-and-reopen path.

The module's own discipline cuts both ways. "Persistence cannot become a back
door for a request the API would refuse" is the half it already enforces; a
configuration that cannot express a request the API *accepts* is the other
half, and it makes the stored intent quietly different from the intent.

## Objective

A saved observations configuration can record either reduction, and cannot
record a contradictory one.

## Acceptance criteria

1. `AnalysisDocument` carries `newest_per_geography` and
   `newest_release_per_period`, both defaulting to false — so every already
   stored document keeps replaying exactly as it does today.
2. `validate_document` refuses the same contradictions the live route
   refuses, with the same reasons: `newest_per_geography` only under
   `scope=latest`; `newest_release_per_period` only under
   `scope=as_released`; never both; and the latter never with a pinned
   `release`.
3. A document is refused at write and re-validated on read, unchanged in
   either case from how every other contradiction in this module behaves.
4. `extra="forbid"` still holds: these are two declared fields, not an open
   door.
5. The reviewed OpenAPI snapshot is regenerated deliberately, and the
   behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Migrating `apps/web` to store or replay them; that is downstream of this
  contract and is filed separately.
- Recording a reduction on comparison or distribution configurations, whose
  routes serve neither.
- Rewriting stored documents. A configuration that predates this is not
  thereby wrong; it says what it said.

## Evidence

### The gap, established first

A parametrised test over six documents failed 3/6 before the fields existed:
the two the live route accepts were rejected as undeclared keys, and the
default-replay test could not read fields that were not there. The other
three — the contradictions — passed from the start, because `extra="forbid"`
already refused an unknown key, and they are kept as the guard that the
fields did not arrive by loosening that.

### What changed

- Two boolean fields on `AnalysisDocument`, both defaulting to false.
- Four refusals in `validate_document`, worded as the live route words them.
- The reviewed snapshot's diff is those two properties on one schema and
  nothing else.

### Why defaults matter here

A stored document is replayed, not migrated. Both fields defaulting to false
means every configuration written before today asks exactly what it asked
before, and a test pins that rather than leaving it to the reader of the
schema.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_saved_analysis.py -q` | 30 passed |
| `pytest -m "unit and api" tests/unit/api -q` | 331 passed |
| `pytest tests/unit -q` | 1396 passed |
| `npm --prefix apps/web run test:unit` | 253 passed |
| `python -m tests.support.catalog_evidence` | 314-row register renders; API-082 is `FULL` |
| `ruff check .` / `ruff format --check .` | clean |

### Not run

`make test-integration` needs PostgreSQL, which this environment has no
Docker daemon for. API-072 owns that the real schema stores and round-trips a
document; this change adds two scalar fields to the JSONB the same statement
already writes, and adds no column.

## Remaining work

None here. The client half — `explorerDocument` records neither field yet —
is filed as `docs/plans/to_do/EXPLORER_SAVED_REDUCTION_PLAN.md`.
