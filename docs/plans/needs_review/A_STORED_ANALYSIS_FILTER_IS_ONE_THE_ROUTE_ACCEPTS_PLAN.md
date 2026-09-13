---
id: a-stored-analysis-filter-is-one-the-route-accepts
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-retired-measure-is-not-served-as-current]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api/test_saved_analysis.py tests/unit/api/test_evidence_packets.py tests/unit/api/test_distribution.py tests/unit/api/test_comparison.py -q
---

# A stored distribution or comparison names only filters its live route accepts

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; the sibling of API-091 and API-105 for the analysis kinds.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/saved_analysis_service.py`

## Context

The guide: "Documents are validated on write against the same capability
and compatibility contracts above, so a saved configuration cannot encode a
request the API would refuse." API-091 made that true for filter *names*
and API-105 for filter *values*, for the observations kind.

For `kind="distribution"` and `kind="comparison"` the validator unions
`_ANALYSIS_UNIVERSAL_FILTERS = {"geo_level", "state_fips"}` into the
accepted set (`saved_analysis_service.py:44, 150, 163`). The live routes
union nothing: `distribution_service.py` and `comparison_service.py` reject
any name outside `dispatch.filter_conditions`, and Census PEP declares no
`state_fips` -- deliberately, and tested (`test_distribution.py:403`).

## Findings

- `POST {"kind":"distribution","metric_code":"CENSUS_PEP:...","filters":{"state_fips":"06"}}`
  is 201 and valid; its replay is 422 "filters not supported for source
  'CENSUS_PEP': state_fips".
- The mirror gap: the accepted set admits filters that are not parameters
  of the analysis routes at all (`geo_id`, `year_from`, `county_fips` for
  an ACS distribution), and their replay is 422 from the strict-parameter
  guard (API-093).
- `test_saved_analysis.py::test_every_filter_a_source_declares_has_a_bound`
  seeds the widened set before comparing, so it asserts the widening.

## Acceptance criteria

1. The accepted filter set for an analysis kind is derived from the same
   declaration the live route reads -- the route's query parameters
   intersected with the source's `filter_conditions` -- with no
   hand-written universal set.
2. Failing-first tests: the PEP `state_fips` case and the ACS `year_from`
   case are refused on write with the route's own wording; a stored
   document from before the change is reported invalid on read.
3. The widened-set test is corrected to read the declaration.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-114 at authoring time).

## Non-goals

- Changing which filters the analysis routes accept.

## What changed

- `CONFIGURATION_FILTER_PARAMETERS` in the reviewed registry names the
  filters each kind's route takes, beside `CONFIGURATION_ROUTES` and
  `CONFIGURATION_DOCUMENT_FIELDS` (API-112). `observations` is `None` —
  "narrow nothing" — because `/observations` declares one query parameter
  per filter in the union of every source's declared set.
- `_require_declared_filters` takes the kind rather than an `allowed_extra`
  set, and makes two checks instead of one union: a filter the route has no
  parameter for is refused naming the route and what it accepts, and a
  filter the route takes that the source does not declare is refused in the
  live route's own words. The hand-written `_ANALYSIS_UNIVERSAL_FILTERS`
  has no readers left and is gone.
- The guard reads the served document in both directions: for each kind, the
  route's query parameters intersected with every declared filter name must
  equal the registry's set (or the whole set, for `observations`).

## Validation

- `pytest tests/unit/api/test_saved_analysis.py` — **72 passed** (67
  before: +5).
- **The tests fail on the union.** Restoring
  `set(dispatch.supported_filters()) | set(accepted_by_route or ())` leaves
  `4 failed, 68 passed` — all four parametrised cases.
  `saved_analysis_service.py` was restored byte-for-byte afterwards.
- Read off the validator before and after, with the metric resolution
  stubbed:

```
refused  PEP distribution + state_fips: filters not supported for source
         'CENSUS_PEP': state_fips; supported filters: geo_level
refused  ACS distribution + year_from: filters not accepted by
         /api/v1/distribution/bins: year_from; it accepts: geo_level, state_fips
refused  ACS comparison + geo_id: filters not accepted by /api/v1/comparison
ACCEPTED ACS distribution + state_fips
ACCEPTED PEP observations + geo_id
```

- `test_every_filter_a_source_declares_has_a_bound` no longer seeds
  `{geo_level, state_fips}`: it reads the sources' declarations alone, which
  is what it always claimed to do. Both names are declared by sources, so
  nothing is lost by asking.
- `ruff check .` / `ruff format --check .` — clean.
- `python -m tests.support.catalog_evidence` renders API-117 `FULL`.

## Remaining work

- None. Review is the remaining step.
