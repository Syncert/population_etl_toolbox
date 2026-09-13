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
   identifier; API-113 at authoring time).

## Non-goals

- Changing which filters the analysis routes accept.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
