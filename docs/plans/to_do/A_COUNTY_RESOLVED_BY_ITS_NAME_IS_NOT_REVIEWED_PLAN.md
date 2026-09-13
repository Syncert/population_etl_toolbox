---
id: a-county-resolved-by-its-name-is-not-reviewed
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/fbi_ucr -q
  - python -m pytest tests/integration/database/test_fbi_ucr_pipeline.py -m "integration and database" -q
---

# A county resolved from a provider label is not a reviewed resolution

## Plan status

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  divergence from an engineering invariant.**
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/fbi_ucr/silver_fbi/transform.py`

## Context

`AGENTS.md`: "Use authoritative geography codes/mappings; do not infer
identity from names when authoritative identifiers are required."
`silver_ref/geography_contract.py:28, 81`: build identity "only from exact
provider codes, never names"; resolve "without fuzzy or name-based
matching." `fbi_ucr/silver_fbi/agency.py:1-8`: county labels are "retained
as evidence, never turned into a canonical county code here".

`_load_county_relationships` (`transform.py:462-510`) joins the provider's
county **label** to `silver_ref.dim_geo_current.county_name` after
upper-casing and stripping one legal suffix, and writes
`resolution_method='reviewed_county_name_crosswalk'`,
`confidence_class='reviewed'`. No reviewed artifact backs it. The place
path (`:513-550`) earns `'reviewed'` from a real
`silver_fbi.reviewed_place_crosswalk` table; the state path records
`'exact'`.

## Findings

- A label the suffix pattern does not normalise ("Doña Ana County",
  "LaSalle Parish" against `DONA ANA`, `LA SALLE`) matches nothing and the
  agency drops to `agency_only` silently. A rename in a new vintage that
  happens to yield one match re-points the relationship, and the resolved
  county flows into the `publishable` population DQ-FBI-002 measures and
  `gold_fbi.agency_observation_area_filter`.
- DQ-REF-003 checks only `status='resolved' XOR geo_sk IS NULL`; it cannot
  see a "reviewed" verdict with no reviewed evidence.

## Acceptance criteria

1. A county resolved from a label carries a `confidence_class` and
   `resolution_method` that say so (not `reviewed`), and the API and
   quality rules that gate on `reviewed` treat it accordingly; or the
   resolution moves behind an evidence-backed crosswalk table like the
   place path. The plan records the choice; the second is expected for
   `publishable`.
2. A label that fails to normalise lands as `unresolved` with a
   `reason_code`, not as a silent `agency_only`.
3. Failing-first unit coverage in `tests/unit/fbi_ucr` for a diacritic and
   a two-word label; integration coverage asserts the confidence token for
   a name-derived county differs from the crosswalk-derived place.
4. `docs/user-guides/FBI_UCR_PIPELINE_OPERATIONS.md` says how an operator
   reviews a county resolution.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `ETL-`
   identifier; ETL-050 at authoring time).

## Non-goals

- Fuzzy matching. The invariant forbids it.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
