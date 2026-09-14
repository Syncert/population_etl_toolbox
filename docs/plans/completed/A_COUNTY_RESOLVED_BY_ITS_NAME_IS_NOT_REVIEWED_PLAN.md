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

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13.)
- **Last updated:** 2026-09-14
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

- **Criterion 1 — the choice, and why the first arm and not the second.** A
  county established from a label is now `resolution_method =
  'county_label_match'`, `confidence_class = 'derived'`. The second arm (move
  the resolution behind a reviewed county crosswalk) was **not** built, and
  the reasoning belongs in the record:
  - nothing gates on `reviewed` for `publishable`. DQ-FBI-002 measures
    participation coverage, and `gold_fbi.crime_observation`'s own gate is
    `geography_status`, which turns on `resolution_status`, not on
    confidence. The plan's parenthetical ("the second is expected for
    `publishable`") assumes a gate that does not exist.
  - the place path has a crosswalk because the provider publishes **no place
    identifier at all**; the crosswalk is the only evidence there can be. For
    a county the provider publishes a label, and the match against the
    authoritative reference is exact and uniqueness-checked.
  - an empty crosswalk table with a loader and no reviewed rows would darken
    every county filter in the product until someone reviewed 3,000 ORIs,
    while claiming a capability nothing uses. The honest token makes the same
    guarantee legible today, and it is the hook a reviewed county crosswalk
    would hang on later: a consumer that needs reviewed county attribution
    filters `confidence_class = 'reviewed'` and gets nothing, which is the
    correct answer rather than a name match.
  - what the token change actually fixes:
    `gold_fbi.agency_observation_area_filter` publishes
    `filter_confidence_class`, so a consumer asking for reviewed attribution
    was being handed name matches with no way to tell.
- **Criterion 1, the rules that gate on it.** DQ-FBI-004 declared
  "attribution flows only through exact state codes or reviewed crosswalks"
  and its only reading was a fanout count, so it passed over the exact thing
  it declares. It now also reads
  `silver_fbi.agency_geography_relationship` and fails when a resolved row
  claims a confidence its method did not earn. The allowed pairs are a
  reviewed mapping (`_FBI_RESOLUTION_CONFIDENCE`) bound into the query as a
  VALUES list, so adding a method to the mapping extends the rule and adding
  one *without* extending the mapping fails it -- which is how the county
  path's own spelling went unnoticed. The declaration in `inventory.py` now
  says the three-way truth instead of two thirds of it.
- **Criterion 2, with a correction to the plan's framing.** The plan says a
  label that fails to normalise "drops to `agency_only` silently". The
  relationship row was never silent -- it was written as `unresolved` -- but
  two things about it were wrong, and both are fixed:
  - the reason was `canonical_county_absent`, which asserts the reference does
    not hold the county. A zero-match join cannot support that: the county may
    be there under a spelling the normalisation does not reach. It is now
    `county_label_unmatched`, which is what the query established.
  - `_AGENCY_STATUS_CTE` folded an unresolved county into `agency_only`, and
    *that* is the silence. `agency_only` says the provider published no county
    association at all (`NOT SPECIFIED`) -- a fact about the source that no
    review changes. A provider-labelled county that failed to resolve is this
    pipeline's gap, and the only one of the two an operator can act on. It is
    now `agency_county_unresolved`.
- **Criterion 3, unit.** `tests/unit/fbi_ucr/test_fbi_county_resolution.py`
  mirrors the join's own comparison -- `BTRIM(REGEXP_REPLACE(UPPER(
  county_name), COUNTY_SUFFIX_PATTERN, ''))` against the parser's
  `normalize_county_label` -- using the module's pattern rather than a
  transcription, and first asserts the loader really is shaped that way so the
  mirror cannot drift from it. The diacritic case (`Doña Ana County` vs
  `DONA ANA`) and the spacing case (`La Salle Parish` vs `LASALLE`) are
  asserted to **not** match, beside four that do, because the fix for an
  unreachable label is a reviewed mapping and not a looser join: folding
  accents or ignoring spaces is name-based matching, which the invariant
  forbids, and would also make genuinely different counties equal.
  - Break-test: reverting `transform.py` to `HEAD` fails three of the ten
    nodes, naming `'county_label_match'`, `'county_label_unmatched'` and
    `'agency_county_unresolved'` as absent.
- **Criterion 3, integration.** The existing
  `test_agency_geography_status_matches_its_reviewed_evidence` encoded the
  defect -- it asserted `reviewed` for both the county and the place -- and now
  asserts `county_label_match`/`derived` for the two counties beside
  `reviewed_place_crosswalk`/`reviewed` for the place, in one query, which is
  the difference the criterion asks for.
  - A new node removes Brown County from the boundary reference, because
    WI0050700 is the one fixture agency labelled with it alone, so exactly one
    agency's resolution changes: the relationship becomes `unresolved` /
    `county_label_unmatched`, the agency becomes
    `agency_county_unresolved`, and WIWSP0000 (`NOT SPECIFIED`) stays
    `agency_only` in the same result -- the two statuses asserted side by
    side. Break-test: removing the new `WHEN` from `_AGENCY_STATUS_CTE` fails
    it with `('WI0050700', 'agency_only') != ('WI0050700',
    'agency_county_unresolved')`.
  - A new injection node proves DQ-FBI-004 can now see a false claim: it
    drops the CHECK inside the rolled-back transaction (so the rule is read as
    a second line of defence rather than trusting the write path), sets
    `confidence_class = 'reviewed'` on the five `county_label_match` rows, and
    gets `fail` with `observed_count = 5` and evidence naming the method; then
    renames the method to the old `reviewed_county_name_crosswalk` and gets
    `fail` again, because a method the mapping does not know is an offender
    too.
  - `_outcome` in that module unpacked a single outcome, and DQ-FBI-004 now
    returns two; `_outcome_for(..., relation)` selects by relation so each
    node asserts the reading it is about.
- **Criterion 4.** `FBI_UCR_PIPELINE_OPERATIONS.md` gains a "Reviewing a
  county association" section: the three tokens and what earns each, the query
  that lists unresolved labels by reason, what each reason does and does not
  establish, the exact suffix list the comparison strips, the by-hand check
  against `silver_ref.dim_geo_current`, and a plain statement not to relax the
  comparison and why. It also states the `agency_county_unresolved` versus
  `agency_only` distinction and that neither is withheld from `gold_fbi`.
- **Criterion 5.** `ETL-050` is in `TESTING_CONTRACT.md`, the family range
  reads `ETL-001–ETL-050`, `AUDITED_COUNTS["ETL"]` is 50, and the totals are
  405.
- **Migration 025** widens two vocabularies (`derived`,
  `county_label_match`, `agency_county_unresolved`) by dropping and re-adding
  the four CHECK constraints, which is idempotent, and rewrites the rows the
  old code wrote -- the relationships are unchanged, only the claim about how
  they were established is corrected. A genuinely fresh bootstrap of every
  manifest asset into a new `fbi_fresh_test` database printed `FRESH BOOTSTRAP
  OK` and both constraints carry the new words; the database was dropped
  afterwards.
- **Tiers.** `pytest tests/unit` 1587 passed. `pytest tests/unit/fbi_ucr` 135
  passed. `pytest tests/integration -m "integration and (redis or database)
  and not slow"` 164 passed, 2 skipped, 14 deselected.
  `pytest tests/integration/database/test_fbi_ucr_pipeline.py` 14 passed.
  `ruff check .` and `ruff format --check .` clean.

## Remaining work

- None. A reviewed county crosswalk stays unbuilt on purpose, for the reasons
  under Criterion 1; `confidence_class = 'reviewed'` is the hook if a consumer
  ever needs one.
