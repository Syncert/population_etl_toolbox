---
id: a-closed-provider-vocabulary-is-refused
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-grain-that-is-not-one-is-refused]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A closed provider vocabulary is refused, and an empty filter is absent

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **The same defect as `a-grain-that-is-not-one-is-refused`, on
  the parameters that sweep did not reach.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/usda_nass_service.py`,
  `apps/api/routers/cdc.py`, `apps/api/routers/observations.py`,
  `apps/api/services/neutral_observations_service.py`,
  `src/data_ingestion_toolbox/usda_nass/registry.py`,
  `docs/reference/API_CONSUMER_GUIDE.md`

## Context

API-122 closed the closed-value hole for the parameters the API owns:
`geo_level` names a grain from one published vocabulary, and `state_fips` /
`county_fips` have a closed shape, so a word outside either is refused from
one dependency the application mounts on every route. Its own register row
says why: `geo_level=COUNTRY` "is not a grain with no rows; it is not a
grain", and binding it answered 200 with "a total that reads as a complete
answer to the question the caller thought they asked".

That sweep is derived from `registry.CLOSED_VALUE_PARAMETERS`, which holds
the three parameters whose vocabulary the API itself declares. Two routers
take a **provider's** closed vocabulary under the provider's own name, and
those were left where they were. Probing them with a recording session found
four things:

```text
GET /api/v1/usda-nass/observations?source_desc=ADMIN
  -> 422 source_desc must be SURVEY or CENSUS
GET /api/v1/usda-nass/series?source_desc=ADMIN
  -> 200 {"total": 0, "items": []}          # the same word, the sibling route

GET /api/v1/usda-nass/observations?value_status=suppressed
  -> 200 {"total": 0, "items": []}
GET /api/v1/usda-nass/observations?value_status=WITHHELD
  -> 200 {"total": 0, "items": []}

GET /api/v1/usda-nass/observations?commodity_desc=
  -> 200 {"total": 0, "items": []}          # bound commodity_desc = ''
GET /api/v1/usda-nass/observations?agg_level_desc=
  -> 422 agg_level_desc must be one of NATIONAL, STATE, COUNTY

GET /api/v1/cdc/observations?dataset=
  -> 422 dataset must be one of: cdi, places_county
GET /api/v1/observations?release=&metric_code=…
  -> 422 release: String should have at least 1 character
```

**One route refused what its sibling filtered on.** `source_desc` is a closed
set of two words -- `SURVEY` and `CENSUS`, the two programs Quick Stats
publishes under -- and the check lived beside `NassObservationFilters`. The
series route builds `NassSeriesFilters`, which validated only the grain, so
`ADMIN` was bound into `source_desc = :source_desc` and answered an empty
page. The vocabulary itself was written literally in two places (the filter
and the product registry's own validation), which is how the third place that
needed it was never given it.

**`value_status` was never checked anywhere.** The warehouse enumerates the
eight states a row can carry in its own CHECK constraint
(`sql/migrations/012_usda_nass_crop_pipeline.sql`) and the adapter that writes
them exports the same set as `VALUE_STATUSES`. The API filtered on the
parameter and validated nothing. The word this matters for is not a
hypothetical: the consumer guide's own sentence about `value_status` prints
`suppressed` as an example, because CDC publishes that word -- and NASS's word
for the same idea is `withheld`. A caller who reads the guide, filters NASS
for `suppressed`, and gets `total: 0` has been told that nothing was
suppressed.

**An empty value was a filter on the empty string.** Every NASS filter tested
`is not None`, so `?commodity_desc=` became `commodity_desc = ''` -- a
condition no row the provider publishes can satisfy. The rest of the API
reads an empty value as absent, deliberately and in writing:
`closed_value_refusal` refuses nothing for one because "a saved analysis
document records `state_fips: ""` for a source that declares no state filter
(API-117, WEB-075)". Three routes disagreed in the other direction too --
CDC's `dataset`, `geo_type` and `adjustment` refused an empty value by naming
their vocabulary, and `/observations` declared `release` with `min_length=1`,
so a client that serialises its whole parameter set was refused for pinning
no release at all.

## What is wrong

1. A closed provider vocabulary was validated per filter-set rather than per
   vocabulary, so a route that took the parameter without that filter set got
   no check -- and the vocabulary was a literal in each place that had one.
2. `value_status` had a closed set in the warehouse and no rule in the API.
3. An empty filter value meant three different things across the API:
   absent (most routes), a condition on `''` (USDA NASS), and a refusal
   (CDC's three choices, `/observations`' `release`).

## What was changed

- `src/data_ingestion_toolbox/usda_nass/registry.py` declares
  `SOURCE_PROGRAMS = (SURVEY, CENSUS)` beside `SUPPORTED_AGG_LEVELS`, and the
  product registry's own validation reads it instead of a literal.
- `apps/api/services/usda_nass_service.py` gains `_validated_source_program`
  and `_validated_value_status`, both over `_validated_closed_value`, shaped
  exactly like the existing `_validated_grain`: normalise the case the
  relation stores, accept the vocabulary, refuse anything else by naming the
  words. `source_desc` is validated in **both** filter sets. The value-state
  vocabulary is read from `VALUE_STATUSES`, never re-listed.
- Both NASS filter sets read an empty value as absent (`_absent_if_blank`)
  before validating anything, so `?commodity_desc=` binds no condition and
  `?agg_level_desc=` is no longer a refusal.
- `apps/api/routers/cdc.py`'s `_validated_choice` reads an empty value as
  absent and matches case-insensitively, returning the registered word -- the
  two rules `_validated_grain` beside it already applied.
- `/observations` drops `min_length=1` from `release`, and
  `neutral_observations_service` reads it falsy rather than `is not None`, so
  an empty release pins nothing instead of being refused. This is the one
  line of served-contract change, and the reviewed OpenAPI snapshot carries
  it.
- The consumer guide states both rules beside the existing closed-set
  paragraph, including that the status vocabulary is per source.

## Validation

`tests/unit/api/test_usda_nass_api.py` holds three rules, each derived rather
than enumerated: the vocabularies come from the declarations that create the
values (`SOURCE_PROGRAMS`, `VALUE_STATUSES`), and which routes take each
parameter is read from the served OpenAPI document.

- `test_every_route_refuses_a_word_outside_a_closed_nass_vocabulary`
- `test_a_vocabulary_word_is_accepted_in_any_case_and_bound_as_published`
- `test_an_empty_filter_value_is_absent_not_a_filter_on_nothing`

and `tests/unit/api/test_validation_security.py` holds the app-wide half:

- `test_an_empty_filter_value_is_absent_on_every_route`, over every optional
  plain-string query parameter of every served GET route.

The unit tier proves which word is *bound*; only a real query proves the bound
word *matches*, and the two vocabularies are stored in opposite cases --
`source_desc` upper, `value_status` lower -- so normalising to the wrong one
would answer an empty page, which is this defect wearing different clothes.
`tests/integration/api/test_usda_nass_api_contract.py` closes that against the
published fixture: each word a served row carries, sent in that case and the
other, answers exactly the rows carrying it, and an empty value answers the
unfiltered page. Proved by normalising `value_status` to the wrong case:

```text
E  AssertionError: {"detail":"value_status must be one of
   below_rounding_unit, insufficient_reports, missing, not_applicable,
   not_available, quality_flagged, valid, withheld"}
E  assert 422 == 200
```

Each was proved by breaking the fix back:

```text
# /series stops validating source_desc
E  AssertionError: /api/v1/usda-nass/series answered 200 for source_desc=ADMIN:
   {"total":0,"limit":100,"offset":0,"items":[]}

# value_status unchecked
E  AssertionError: /api/v1/usda-nass/observations answered 200 for
   value_status=suppressed: {"total":0,...,"release_scope":"as_released","items":[]}

# value_status checked but not normalised
E  AssertionError: /api/v1/usda-nass/observations bound {'BELOW_ROUNDING_UNIT'}
   for value_status='BELOW_ROUNDING_UNIT', not 'below_rounding_unit'

# an empty value is a filter again
E  AssertionError: /api/v1/usda-nass/observations refused an empty
   agg_level_desc: {"detail":"agg_level_desc must be one of NATIONAL, STATE, COUNTY"}

# the app-wide sweep, with every route's fix reverted
E  AssertionError: GET /api/v1/cdc/observations refused an empty adjustment: …
   GET /api/v1/cdc/observations refused an empty dataset: …
   GET /api/v1/cdc/observations refused an empty geo_type: …
   GET /api/v1/observations refused an empty release: …
   GET /api/v1/usda-nass/observations refused an empty agg_level_desc: …
   GET /api/v1/usda-nass/observations refused an empty source_desc: …
   GET /api/v1/usda-nass/series refused an empty agg_level_desc: …
```

## Deliberately not done

- **`freq_desc`, `unit_desc`, `commodity_desc` and the rest of the Quick
  Stats classification are left unvalidated.** They are not closed: the
  registry declares which values *this* adapter ingests per product, not
  which values the parameter may name, and Quick Stats publishes more. An
  empty page for `commodity_desc=SORGHUM` is a fact about the warehouse,
  which is the distinction `state_fips` already draws between a well-formed
  code and a malformed one.
- **`start_date` / `end_date` still refuse an empty value.** They carry
  `format: date`; an empty string is malformed there rather than absent, and
  FastAPI's machine-readable refusal names the field. The sweep excludes
  formatted strings for that reason, in writing.
