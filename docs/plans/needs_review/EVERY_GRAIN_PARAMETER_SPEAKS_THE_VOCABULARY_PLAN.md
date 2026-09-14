---
id: every-grain-parameter-speaks-the-vocabulary
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_cdc_observations.py tests/unit/api/test_usda_nass_api.py tests/unit/api/test_consumer_guide.py -q
---

# Every parameter that takes a geography grain takes the vocabulary, whatever it is named

## Plan status

- **Status:** Needs review. Investigated and authored 2026-09-13;
  delivered 2026-09-13 (`5faa19a`). The two routes API-094 did not
  sweep; see "What changed" and "Validation".
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/routers/cdc.py`,
  `apps/api/services/usda_nass_service.py`

## Context

The guide: "a grain read from the catalog can be sent straight back ...
The filter is case-insensitive, and it accepts `NATION` as an alias for
`NATIONAL`." API-092 and API-094 made every `geo_level` parameter go through
`normalize_geo_level`. Two routes take the grain under another name and
were not swept:

- `/cdc/observations` validates `geo_type` by exact match against
  `("nation", "state", "county")` -- lowercase only (`cdc.py:62`,
  `cdc_queries.py:27`).
- `/usda-nass/observations` validates `agg_level_desc` by exact match
  against `("NATIONAL", "STATE", "COUNTY")` -- uppercase only
  (`usda_nass_service.py:107-112, 174-179`).

`/catalog/capabilities` advertises both parameters with no hint of their
private vocabularies.

## Findings

- The catalog publishes `COUNTY` for a CDC metric;
  `GET /cdc/observations?geo_type=COUNTY` is 422. `NATION`, which the guide
  guarantees, is 422 on both routes.
- `test_cdc_observations.py:344` and `test_usda_nass_api.py:247, 271` pin
  the private vocabularies as the contract and pass for the wrong reason.

## Acceptance criteria

1. Both parameters accept the vocabulary case-insensitively with the
   `NATION`/`US` aliases, through the same normaliser, and reject an
   unknown word with the vocabulary in the message.
2. The pinned tests are corrected; a sweep in `test_consumer_guide.py`
   finds every capability-map parameter whose name or description says
   grain and asserts the normaliser is applied.
3. The guide's grain section names the two parameters.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-117 at authoring time).

## Non-goals

- Renaming the parameters. The source-scoped routes keep their names.

## What changed

- **CDC.** The router normalises `geo_type` and validates it against the
  grains the relation carries *through the normaliser* --
  `tuple(normalize_geo_level(word) for word in GEOGRAPHY_TYPES)` is
  `NATIONAL, STATE, COUNTY` -- so the request vocabulary is derived from
  CDC's own words rather than written a second time. The filter became
  `gold_glossary.geo_grain(geo_type) = :geo_type`, migration 018's one
  mapping, so the relation keeps its lowercase word and the request does not
  have to know it. Translating `NATIONAL` back to `nation` in the client
  would have been the inverse of a warehouse function written in Python.
- **USDA NASS.** `agg_level_desc` normalises in both query shapes through
  one helper. The relation already stores the vocabulary word, so
  normalising the request is the whole fix.
- Both refusals name the vocabulary words.
- The guide's grain section names the two parameters and says the names stay
  as they are.

## Validation

- `pytest tests/unit/api` — **454 passed**.
- The two pinned tests are corrected: CDC's bound-parameter node now sends
  the catalog's `COUNTY` and asserts both the bound word and the mapping in
  the statement; its refusal message names the vocabulary.
- **The sweep catches both routes.** Restoring either exact-match validation
  fails `test_every_parameter_that_carries_a_grain_takes_the_vocabulary` by
  name and path: `/api/v1/usda-nass/observations refused the alias 'nation'
  for agg_level_desc`. Both files were restored byte-for-byte afterwards.
- The sweep sends only what each route declares: NASS refuses an unknown
  parameter by listing the ones it accepts, and `agg_level_desc` appears in
  that list, so a blanket `metric_code` made the assertion trip on its own
  error message.
- `ruff check .` / `ruff format --check .` — clean.
- `python -m tests.support.catalog_evidence` renders API-116 `FULL`.

## Remaining work

- None. Review is the remaining step.
