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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; the two routes API-094 did not sweep.**
- **Last updated:** 2026-09-13
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
   identifier; API-116 at authoring time).

## Non-goals

- Renaming the parameters. The source-scoped routes keep their names.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
