---
id: bls-laus-measure-metrics
branch: feat/bls-laus-measure-metrics
depends_on:
  - api-platform
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 web-unit
---

# BLS LAUS measure-level metrics and national geography fix

## Plan status

- **Status:** Approved, unclaimed
- **Last updated:** 2026-09-10
- **Source owner:** U.S. Bureau of Labor Statistics, Local Area Unemployment Statistics (LAUS) program, plus the national-grain serving fix for BLS, Census ACS, and FRED
- **Geography scope:** State and county for LAUS measures; the national grain is a serving-vocabulary repair only
- **Depends on:** `API_DEVELOPMENT_PLAN.md` accepted into `completed/` (satisfied 2026-09-01). No open plan is a prerequisite. This plan changes warehouse objects first, so it should integrate before any web work that assumes the new BLS metric identities.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** none claimed

**Next pickup:** claim the plan, then start at BLM-001 (the `us` versus `NATIONAL` serving fix), because every later phase's evidence depends on national reads answering.

### Completed in the current slice

- [ ] BLM-001 national geography vocabulary in the served relations
- [ ] BLM-002 LAUS measure identity in the BLS reporting and latest relations
- [ ] BLM-003 BLS metric publisher emits one metric per LAUS measure
- [ ] BLM-004 full BLS serving refresh and glossary harvest
- [ ] BLM-005 API and web contract synchronisation
- [ ] BLM-006 evidence record and consumer-facing documentation

## Objective

Make BLS LAUS explorable as a spatial product. Today every LAUS series is its own catalog metric (one code per county or state), so no BLS metric spans geographies and the explorer's map, distribution bins, and comparison routes have nothing to draw. After this plan, seven LAUS measures are catalog metrics whose observations span every published state and county, and a national series returns its rows when asked for at the `NATIONAL` grain.

The plan follows the repository's architecture order: stable warehouse objects, then API contracts, then web features. It deliberately adds no new ingestion. `silver_bls.fact_labor_statistics` already carries `measure_code`, `measure_name`, `geo_level`, `geo_id`, `state_fips`, and `county_fips` on every LAUS row.

## Evidence gathered 2026-09-10

Observed against the live development stack (`docker-analytics_postgres-1`, API on `localhost:8000`):

- `GET /observations?metric_code=BLS:CES0000000001` answers 1 row with `geo_level: "us"`; adding `geo_level=NATIONAL` answers 0 rows. The same is true for `CENSUS_ACS:acs5:B01003_001` at the national grain.
- Cause: the reporting refresh procedures prefer `silver_ref.dim_geo.geo_level`, whose vocabulary is `us`, `state`, `county`, `place`, `agency` ([silver_ref.sql:148](../../../src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql#L148)), over the fact view's normalised `NATIONAL` / `STATE` / `COUNTY`:
  - [gold_bls.sql:261](../../../src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql#L261) `COALESCE(gl.geo_level, b.geo_level)`
  - [gold_acs.sql:286](../../../src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql#L286) `COALESCE(gl.geo_level, ao.geo_level)`
  - [gold_fred.sql:233](../../../src/data_ingestion_toolbox/fred/gold_fred/DDL/gold_fred.sql#L233) `COALESCE(gl.geo_level, 'NATIONAL')`
- The dispatch filter is `UPPER(geo_level) = UPPER(:geo_level)` ([registry.py:242](../../../apps/api/registry.py#L242)). State and county match only because the words coincide across the two vocabularies.
- Served rows carrying `us` today: `gold_bls.rpt_bls_observations` 22,055; `gold_census.rpt_acs_observations` 4,447; `gold_fred.mv_fred_latest` 24.
- The real-database contract test asserts `geo_level == "NATIONAL"` on a served FRED row and passes only because its fixture never seeds `silver_ref.dim_geo`, so the coalesce falls through to the normalised side.
- BLS catalog: 13,317 active metrics. 56 national series (CES, CPI, JOLTS, LNS), 361 LAUS state series, 12,900 LAUS county series. Each LAUS metric is one place's own series (`BLS:LAUCN010010000000003` is "Unemployment Rate: Autauga County, AL (U)") with `valid_geo_grains` of exactly one level.
- LAUS is unadjusted only at both grains (`seasonal_adjustment = 'U'` on every LA row), so one measure per geography yields exactly one latest row and the explorer's stratification guard never fires.
- Silver LAUS coverage by measure: 03, 04, 05, 06 at 3,225 counties and 52 states; 07, 08, 09 at 51 states.
- The intended identity already exists downstream of the warehouse: `apps/web/lib/productTemplates.ts` and `tests/unit/api/test_source_observations.py` both use `BLS:LAU:UNEMP_RATE`, a code the catalog has never published. The profile "Labor market" section therefore resolves nothing today.

## Target metric identities

| Measure code | Metric code | Display name | Units | `measure_kind` | Grains |
| --- | --- | --- | --- | --- | --- |
| 03 | `BLS:LAU:UNEMP_RATE` | Unemployment rate | Percent | RATE | STATE, COUNTY |
| 04 | `BLS:LAU:UNEMP_LEVEL` | Unemployment level | Persons | LEVEL | STATE, COUNTY |
| 05 | `BLS:LAU:EMP_LEVEL` | Employment level | Persons | LEVEL | STATE, COUNTY |
| 06 | `BLS:LAU:LABOR_FORCE` | Labor force level | Persons | LEVEL | STATE, COUNTY |
| 07 | `BLS:LAU:EMP_POP_RATIO` | Employment-population ratio | Percent | RATIO | STATE |
| 08 | `BLS:LAU:LFPR` | Labor force participation rate | Percent | RATE | STATE |
| 09 | `BLS:LAU:CNIP` | Civilian noninstitutional population | Persons | LEVEL | STATE |

Grains are aggregated from the fact rows at publish time, as `gold_pep.measure_export` does, not declared as a constant. Non-LA programs (CES, CU, CW, JT, LN) keep their series-level identity and national grain unchanged.

## Decisions

Recorded so the implementer does not re-litigate them. A change to any of these is a material scope change requiring user approval.

1. **Retire the series-level LAUS codes; do not dual-publish.** Dual publication doubles served rows and keeps 12,900 single-county entries in the picker. The glossary harvest marks unpublished keys `retired` after `retirement_grace_harvests` (default 2), so an existing link to a series code resolves to a retired catalog row rather than a 404, and `active_only=true` hides it from the explorer.
2. **The national LNS unemployment series stays a separate metric.** It is a different survey and seasonally adjusted; folding it into `BLS:LAU:UNEMP_RATE` as a `NATIONAL` grain would assert a comparability BLS does not claim.
3. **Metric code spelling is `BLS:LAU:<MEASURE>`**, matching the shape already used by the profile templates and API fixtures, and giving BLS codes the `SOURCE:dataset:variable` form the explorer's dataset facet reads.
4. **The `us` fix lands in the refresh procedures, not the API.** The served row is promised to carry `NATIONAL` (real-database contract test); normalising at the API would leave the warehouse contract wrong and the union-family views inconsistent.
5. **The series id stays on every row** as the `series_id` dimension the dispatch already serves, so lineage back to the BLS series is not lost.

## Non-goals

- No new BLS ingestion, no seasonally adjusted LAUS, no metropolitan or place-level LAUS.
- No change to the legacy source-scoped `/bls/observations/latest` and `timeseries` routes beyond what the shared relations carry; API_CONSUMER_GUIDE already marks them retiring.
- No explorer feature work. The map, bins, and comparison must appear for LAUS through the existing capability-driven paths without client changes; if they do not, that is a defect to fix in the warehouse or API layer, not a reason to add a client special case.
- No change to how the national CES, CPI, JOLTS, or LNS series are catalogued.

## Implementation phases

### BLM-001 — National geography vocabulary in the served relations

Deliverables:

- In `gold_bls.refresh_rpt_bls_observations`, the ACS reporting refresh in `gold_census`, and the FRED reporting refresh in `gold_fred`, the served `geo_level` is always the normalised `NATIONAL` / `STATE` / `COUNTY` vocabulary. Prefer the fact view's already-normalised value; if the geography dimension's level is retained as a fallback, map it through the same `CASE` the fact views use (`us` becomes `NATIONAL`).
- A unit test over the DDL text (alongside `tests/unit/shared/test_incremental_serving_contract.py`) asserting no reporting refresh writes the dimension's raw `geo_level` unmapped.
- The real-database contract fixture seeds `silver_ref.dim_geo` with `us:1` so the `NATIONAL` assertion exercises the coalesce path it was written to guard.

Acceptance:

- After a refresh, `SELECT DISTINCT geo_level` over `gold_bls.rpt_bls_observations`, `gold_bls.mv_bls_latest`, `gold_census.rpt_acs_observations`, `gold_census.mv_acs_latest`, `gold_fred.rpt_fred_observations`, and `gold_fred.mv_fred_latest` returns only `NATIONAL`, `STATE`, `COUNTY`.
- `GET /observations?metric_code=BLS:CES0000000001&geo_level=NATIONAL` answers the same row count as the unfiltered read, and the row carries `geo_level: "NATIONAL"`. Same for `CENSUS_ACS:acs1:B01001_001` and a FRED series.
- `tests/run.ps1 integration` passes with the seeded geography row.

### BLM-002 — LAUS measure identity in the BLS reporting and latest relations

Deliverables:

- A measure mapping owned by `gold_bls` (either a `dim_bls_measure` table populated by `transform.py` alongside `dim_bls_series`, or a reviewed `CASE` on `program_code = 'LA'` and `measure_code`) that yields the metric code, display name, units, and `value_type` in the table above.
- `gold_bls.refresh_rpt_bls_observations` writes `metric_code = 'BLS:LAU:<MEASURE>'` and `metric_display_name` from the mapping for LA rows, and keeps `'BLS:' || series_id` for every other program. `series_id` remains populated on every row.
- `gold_bls.refresh_mv_bls_latest` keeps its `DISTINCT ON (geo_id, series_id, metric_code)` key; a test proves one LAUS geography has exactly one latest row per measure.
- `gold_bls.dim_bls_series.geographic_level` and `measure_name` remain the inputs; no silver change.

Acceptance:

- `SELECT metric_code, COUNT(DISTINCT geo_id)` over `gold_bls.mv_bls_latest` for the seven codes matches the silver coverage counts (3,225 counties and 52 states for 03 to 06; 51 states for 07 to 09).
- No LA row in `rpt_bls_observations` carries a series-shaped code after a full refresh.
- `tests/unit/bls` and `tests/unit/shared/test_incremental_serving_contract.py` pass, with a new case for the LA identity branch.

### BLM-003 — BLS metric publisher emits one metric per LAUS measure

Deliverables:

- `gold_bls.metric_publisher` publishes one row per LAUS measure with `source_object_type = 'measure'`, `source_object_key = 'LAU:<MEASURE>'`, `valid_geo_grains` aggregated from the fact rows (`ARRAY_AGG(DISTINCT ...)` uppercased), `valid_time_grains = ['MONTHLY']`, units and `measure_kind` from the mapping, and `physical_lineage` keyed on the measure. Non-LA programs continue to publish per series.
- `tests/unit/shared/test_publisher_contract_shape.py` covers the new shape; the ARC-001 shared-glossary boundary test still passes because no `gold_glossary` DDL changes.

Acceptance:

- `SELECT COUNT(*) FROM gold_bls.metric_publisher` is 63 (56 series plus 7 measures) on the development warehouse.
- Each of the seven rows publishes the grains in the identity table, read from data rather than hard-coded.

### BLM-004 — Full BLS serving refresh and glossary harvest

Deliverables:

- An operator note (in this plan's evidence record, and in `docs/reference/BETA_RESET_REINGESTION.md` if the procedure there is the right home) stating that a metric-identity change requires `gold_bls.refresh_dashboard_serving_layer_bls(NULL, NULL, TRUE)` rather than the DAG's changed-year chunking, because unchanged years would otherwise keep the old codes.
- The ACS and FRED reporting relations refreshed in full once for BLM-001.
- The `glossary_harvest` DAG (or `harvest_all_publishers`) run after the refresh; the 13,261 series-level LAUS codes reach `freshness_state = 'retired'` after the configured grace, and the seven measure codes are `current`.

Acceptance:

- `GET /catalog/metrics?source_code=BLS&active_only=true` total is 63.
- `GET /catalog/metrics/BLS:LAUCN010010000000003` still resolves, with `freshness_state: "retired"`, once the grace has elapsed.
- Refresh wall-clock and row counts recorded in the evidence section (the BLS reporting relation is roughly 5.8 million rows).

### BLM-005 — API and web contract synchronisation

Deliverables:

- API unit fixtures that use series-style LAUS codes (`tests/unit/api/test_neutral_observations.py`, `tests/unit/api/test_catalog_discovery.py`) updated to the published measure codes; the source-observation and comparison fixtures already use `BLS:LAU:UNEMP_RATE`.
- No change to `OBSERVATION_DISPATCH["BLS"]`: `geo_level`, `state_fips`, `county_fips`, `analysis_ready`, and `publishes_geo_attribution` already cover the measure-level rows. Record in the evidence section that this was verified rather than assumed.
- Web: verify the BLS tab in `SourceExplorerPage.tsx` with a mixed catalog (two-part CES codes and three-part LAU codes). `datasetFacetOptions` yields one facet, so the dataset selector stays hidden and the full list is offered; confirm `pickPreferredMetric` does not select a national series by default when a mappable LAUS measure exists, and adjust the preference in `explorerViewModel.ts` only if the observed default is a national series.
- Web: a unit test in `tests/frontend/unit` proving `describeViewModes` reports the map as supported for `BLS:LAU:UNEMP_RATE` at `COUNTY` given the discovered tile fields, and a browser test asserting the BLS tab renders the choropleth and the distribution legend for that metric.
- Profile product: `tests/frontend/browser/profiles.spec.js` already fixtures `BLS:LAU:UNEMP_RATE`; confirm the "Labor market" section resolves against the live catalog and record it.

Acceptance:

- `tests/run.ps1 api`, `web-unit`, and `web-browser` pass.
- `/explore?source=bls&metric=BLS%3ALAU%3AUNEMP_RATE&geo_level=COUNTY` renders a coloured map, API bins, the state filter, and the trend chart for a clicked county, with no client-side source special case added.
- `/api/v1/comparison/preflight?metric_code_a=BLS:LAU:UNEMP_RATE&metric_code_b=CENSUS_ACS:acs5:B19013_001` returns a decision rather than an error, and `/api/v1/distribution/bins?metric_code=BLS:LAU:UNEMP_RATE&geo_level=COUNTY` returns bins.

### BLM-006 — Evidence record and consumer-facing documentation

Deliverables:

- `docs/reference/API_CONSUMER_GUIDE.md`: note that BLS LAUS is published per measure across geographies, that the series id is available under `dimensions.series_id`, and that series-level LAUS codes are retired catalog rows.
- `docs/reference/TESTING_CONTRACT.md`: a row for the served geography vocabulary invariant and one for the BLS measure identity, mapped in `CI_EVIDENCE_MAP.md`.
- This plan's evidence section filled with the exact commands and results from BLM-001 to BLM-005.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| DDL text contract | `unit` | Reporting refreshes never write the raw dimension vocabulary; the LA identity branch exists |
| Publisher shape | `unit` | Seven measure rows with data-derived grains, series rows untouched |
| Registry dispatch | `api` | Measure-level rows answer `geo_level`, `state_fips`, `county_fips`, bins, comparison |
| Real-database contract | `integration` | Seeded `us:1` geography still serves `NATIONAL`; LAUS latest has one row per geography per measure |
| Explorer view model | `web-unit` | Map supported for a LAUS measure at county grain; default metric is mappable |
| Explorer browser | `web-browser` | BLS tab draws the choropleth and bins from live-shaped fixtures |

## Risks and mitigations

- **Full-refresh runtime.** The BLS reporting relation is about 5.8 million rows and the ACS one is similar. Run the forced full refresh out of hours and record the duration; the procedures already set a 60-minute statement timeout per call.
- **Saved analyses referencing series codes.** The saved-analysis store lives in the service database, not the warehouse, and was not inspected during scoping. Before retiring codes, query it for series-shaped BLS LAUS metric codes and either migrate them to the measure code with the same measure suffix or record that none exist.
- **Retirement grace hides nothing new.** Retirement applies to the old codes; the new measure codes are `current` from the first harvest. If a consumer sees both during the grace window, that is by design and bounded to two harvests.
- **Default explorer selection.** With 63 BLS metrics the first catalog row is a national CES series, which is non-spatial. BLM-005 verifies the default and fixes the preference only if needed.

## Open questions for the reviewer

None blocking. Two are recorded as decisions above (retire versus dual-publish, and keeping LNS separate); reverse either by editing the Decisions section before the plan is claimed.

## Implementation evidence

_Empty until claimed._
