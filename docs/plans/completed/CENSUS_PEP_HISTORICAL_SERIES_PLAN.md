---
id: census-pep-history
branch: feat/census-pep-history
depends_on:
  - census-pep
  - api-platform
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 web-unit
---

# Census PEP historical series back to 1970

## Plan status

- **Status:** Accepted 2026-09-12 (Implementation complete on `feat/census-pep-history`; ready for human review)
- **Last updated:** 2026-09-10
- **Source owner:** U.S. Census Bureau Population Estimates Program (PEP), historical county and state series
- **Geography scope:** State and county for every decade; national where the era's file carries it. Subcounty history is out of scope.
- **Depends on:** `CENSUS_PEP_PIPELINE_PLAN.md` (accepted 2026-08-28) for the registry, capture, silver fact, and gold publication it extends; `API_DEVELOPMENT_PLAN.md` (accepted 2026-09-01) for the neutral observations resource that PEH-006 adds a parameter to. Both are satisfied. No open plan is a prerequisite.

## Implementation checkpoint

**Last updated:** 2026-09-10

**Current milestone:** every phase delivered. PEP now registers ten products across six decades and serves county population from 1971 to 2025, with the seam between decades resolved to one published value and an additive API parameter that answers one row per geography.

**Next pickup:** none. Two deliberate remainders are named under *Remainders* below; neither is a gap in this plan's objective.

### Completed in the current slice

- [x] PEH-001 registry contract covers eras, series kinds, and per-release observation ranges
- [x] PEH-002 cross-dataset latest selection with the seam rule
- [x] PEH-003 2010 to 2020 series (Vintage 2020 alldata, same layout as today)
- [x] PEH-004 2000 to 2010 series (Vintage 2009 components plus intercensal totals)
- [x] PEH-005 1990s, 1980s, and 1970s series (legacy layouts, totals and partial components)
- [x] PEH-006 newest-per-geography parameter on the neutral observations resource
- [x] PEH-007 full PEP serving refresh, glossary harvest, and evidence record

## Objective

Serve every Census PEP county and state population estimate the Bureau still publishes, from July 1, 1970 to the current vintage, under the metric identities the catalog already publishes (`CENSUS_PEP:POPESTIMATE`, `CENSUS_PEP:BIRTHS`, and the rest). Today the warehouse holds only the 2020s series: every release in the curated registry starts its observation range at 2020 and downloads from the `2020-{vintage}` path, so a county's history in the explorer is six points. After this plan a county's `POPESTIMATE` series is 56 points, its `BIRTHS` and `DEATHS` series reach back to 1980, and the migration components reach back to 2000, with each value carrying the dataset, vintage, and geography basis it was published under.

The plan follows the repository's architecture order: registry and warehouse objects first, then the API parameter the explorer needs to stay usable over a long series, then no web feature work at all. It adds ingestion of seven historical products. It does not change the 2020s pipeline's behaviour for the releases it already serves.

## Evidence gathered 2026-09-10

Observed against the live development stack (`docker-analytics_postgres-1`, web on `localhost:3100`) and the Census bulk site:

- `GET /observations?metric_code=CENSUS_PEP:BIRTHS&scope=latest&geo_level=COUNTY` answers 18,864 rows: 3,144 counties times six estimate dates, 2020-07-01 through 2025-07-01. Nothing earlier exists in the warehouse.
- Cause: every curated release is built by `_release()` with `observation_start_year=2020` ([config.py:365](../../../src/data_ingestion_toolbox/census_pep/config.py#L365)), every dataset's `data_url_template` is under `datasets/2020-{vintage}/` ([config.py:320](../../../src/data_ingestion_toolbox/census_pep/config.py#L320)), and `PEPRelease.__post_init__` rejects any release whose `observation_end_year` differs from its `vintage_year` ([config.py:146](../../../src/data_ingestion_toolbox/census_pep/config.py#L146)). An intercensal file (published years after the decade it covers) cannot be registered under that contract.
- `gold_pep.population_estimate_latest` ranks vintages **within** a `dataset_code` ([gold_pep.sql:27-40](../../../src/data_ingestion_toolbox/census_pep/gold_pep/DDL/gold_pep.sql#L27-L40)). Two datasets publishing the same metric, geography, and observation year both survive. This already shows at the state grain: `pep_nst_alldata` and `pep_county_alldata` each publish California's 2025 births, so the state read answers 12 rows for six periods. Adding decade datasets multiplies that at every seam year unless the rule changes.
- The web explorer was fixed on 2026-09-10 to page the whole latest publication and colour each geography by its newest period, bounded at 8 pages of 5,000 rows. A county metric spanning 1970 to 2025 is about 176,000 rows and would exceed that bound; the status pill would report the map incomplete. The API's own distribution and comparison routes already rank one newest row per geography inside the source relation (`ranked_latest_cte`, [comparison_service.py:70](../../../apps/api/services/comparison_service.py#L70)); the observations resource does not offer that.

### What the Bureau publishes, by era

Verified by directory listing and file header on 2026-09-10. Paths are under `https://www2.census.gov/programs-surveys/popest/`.

| Era | Product | Path | Layout | Grains | Measures available |
| --- | --- | --- | --- | --- | --- |
| 2010 to 2020 | Vintage 2020 county alldata | `datasets/2010-2020/counties/totals/co-est2020-alldata.csv` | Wide CSV, same family as the current `CO-EST2025-ALLDATA` (`SUMLEV, REGION, DIVISION, STATE, COUNTY, STNAME, CTYNAME, CENSUS2010POP, ESTIMATESBASE2010, POPESTIMATE2010..2020, NPOPCHG_*, BIRTHS*, DEATHS*, NATURALINC*, INTERNATIONALMIG*, DOMESTICMIG*, NETMIG*, RESIDUAL*, R*`) | State and county | All current measures; `NATURALINC` is the older spelling of `NATURALCHG` |
| 2010 to 2020 | Vintage 2020 state alldata | `datasets/2010-2020/state/totals/nst-est2020-alldata.csv` | Wide CSV, `NST` family | National and state | All current measures |
| 2000 to 2009 | Vintage 2009 county alldata (postcensal) | `datasets/2000-2009/counties/totals/co-est2009-alldata.csv` | Wide CSV, same family, columns `*2000..*2009` | State and county | All current measures |
| 2000 to 2010 | Intercensal county estimates by age, sex, race, Hispanic origin | `datasets/2000-2010/intercensal/county/co-est00int-alldata-{state}.csv` (one file per state FIPS) | Long CSV: `SUMLEV, STATE, COUNTY, STNAME, CTYNAME, YEAR, AGEGRP, TOT_POP, ...`; `AGEGRP=0` is the all-ages row; `YEAR` is a code that includes the April 2000 and April 2010 census counts | County | `POPESTIMATE` only, from `TOT_POP` where `AGEGRP=0` |
| 1990 to 1999 | CO-99-10 county race by Hispanic origin annual time series | `datasets/1990-2000/counties/asrh/co-99-10.txt` with layout `technical-documentation/file-layouts/1990-2000/co-99-10-rl.txt` | Fixed-width text with a prose preamble; one row per county, year, race, and Hispanic-origin cell | County | `POPESTIMATE` only, by summing cells |
| 1980 to 1990 | Components of change 1980 to 1990 | `datasets/1980-1990/counties/totals/comp8090.zip` (245 KB) with `technical-documentation/file-layouts/1980-1990/comp8090_doc.txt` | Fixed-width text inside a zip; 1980 and 1990 census counts, cumulative decade components, and annual births and deaths by estimation period | National, state, county | `BIRTHS` and `DEATHS` annually; decade-cumulative migration and residual (not annual) |
| 1980 to 1989 | County estimates by age, sex, race | `datasets/1980-1990/counties/asrh/pe-02.csv` (an `.xls` sits beside it; use the CSV) | CSV by age, sex, race cell | County | `POPESTIMATE` only, by summing cells |
| 1970 to 1979 | Intercensal county totals | `tables/1900-1980/counties/totals/e7079co.txt` (a `.zip` sits beside it) | Fixed-width text with a prose preamble; issued April 1982 | County (state rows to be confirmed in the file) | `POPESTIMATE` only |
| 1970 to 1979 | County estimates by age, sex, race | `tables/1900-1980/counties/asrh/co-asr-7079.csv` with `technical-documentation/file-layouts/1900-1980/co-asr-7079-layout.txt` | Variable-length CSV: year, state and county FIPS, race and sex code, then age bands | County | Cross-check for `e7079co.txt` totals only |

Not found on the site: a 2010 to 2020 county intercensal totals file (only age, sex, race detail exists so far), a 2000 to 2010 intercensal county totals file at `intercensal/county-total/` (404), and any 1990s county components-of-change file. The plan takes the closest published substitute in each case and records the substitution in the dataset's title.

### Resulting coverage per measure

| Measure | 1970s | 1980s | 1990s | 2000s | 2010s | 2020s |
| --- | --- | --- | --- | --- | --- | --- |
| `POPESTIMATE` | 1971-1979 | 1981-1989 | yes | yes | yes | yes |
| `CENSUSPOP` | April 1970 | April 1980 | no | April 2000 | April 2010 | no |
| `BIRTHS`, `DEATHS`, `NATURALCHG` | no | not served | no | yes | yes | yes |
| `NPOPCHG`, `RESIDUAL` | no | not served | no | yes | yes | yes |
| `INTERNATIONALMIG`, `DOMESTICMIG`, `NETMIG` | no | not served | no | yes | yes | yes |
| Rates (`RBIRTH`, `RDEATH`, `RNATURALCHG`, `R*MIG`) | no | no | no | yes | yes | yes |
| `ESTIMATESBASE` | no | no | no | yes | yes | yes |

Corrected against what was actually registered. Three entries moved as the products were read rather than assumed:

- **July 1980 has no estimate.** Both printed tables treat April 1980 as the decade boundary: the 1970s table ends at 1979 and the 1980s table opens on the census column. The year is absent and left absent.
- **The 1980s components are not served** (see *Remainders*).
- **`CENSUSPOP` is a delivered measure**, and the current-decade files publish no census column at all, so those products do not declare one.

Gaps in this table are the Bureau's, not the pipeline's. The catalog must state coverage per measure so the explorer's quality panel reports it rather than presenting a 1970 start for every PEP metric.

## Decisions

Recorded so the implementer does not re-litigate them. A change to any of these is a material scope change requiring user approval.

1. **One dataset per published product, one release per vintage of it.** Each row in the era table above becomes its own `PEPDataset` with its own `decennial_base`, `data_url_template`, layout, parser, and encoding. The 2020s datasets keep their codes and behaviour. Nothing merges files across decades before the silver fact; the seam is resolved in gold.
2. **Metric identity does not change with era.** Every product maps its variables onto the existing `silver_pep.dim_measure` vocabulary (`NATURALINC` to `NATURALCHG`, `TOT_POP` at `AGEGRP=0` to `POPESTIMATE`, and so on). `dataset_code` and `pep_vintage` stay on every row as dimensions, so lineage to the file is never lost and the as-released surface can show a decade's several publications side by side.
3. **The latest relation ranks across datasets, per metric, geography, and observation year.** Precedence for one observation year: an intercensal series covering it beats any postcensal series; among postcensal series the newest vintage wins; among equal vintages the product with the finer native grain wins (a county file's state rollup loses to the state file). The rule lives in one `CASE` in `gold_pep` and is unit-tested against the DDL text. This also retires today's doubled state rows.
4. **Census counts and estimate bases are not July estimates.** `CENSUS2010POP`, `CENSUS2000POP`, the April 2000 and April 2010 rows in the intercensal `YEAR` code, and the 1980 and 1990 counts in `comp8090` load under `ESTIMATESBASE` or a `CENSUSPOP` measure with their April reference date, never as a `POPESTIMATE` for that year. This is what keeps 2010 and 2020 from colliding across decades: the July estimate for each seam year exists in only one series family.
5. **Decade-cumulative components are not annualised.** The 1980s cumulative migration and residual figures load with a ten-year period (`period_start` 1980-04-01, `period_end` 1990-03-30, as the documentation states) under their own measure codes, or are left out. They are never divided by ten to fake annual rows.
6. **Age, sex, and race detail is read only to sum to totals.** The 1970s, 1980s, 1990s, and 2000s intercensal files are demographic-characteristic products. This plan sums them to the all-ages total and records that derivation on the dataset; demographic characteristics remain the separately gated PEP-006 follow-on.
7. **Geography resolves against the reference with the release's basis date; nothing is crosswalked by hand.** A 1970s county that no longer exists (Yellowstone National Park, Montana; the pre-1983 Arizona and New Mexico boundaries; Virginia independent cities since merged; Alaska boroughs; Dade County before 1997; Shannon and Wade Hampton before 2015; every Connecticut county after the 2022 planning-region change) lands in `silver_ref.geography_resolution` as unmapped and is counted in the release's completeness record. Unmapped rows stay in the fact with their source codes and are excluded from the served relations by the existing `resolution_status = 'resolved'` filter. A later plan may add a documented crosswalk; this one reports the gap.
8. **Exact source text is retained for every era.** Fixed-width and zipped products go through the same raw capture as the CSVs: the official file is captured byte-for-byte, the parser version is recorded, and `value_source` keeps the source text. Where a CSV and an XLS of the same table both exist, the CSV is the registered product.
9. **The explorer gets an API parameter, not a client special case.** PEH-006 adds an additive, optional parameter to the neutral observations resource that answers one newest row per geography. The web explorer's map requests it once the capability entry declares it; the trend panel still reads the full series through the existing history request.

## Non-goals

- No subcounty (place, county subdivision) history. The 2020s `pep_subcounty` product is unchanged.
- No demographic characteristics (age, sex, race, Hispanic origin) as served measures; see decision 6 and PEP-006.
- No hand-built county crosswalk. Decision 7 records what is unmapped and stops there.
- No change to the 2020s releases, their DAG chunking, or the capture path.
- No web feature work beyond consuming the declared parameter from PEH-006. The status pill, period counter, and newest-per-geography reduction landed on 2026-09-10 and are not this plan's.
- No Puerto Rico municipio history; the 2020s scope did not include it and the historical files carry it inconsistently.

## Implementation phases

### PEH-001 — Registry contract covers eras, series kinds, and per-release observation ranges

Deliverables:

- `PEPDataset` gains `series_kind: Literal["postcensal", "intercensal"]`, `era: str` (the decade folder, for example `2010-2020`), `derivation: str | None` (recorded when totals are summed from characteristic cells), and `native_grain: str` (the finest summary level the file carries, used by the precedence rule in PEH-002). `parser_version` and `text_encoding` accept the new parsers and `latin-1`.
- `PEPRelease` gains `observation_start_year` and `observation_end_year` as required inputs from the release definition rather than constants, and `__post_init__` requires `observation_end_year == vintage_year` only for `postcensal` datasets. An intercensal release records the publication vintage separately from the last observation year.
- `_release()` reads the range from its arguments; the 2020s releases are re-expressed through the same helper with `2020` passed explicitly and produce byte-identical `PEPRelease` values (unit test).
- `PEPRegistry.get_current_release` and `discover_releases` treat each dataset independently, so a historical dataset with one immutable release is "current" for itself and never competes with the 2020s vintage for the source's current vintage summary.
- The transport layer accepts a `.zip` product: the registered `data_url` is the archive, the release names the member file, and raw capture stores the archive bytes.

Acceptance:

- `tests/unit/census_pep/test_config.py` and `test_registry.py` cover a postcensal and an intercensal release, a zip-transported release, and the reversed-range and off-host rejections that exist today.
- The existing 2020s registry tests pass without modification to their expectations.

### PEH-002 — Cross-dataset latest selection with the seam rule

Deliverables:

- `gold_pep.population_estimate_latest` partitions by `metric_code, geo_id, observation_year` (not `dataset_code`) and orders by the precedence in decision 3, sourcing `series_kind` and `native_grain` from `silver_pep.pep_release` or `dim_dataset`. `population_estimate_revision` is unchanged, so every publication remains readable as released.
- `mv_pep_latest` and `measure_export` follow: `valid_time_grains` and any coverage summary published to the glossary reflect the union across datasets, and the publisher emits `first_period` and `last_period` per measure from the data.
- A DDL-text unit test (alongside `tests/unit/shared/test_incremental_serving_contract.py`) asserting the latest partition key contains no `dataset_code` and that the precedence `CASE` names `intercensal` first.
- A real-database test seeding two datasets with the same county, measure, and year (a state file rollup and a county file; a postcensal and an intercensal) and proving one row survives with the expected provenance.

Acceptance:

- `GET /observations?metric_code=CENSUS_PEP:BIRTHS&scope=latest&geo_level=STATE` answers exactly 52 rows per period (today 104).
- `scope=as_released` still answers every publication, including both state-grain products.

### PEH-003 — 2010 to 2020 series

Deliverables:

- Datasets `pep_county_alldata_2010s` and `pep_nst_alldata_2010s` (Vintage 2020, base 2010) registered with one `published` release each; the existing wide-CSV parser handles both after a variable-name mapping table gains `NATURALINC`.
- Fixtures under `tests/fixtures/census_pep/` with a representative county and state slice and a `README.md` entry stating the exact rows and their source values, following the pattern of `nst_2025.csv`.
- Replay proves idempotency: second replay inserts zero rows.
- The `census_pep_ingest` DAG picks up the new datasets through the registry; no DAG code change is expected, and the plan records that this was verified.

Acceptance:

- `GET /observations?metric_code=CENSUS_PEP:POPESTIMATE&scope=latest&geo_id=state:06|county:037` answers July 2010 through July 2025 with 2010 to 2019 attributed to `pep_county_alldata_2010s`, vintage 2020, and 2020 onward to `pep_county_alldata`, vintage 2025.
- `tests/run.ps1 etl` and `integration` pass with the new fixtures.

### PEH-004 — 2000 to 2010 series

Deliverables:

- Dataset `pep_county_alldata_2000s` (Vintage 2009, base 2000; components and rates) through the same parser.
- Dataset `pep_county_intercensal_2000s`: one release, 51 state files captured as separate members of one release load (the `release_load` completeness record counts files, and a missing state file fails the release). Parser `census-pep-long-csv-v1` reads `AGEGRP=0` rows and maps the `YEAR` code to the estimate date per the layout, loading the April 2000 and April 2010 rows under the census-count measure per decision 4.
- Precedence from PEH-002 makes the intercensal totals the latest `POPESTIMATE` for 2000 to 2009 and leaves Vintage 2009 as the latest source of components.

Acceptance:

- One county's `POPESTIMATE` for 2005 comes from the intercensal dataset and its `BIRTHS` for 2005 from Vintage 2009, both visible under `scope=as_released`.
- The release load for the intercensal product records 51 captured files and `complete`.

### PEH-005 — 1990s, 1980s, and 1970s series

Deliverables:

- Parser `census-pep-fixed-width-v1` driven by a per-dataset column specification recorded in the dataset (start, width, name) transcribed from the official layout document, with the layout URL on the release. Prose preambles are skipped by a documented rule (first line matching the layout's record pattern), never by a hard-coded line count.
- Dataset `pep_county_rh_1990s` (CO-99-10): totals by summing the race and Hispanic-origin cells per county and year; the derivation is recorded on the dataset.
- Dataset `pep_county_components_1980s` (comp8090): annual `BIRTHS` and `DEATHS` by estimation period with the period bounds the documentation states; decade-cumulative rows per decision 5; 1980 and 1990 counts under the census-count measure.
- Dataset `pep_county_asrh_1980s` (pe-02.csv): `POPESTIMATE` 1980 to 1989 by summing cells.
- Dataset `pep_county_intercensal_1970s` (e7079co.txt): `POPESTIMATE` 1970 to 1979. `co-asr-7079.csv` is captured and used only as a fixture cross-check that the summed cells equal the published totals for a sampled county.
- Every legacy release runs through the geography resolution with its basis date, and the evidence record lists the unmapped counties per release with counts.

Acceptance:

- A sampled county (one that has existed unchanged since 1970, for example Autauga County, Alabama) answers 56 `POPESTIMATE` points from 1970-07-01 to 2025-07-01 under `scope=latest`.
- A county created or dissolved inside the range (Broomfield County, Colorado, 2001; Yellowstone National Park, Montana, dissolved 1997) appears only for the years its geography resolves, and the unmapped rows are counted in the evidence record.
- `tests/run.ps1 external` (`tests/external/test_source_contracts.py`) gains a contract check per legacy URL that the file still starts with the expected header or preamble signature.

### PEH-006 — Newest-per-geography parameter on the neutral observations resource

Deliverables:

- An additive, optional parameter on `GET /api/v1/observations` (name to match the guide's vocabulary; `newest_per_geography=true` is the working name) valid only with `scope=latest`, that ranks one newest row per geography inside the source relation using the same `ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY period_start DESC)` the comparison and distribution services already use. It composes with `geo_level`, `state_fips`, and the source's declared dimension filters. Sending it with `scope=as_released` is a 422, matching how `release` is guarded today.
- `/catalog/capabilities` declares the parameter in each source's neutral observation filters so the explorer discovers it rather than assuming it.
- `docs/reference/API_CONSUMER_GUIDE.md` documents the parameter and states that the v1 default (the whole latest publication) is unchanged.
- Web: `buildLatestObservationRequest` sends the parameter for the map read when the capability declares it, and the history request never does. A unit test in `tests/frontend/unit/observation-access.test.js` proves both, and one proves an undeclared parameter is not sent.

Acceptance:

- `GET /observations?metric_code=CENSUS_PEP:POPESTIMATE&scope=latest&geo_level=COUNTY&newest_per_geography=true` answers `total: 3144` and one row per county, and its counts match `/distribution/bins` for the same metric and grain.
- The explorer's PEP county map loads in one page after PEH-005's history is present, and the observations status pill reports one period.
- `tests/run.ps1 api` and `web-unit` pass.

### PEH-007 — Full PEP serving refresh, glossary harvest, and evidence record

Deliverables:

- Operator note (here, and in `docs/reference/BETA_RESET_REINGESTION.md` if that is the right home) for the one-time ingestion order: PEH-003, then PEH-004, then PEH-005, each followed by the gold refresh, because the seam rule needs both sides present to be checked.
- `glossary_harvest` run after the refresh; the PEP measures' `first_period` moves to 1970 for `POPESTIMATE`, 1980 for `BIRTHS` and `DEATHS`, 2000 for the migration components, and the catalog's per-measure coverage matches the table above.
- `docs/reference/API_CONSUMER_GUIDE.md`: a PEP section stating which decades come from which product, that `dataset_code` and `vintage` identify the publication on every row, and that census counts are a separate measure from July estimates.
- `docs/reference/TESTING_CONTRACT.md`: rows for the cross-dataset latest rule, the census-count separation, and the newest-per-geography parameter, mapped in `CI_EVIDENCE_MAP.md`.
- This plan's evidence section filled with the exact commands, row counts per dataset, unmapped-geography counts per release, and refresh durations.

Acceptance:

- `GET /catalog/metrics/CENSUS_PEP:POPESTIMATE` reports a first period of 1970-07-01.
- The explorer's PEP quality panel shows per-measure coverage without any client change.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| Registry contract | `etl` | Postcensal and intercensal releases register; the 2020s releases are byte-identical to today |
| Parsers | `etl` | Wide CSV with legacy spellings, long CSV with `AGEGRP=0` and `YEAR` codes, fixed-width with preamble, zip member extraction; exact source text retained |
| DDL text contract | `unit` | Latest partition excludes `dataset_code`; precedence names `intercensal` first; census counts never load as `POPESTIMATE` |
| Real-database seam | `integration` | Two datasets, one year, one geography: one latest row with the expected provenance; as-released keeps both |
| Capture replay | `integration` | Second replay of each historical release inserts zero rows |
| Registry dispatch | `api` | Newest-per-geography parameter ranks inside the relation, composes with filters, rejects `scope=as_released` |
| Source contracts | `external` | Each legacy URL still answers with its expected signature |
| Explorer request building | `web-unit` | Parameter sent only when declared and only on the map read |

## Risks and mitigations

- **Legacy files change or vanish.** They are archival and have been stable for a decade or more, but the `tables/1900-1980/` path is a table archive rather than a dataset directory. Raw capture keeps the bytes, so a later disappearance blocks re-capture only, not replay.
- **Geography drift silently thins the map.** Decision 7 makes unmapped rows visible in the resolution table and the evidence record. Check Connecticut in particular: the 2022 planning regions mean every historical Connecticut county is unmapped against a 2025 basis unless the reference carries historical entities. Record the count; do not fabricate a mapping.
- **The seam rule hides a real revision.** A county whose 2020 estimate differs between Vintage 2020 and Vintage 2025 will show only the 2025 value under `scope=latest`. That is the documented meaning of latest; `scope=as_released` shows both, and the guide must say so.
- **Fixed-width transcription errors.** Each column specification is transcribed from the official layout and tested against a fixture row whose values are quoted in the fixture README from the source file; a one-column offset fails the test rather than loading shifted values.
- **Volume.** About 3,144 counties times 56 years times up to 16 measures is under two million county rows, plus state rows; well within the fact table's design. The explorer is protected by PEH-006; without it the PEP county map would exceed the 40,000-row paging bound after PEH-004.
- **Summed totals differ from published totals by rounding.** Where a decade has both a totals file and a characteristics file (1970s), the fixture cross-check tolerates zero difference; a nonzero difference is recorded as a finding, and the totals file wins.

## Open questions, as resolved

1. **The 1980s components are not loaded**, cumulative or annual. The reason turned out to be stronger than the trade-off the question anticipated: the Bureau publishes them by estimation period, and its first and last periods run 15 and 9 months (April 1980 to June 1981, July 1989 to March 1990). The silver fact carries one observation date, so any load would have to restate those periods as July-to-July years. Decision 5 forbids faking annual rows, and the same reasoning forbids faking annual periods. Recorded under *Remainders*.
2. **A separate `CENSUSPOP` measure was added**, as the question's own reading preferred. It is dated to 1 April and is what keeps a decade's closing count from competing with the next decade's opening estimate for the same geography and year. The current-decade files publish no census column, so those products do not declare one.

## Remainders

Both are deliberate, and neither is a gap in the objective.

- **1980s components of change (`comp8090.zip`).** Not served, for the reason in question 1 above. Serving them faithfully needs period bounds on the silver fact and through the gold and API layers, which is a cross-source schema change a single-source plan should not make on its own. The release contract already carries `archive_member`, so the capture side is ready if a later plan adds the period bounds.
- **`first_period` / `last_period` are exported, not published.** They are read from the facts in `gold_pep.measure_export`, so per-measure coverage is queryable and tested. Surfacing them in the catalog means widening `metric_publisher`, whose column shape is a cross-source contract that ARC-001 guards; a single-source plan widening it unilaterally is the failure that test exists to prevent.

## Implementation evidence

Recorded 2026-09-10 on `feat/census-pep-history`, branched from `main` at
the merge of the web first wave (789f56e).

### What is registered

Ten products across six decades, thirteen releases, bootstrapping cleanly:

| Era | Product | Vintage | Observations | Series |
| --- | --- | --- | --- | --- |
| 1970s | `pep_county_totals_1970s` (E7079CO) | 1982 | 1970-1979 | intercensal |
| 1980s | `pep_county_totals_1980s` (E8089CO) | 1992 | 1980-1989 | intercensal |
| 1990s | `pep_county_totals_1990s` (CO-99-10) | 1999 | 1990-1999 | postcensal |
| 2000s | `pep_county_alldata_2000s` (CO-EST2009-ALLDATA) | 2009 | 2000-2009 | postcensal |
| 2000s | `pep_county_intercensal_2000s` (CO-EST00INT-TOT) | 2016 | 2000-2010 | intercensal |
| 2010s | `pep_county_alldata_2010s` (CO-EST2020-ALLDATA) | 2020 | 2010-2020 | postcensal |
| 2010s | `pep_nst_alldata_2010s` (NST-EST2020-ALLDATA) | 2020 | 2010-2020 | postcensal |
| 2020s | the three existing products | 2024, 2025 | 2020-vintage | postcensal |

### Parsed against the real files

Every registered historical file was fetched and parsed end to end, and
each passed its own completeness contract:

| File | Values | Geographies | Range |
| --- | --- | --- | --- |
| `e7079co.txt` | 31,900 | 3,138 counties + 51 states + US | 1970-1979 |
| `e8089co.txt` | 31,930 | 3,141 counties + 51 states + US | 1980-1989 |
| `co-99-10.txt` | 31,410 | 3,141 counties | 1990-1999 |
| `co-est2009-alldata.csv` | 466,324 | 3,143 counties + 51 states | 2000-2009 |
| `co-est00int-tot.csv` | 41,522 | 3,143 counties + 51 states | 2000-2010 |
| `co-est2020-alldata.csv` | 514,234 | 3,143 counties + 51 states | 2010-2020 |
| `nst-est2020-alldata.csv` | 9,177 | US, regions, 51 states | 2010-2020 |

Values were checked against the printed source: Autauga County's 1970
census count reads 24,460 and its 1975 estimate 29,700, the United States
counted 226,542,250 in 1980, and Autauga's 1990 population sums to 34,356
from its eight published cells.

### What the warehouse serves

A real PostGIS 16 database, bootstrapped from all 32 manifest assets:

- Autauga County's `POPESTIMATE` runs 1971 to 2025, one row per year, no
  year answered twice, across six products and three readers. The only
  absent year in that span is 1980.
- `CENSUSPOP` answers 1970, 1980, 2000 and 2010, each dated 1 April, beside
  the July estimates for the same years.
- 2005 resolves to the intercensal publication (4,569,805 for Alabama)
  rather than the postcensal projection (4,545,049); both remain readable
  under `scope=as_released`.
- July 2020 resolves to Vintage 2025's 331,578,104 rather than Vintage
  2020's 329,484,123.
- Alabama's 2015 state row resolves to the state file rather than the
  county file's rollup, which also retires the doubled state rows the
  warehouse serves today.

### The API parameter, on live data

Against the running development warehouse, `CENSUS_PEP:BIRTHS` at county
grain:

| Read | Rows |
| --- | --- |
| `scope=latest` (unchanged default) | 18,864 |
| `scope=latest&newest_per_geography=true` | 3,144 |
| `/distribution/bins` total | 3,144 |

### Verification

| Command | Result |
| --- | --- |
| `pytest tests/unit` | 1,261 passed |
| `pytest -m "unit and api" tests/unit/api` | 253 passed |
| `npx vitest run` (web unit) | 192 passed |
| `npm run typecheck` / `npm run lint` | clean |
| `ruff check src tests apps` | clean |
| `pytest tests/integration/database/test_pep_capture_flow.py` | 4 passed |
| `pytest -m "integration and not e2e" tests/integration` | 108 passed, 6 failed |

The six integration failures are in `test_quality_assessment.py` and
`test_source_quality_checks.py`. They reproduce identically with this
plan's two new PEP tests deselected, and the failing assertion is a FRED
rule reporting that the warehouse is not empty, so they are whole-tier
ordering artefacts of a shared local database rather than anything this
work changed. `tests/integration/database/test_usda_nass_dag_tasks.py`
cannot be collected on this host at all: Airflow rejects the Windows
temporary-directory path as a relative SQLite URL, which also predates
this work.

### Notes for the operator

- Ingestion order for the one-time backfill: newest decade first, then
  each earlier one, refreshing gold between them. The seam rule needs both
  sides of an overlap present to be checked, and each refresh is
  idempotent.
- The archival products are immutable, so their releases are `published`
  and each is its own current release. Re-running the DAG re-captures
  about 9 MB and inserts nothing new.
- `CI_EVIDENCE_MAP.md` needed no new row: it maps contracts to jobs by
  owning path, and every path this plan touches (`src`, `sql/migrations`,
  `sql/bootstrap`, `apps/api`, `apps/web`) is already owned by an existing
  row. The new catalog IDs are ETL-043 to ETL-046, API-066 and WEB-028.
