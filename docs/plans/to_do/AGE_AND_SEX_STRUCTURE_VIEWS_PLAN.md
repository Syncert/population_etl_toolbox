---
id: age-and-sex-structure-views
depends_on:
  - place-pages
  - api-per-capita-and-shares
  - web-cross-level-compare
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/api tests/unit/semantics tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "age_structure" -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place shows who lives there by age and sex, the way demographers draw it

## Status

To do: **discovery draft**, written 2026-10-31 at Nick's request ("age
analytics requires pretty specialized charts if not also specialized data
views ... better with stacked bars spanning both ways for male and female").
Nick approved the direction on 2026-10-31 (19:05 UTC, "I like this") with
one amendment: the comparison is a **toggle across levels** -- a city
against its county, state or the nation; a county against its state or the
nation. [ADR-0013](../../decisions/0013-age-and-sex-structure.md) is
accepted with that amendment. His approval is read as taking the
recommended option of each decision below (2a, 3 yes, 4 yes); a different
answer from him replaces that reading. Next it splits into an API item and
a web item in the usual way (A-n then D-n); the deliverables below are
already grouped that way.

A visual mock of the pyramid, cards and difference bars with illustrative
numbers (not warehouse data) is at
<https://claude.ai/artifact/Pt7U3yxUU5EX4MZcMZyaE4> (private to Nick).

## Decisions (recommended options taken 2026-10-31)

1. **ADR-0013** as proposed (eighteen five-year bands regrouped from B01001
   with Bureau margins; pyramids drawn as shares of each place's own total;
   dependency ratios on the Bureau's under-18 / 18–64 / 65+ definition;
   cohorts only across non-overlapping periods). Accept, or amend.
2. **Where grain-wide age indicators come from** (for maps and rankings of
   share 65+, dependency ratios):
   - (a) *recommended*: a derived gold relation
     `gold_census.age_structure_summary`, one row per place and release,
     built in the ACS publish step (ADR-0013 rule 8);
   - (b) compute at request time only, and offer no grain-wide age maps
     beyond the provider's median age;
   - (c) ingest the Bureau's S0101 subject table for provider-published
     ratios (recent releases only, from about 2017; a new ingestion path), and derive before 2017.
3. **Annual county age change from PEP.** Add the Population Estimates
   Program's age-and-sex characteristics (county, state, nation; annual;
   no sampling error), starting by re-parsing the 2000–2010 intercensal
   files the warehouse **already captured** (only `AGEGRP=0` is parsed
   today), then the 2010s and 2020s characteristics files.
   *Recommended: yes*, as a second item after the ACS views; ACS stays the
   only source for cities, metros and tracts, and the two programs are never
   mixed in one chart.
4. **Census 2000 pyramid.** Crosswalk SF1 table P012 (sex by age, the same
   23 bands as B01001, 46 lines) so every pyramid can reach back to 2000.
   *Recommended: yes*; it is a crosswalk entry, not a new source. 1990
   (STF1A) needs its tables checked first (open question 3).

## Why

Age is the single strongest structural fact about a place: a college town,
a retirement county, a young suburb and a shrinking rural county have
recognisably different shapes, and those shapes explain much of what the
other chapters show (school enrolment, labour force, health, housing turnover).
The site currently shows median age, people under 18 and people 65 and over
as three numbers in the People chapter, and any B01001 line as a one-measure
map. A map of "women aged 22 to 24" tells a reader almost nothing. The
established views are the population pyramid, its comparison against a
reference population, broad-group shares over time, and dependency ratios.

## Inventory: what the warehouse holds that splits by age

Drawn from the source modules and `docs/warehouse/data-sources.md` on
2026-10-31 (dev at 923d777). "Loaded" means the code registers and publishes
it; live coverage is still the `coverage.sql` count.

### Loaded and served: age by sex

| Source | Table / measure | Split | Grains | Years | Notes |
|---|---|---|---|---|---|
| ACS (`CENSUS_ACS`) | **B01001** Sex by age | 23 unequal bands × 2 sexes (46 lines + 3 totals) | nation, state, county, place, metro, **tract** | acs1 2005–2024 (no 2020), acs5 2009–2024 | The pyramid's source. In `tract_tables`; tract history reload in progress (2019–2024 done 10-31). |
| ACS | **B01002** Median age by sex | median: all, men, women | all of the above, tract | as above | Provider medians; never re-estimated from bands. |
| ACS | B17001 Poverty status by sex by age | 2 sexes × 13 age bands × below/at-or-above poverty | all, tract | as above | Gives a *poverty pyramid* / poverty rate by age and sex. |
| ACS | B21001 Veteran status by sex by age | 2 sexes × 5 age bands (18–34 … 75+) | all but tract | as above | Veterans' age profile. |
| Census 2000 (`dec2000sf1`) | P013001 Median age | median, both sexes | as loaded | 2000 | Only the median is crosswalked; P012 (sex by age) is not (decision 4). |

### Loaded and served: age only (no sex)

| Source | Table / measure | Split | Grains | Years |
|---|---|---|---|---|
| ACS | B09001 Population under 18 by age | 7 child bands (under 3 … 15–17) | all but tract | as above |
| ACS | B09020 Population 65+ by relationship | total 65+ by household type | all but tract | as above |
| ACS | B07001 Mobility in the past year by age | 15 age bands × 5 mobility categories | all but tract | as above |
| ACS | C18108 Age by number of disabilities | under 18, 18–64, 65+ | all but tract | as above |
| ACS | B27010 Health insurance by age | under 19, 19–34, 35–64, 65+ | all, tract | as above |
| ACS | B11003 / B11005 Households and families by presence of children | presence of own children under 18 / of people under 18 | all but tract | as above |
| SAIPE | `SAEPOV0_17`, `SAEPOVRT0_17` | children under 18 in poverty, count and rate | nation, state, county | 1989–2024 |
| SAHIE | `NUI`, `PCTUI` | uninsured people under 65 (`AGECAT=0` only) | nation, state, county | 2006–2023 |
| BLS CPS (`ln`) | `LNS12300060`, `LNS11300060` | employment-population ratio and participation, ages 25–54 | nation | monthly |
| CDC PLACES | every county measure | adults 18+; `AgeAdjPrv` (age-adjusted) and crude | county | 2025 release |
| USDA ERS | `retirement_destination` | county typology flag | county | 2025 codes |

### Captured or published by the provider, not modelled

| Source | What exists | Where it stops today |
|---|---|---|
| PEP intercensal 2000–2010 | `co-est00int-alldata` long files with `AGEGRP` (18 five-year groups) × sex × year | **Captured**; the parser keeps `AGEGRP=0` only. Re-parsing gives annual county pyramids 2000–2010 without a new download. |
| PEP 2010s and 2020s characteristics | `cc-est20xx-agesex` / `alldata` (county, annual, age groups by sex) | Not captured. |
| Census 2000 SF1 P012 | sex by age, the same 23 bands as B01001 | Not in `decennial_crosswalk.json`. |
| LODES RAC / WAC | `CA01` (29 or younger), `CA02` (30–54), `CA03` (55+) jobs, by home and work block | Columns are in the registered layout; gold publishes only `C000`-based totals. Gives "age of the workforce living / working here", 2002–2023. |
| NCES CCD membership | `GRADE` × `SEX` × race student counts per school | Only the education-unit total is registered. Grade-by-sex enrolment is a school-age pyramid. |
| SAIPE | `SAEPOV5_17R`, `SAEPOV0_4` (related children 5–17, under 5) | Not registered. |
| SAHIE | `AGECAT` 1–5 (18–64, 40–64, 50–64, under 19, 21–64) | Pinned to `AGECAT=0`. |
| CDC CDI | stratified rows (`dim_stratum`), state and nation | Strata are kept in silver and gold; which indicators carry age strata needs a live count. |

### No age split

FBI UCR (summarized offenses; arrests by age would be a different FBI
product), BEA, QCEW, CBP, BPS, IRS county-to-county migration, HUD FMR/IL,
FHFA, FEMA NRI, FCC, EPA, NOAA, EIA, FRED.

## Proposed views

Each is a component in the People chapter's new **"Age and sex"** section on
the place page (nation, state, county, city, metro, tract), unless noted.

1. **Population pyramid with a reference outline and a level toggle.**
   Eighteen five-year bands, men left and women right, each bar the band's
   share of the place's total. The comparison place is drawn as an outline
   over the bars and chosen with a toggle of the place's enclosing levels
   (Nick, 2026-10-31):

   | Page | Toggle (default first) |
   |---|---|
   | City | County · State · Nation · None |
   | Tract | County · State · Nation · None |
   | County | State · Nation · None |
   | Metro | Nation · State (only when the metro lies in one state) · None |
   | State | Nation · None |
   | Nation | None |

   A city that spans counties compares against the county holding most of
   its area (the reference's recorded overlap), named in the toggle, from
   the existing relationships. The same toggle drives the summary cards'
   comparison values and the "where this place differs" bars, so one choice
   changes the whole section. The compare page can still overlay any other
   place. Margin of error as a whisker on each bar; a band
   whose coefficient of variation exceeds 30 % is hatched and says so. Hover
   or focus shows count, share, margin and the parent's share. A table
   toggle shows the same numbers. Release picker (1-year where published,
   else 5-year).
2. **"Where this place differs" bars.** For each band, the place's share
   minus the reference's share, men and women, as a diverging bar. This is
   the clearest read of a college town or a retirement county, and it is
   what the eye is doing with the outline anyway. Shown only where the
   intervals do not overlap or marked where they do (no verdict words,
   ADR-0010).
3. **Summary cards.** Median age (men, women, all; provider's), share under
   18, share 65+, share 85+, child, old-age and total dependency ratios, sex
   ratio. Each card with the parent's value beside it and its margin.
4. **Age structure over time.** Two forms, one data read:
   - shares of under 18, 18–64, 65+ and 85+ as lines across releases
     (non-overlapping 5-year windows by default; every 1-year release for
     places that have them);
   - the pyramid with an earlier release as a ghost outline (2000 census when
     decision 4 is taken).
   For counties and up, PEP's annual series feeds the same chart, labelled as
   PEP (decision 3).
5. **Cohort tracking.** "People aged 25 to 29 in 2009–2013 were 30 to 34 in
   2014–2018 and 35 to 39 in 2019–2023": a slope chart per band (or a
   cohort-shift pyramid: each bar is the band, its outline the same
   generation five years earlier, shifted up one band). Only
   non-overlapping periods; refusals as ADR-0013 rule 6 words them. The
   sentence is a template under ADR-0011 ("grew by" / "fell by" with the
   interval, never "people fled").
6. **Age-specific rates.** Paired bars by age band for the by-age tables:
   poverty rate by age and sex (B17001), uninsured share by age (B27010),
   disability by broad age (C18108), moved in the past year by age (B07001),
   veterans by age (B21001). Each is a share pairing in the ADR-0013 scheme
   record, so the API serves the rate and its margin.
7. **Maps of summary indicators only.** Median age (already a published
   measure), share 65+, old-age and child dependency ratios on the
   one-measure map (`/map/<metric>`), quantile bands as every county map.
   Single B01001 lines stop being offered as maps (they remain readable as
   data in the explorer). Depends on decision 2.
8. **Compare pages.** `/compare` (two places) draws both pyramids as shares
   on a shared axis, or one overlaid on the other; ADR-0010 already allows
   shares across levels.
9. **Later, once modelled:** workforce age (LODES CA01–CA03: "jobs held by
   people 55 and over" living here vs working here), school enrolment by
   grade and sex (NCES), PEP annual county pyramids animated by year.

## Deliverables

### Semantics and ADR

1. ADR-0013 accepted (or amended) by Nick.
2. `docs/semantics/age_bands.json` with its JSON Schema and loader: the
   five-year scheme for B01001 (acs1, acs5) and, with decision 4, SF1 P012;
   the broad groups (under 18, 18–64, 65+, 85+); the indicator definitions;
   the by-age rate schemes for B17001, B27010, C18108, B07001, B21001. Every
   entry `draft` until Nick reviews it, like the plain labels.
3. If decision 4: P012 lines in `decennial_crosswalk.json` (46 lines plus
   the two sex totals), the 2000 load re-run for those lines only.

### API (one item)

4. `GET /api/v1/age-structure?geo_id=&dataset=acs5&release=latest|YYYY`:
   the place's eighteen bands per sex (count, share of total, margins),
   totals, provider medians, the summary indicators, the release and
   period, the scheme's identity and version, `derived: true` on every
   computed value, and refusals per band (`suppressed_input`,
   `line_missing`) never zero, **and the same block for every enclosing
   level** the toggle offers (`references: [{level, geo_id, name,
   share_of_place_inside, ...}]`, resolved once from the existing
   relationships), with `default_reference`. Answering every level in one
   response means the toggle switches in the browser with no further
   request. Up to three extra `compare_geo_ids` for the compare page.
5. `GET /api/v1/age-structure/history?geo_id=&dataset=acs5&periods=non_overlapping|all`:
   per release, the broad-group shares, the indicators and (on request) the
   bands, as released (`rpt_acs_observations`, not `mv_acs_latest`, which
   holds only each key's newest period).
6. `GET /api/v1/age-structure/cohorts?geo_id=&dataset=acs5&from=YYYY&to=YYYY`:
   each band in `from` beside the next band up in `to`, with change and its
   margin; refusals `overlapping_periods`, `boundary_changed`,
   `period_gap_not_five_years`.
7. `GET /api/v1/age-structure/rates?geo_id=&scheme=<id>`: the by-age rates
   with margins for one scheme, place and reference.
8. With decision 2(a): the summary indicators as served metrics
   (`CENSUS_ACS_DERIVED:acs5:old_age_dependency_ratio` style codes, flagged
   derived) so the existing one-measure map, ranking and compare routes
   read them unchanged.
9. Consumer guide section, OpenAPI snapshot, catalog rows.

### Warehouse (only with decision 2(a))

10. `gold_census.age_structure_summary`: one row per (`geo_id`,
    `dataset_code`, `vintage_year`, `observation_date`) with `geo_level`,
    `state_fips`, the broad-group counts and shares, the three dependency
    ratios and the sex ratio, each with its margin and a `unavailable_reason`
    column; scheme id and version on every row. Built from
    `rpt_acs_observations` inside the existing per-state publish, after the
    year's rows are written, so it never reads a half-published year.
    Indexes: unique on the key; `(geo_level, state_fips, dataset_code,
    vintage_year)` for one grain in one state. Size estimate: about 2 million
    rows (tracts 85,000 × 16 five-year releases dominate). A full rebuild is
    one grouped pass per year partition (parallel per state, as the publish
    already is).

### Web (one item)

11. `components/AgePyramid.tsx` (SVG, hand-drawn like `BarChart.tsx`; no
    new chart library, the `/layout` budget is 507 kB), loaded only on place
    and compare routes. Reference outline, whiskers, hatching for unreliable
    bands, keyboard focus per bar, table fallback, desktop and 390 px.
12. `AgeDifferenceBars`, `AgeSummaryCards`, `AgeStructureHistory`,
    `CohortChart`, `AgeSpecificRates` as described above, all reading the
    API's values and computing nothing (AGENTS.md).
13. The People chapter's "Age and sex" section in `lib/placeChapters.ts`;
    `/compare` pyramid; the one-measure map offers the age indicators (with
    decision 2) and stops offering single B01001 lines as maps.
14. Plain labels for every band and indicator (`MetricName` /
    `displayMetricName`; raw codes only inside `data-provenance`).

## Performance (prevention list applied)

- **One place's pyramid** reads 49 B01001 codes for at most 4 places (the
  place and its county, state and nation; the compare page adds up to 3):
  one statement,
  `metric_code = ANY(:codes) AND geo_id = ANY(:geos)` on
  `gold_census.mv_acs_latest`, served by `ix_mv_acs_latest_metric_geo`
  (`metric_code, geo_id`). About 200–350 rows. No new index. Plain equality
  only, no `UPPER()` or computed `COALESCE(...)::TEXT` in `WHERE`.
- **A release pin** reads `rpt_acs_observations` with `vintage_year = :n`
  (API-182) and `observation_date` bounds so partitions prune; served by
  `ix_rpt_acs_observations_metric_geo_date`.
- **History** for one place is 49 codes × one geography across releases:
  about 1,700 rows over the year partitions. With `periods=non_overlapping`
  the statement names three or four `observation_date` ranges so only those
  partitions are probed. `EXPLAIN (ANALYZE, BUFFERS)` on OFFICE_LAB for a
  tract, a city and a county before merge.
- **Grain-wide maps** never sum 46 lines per place per request: they read
  the summary relation (decision 2(a)) scoped to one grain in one state, the
  way city and tract maps already read (#192, #210). Without 2(a), no
  grain-wide age map beyond median age is offered.
- **The page batches**: the "Age and sex" section makes one `age-structure`
  request (the place and every level its toggle offers) and, when opened, one `history` request;
  never one request per band. The summary cards come from the same answer.
- **Per-request lookups once**: the reference place is resolved once per
  request, not per band.
- **Building the summary relation** reads each year partition once per state
  through `ix_rpt_acs_observations_level_state`; its indexes are pre-built
  `CONCURRENTLY` on OFFICE_LAB before `ensure_*` re-applies the DDL (same
  names; `ON ONLY` parent and `ATTACH` per partition if it is partitioned).
  It is never rebuilt in full during a publish.
- **The PEP characteristics** (decision 3) are about 3,200 counties × 18
  groups × 2 sexes × ~25 years ≈ 2.9 million rows: small; served from
  `silver_pep` facts with an index led by `(metric_code, geo_id)` as every
  silver serving read needs (#196).

## Acceptance criteria

1. For Dane County, Wisconsin (`state:55|county:025`), ACS 5-year newest
   release, the pyramid's eighteen bands per sex sum to B01001_002 and
   B01001_026 within rounding, each band's margin equals the root sum of
   squares of its declared lines, and the 20–24 band visibly exceeds the
   state outline (the university). A band with a suppressed line is drawn
   absent and named, never as zero (unit tests with fixtures; live check on
   OFFICE_LAB recorded in the plan).
2. On a city page the toggle offers County, State, Nation and None (County
   first); on a county page State, Nation and None; switching it redraws the
   outline, the cards' comparison values and the difference bars without a
   new request (browser tier counts requests). A city spanning counties
   names the county holding the largest share of its area and that share.
3. The child, old-age and total dependency ratios for a county equal the
   Bureau's S0101 published ratios for the same release within rounding
   (unit fixture copied from data.census.gov for two counties).
4. Median age shown is B01002's published value, never one estimated from
   bands.
5. A cohort request between overlapping 5-year releases is refused with
   `overlapping_periods`; between 2010-vintage and 2020-vintage tracts with
   `boundary_changed`; the sentence uses only ADR-0011's verbs.
6. Every derived value carries `derived: true`, the scheme id and version,
   and its margin method; a `draft` scheme is marked "Draft view awaiting
   review" on the page.
7. The "Age and sex" section makes at most two API requests per page view
   (browser tier counts them); the pyramid statement's `EXPLAIN` on
   OFFICE_LAB shows index scans on `ix_mv_acs_latest_metric_geo` and no
   sequential scan, recorded in the plan's evidence.
8. The pyramid is keyboard-navigable, has a table equivalent, passes the
   axe gate, reads at 390 px, and shows no raw variable code outside
   `data-provenance`.
9. With decision 2(a): the summary relation's rows equal the request-time
   computation for every county in one state (database tier), and the share
   65+ map for Texas counties renders from it in one request.
10. `/layout`, `/explore` and `/map` stay within their bundle budgets.

## Tests

- API unit: band regrouping per scheme; margin sums; proportion and ratio
  margins; controlled margins as zero; suppressed and missing lines;
  reference resolution per grain; cohort refusals; census year without
  margins; OpenAPI snapshot. New catalog rows (API-18x).
- Semantics: `age_bands.json` schema and loader; every declared line exists
  in `dim_acs_variable` fixtures; every band of a scheme is covered exactly
  once per sex.
- Database tier (decision 2(a)): `test_age_structure_summary.py` builds the
  relation on the Wisconsin fixture and compares it with the API service.
- Web unit: pyramid geometry (bars symmetric, outline on the same scale,
  absent band), difference bars, card formatting. Browser: a county page's
  "Age and sex" section, the compare pyramid, a tract with hatched bands,
  request count, axe, 390 px. New catalog rows (WEB-15x).
- `TESTING_CONTRACT` total recounted from the ranges table after merging
  dev, never hand-picked.

## Open questions

1. Small places: a tract's eighteen five-year bands often carry margins as
   large as the estimate. Collapse to ten-year bands below a population
   threshold, or always draw five-year bands with hatching? (Draft: always
   five-year, hatched; a "broad groups" toggle.)
2. Which release by default on a city page: newest 5-year (every place) or
   newest 1-year where it exists (65,000+)? (Draft: 5-year, with 1-year on
   the picker, as the rest of the page does.)
3. 1990: does STF1A publish sex by age in bands that regroup to the
   eighteen? Check `tables.dbf` on the archived discs before promising it.
4. Colours for men and women: two neutral hues from the site palette rather
   than blue and pink (the mock uses the site palette's blue and orange,
   which pass the colour-vision checks).
5. Should the PEP series (decision 3) carry 85+ only, or the Bureau's
   detailed 85–89 … 100+ where published? (Draft: 85+, matching the ACS.)

## Checkpoint

Split on 2026-10-31 into
[`API_AGE_AND_SEX_STRUCTURE_PLAN.md`](../in_progress/API_AGE_AND_SEX_STRUCTURE_PLAN.md)
(part 1: the semantics record and the three routes, with the contract fixed)
and [`WEB_AGE_AND_SEX_STRUCTURE_PLAN.md`](WEB_AGE_AND_SEX_STRUCTURE_PLAN.md).
Still held here for later items: API part 2 (the summary relation with its
index DDL, map metrics, by-age rates), PEP age-and-sex characteristics, and
the 2000 P012 crosswalk.

Next pickup: API part 1.
