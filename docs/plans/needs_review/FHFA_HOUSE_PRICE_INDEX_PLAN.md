---
id: fhfa-house-price-index
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/fhfa_hpi -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_fhfa_hpi_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# FHFA House Price Index: annual county repeat-sales price change

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/fhfa-house-price-index`, which is
`docs/scout-county-sources` (where this plan was written) with the work on
top.

## Why

ACS home values are survey medians that mix price change with changes in the
housing stock, and FRED gives only national price series. FHFA publishes an
annual, constant-quality, repeat-sales index for counties, which FHFA itself
contrasts with ACS medians
([HPI FAQ, Q3](https://www.fhfa.gov/document/d/hpi/hpi-faqs)). It gives the
Housing chapter a local "how much have prices risen here" figure beside the
ACS value cards, without claiming to be a price level.

## Verified contract

- **Index family.** The "Annual" HPI uses all-transactions data (purchase
  prices plus refinance appraisals) on Enterprise-acquired, conforming,
  conventional single-family mortgages, built yearly for small areas such as
  counties, ZIP codes, and tracts; it is nominal and not seasonally adjusted
  ([HPI FAQ, Q5 and Q8](https://www.fhfa.gov/document/d/hpi/hpi-faqs);
  [datasets page](https://www.fhfa.gov/data/hpi/datasets)). FHFA labels these
  files "developmental" in the workbook notes.
- **Files** (static downloads, no API, no key), listed on the
  [datasets page](https://www.fhfa.gov/data/hpi/datasets):
  - County: `https://www.fhfa.gov/hpi/download/annual/hpi_at_county.xlsx`
    (sheet `county`, about 5.3 MB).
  - ZIP5: `https://www.fhfa.gov/hpi/download/annual/hpi_at_zip5.xlsx`
    (sheet `ZIP5`, about 40 MB).
  - ZIP3: `https://www.fhfa.gov/hpi/download/annual/hpi_at_zip3.xlsx`.
  - Tract: `https://www.fhfa.gov/hpi/download/annual/hpi_at_tract.csv`.
- **County layout** (observed in the file downloaded 2026-10-06, stated in its
  own header notes): five preamble rows (title, blank, notes, `Last updated:
  March 31, 2026.`, `Not Seasonally Adjusted (NSA)`), then header `State,
  County, FIPS code, Year, Annual Change (%), HPI, HPI with 1990 base, HPI
  with 2000 base`. Observed: 2,795 counties, years 1975 to 2025, 50 states
  plus DC, Connecticut reported by planning region (`09110` to `09190`).
  ZIP5 and ZIP3 have the same columns with the ZIP in place of
  state/county/FIPS; the tract CSV has `tract, state_abbr, year,
  annual_change, hpi, hpi1990, hpi2000`.
- **Index base.** `HPI` is 100 in the first year the area is recorded; the
  1990- and 2000-based columns rescale the same series, so annual change is
  identical across the three (workbook notes).
- **Cadence.** Monthly and quarterly HPI releases follow a published calendar
  ([HPI page](https://www.fhfa.gov/data/hpi)). The annual files carry their
  own "Last updated" date (March 31, 2026 in the current files); the official
  pages do not state which release refreshes them. See open items.
- **Revision policy.** Every release revises history: new repeat
  transactions change appreciation since the prior sale, seasoned-loan
  purchases add data for earlier periods, and late deliveries revise recent
  periods most ([HPI FAQ, Q12 and Q13](https://www.fhfa.gov/document/d/hpi/hpi-faqs)).
  The workbook notes repeat that the annual indexes are revised the same
  way. The adapter therefore treats each file as a full vintage, not an
  append.

## Geography

Native grain for the first release is the county, keyed by the five-digit
`FIPS code` column. Resolve it through the existing
`silver_ref.geography_contract.resolve_provider_geography` path with
`state_fips` and `county_fips` split from that code; never by the `County`
name or `State` abbreviation. The cell type is mixed in the workbook
(observed: 283 codes stored as text with leading zeros, 2,512 as integers),
so silver must read it as text and left-pad to five digits, quarantining any
code that does not resolve. The file uses the Connecticut planning regions
and the post-2019 Alaska code `02063` (Chugach); resolution must use a
geography vintage that contains them, never a remap to legacy counties. ZIP5, ZIP3, and tract files are out of scope until the
[sub-county geography plan](../to_do/SUB_COUNTY_GEOGRAPHY_PLAN.md) supplies ZCTA and
tract identities; USPS ZIP codes are not ZCTAs and must not be joined as if
they were.

## Suppression and missing values

The workbook notes say a thin-market index is either not reported before
recording starts or reported as missing with a period (`"."`). In the
current county file the missing cells are empty rather than literal periods
(observed: 1,057 empty `HPI` cells, 118 counties missing for 2025, 438
county series with an interior gap). Silver records a missing observation
with a reason code (`not_yet_recorded` for rows absent before the first
year, `provider_missing` for empty or `"."` cells) and a null value, never
zero. `Annual Change (%)` is empty in each series' first year by
construction and is recorded as `not_applicable`, not missing. Blank 1990
or 2000 base columns follow from a missing base year, per the notes.
Coverage rule from the methodology paper: an area is indexed only with at
least 100 repeat sales, starting once 25 half-pairs occur in a year
([Working Paper 16-01, p. 6 and Table 2 note](https://www.fhfa.gov/document/wp1601.pdf)).
Whether the current production files still use those thresholds is not
stated on the official pages. The sample excludes jumbo, FHA/VA,
condominium, co-op, and multi-unit loans ([HPI FAQ, Q8](https://www.fhfa.gov/document/d/hpi/hpi-faqs)),
so high-cost and rental-heavy counties are thinly covered; the page states
this limitation instead of filling gaps.

## Terms of use and licensing

FHFA-produced materials are generally public domain, excluding seals and
®-marked trademarks; services using FHFA data must display "This product
uses FHFA data but is neither endorsed nor certified by FHFA." and may cite
"Source: FHFA®" ([website policy](https://www.fhfa.gov/about/fhfa-policies/website-privacy-policy)).
The HPI FAQ invites reuse and suggests "Source: FHFA HPI" with the index
type ([HPI FAQ, Q32](https://www.fhfa.gov/document/d/hpi/hpi-faqs)). No
key is required for the static files; no published rate limit applies to
them. The automated county use is permitted.

## Proposed adapter

- Package `src/data_ingestion_toolbox/fhfa_hpi/`, `source_code`
  `fhfa_hpi`, from the source-adapter starter.
- First measures, county grain, annual: `hpi_annual_change_pct` (annual
  change, percent) and `hpi_index_base_2000` (index, 2000 = 100), plus the
  first-recorded-base index retained in silver for fidelity. Metric identity
  states "all-transactions, nominal, NSA, developmental".
- Feeds the Housing chapter: a "home price change" card (one-year and
  since-2000 change) beside the ACS home value card, labelled as a
  repeat-sales index of Enterprise-backed loans, not a median price.

## Deliverables

1. **Raw capture.** Byte-exact capture of `hpi_at_county.xlsx` with checksum,
   retrieval time, HTTP metadata, and the in-file "Last updated" date as the
   provider vintage; capture before parsing.
2. **Control state.** One slice per (file, provider vintage); attempts,
   retries, changed-checksum detection, and quarantine in the control plane.
3. **Silver.** Parse the sheet past the preamble by header match, type the
   FIPS as text, resolve geography by code, record missing reasons, and keep
   each vintage as a revision so history changes are retained.
4. **Gold.** Deterministic latest and as-released publication per (county,
   measure, year) under the package's `gold_*/DDL/`, registered in
   `sql/bootstrap/warehouse_manifest.json`.
5. **Publisher.** Versioned metric publisher stating basis, base year,
   nominal/NSA, developmental status, and the required FHFA notice.
6. **Glossary harvest.** Glossary publisher contract entries for both
   measures without writing `gold_glossary` objects.
7. **API dispatch.** `SOURCE_DISCOVERY`, `ServingContract`, and
   `OBSERVATION_DISPATCH` entries in `apps/api/registry.py`; consumer guide
   section and OpenAPI snapshot; new-surface registration gates.
8. **DAG.** `ensure_*_schema` upstream of capture; annual schedule plus a
   change-detecting check against the file's `Last-Modified`/checksum.
9. **Data quality.** Rules for unresolved FIPS, duplicate (county, year),
   negative index values, and base-2000 column equal to 100 in 2000 where
   present.
10. **Fixtures.** A trimmed checked-in workbook with a text and an integer
    FIPS, a Connecticut planning region, an interior gap, a first-year row,
    and a malformed variant.
11. **Tests.** Unit, replay, quarantine, rerun, revision, bootstrap, API,
    DAG, and a `tests/external/` module asserting sheet name and header.

## Acceptance criteria

- Configuration imports without I/O; no credential is introduced.
- The fixture replays offline into silver; FIPS stored as an integer resolves
  to the same geography as its zero-padded text form; a malformed fixture is
  quarantined.
- Empty or `"."` index cells yield null values with a missing reason, never
  zero; first-year annual change is `not_applicable`.
- A second vintage with changed history retains both checksums and both
  revisions; re-running one vintage is idempotent.
- `/api/v1/observations` serves both measures for a county fixture;
  capabilities advertise `fhfa_hpi`; the FHFA notice is in the publisher
  contract and consumer guide.
- Quality rules, DAG parse, manifest test, and external contract
  registration pass; unit, integration, DAG, and Ruff checks pass with
  evidence recorded here.

## Open items to resolve during implementation

- Which HPI release refreshes the annual files, and in which month; confirm
  with FHFA documentation or `HPIQuestions@fhfa.gov` (not stated on the
  official pages read).
- Whether the 100-repeat-sales and 25-half-pair thresholds in Working Paper
  16-01 still govern the production files.
- Whether `"."` ever appears as a literal in any file; the current county
  file uses empty cells.
- Whether to publish the first-recorded-base index in gold or keep it in
  silver only, given its base year differs by county.
- ZIP5 and tract onboarding after the sub-county geography plan lands.

## Decisions (open items resolved)

- **Refresh month (open item):** not found on FHFA's pages, and not asked of
  FHFA. The DAG reads the workbook monthly (15:00 UTC on the 25th); the
  download sends no `Last-Modified` or `ETag` (checked 2026-10-06), so
  change detection is the payload checksum. A read whose bytes equal the
  last published file's is `unchanged` and replays nothing.
- **Coverage thresholds (open item):** not verifiable from official pages;
  the operations guide and consumer guide say a thin county is missing,
  and no threshold is asserted in code.
- **`"."` cells (open item):** the current file writes empty cells; both
  empty and `.` are treated as missing (`MISSING_MARKS`), so either form
  reads the same.
- **First-recorded-base index (open item):** kept in silver only, with the
  1990-based index. Gold publishes the annual change and the 2000-based
  index, whose base is the same year everywhere.
- **No spreadsheet dependency.** `utility/workbook.py` reads the sheet
  with the standard library (zip and XML), so the Airflow image needs no
  `openpyxl`. Cells keep their stored text; values are quantized to the
  workbook's `0.00` format and the stored double is kept in `value_source`.
- **Missing reasons:** `provider_missing` (empty or `.` cell),
  `base_year_unavailable` (an indexed county with no 2000 or 1990 index),
  `first_recorded_year` and `prior_year_missing` (annual change, status
  `not_applicable`). The plan's `not_yet_recorded` describes years with no
  row at all; no row is invented for them.
- **Release identity:** the workbook's "Last updated" date; a second file
  with the same date and different bytes is `<date>.2`.
- **Fixture:** FHFA's workbook trimmed to seven counties with every kept
  row's XML copied verbatim: Autauga AL (text FIPS), St. Clair AL (interior
  gap), Chugach AK `02063` (no 2000 base, trailing gaps), the Connecticut
  planning region `09110`, and Delaware's three counties (numeric FIPS).
  The integer-FIPS-without-leading-zero and malformed variants are built
  from these bytes in the unit tests, because no current row stores a
  zero-led code as a number.
- **Data quality:** `DQ-HPI-001` uniqueness and `DQ-HPI-003` missing-has-no-
  number are enforced by key and CHECK constraints; a repeated county-year
  is quarantined by the parser. `DQ-HPI-002` is the file ledger;
  `DQ-HPI-004` covers unresolved FIPS, non-positive indexes and the 2000
  base equal to 100.
- **ZIP5 and tract (open item):** not registered; the operations guide
  says why.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2184 passed, including
  `tests/unit/fhfa_hpi` (5).
- Database: `tests/integration/database/test_fhfa_hpi_capture_replay.py`
  -- 5 passed: the workbook to gold with values, missing reasons and the
  vintage release; an unchanged read replays nothing and a revised file is
  kept beside the first with both checksums; a non-workbook fails capture;
  `DQ-HPI-002` passes then fails on lost facts; `DQ-HPI-004` passes then
  warns on a 2000 base that is not 100 and an unresolved county; the schema
  reapplies; the harvest names two metrics with FHFA's notice.
- End to end: `tests/e2e/test_fhfa_hpi_pipeline.py` serves Kent County's
  annual change with the vintage and notice, and Chugach's missing 2023
  index as `missing` with its reason, through `/api/v1/observations`.
- Live: `tests/external/test_fhfa_hpi_source_contracts.py` -- 5 passed
  against www.fhfa.gov (every row of the current workbook read, nothing
  quarantined, Connecticut by planning region).
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 459 passed, 1 failed: the PEP teardown node, which fails on
  `main` too (fixed on `test/catalog-agreement-fixture-residue`).
- DAG: `tests/dags` in the scheduler container -- 153 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `fhfa_hpi_ingest` in the orchestrated run.
- Deliverable 11's external-credential registration does not apply: the
  workbook takes no credential.
- `ruff check .` and `ruff format --check .` clean; schema snapshot,
  OpenAPI contract, viz coverage and plan environments regenerated.

## Checkpoint

Awaiting human review.
