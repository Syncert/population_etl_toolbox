---
id: nces-common-core-of-data
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/nces_ccd -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_nces_ccd_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# NCES Common Core of Data: public schools, districts, enrollment, and lunch eligibility

## Status

To do. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

The almanac adds a Schools chapter, and no current source covers it. The Common Core of Data (CCD) is
the Department of Education's annual national database of all public elementary and secondary schools
and districts ([About CCD](https://nces.ed.gov/ccd/aboutCCD.asp)). States submit it through EDFacts. It
gives every county the number of schools and districts, student membership, teacher FTE, and the count
of students eligible for free or reduced-price lunch.

## Verified contract

- **Components per school year.** The school universe has five files: Directory, Membership (by grade,
  race/ethnicity, and sex), Staff (teacher FTE), School Characteristics, and Lunch Program Eligibility
  ([school universe page](https://nces.ed.gov/ccd/pubschuniv.asp)). Districts (LEAs) have matching
  universe files ([LEA universe page](https://nces.ed.gov/ccd/pubagency.asp)). Fiscal files (F-33, SLFS)
  are out of scope here.
- **File names and URLs.** For 2018-19 these were published as zipped files under
  `https://nces.ed.gov/ccd/data/zip/`, for example `ccd_sch_029_1819_w_1a_091019.zip` (directory),
  `ccd_sch_052_1819_l_1a_091019.zip` (membership), `ccd_sch_059_1819_l_1a_091019.zip` (staff),
  `ccd_sch_129_1819_w_1a_091019.zip` (characteristics), and `ccd_sch_033_1819_l_1a_091019.zip` (lunch)
  ([data.gov catalog entry for 2018-19](https://catalog.data.gov/dataset/common-core-of-data-ccd-school-nonfiscal-data-files-and-documentation-2018-19-a0aaf)).
  The current file index is the [CCD Data File Tool](https://nces.ed.gov/ccd/files.asp), which renders
  with script and could not be read as static text. NCES notes that membership files can exceed 1 GB.
- **Release versions.** A "preliminary" file has not gone through the full data-quality follow-up and
  uses suffixes 0a, 0b, and so on. A "provisional" file has been reviewed after follow-up with states,
  is considered final, and uses 1a, 1b, and so on ([CCD FAQ](https://nces.ed.gov/ccd/quickfacts.asp)).
  The preliminary directory files come out first. For 2023-24 they were based on an October 1, 2023
  snapshot ([2023-24 preliminary directory](https://nces.ed.gov/use-work/resource-library/data/data-file/2023-24-common-core-data-ccd-preliminary-directory-files)).
  Universe files version 1a came later: 2022-23 in January 2024
  ([NCES 2024-151](https://nces.ed.gov/use-work/dataset/2022-23-common-core-data-ccd-universe-files-version-1a?pubid=2024151))
  and 2024-25 in December 2025
  ([NCES 2026-005](https://nces.ed.gov/use-work/dataset/2024-25-common-core-data-ccd-universe-files-version-1a)).
  NCES aims to publish files within four months of the July submission deadline
  ([NCES blog](https://nces.ed.gov/learn/blog/common-core-data-ccd-nonfiscal-data-releases-how-national-center-education-statistics-improved)).
  The release version is part of the file name, so a later release arrives as a new file and is stored
  as a new revision. It never overwrites the earlier one.
- **Identifiers.** `LEAID` has seven characters: the two-digit state FIPS code followed by a five-digit
  LEA code. `NCESSCH` has twelve: state FIPS, LEA code, and a five-digit school code
  ([EDGE geocode file documentation, NCES 2018-080, section 5.2](https://nces.ed.gov/programs/edge/docs/EDGE_GEOCODE_PUBLIC_FILEDOC.pdf)).
  Both are strings and keep their leading zeros. The FAQ warns that spreadsheets can corrupt ID fields.
- **Lunch eligibility definitions.** Free lunch covers family income below 130 percent of the poverty
  level or direct certification. Reduced-price lunch covers 130 to 185 percent. FRPL is the sum of the
  two. Direct certification is the count of categorically eligible students reported to USDA on the
  FNS-742. Since SY 2016-17 states may report FRPL, direct certification, or both. Schools in the
  Community Eligibility Provision (CEP) may report every student as free-eligible
  ([NCES blog on lunch eligibility](https://ies.ed.gov/blogs/nces/post/understanding-school-lunch-eligibility-in-the-common-core-of-data)).
  The adapter publishes FRPL and direct certification as separate measures. It never substitutes one
  for the other.
- **Update cadence.** Annual, one school year at a time.

## Geography

The native grains are the school (`NCESSCH`) and the district (`LEAID`). Every geography is resolved
by code, never by name:

- **School to county.** The EDGE public school geocode file assigns `CNTY`, a five-digit county FIPS
  code, from each school's latitude and longitude, along with `STFIP`
  ([NCES 2018-080, sections 5.9-5.10](https://nces.ed.gov/programs/edge/docs/EDGE_GEOCODE_PUBLIC_FILEDOC.pdf);
  [School Locations](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)). School counts and
  sums roll up to county through `CNTY`, which joins the authoritative FIPS geography layer.
  `OPSTFIPS` (operating state) can differ from the physical state. BIE schools use `59` and DoDEA
  schools use `63` (section 5.4). These codes are not real state FIPS codes and must not be resolved as
  states.
- **District to county and place.** The LEA geocode file gives each district a single county, based on
  the location of its administrative office. NCES says these single associations are "not necessarily
  complete" and points to the School District Geographic Relationship Files (GRF) for the full set (same
  document, section 2.0). `grfYY_lea_county` (`LEAID`, `STCOUNTY`, `COUNT`, `LANDAREA`, `WATERAREA`) and
  `grfYY_lea_place` (`LEAID`, `PLACE` as a seven-character ID) have one record per part of a district
  ([GRF documentation, NCES 2018-076](https://nces.ed.gov/programs/edge/Docs/EDGE_SDGRF_FILEDOC.pdf)).
  They are published as `/programs/edge/data/GRF[YY].zip`
  ([Relationship Files](https://nces.ed.gov/programs/edge/Geographic/RelationshipFiles)).
- **Rule.** County figures are school-level rollups. District facts are published at district grain
  with their GRF overlaps as lineage. They are never apportioned to counties by land area. Place grain
  waits for `sub-county-geography` and `acs-place-grain`.

## Suppression and missing values

Older CCD documentation defines these reserve codes. In numeric fields, `-9` means not reported but
expected in the final file, `-1` means the state reported the value was not measured, and `-2` means
not applicable. In character fields the equivalents are `B`, `M`, and `N`
([2009-10 preliminary school universe documentation](https://nces.ed.gov/ccd/pdf/psu09pgen.pdf)). The
EDGE files set a missing address to `M`. Silver keeps each code as a typed status (not reported, not
measured, not applicable, suppressed) with a null value. It is never zero. A county rollup that contains
any non-reported school carries a completeness flag rather than a silently partial sum. The codes and
any privacy-suppression descriptor in the current long-format files are unverified (see open items).

## Terms of use and licensing

NCES publications state "Unless specifically noted, all information contained herein is in the public
domain" ([NCES 2026-003 front matter](https://nces.ed.gov/ccd/pdf/2026003.pdf)). The data.gov record
lists the files as public access under the U.S. open-licenses policy
([data.gov](https://catalog.data.gov/dataset/common-core-of-data-ccd-school-nonfiscal-data-files-and-documentation-2018-19-a0aaf)).
No API key is involved: these are static file downloads. No rate limit was found in the documentation.
The adapter makes polite, sequential downloads. The ed.gov copyright notice page returned HTTP 403 to
the fetcher and was not read.

## Proposed adapter

- Package `src/data_ingestion_toolbox/nces_ccd/`, `source_code` `nces_ccd`, built from the
  source-adapter starter.
- First measures (county, state, nation, from school rollups) feed the new **Schools** chapter:
  operating and charter schools, membership, teacher FTE, free, reduced-price, and FRPL eligible
  counts, directly certified count, and districts by office location (labelled as such). Ratios such
  as students per teacher are derived analysis, labelled and kept apart from provider facts.

## Deliverables

1. **Config and identity.** `config.py` without import-time I/O (CCD and EDGE base URLs, school years,
   components, timeouts, pool, connection ID); provider-neutral identity; no credential, documented.
2. **Raw capture.** Each zip (CCD component, EDGE geocode, GRF) stored losslessly before parsing with
   checksum, retrieval time, HTTP metadata, file name, version suffix, and run lineage; append-only.
3. **Control state.** Slices per (component, school year) with attempts, retries, version watermark,
   and quarantine in the control plane.
4. **Silver** (`nces_ccd/DDL/`). Typed school, district, membership, staff, and lunch facts; string IDs;
   typed reserve-code statuses; revision selection by version (0x < 1a < 1b); `CNTY` joined by code.
5. **Gold** (`nces_ccd/gold_schools/DDL/`). Deterministic county, state, nation rollups with
   completeness counts, in `warehouse_manifest.json`, applied by an `ensure_*_schema` task.
6. **Publisher and glossary harvest.** Versioned contract carrying NCES definitions (FRPL, direct
   certification, CEP caveat, membership snapshot date); never writes `gold_glossary`.
7. **API dispatch.** `SOURCE_DISCOVERY`, `ServingContract`, `OBSERVATION_DISPATCH`, consumer guide,
   OpenAPI snapshot, and the new-surface registration gates.
8. **DAG.** Annual: ensure schema, geocode/GRF capture, CCD capture, silver, gold.
9. **Data quality.** ID shape, `CNTY` present in the geography layer, no negative counts after
   reserve-code mapping, FRPL above membership flagged.
10. **Fixtures.** Trimmed two-state extracts (directory, membership, lunch, geocode, GRF county) and one
    malformed file.
11. **Tests and docs.** Unit, replay, quarantine, rerun, revision, bootstrap, DAG-parse, and
    `tests/external/` contract tests; operations guide, reset instructions, and testing catalog.

## Acceptance criteria

- Config imports without I/O; no credential appears in captures, fingerprints, or logs.
- Fixtures replay offline into silver; IDs keep leading zeros; reserve codes become typed statuses with
  null values, never zero; a malformed zip is quarantined.
- Idempotent rerun; a 1b file after 1a keeps both captures and silver selects 1b.
- County rollups resolve only through `CNTY`; operating codes 59/63 never resolve as states; district
  facts are never apportioned by area.
- FRPL and direct certification are distinct metrics with CEP and reporting-basis caveats in the
  glossary contract, whose test passes.
- `/api/v1/observations` serves county membership and FRPL for a fixture county; capabilities advertise
  `nces_ccd`; consumer guide and OpenAPI snapshot updated.
- Unit, database integration, DAG, and Ruff checks pass, with evidence recorded here.

## Open items to resolve during implementation

- Current file names, suffixes, and URLs for 2022-23 to 2024-25 (`files.asp` is script-rendered), and
  the meaning of the `_l_`/`_w_` name segments (assumed long/wide, unverified).
- Reserve codes and any small-count suppression in current long-format files (verified only for 2009-10).
- Current EDGE geocode and GRF layouts and URLs (verified layouts are NCES 2018-080 and 2018-076).
- Whether 1b-or-later revisions are still issued; Puerto Rico, outlying areas, BIE, and DoDEA in
  state and nation totals; any rate limit or robots policy on the download hosts.

## Checkpoint

Next pickup: open the CCD Data File Tool in a browser, record the exact 2024-25 directory, membership,
and lunch file names and documentation PDFs here, copy the starter, and write the failing replay test
for one state's directory and lunch fixture.
