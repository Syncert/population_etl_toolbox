---
id: census-lehd-lodes
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/census_lodes -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_census_lodes_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# Census LEHD LODES: where county residents work and where county jobs are filled from

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/census-lehd-lodes`, which is
`docs/scout-county-sources` (where this plan was written) with the work on
top.

## Why

A county's resident workers and its jobs are different populations. LODES
counts both from one administrative frame and links them (who lives and works
in the county, who commutes in, who commutes out): the Work and Money
chapter's "residents versus jobs" story, which neither ACS nor QCEW tells.

## Verified contract

Primary reference: LODES Dataset Structure, Format Version 8.4, Rev. 20251203
([LODESTechDoc8.4.pdf](https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.4.pdf)).

- **Distribution.** Gzipped CSV files under
  `https://lehd.ces.census.gov/data/lodes/LODES8/<st>/` with `od/`, `rac/`,
  `wac/`, `<st>_xwalk.csv.gz`, `lodes_<st>.sha256sum`, and `version.txt`
  ([directory](https://lehd.ces.census.gov/data/lodes/LODES8/),
  [wi example](https://lehd.ces.census.gov/data/lodes/LODES8/wi/)). The tech doc
  names this root for automated download. A `us/` directory holds only a
  national crosswalk, checksum, and version file, no data files
  ([us](https://lehd.ces.census.gov/data/lodes/LODES8/us/)).
- **Three file families** (tech doc, "Data Files"): OD, jobs keyed by home
  and work block; RAC, jobs totalled by home block; WAC, jobs totalled by
  work block.
- **OD**: `[ST]_od_[PART]_[TYPE]_[YEAR].csv.gz`, PART `main` (home and work in
  state) or `aux` (work in state, home out of state); columns `w_geocode`,
  `h_geocode` (Char15), `S000`, `SA01`-`SA03`, `SE01`-`SE03`, `SI01`-`SI03`,
  `createdate`.
- **RAC / WAC**: one file per segment (`S000`, `SA01`..`SI03`) and job type;
  columns `h_geocode` or `w_geocode`, `C000`, `CA01`-`CA03`, `CE01`-`CE03`,
  `CNS01`-`CNS20` (2-digit NAICS sectors), `CR01`-`CR05`, `CR07`, `CT01`-`CT02`,
  `CD01`-`CD04`, `CS01`-`CS02`, `createdate`; WAC adds `CFA01`-`CFA05` and
  `CFS01`-`CFS05`.
- **Job types**: `JT00` all, `JT01` primary, `JT02` all private, `JT03`
  private primary, `JT04` all federal, `JT05` federal primary. From 2010 a
  full state-year is 12 OD, 60 RAC, 60 WAC files; 2009 and earlier 8/40/40.
- **Years**: 2002-2023 for most states (tech doc coverage table).
- **Volume**: Wisconsin `wi_od_main_JT00_2023.csv.gz` is 14 MB compressed and
  `wi_rac_S000_JT00_<year>` files for 2009-2016 about 3 MB each
  ([wi/od](https://lehd.ces.census.gov/data/lodes/LODES8/wi/od/),
  [wi/rac](https://lehd.ces.census.gov/data/lodes/LODES8/wi/rac/)).
- **No county files are published.** Every OD/RAC/WAC file is at 2020
  census block grain; county figures must be aggregated by this adapter.
- **Revision policy**: `version.txt` carries a data vintage `YYYYMMDD`
  (Wisconsin currently `20251202_1657`); new or corrected data produce newer
  vintages containing only new or changed files; `createdate` inside files is
  an internal processing date, not the vintage (tech doc, "Metadata Files").
- **Cadence**: annual cross-section; release no sooner than 18 months after
  the reference period, sometimes later
  ([CES-WP-25-52, sec. 3.1.3-3.2](https://www2.census.gov/library/working-papers/2025/adrm/ces/CES-WP-25-52.pdf);
  working paper, not an official publication).

## Geography

Native grain is the 15-character 2020 tabulation block code (`w_geocode`,
`h_geocode`, `tabblk2020`). Its first two characters are the state FIPS code
and its first five the 2020 state+county FIPS code. The crosswalk gives `st`
and `cty` per block but describes non-block codes as "current" definitions
(tech doc, "Code Vintages"). The adapter derives county identity from the
block code (or `cty`, once the open item below is settled), resolves it
through the shared FIPS-keyed geography dimension, and quarantines any code
that does not resolve. Names (`ctyname`, `stname`) are captured but never used
for identity. Crosswalk latitude/longitude are internal points, not centroids.

## Suppression and missing values

- No suppression codes or flags exist; all published counts are integers.
  Workplace counts carry noise infusion and small-cell synthesis; residence
  locations are synthesized under probabilistic differential privacy
  (CES-WP-25-52 sec. 4.3). Gold labels county figures as protected estimates.
- **Zeros that are not zeros**: race, ethnicity, education, and sex columns
  are all zeros before 2009; firm age and size are zeros outside 2011-2023
  and outside `JT02` (tech doc, "Data Coverage"). Silver maps these to
  "not available", never to 0. Education covers only workers 30 and older.
- **Absent state-years**: states without OD and WAC files in a year (e.g.
  Alaska 2017-2023, Michigan 2022-2023) publish no workplace measure, not
  zero. Their RAC files exist but residence data may be incomplete
  (CES-WP-25-52 sec. 3.1.3).
- A header-only file means no jobs for that combination; a block absent
  from a file has zero jobs. Both are true zeros.

## Terms of use and licensing

No LODES-specific license page was found. The tech doc documents automated
download from the LODES8 root; no key is required and no rate limit is
published. Cite Bureau, product, vintage, URL, and access date
([citation policy](https://www.census.gov/about/policies/citation.html));
the Bureau publishes data as open data
([open data](https://www.census.gov/about/policies/open-gov/open-data.html)).
Automated county-level use is consistent with these terms.

## Proposed adapter

Package `src/data_ingestion_toolbox/census_lodes/`, source_code
`census_lodes`. First measures, county by year, job type `JT00` and segment
`S000` only: resident workers (RAC `C000`), jobs located in the county (WAC
`C000`), and from OD `main` plus `aux`: live-and-work-in-county, inbound
commuters, and outbound commuters (in-state; out-of-state outflow needs other
states' `aux` files). Feeds the Work and Money chapter.

## Deliverables

1. **Config and identity**: LODES8 root, format version, years, job types,
   segments; no I/O at import.
2. **Raw capture**: lossless gz bytes per file checked against the state
   sha256sum, with vintage, retrieval time, HTTP metadata, run lineage.
3. **Control state**: slices per (state, family, part/segment, job type,
   year); vintage watermark so only changed files are refetched.
4. **Silver**: block rows typed, not-available columns nulled by year and job
   type, county aggregation by FIPS, county-to-county OD flows.
5. **Gold**: deterministic county, state, and nation publication per
   (geography, measure, year, job type) with the protected-estimate basis.
6. **Glossary publisher** contract and harvest for each measure.
7. **API dispatch**: `SOURCE_DISCOVERY` and `ServingContract`; consumer guide
   section; OpenAPI snapshot; new-surface registration gates.
8. **DAG** with dependency-ordered tasks and Airflow pool.
9. **Data quality**: OD totals reconcile to WAC `C000` by work county; RAC
   `C000` reconcile to OD by home county within the state's domain.
10. **Fixtures**: trimmed real files for two small counties, one header-only
    file, one pre-2009 file, one malformed file.
11. **Tests**: unit, capture-replay integration, rerun idempotency,
    quarantine, external contract module under `tests/external/`.

## Acceptance criteria

- Config imports without I/O; the source needs no credential and the
  external contract registration states that.
- A checksum mismatch fails capture; a malformed fixture is quarantined.
- Replay of the fixtures yields county totals equal to hand-summed blocks.
- Pre-2009 demographic and non-`JT02` firm columns reach silver as not
  available, not zero; a test asserts it.
- A state-year with no WAC files yields no workplace row, not a zero row.
- Re-running the same vintage writes nothing new; a new vintage retains both
  checksums.
- `/api/v1/observations` serves resident workers and jobs-in-county for a
  fixture county; capabilities advertise the source.
- Unit, integration, DAG, and Ruff checks pass; evidence recorded here.

## Open items to resolve during implementation

- Connecticut: whether county identity follows the block code prefix
  (legacy counties) or crosswalk `cty` ("current", possibly planning
  regions). Not verified.
- The tech doc's RAC template shows a `_1` suffix; live files have none
  (`wi_rac_S000_JT00_2002.csv.gz`). Build names from the directory listing.
- Explicit public-domain statement for LODES files not located on an
  official page.
- Whether job type `JT01` (primary jobs) is the better almanac default.
- Storage budget for capturing all states and years of OD `main`.

## Decisions (open items resolved)

- **Connecticut:** county identity is the block code's first five digits.
  The 2020 tabulation blocks carry Connecticut's eight legacy county codes,
  not the planning regions the crosswalk's "current" `cty` may name; they
  resolve only where the shared dimension holds those codes, and a county
  that does not resolve is `canonical_geography_absent` in the ledger and
  not served (its blocks still count toward the state). The crosswalk is not
  captured.
- **File names:** no `_1` suffix. Names are built from the registry and
  checked against the state's own checksum list, which every run captures;
  a name the list lacks is `not_published`, and EXT-021 asserts every
  registered name for the newest year is listed.
- **Licensing:** no LODES-specific public-domain page was located; the
  operations guide and gold basis cite the Bureau and vintage. Recorded,
  not blocking.
- **Job type:** `JT00` (all jobs), as the adapter section specifies. `JT01`
  would need a second registry entry; not added.
- **Storage:** an ordinary run captures only the newest `recent_years`
  (default 1) registered year and fetches nothing beyond the two metadata
  files while a state's vintage is unchanged. State-years run one at a time
  through the one-slot `census_lodes_files` pool.
- **Control grain:** one slice per state-year (`control.census_lodes_slice`)
  with one row per family (`control.census_lodes_file`), not per segment
  and job type, because only `S000`/`JT00` is registered.
- **Gold grains: county and state, not nation (deviation from deliverable
  5).** LODES publishes no national data file, and a national sum is only
  true when every state's same year is captured; a partial sum would be
  served as the nation. The state's `live_and_work` is every in-state
  commute and its `inbound` the out-of-state homes; `outbound_in_state` is
  county only.
- **A failed capture withdraws its slice.** The run and request keep their
  errors in the ingestion ledger; the half-registered slice is deleted so
  DQ-LODES-002 does not read it as work waiting to be replayed.
- **Fixtures:** Delaware 2023 (all four files) and 2008 (residence and
  workplace) trimmed to the blocks of tracts 100010401 and 100050501, with
  origin-destination rows kept by work block so they reconcile to the
  workplace file; the checksum list is recomputed for the trimmed files and
  `version.txt` is the state's real one. The header-only and malformed
  cases are built from these bytes in the unit tests.

## Evidence (2026-10-06, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2186 passed, including
  `tests/unit/census_lodes` (7); the one documentation-link failure seen
  mid-run was this plan's own link before it moved here, and passes now.
- Database: `tests/integration/database/test_census_lodes_capture_replay.py`
  -- 6 passed: county and state sums equal the hand-summed blocks; 2008
  demographics and `JT00` firm columns `not_available`; a state-year with
  no workplace or origin-destination file serves residents only; the same
  vintage fetches only the two metadata files and a new vintage is kept
  beside the old; a checksum mismatch fails capture and withdraws the
  slice; `DQ-LODES-002` and `DQ-LODES-004` pass then catch a loss; the
  schema reapplies; the harvest names five metrics.
- End to end: `tests/e2e/test_census_lodes_pipeline.py` serves a county's
  jobs, live-and-work and inbound counts with the vintage and the
  protected-estimate basis through `/api/v1/observations`.
- Live: `tests/external/test_census_lodes_source_contracts.py` -- 5 passed
  against lehd.ces.census.gov.
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 461 passed, 1 failed: the PEP teardown node, which fails on
  `main` too (fixed on `test/catalog-agreement-fixture-residue`).
- DAG: `tests/dags` in the scheduler container -- 153 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `census_lodes_ingest` in the orchestrated run.
- `ruff check .` and `ruff format --check` clean on the changed files;
  schema snapshot, OpenAPI contract, viz coverage and plan environments
  regenerated.

## Checkpoint

Awaiting human review.
