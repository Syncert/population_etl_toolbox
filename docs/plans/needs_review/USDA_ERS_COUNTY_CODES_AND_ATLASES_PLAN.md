---
id: usda-ers-county-codes-and-atlases
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/usda_ers -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_usda_ers_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# USDA ERS county codes and atlases: rural-urban continuum, typology, food environment

## Status

Ready for review. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
Implemented on branch `feat/usda-ers-county-codes`, which is
`docs/scout-county-sources` (where this plan was written) with the work on
top.

## Why

Peer comparison needs an honest, provider-published way to say which counties
resemble each other. The USDA Economic Research Service (ERS) publishes, for
every county, the Rural-Urban Continuum Code (metro size and nonmetro
urbanization/adjacency) and the County Typology Codes (economic dependence and
demographic flags). These are provider classifications, not derived scores,
so peer grouping can cite them directly. The Food Environment Atlas adds
county store access, SNAP-authorized store and food-assistance indicators for
the Land and Farms chapter.

## Verified contract

All files are static downloads; there is no key and no query API for these
three products.

- **Rural-Urban Continuum Codes (RUCC)** — product page
  <https://www.ers.usda.gov/data-products/rural-urban-continuum-codes>.
  Vintages: 2023 (XLSX and CSV, updated 1/22/2024), 2013 (XLS), 2003 (XLS plus
  a separate Puerto Rico XLS), 1993, 1983+1993 combined, 1974. 2023 CSV:
  `https://www.ers.usda.gov/media/5768/2023-rural-urban-continuum-codes.csv`
  (the page appends a `?v=` cache token). Long layout, header
  `FIPS,State,County_Name,Attribute,Value`; Attribute values
  `Population_2020`, `RUCC_2023`, `Description` (read from the file itself).
  Codes 1–3 metro by metro-area size, 4–9 nonmetro by urban population
  (20,000+, 5,000–20,000, under 5,000) and metro adjacency; the 2023 edition
  uses OMB July 2023 delineations, the 2020 Census and 2016–2020 ACS commuting
  data, and documentation was last updated 2025-01-07
  (<https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/documentation>).
  Cadence: decennial.
- **County Typology Codes** — <https://www.ers.usda.gov/data-products/county-typology-codes>.
  Editions 2025 (XLSX and CSV, updated 4/11/2025), 2015 (XLS and CSV), 2004,
  1989, 1979/1986. 2025 CSV:
  `https://www.ers.usda.gov/media/6174/ers-county-typology-codes-2025-edition.csv`.
  Header `FIPStxt,State,County_Name,Metro2023,Attribute,Value,PublicationDate,Source`;
  Attribute values observed include `High_Farming_2025`, `High_Mining_2025`,
  `High_Manufacturing_2025`, `High_Government_2025`, `High_Recreation_2025`,
  `Nonspecialized_2025`, `Industry_Dependence_2025`,
  `Low_PostSecondary_Ed_2025`, `Low_Employment_2025`, `Population_Loss_2025`,
  `Housing_Stress_2025`, `Retirement_Destination_2025`,
  `Persistent_Poverty_1721`. Thresholds and data years (2019/2021/2022
  earnings averages; 2018–22 ACS; 1990–2017–21 poverty) are in
  <https://www.ers.usda.gov/data-products/county-typology-codes/documentation>
  (updated 2026-03-18). The product page states vintages "are not directly
  comparable because of methodological changes".
- **Food Environment Atlas** — <https://www.ers.usda.gov/data-products/food-environment-atlas>;
  downloads at
  <https://www.ers.usda.gov/data-products/food-environment-atlas/data-access-and-documentation-downloads>
  (current version updated 7/30/2025): XLSX
  `/media/5569/food-environment-atlas-data-download.xlsx` and a ZIP of CSVs
  `/media/5570/food-environment-atlas-csv-files.zip`, plus a variable lookup
  file mapping short indicator names to descriptions; seven archived versions
  back to 2011. Over 300 state and county variables in categories Access,
  Stores (including SNAP- and WIC-authorized stores), Restaurants, Food
  Assistance, State Food Insecurity, Food Taxes, Local Foods, Health,
  Socioeconomic; years vary by indicator (SNAP-authorized stores 2017 and 2023;
  SNAP households with low store access 2015 and 2019). Updated every 2–3
  years (<https://www.ers.usda.gov/data-products/food-environment-atlas/documentation>).
- **Revision policy.** ERS standards require that revisions be described with
  reasons and implications
  (<https://www.ers.usda.gov/about-ers/policies-and-standards/data-product-quality/ers-data-product-quality-standards>);
  files are replaced in place, so the `?v=` token and checksum are the only
  change signals.

## Geography

Native grain is the county or county-equivalent, keyed by five-digit FIPS
(`FIPS`, `FIPStxt`, or the Atlas `FIPS`/`GEOID`, which the Atlas documents as
interchangeable). Resolve each row through the shared FIPS geography layer in
`silver_ref` with the source's boundary vintage recorded; never match on
`County_Name`. RUCC 2023 uses the nine Connecticut planning regions (for
example 09110, 09120, 09170). Typology 2025 mixes grains: ACS-based flags use
the planning regions (3,144 geographies), other flags use the eight legacy
Connecticut counties (3,143). The Atlas uses 2010/2015/2020 county boundaries
with a 2023 exception for Connecticut. RUCC 2023 also covers 89 county
equivalents in the territories. Rows whose FIPS does not resolve in the
declared vintage are quarantined, not dropped.

## Suppression and missing values

- Atlas: `-9999` or blank means unavailable, suppressed, or not applicable;
  `-8888` means the county did not exist that year; `N/A` means incomplete
  data (Atlas download and documentation pages above). Each maps to a
  distinct null-reason status in silver with a null value; none becomes zero.
- RUCC and Typology: codes are categorical. Typology flags are 0/1 and `0` is
  a real "not flagged" value, distinct from absence. RUCC `Description` is
  provider text kept as the code label, not a measure.

## Terms of use and licensing

ERS states that its data products should "impose no barriers to any person
or group of persons" and "must be branded as coming from ERS"
(<https://www.ers.usda.gov/about-ers/policies-and-standards/data-product-quality/ers-data-product-quality-standards>).
No API key, registration, or published rate limit applies to the static
downloads. Attribution to "USDA, Economic Research Service" is carried in the
publisher contract (the Typology CSV embeds this `Source` string). The USDA
site-wide public-domain statement could not be fetched (HTTP 403); see open
items.

## Proposed adapter

- Package `src/data_ingestion_toolbox/usda_ers/`, `source_code = "USDA_ERS"`,
  one dataset slice per product and vintage (`rucc/2023`, `typology/2025`,
  `fea/2025-07`).
- First measures: `RUCC_2023` (code plus provider label), metro/nonmetro
  derived only as the provider's 1–3 vs 4–9 split; all thirteen 2025 Typology
  attributes; Atlas SNAP-authorized store count and density and SNAP
  households with low store access.
- Feeds: peer grouping (RUCC, Typology) for
  `WHAT_MAKES_THIS_PLACE_DISTINCTIVE_PLAN`, and the Land and Farms chapter
  (Typology farming dependence, Atlas food environment).

## Deliverables

1. **Raw capture.** Lossless append-only capture of each CSV/ZIP with
   checksum, URL including `?v=` token, HTTP metadata, and run lineage.
2. **Control state.** Slices per (product, vintage), retries, change
   detection by checksum, quarantine status.
3. **Silver.** Long facts per (FIPS, geography vintage, product, attribute,
   reference period) with typed value, provider label, and null reason; the
   Atlas variable lookup harvested as metadata.
4. **Gold.** Deterministic publication per (geography, measure, vintage);
   classification codes published as categorical metrics, not numerics.
5. **Publisher.** Versioned glossary publisher contract with ERS attribution
   and the "vintages not comparable" note.
6. **Glossary harvest.** Code definitions and Atlas variable descriptions.
7. **API dispatch.** `SOURCE_DISCOVERY` and `OBSERVATION_DISPATCH` entries in
   `apps/api/registry.py`; consumer guide and OpenAPI snapshot updated.
8. **DAG.** `ensure_*_schema` upstream of capture; manifest entries in
   `sql/bootstrap/warehouse_manifest.json`.
9. **Data quality.** Completeness against the county inventory per vintage,
   code-domain checks (RUCC 1–9, flags 0/1), sentinel accounting.
10. **Fixtures.** Trimmed checked-in RUCC, Typology, and Atlas files including
    Connecticut, a territory row, and each Atlas sentinel.
11. **Tests.** Unit, replay, malformed/quarantine, rerun, external contract
    module under `tests/external/`, and catalog updates.

## Acceptance criteria

- Configuration imports without I/O; no credential is required or logged.
- Fixtures replay offline into silver; `-9999`, `-8888`, `N/A`, and blank
  each produce a distinct null reason and never zero.
- Connecticut planning-region and legacy-county rows resolve by FIPS to the
  right geography vintage; an unknown FIPS is quarantined.
- Re-running unchanged files is idempotent; a changed checksum produces a new
  revision with both captures retained.
- `/api/v1/observations` serves `RUCC_2023` and one Typology flag for a
  county fixture; capabilities advertise the source.
- Glossary contract, DAG parse, manifest, and Ruff checks pass; evidence
  recorded here.

## Open items to resolve during implementation

- Verify the USDA public-domain statement on an official page (the USDA
  policies page returned 403 during scouting).
- Exact Atlas CSV column layout, sheet/file names inside the ZIP, and SNAP
  variable codes were not visible on the official pages; read them from the
  downloaded variable lookup file.
- Integer meaning of `Industry_Dependence_2025` values (documentation names the
  five industries but the code mapping was not confirmed).
- Whether older RUCC (2013, 2003) and Typology (2015) vintages are onboarded,
  given ERS's non-comparability note.
- Whether RUCC 2023 combines Virginia independent cities; the documentation
  fetch mentioned this ambiguously.
- Whether the Atlas SNAP participation indicators are state-level only.

## Decisions (open items resolved)

- **Atlas scope:** the July 2025 zip is captured whole, but only the eight
  registered county variables are loaded (SNAP-authorized stores and their
  rate for 2017 and 2023; SNAP households with low store access, count and
  percent, for 2015 and 2019); the rest are counted as out of scope. The SNAP
  participation variables are state-level (`*` in `VariableList.csv`) and
  are not registered. The variable codes, units and labels were read from
  `VariableList.csv` in the downloaded zip.
- **Typology `99` and `-1`:** `99` is how the 2025 file marks a flag not
  computed for a geography (Connecticut publishes ACS-based flags for its
  planning regions and the rest for its eight legacy counties), served as
  `not_applicable` `not_computed_for_geography`. `-1` appears only on
  `Persistent_Poverty_1721` for 24 counties; ERS's documentation does not
  define it, so it is served as `not_applicable` `not_determined` (inferred),
  never as a flag value.
- **`Industry_Dependence_2025` codes (open item):** the documentation names
  the five industries but not the code mapping; the code is served without a
  label and the operations guide says so.
- **Classifications are not analysed:** the source is not `analysis_ready`
  (codes and flags would be averaged or correlated as quantities); the
  aligned-analysis and reduction screens decline it as reviewed policy.
- **Unknown FIPS (deviation from "quarantined"):** a well-formed FIPS the
  shared geography does not hold is kept in silver as `unmapped`, recorded
  in the resolution ledger as `canonical_geography_absent` and not served --
  the same rule as every other source, so a Connecticut legacy county or a
  territory is not lost. A malformed FIPS (`N/A`, an unknown state code) is
  quarantined.
- **Release identity:** ERS names no release; the release key is product,
  edition and read time, and a replaced file is kept beside the old one.
- **Editions:** only RUCC 2023, Typology 2025 and the July 2025 Atlas are
  registered; older editions are not, given ERS's non-comparability note.
- **Virginia independent cities (open item):** RUCC 2023 lists them as their
  own county equivalents (each has its own FIPS row); nothing is combined.
- **USDA public-domain statement (open item):** still not verified on an
  official page; served rows carry "Source: USDA, Economic Research
  Service." as ERS's standards ask.
- **Fixtures:** each file trimmed to a few counties with every kept row
  copied verbatim: Delaware, Connecticut's Capitol Planning Region (and
  legacy Hartford County in the Typology and Atlas), Puerto Rico's Adjuntas
  and American Samoa's Rose Island (RUCC), and Alaska's Chugach (an Atlas
  `-8888` and `-9999`, a Typology `-1`). The Atlas zip keeps the full
  `VariableList.csv` and read-me. `N/A` and blank Atlas cells are built from
  these bytes in the unit tests.

## Evidence (2026-10-07, Windows host, local Docker test stack)

- Unit: `python -m pytest tests/unit` -- 2184 passed, including
  `tests/unit/usda_ers` (5).
- Database: `tests/integration/database/test_usda_ers_capture_replay.py`
  -- 4 passed: every file to gold with RUCC labels, flags, unset marks and
  Atlas sentinels; Adjuntas (a territory) served and legacy Hartford County
  recorded unmapped; an unchanged read replays nothing and a replaced file
  is kept beside the first; a moved Atlas fails capture; `DQ-ERS-002` and
  `DQ-ERS-004` pass or warn as expected and then catch a fault; the schema
  reapplies; the harvest names eighteen metrics with classifications marked.
- End to end: `tests/e2e/test_usda_ers_pipeline.py` serves Kent County's
  RUCC with its label, the Capitol Planning Region's unset farming flag, and
  Chugach's two Atlas sentinels through `/api/v1/observations`.
- Live: `tests/external/test_usda_ers_source_contracts.py` -- 7 passed
  against www.ers.usda.gov; offline, the full files parse with nothing
  quarantined (RUCC 9,703 rows; Typology 40,976; Atlas 25,152 in scope of
  957,753).
- Integration and end to end: `tests/integration tests/e2e -m "not
  external"` -- 458 passed, 1 failed: the PEP teardown node, which fails on
  `main` too (fixed on `test/catalog-agreement-fixture-residue`).
- DAG: `tests/dags` in the scheduler container -- 153 passed;
  `test_dag_pipeline_execution.py` on a fresh database -- 4 passed with
  `usda_ers_ingest` in the orchestrated run (a first attempt started before
  the recreated database finished its init scripts and was refused
  connections; rerun once init completed).
- `ruff check .` and `ruff format` clean; schema snapshot, OpenAPI contract,
  viz coverage (ERS's seven analysis and reduction screens recorded as
  reviewed declines) and plan environments regenerated.

## Checkpoint

Awaiting human review.
