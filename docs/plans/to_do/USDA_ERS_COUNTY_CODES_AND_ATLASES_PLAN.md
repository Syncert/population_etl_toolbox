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

To do. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

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

## Checkpoint

Next pickup: copy the starter into `usda_ers`, check in a trimmed RUCC 2023
CSV fixture with a Connecticut row, and write the failing replay test.
