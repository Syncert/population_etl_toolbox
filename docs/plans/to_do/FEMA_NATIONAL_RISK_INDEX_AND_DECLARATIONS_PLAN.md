---
id: fema-nri-declarations
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/fema_nri -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_fema_nri_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# FEMA National Risk Index and disaster declarations: hazard losses and declared disasters by county

## Status

To do. Drafted 2026-10-06 by the second-tier source scouting plan from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

Floods, tornadoes, and wildfire are place facts. FEMA publishes, for every county and census tract, an Expected Annual Loss in dollars for each of 18 natural hazards, and OpenFEMA lists every federally declared disaster by designated county since 1964. Together they fill the hazards half of the new Land and Environment chapter with provider-published figures rather than a third-party ranking.

## Verified contract

- **NRI files.** Version 1.20 (December 2025), as county and census-tract downloads in table (CSV), shapefile, and geodatabase formats, e.g. `https://www.fema.gov/about/reports-and-data/openfema/nri/v120/NRI_Table_Counties.zip` and `.../NRI_Table_CensusTracts.zip` ([OpenFEMA NRI data page](https://www.fema.gov/about/openfema/data-sets/national-risk-index-data); formats also in the [NRI FAQ, Dec 2025](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). The versioned path segment (`v120`) means each version is a distinct URL.
- **NRI data dictionary** (CSV, 479 fields, every row tagged `Version 1.20.0`): [data dictionary](https://fema.maps.arcgis.com/sharing/rest/content/items/4b9db412e99542029b3c37c37ad714bb/data). Identity: `STATEFIPS`, `COUNTYFIPS`, `STCOFIPS`, `TRACTFIPS`, `NRI_ID`, `COUNTYTYPE`, `NRI_VER`. Context: `POPULATION` (2020), `BUILDVALUE` ($), `AGRIVALUE` ($), `AREA` (sq mi). Composite EAL: `EAL_VALT`, `EAL_VALB`, `EAL_VALP`, `EAL_VALPE`, `EAL_VALA`. Per hazard, with prefixes `AVLN CFLD CWAV DRGT ERQK HAIL HWAV HRCN ISTM LNDS LTNG IFLD SWND TRND TSUN VLCN WFIR WNTW`: `_EVNTS`, `_AFREQ` (annualized frequency), `_EXPT` and its parts (exposure), `_HLRB/_HLRP/_HLRA` (historic loss ratio), `_EALT/_EALB/_EALP/_EALPE/_EALA`, and the scores and ratings `_EALS`, `_EALR`, `_RISKV`, `_RISKS`, `_RISKR`.
- **Units.** EAL is "the average economic loss in dollars resulting from natural hazards each year", the product of annualized frequency, exposure, and historic loss ratio, for buildings, population, and agriculture ([NRI version and update documentation](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_data-version-update-documentation.pdf); [FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). Population EAL appears both as `EAL_VALP` and as a dollar `Population Equivalence` field (`EAL_VALPE`); the exact unit of `EAL_VALP` is an open item.
- **Composite scores are FEMA composites.** `RISK_VALUE/RISK_SCORE/RISK_RATNG` multiply EAL by a Community Risk Factor derived from social vulnerability and community resilience; scores are national percentiles within the same level (county or tract) and ratings come from k-means clustering on cube-rooted values ([FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). They are relative ranks, not measurements.
- **Revisions.** Versions v1.17.0 (2020), v1.18.0 (2021), v1.18.1 (2021), v1.19.0 (March 2023), v1.20.0 change methods, periods of record, and boundaries; v1.20.0 renamed "Riverine Flooding" to "Inland Flooding" and redesigned that model ([version documentation](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_data-version-update-documentation.pdf)). No fixed cadence is published. The NRI also states it reports no margins of error ([FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)).
- **Declarations.** `https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries` (dataset `openfema-47`, records from 1953, county not available before 1964, Fire Management records partial, refresh `R/PT20M`), also as full `.csv`/`.json` downloads ([OpenFEMA DataSets metadata](https://www.fema.gov/api/open/v1/DataSets?$filter=name%20eq%20'DisasterDeclarationsSummaries'); [data dictionary](https://www.fema.gov/openfema-data-page/Disaster-Declarations-Summaries-v2)). One row per declaration per designated area. Fields include `femaDeclarationString`, `disasterNumber`, `declarationType` (DR, EM, FM), `declarationDate`, `incidentType`, `designatedIncidentTypes`, `incidentBeginDate`, `incidentEndDate`, `ihProgramDeclared`, `iaProgramDeclared`, `paProgramDeclared`, `hmProgramDeclared`, `tribalRequest`, `fipsStateCode`, `fipsCountyCode`, `placeCode`, `designatedArea`, `lastRefresh`, `hash`, `id` ([field metadata](https://www.fema.gov/api/open/v1/DataSetFields?$filter=openFemaDataSet%20eq%20'DisasterDeclarationsSummaries'%20and%20datasetVersion%20eq%202)).
- **API mechanics.** No key; `$filter`, `$select`, `$orderby`, `$skip`, `$top` (default 1,000, maximum 10,000); formats JSON, CSV, JSONA, JSONL, GeoJSON, Parquet; `$allrecords=true` for full downloads; `lastRefresh` supports incremental pulls, no more often than the dataset refresh ([OpenFEMA API documentation](https://www.fema.gov/about/openfema/api)).

## Geography

- **NRI.** Native grain is county and census tract, keyed by `STCOFIPS` and `TRACTFIPS`. Boundaries are 2021 TIGER/Line for tracts and counties, and 2024 for Connecticut, which uses the nine planning regions as county equivalents ([FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). Counties resolve through `silver_ref.geography_resolution` / `silver_ref.dim_geo_entity` by FIPS code and boundary vintage. Connecticut rows resolve to the planning-region entities and are never mapped to the legacy counties. Tracts are captured and kept in silver, but they are published only after [`SUB_COUNTY_GEOGRAPHY_PLAN.md`](SUB_COUNTY_GEOGRAPHY_PLAN.md) lands tract identity. Territory rows resolve by FIPS where the geography layer has them. Otherwise they are quarantined with a reason, not dropped.
- **Declarations.** One designated area per row, keyed by `fipsStateCode` + `fipsCountyCode`. `fipsCountyCode = '000'` marks a statewide designation or a non-county area such as an Indian reservation. Those rows are identified by `placeCode` (which is '99' + county FIPS for counties) and are not county rows ([field metadata](https://www.fema.gov/api/open/v1/DataSetFields?$filter=openFemaDataSet%20eq%20'DisasterDeclarationsSummaries'%20and%20datasetVersion%20eq%202)). Tribal declarations are filed under their state. Connecticut declarations in 2024 still carry legacy county codes (observed: `09009`, `09003`). The county grain therefore needs a vintage-aware resolution. `designatedArea` text is kept for display only and is never used to match.

## Suppression and missing values

The NRI encodes missingness as ratings: `Not Applicable` (hazard not geographically possible), `Insufficient Data`, `Data Unavailable` (social vulnerability and resilience only), `No Rating` (EAL equal to zero), and `No Expected Annual Losses` (an EAL factor equals zero) ([FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). Silver stores the value as NULL with a typed status from the rating column whenever the rating is one of the non-measure codes. A recorded `0` is kept as zero only when its rating is `No Rating` or the value is published as zero. A blank cell is never turned into zero. Territories have no Risk Index, social vulnerability, or resilience values, and that state is recorded as unavailable. For declarations, nullable dates (`incidentEndDate`, `disasterCloseoutDate`) stay NULL and mean "not ended / not closed".

## Terms of use and licensing

NRI data "are meant for planning purposes only" and "may be used for commercial purposes", with attribution to FEMA and the preferred dataset citation "Federal Emergency Management Agency. (2025). FEMA National Risk Index Data v 1.20.0 [Dataset]" ([FAQ](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_faq-page-documentation.pdf)). The OpenFEMA terms require citing the dataset name, version, and access date, plus the statement "This product uses the Federal Emergency Management Agency's OpenFEMA API, but is not endorsed by FEMA". They also forbid using the FEMA or DHS logos, presenting modified data as FEMA's, and re-identification ([OpenFEMA terms and conditions](https://www.fema.gov/about/openfema/terms-conditions)). No API key is required and no rate limit is published ([API documentation](https://www.fema.gov/about/openfema/api)). Automated county-level use is permitted. Note that fema.gov returned HTTP 403 to a plain scripted `curl` HEAD of the NRI zip during scouting, while the `/api/open/` endpoints answered normally.

## Proposed adapter

- Package `src/data_ingestion_toolbox/fema_nri/`, with `source_code = "fema_nri"` and two capture streams, `nri` (versioned file) and `declarations` (OpenFEMA API).
- First measures, at county grain, all labeled exactly as FEMA publishes them:
  - `EAL_VALT` and the per-hazard `*_EALT` for the hazards present.
  - `*_AFREQ` (annualized frequency) for flood, tornado, wildfire, hurricane, and heat wave.
  - The count of declarations per county per year by `declarationType` and `incidentType`, which is a deterministic count of provider rows.
- `RISK_*`, `SOVI_*`, `RESL_*`, and the score and rating fields are captured and kept in silver. They are not published in the first release. If a later release publishes them, they carry FEMA's labels verbatim ("National Risk Index - Score - Composite"), the relative-percentile basis, and the version.
- Feeds the new **Land and Environment** chapter.

## Deliverables

1. **Config and identity.** `config.py` from the starter, with no I/O at import. It declares the NRI version path (`v120`), the field list onboarded, and the declarations endpoint, timeouts, pool, and connection ID. Register the provider-neutral source identity. No key is needed, which is documented, and the credential-hygiene test asserts that none is sent.
2. **Raw capture.** Append-only lossless capture of the NRI county and tract table zips (checksum, retrieval time, HTTP metadata, `NRI_VER`) and of declaration pages (`$orderby=id`, `$skip` paging, request fingerprint) before parsing.
3. **Control state.** Slices per (stream, version) and per declarations page or `lastRefresh` watermark. Retries, errors, and quarantine live in the control plane.
4. **Silver.** Typed NRI facts per (geo, field, version) with the status column from the rating codes above. Declaration rows are keyed by `id`, revised by `hash`, with vintage-aware geography resolution and statewide/`000` rows kept as non-county designations.
5. **Gold.** Deterministic county publication of the first measures with version as the release identity. Metric identity includes the hazard and consequence type, so `RFLD` (v1.19) and `IFLD` (v1.20) are not silently joined. Declaration counts carry the counting rule in lineage.
6. **Publisher and glossary.** A versioned glossary publisher contract for each published field, using FEMA's field alias and units, with no `gold_glossary` DDL.
7. **API dispatch.** `SOURCE_DISCOVERY` and `OBSERVATION_DISPATCH` entries in `apps/api/registry.py`, with the consumer-guide section (EAL in dollars per year, FEMA-modeled, no margins of error, version comparability) and the OpenAPI snapshot updated.
8. **DAG.** An `ensure_fema_nri_schema` task upstream of capture. Declarations run on a daily schedule and the NRI file on manual or version-change triggers. Manifest entries go in the silver, gold, and publisher phases.
9. **Data quality.** Every county FIPS resolves or is quarantined. `EAL_VALT` is at least 0 when present. Per-hazard EAL statuses are consistent with their rating codes. Declaration `id` is unique, and every `fipsCountyCode` is either `000` or resolvable.
10. **Fixtures.** A small NRI county CSV slice (one state, including a `Not Applicable` hazard and a Connecticut planning region), a tract slice, and two declarations pages including a `000` row and a legacy Connecticut row.
11. **Tests.** Unit, contract, offline capture-replay, malformed-payload quarantine, rerun idempotency, a `tests/external/` live contract module registered in the `external-contract` workflow, and catalog, operations, and bootstrap/reset documentation.

## Acceptance criteria

- Config imports with no I/O. No credential is sent or captured.
- Fixtures replay offline into silver. A rating code yields NULL with a typed status, never `0`. A malformed CSV row or page is quarantined.
- Connecticut NRI rows resolve to planning regions and legacy declaration rows resolve by vintage, both by FIPS. No row is matched by name.
- Gold publishes `EAL_VALT` and per-hazard `*_EALT` per county with the NRI version as the release. No `RISK_*`, `SOVI_*`, or `RESL_*` value is published as a measure.
- Rerun is idempotent. A changed declaration `hash` creates a revision and keeps both captures.
- `/api/v1/observations` serves a county EAL and a declaration count from the fixture, and capabilities advertise the source. The consumer guide and OpenAPI snapshot are updated.
- Quality rules, DAG parse, manifest registration, and the external contract are in place. Unit, integration, DAG, and Ruff checks pass, and the evidence is recorded here.

## Open items to resolve during implementation

- The unit of `EAL_VALP` (people or dollars) versus `EAL_VALPE` (dollars), from the [technical documentation](https://www.fema.gov/sites/default/files/documents/fema_national-risk-index_technical-documentation.pdf), which was not read in full during scouting.
- The v1.20.0 date: the version documentation says "publicly released on June 25, 2025", while the data page and dictionary say December 2025. Pick the release date from the file itself.
- Whether the table zips contain only CSV or also a state-level table, and the exact CSV header casing. Not verified, because the download was refused to scripted HEAD.
- The full text of the NRI-specific Terms and Conditions, which the data page points to but which was not located.
- The declaration count rule: count distinct `disasterNumber` per county, and decide whether a statewide (`000`) designation counts toward every county. Default is no, shown separately.
- Whether the declarations stream belongs in this package or in a separate `fema_openfema` adapter.

## Checkpoint

Next pickup: copy the starter into `fema_nri`, fetch one state's county rows from the v1.20 table zip as a fixture, and write the failing replay test that asserts a `Not Applicable` hazard lands as NULL with status, not zero.
