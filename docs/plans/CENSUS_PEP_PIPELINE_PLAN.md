# Census Population Estimates Program pipeline plan

## Plan status

- **Status:** Proposed; PEP is described in product documents but no adapter is currently implemented
- **Last updated:** 2026-08-18
- **Source owner:** U.S. Census Bureau Population Estimates Program
- **Geography scope:** National, state, county, and city/place; place is the lowest canonical level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-004 in [GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md](./GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md)

## Implementation checkpoint

**Last updated:** 2026-08-18

**Current milestone:** Planning complete; implementation has not started

**Next pickup:** Complete PEP-001 by registering the first totals/components datasets and freezing their vintage and geography contracts.

### Completed in the current slice

- [x] Defined the initial national/state, county, and incorporated-place product sequence.
- [x] Defined separate observation-year, release-vintage, revision, and geography-basis contracts.
- [x] Split delivery into acceptance-tested registry, capture/replay, silver, publication, and follow-on demographic phases.

### Remaining

- [ ] PEP-001 — Implement the dataset/vintage registry and prove release discovery.
- [ ] PEP-002 — Implement lossless API/bulk capture, completeness checks, and offline replay.
- [ ] PEP-003 — Implement national/state and county totals/components silver data.
- [ ] PEP-004 — Implement incorporated-place totals and geography reconciliation.
- [ ] PEP-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage.
- [ ] PEP-006 — Add demographic-characteristics datasets after the totals contract is proven.

## Objective

Publish annual population estimates, population change, demographic components, and selected demographic characteristics while preserving PEP dataset, estimate vintage, estimate date, revision history, universe, and geographic basis.

PEP is an observation pipeline. It does not own city/state/county reference rows. Exact source geography codes resolve to the independent Census geography pipeline.

## Source-product scope

PEP comprises multiple releases with different measures and supported geographies. The adapter must use an explicit dataset registry rather than assume every PEP endpoint supports every level.

Initial implementation order:

1. national/state total population and components of change;
2. county total population and components of change;
3. incorporated-place population totals (“cities and towns”); and
4. optional demographic-characteristics datasets after totals/vintage behavior is proven.

Housing-unit estimates and metropolitan/micropolitan products are separate follow-on dataset contracts. Minor civil division (MCD) records may be retained source-faithfully when present in subcounty files, but they use `county_subdivision`, are not labeled city, and are not part of the initial public city hierarchy.

## Vintage and revision contract

A PEP vintage is not the same as the observation year. Each annual vintage contains a time series back to the latest decennial census and supersedes the prior vintage for current-use estimates. Therefore the fact identity must retain both:

- `estimate_date` or estimate year, such as July 1, 2023; and
- `pep_vintage`, such as Vintage 2025.

No load may overwrite Vintage 2024 observations merely because Vintage 2025 contains estimates for the same year. Gold provides both:

- an **as-released/revision** product containing all vintages; and
- a **latest-vintage** projection selecting the newest complete release.

The geography resolution basis follows the documented PEP release/vintage, not blindly the estimate year. A 2021 estimate published in Vintage 2025 must not automatically join to Gazetteer 2021 if PEP documents another geographic basis.

## Geography contract

```text
us:1
state:SS
state:SS|county:CCC
state:SS|place:PPPPP
```

- “City” in the serving product means a PEP incorporated-place observation resolved to a canonical Census `place`.
- Preserve Census legal/statistical classification so incorporated places are distinguishable from CDPs and other place types.
- County and place are sibling branches under state; a place can cross county boundaries.
- Never insert a county code into the canonical place key merely to force a strict national/state/county/city tree.
- MCD/county-subdivision records remain a separate type and cannot be mixed with place observations.
- Source names are retained for lineage but exact codes drive resolution.

## Target package and runtime

```text
src/data_ingestion_toolbox/census_pep/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_pep/
│   ├── totals.py
│   ├── components.py
│   ├── subcounty.py
│   └── transform.py
└── gold_pep/
    └── publisher.py

dags/census_pep_ingest_dag.py
sql/migrations/{sequence}_census_pep_pipeline.sql
tests/fixtures/census_pep/
```

The registry records the official Census API dataset path or bulk-file product, supported vintages, variables/layout version, geography levels, release status, and parser version. Bulk subcounty files and API responses may share conformed silver tables but retain distinct capture/media/source-product lineage.

Use `CENSUS_API_KEY`, required by the current Census API contract. Validate it only during requests and exclude it from fingerprints, captures, logs, headers, and error text.

## Capture and control design

- Discover supported datasets, variables, groups, examples/geographies, and release metadata from official Census endpoints/files.
- Capture metadata/variable schemas and observation responses before parsing.
- Preserve Census API two-dimensional arrays exactly, including header order and value strings.
- Preserve bulk ZIP/CSV/XLSX source bytes before workbook/table parsing.
- Slice deterministically by PEP dataset, vintage, geography, state when required, variables, and page/file identity.
- Keep run/request/retry/slice/watermark/quarantine state in `control`.
- Treat a missing geography slice or changed file layout as an incomplete release, not a partial successful publication.
- Retain every changed vintage/file checksum.

## Target silver model

### `silver_pep.dim_dataset_release`

One row per PEP source product and vintage, including release code/title, API/bulk identity, vintage year, release/publication date, decennial base, geography basis, status, schema version, and capture lineage.

### `silver_pep.dim_measure`

One source-backed PEP variable/measure identity. Examples include resident population, numeric/percent change, births, deaths, domestic migration, international migration, and residual where supported. Required attributes include exact variable code/label, concept, unit, value type, component status, and population universe.

### `silver_pep.dim_demographic_slice`

Normalized age, sex, race, Hispanic-origin, and other categorical dimensions for the later characteristics datasets. Preserve source codes and labels. Overall/all-person categories must be explicit members rather than nulls.

### `silver_pep.fact_population_estimate`

Proposed grain:

```text
PEP dataset release/vintage
× measure
× geo_id
× estimate date/year
× demographic slice where applicable
```

Required fields include exact source value text, parsed numeric value, unit, estimate date, vintage, census-base/estimate marker where supplied, source geography codes/name, `geo_sk`, resolution basis/status, source record identity, capture ID, and transform version.

Population levels and components may share the fact contract only when measure identity and units remain explicit. Rates, counts, and percentages cannot collide.

## Gold and serving products

- `gold_pep.population_estimate_revision`: every validated vintage.
- `gold_pep.population_estimate_latest`: newest complete vintage per product and observation key.
- `gold_pep.population_change`: deterministic change fields only when directly source-published or calculated from compatible levels within the same vintage and documented as derived.
- `gold_pep.measure_export`: provider-neutral glossary publisher contract.

The API/source explorer must expose PEP vintage, estimate date, geography/boundary basis, measure/unit, release, and revision context. PEP estimates remain distinct from ACS survey estimates and decennial counts.

## Data-quality rules

- Exact uniqueness at dataset/vintage/measure/geography/date/demographic grain.
- Every geography resolves at the documented release basis or is quarantined with its exact source codes.
- Population counts cannot be negative; component measures may be negative where source semantics permit.
- Census estimate-base, census count, and annual estimate labels remain distinct.
- National/state/county/place coverage is reconciled against the release manifest, not a hard-coded timeless count.
- Place records are validated against incorporated-place classification for city serving.
- Prior vintages remain immutable after parsing; changed captures produce a new source-revision lineage.
- Cross-vintage comparisons are labeled revisions, not time changes.
- Intra-vintage population change uses compatible rows and dates only.

## Scheduling and release discovery

PEP products are released on different annual schedules by geography and demographic detail. Use periodic official metadata/release checks rather than one assumed annual date. Publish each registered product/vintage independently only when all expected slices validate. A later product release must not block already complete products, and glossary harvest must remain downstream and independent.

## Implementation phases

### PEP-001 — Dataset and vintage registry

- Inventory current official API and bulk products for national/state, county, and incorporated place.
- Record supported geography, variables, layouts, release identity, revision semantics, and geographic basis.
- Select representative current and prior-vintage fixtures, including revised same-year estimates.
- Define source codes and metric keys without editing a closed provider enum.

**Acceptance:** Every initial product has a versioned contract and the plan can distinguish estimate year from PEP vintage in every key.

### PEP-002 — Capture and offline replay

- Implement Census API and bulk-file capture through the shared raw/control foundation.
- Capture metadata/layouts and observations before parsing.
- Add deterministic slices, completeness manifests, schema-drift quarantine, and offline replay.

**Acceptance:** Fixtures for API and subcounty bulk data replay with network disabled, and incomplete state/place files cannot publish.

### PEP-003 — National/state and county totals/components

- Normalize measures, dates, vintages, exact values, and geographic codes.
- Resolve national/state/county entities using the documented geography basis.
- Retain source-published totals and components without cross-product inference.

**Acceptance:** Two vintages for the same estimate year coexist and latest-vintage selection is deterministic.

### PEP-004 — Incorporated-place/city totals

- Parse subcounty incorporated-place records separately from MCD records.
- Resolve state/place codes to canonical `place` entities.
- Validate place classification and cross-county relationships without forcing a county parent.
- Withhold unsupported/unresolved entities from city gold while retaining them in silver/quarantine.

**Acceptance:** Incorporated-place fixtures serve as cities, MCD fixtures do not, and a cross-county place retains all authoritative relationships.

### PEP-005 — Gold, glossary publisher, DAG, and API

- Publish revision and latest-vintage products atomically by dataset release.
- Expose source explorer filters for product, vintage, estimate period, measure, geography, and demographic slice.
- Publish source-backed measure metadata and emit publisher-ready events.

**Acceptance:** Consumers cannot accidentally mix PEP vintage revisions with year-over-year population change or ACS population estimates.

### PEP-006 — Demographic characteristics follow-on

- Add age/sex/race/Hispanic-origin datasets only after totals are operational.
- Preserve universes and categorical codebooks per dataset/vintage.
- Add dimensional explosion/volume tests and stratified API limits.

**Acceptance:** Overall and subgroup rows have distinct keys and no subgroup can be summed without a source-supported partition contract.

## Test plan

- Unit: two-dimensional API parsing, bulk layouts, vintage keys, negative component handling, demographic slices, place/MCD distinction, geography formatting.
- Replay: current and prior vintages, changed same-year estimate, malformed header, missing state slice.
- Contract: raw-before-parse, secret redaction, no shared glossary ownership, no policy-bearing gold columns.
- Integration: fresh bootstrap, full reset/re-ingestion, atomic product publication, rerun idempotency, historical vintage retention, geography quarantine.
- Reconciliation: source row counts and published totals only where source documentation permits.
- API: latest/as-released selection, city/place filters, vintage visibility, source notes, ACS/PEP distinction.

## Non-goals for the first release

- Population projections.
- Treating ACS estimates as substitutes for PEP or vice versa.
- Labeling every Census place or MCD as a city.
- Assigning a place to one county when it crosses county boundaries.
- Destructively replacing prior vintages.
- Generating local estimates for geographies PEP does not publish.

## Primary references

- [Census Population Estimates APIs](https://www.census.gov/data/developers/data-sets/popest-popproj/popest.html)
- [Census city and town population datasets](https://www.census.gov/data/datasets/time-series/demo/popest/2020s-total-cities-and-towns.html)
- [Census Population Estimates release schedule](https://www.census.gov/programs-surveys/popest/about/schedule.html)
- [Census Data API geography guidance](https://www.census.gov/data/developers/geography.html)
- [ADR-0001 data-layer ownership boundaries](../decisions/0001-data-layer-boundaries.md)
