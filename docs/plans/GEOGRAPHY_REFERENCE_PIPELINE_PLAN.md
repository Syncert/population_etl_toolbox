# Census geography reference pipeline plan

## Plan status

- **Status:** Proposed
- **Last updated:** 2026-08-18
- **Primary owner:** Shared reference-data pipeline
- **Depends on:** ARCH-004 raw capture/control foundation and the new-source expansion gate in [DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md](./DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md)
- **Enables:** Census PEP, CDC, FBI, USDA NASS, broader ACS geography, cross-source maps, and stable geographic joins

## Implementation checkpoint

**Last updated:** 2026-08-18

**Current milestone:** Target design complete; implementation has not started

**Next pickup:** Complete GEO-001 by freezing the canonical geography identifiers and adding nation/state/county/place contract tests.

### Completed in the current slice

- [x] Audited the existing `silver_ref` runtime, table grain, refresh behavior, and ownership limitations.
- [x] Chose an independent shared geography pipeline with stable identities and versioned attributes, geometry, and relationships.
- [x] Defined source-resolution, replay, cutover, rollback, validation, and provider-enablement contracts.

### Remaining

- [ ] GEO-001 — Freeze and test canonical nation/state/county/place identifiers.
- [ ] GEO-002 — Implement lossless reference-file capture, quarantine, and offline replay.
- [ ] GEO-003 — Implement versioned geography identity and attribute dimensions.
- [ ] GEO-004 — Implement versioned geometry and cross-boundary relationships.
- [ ] GEO-005 — Cut ACS and BLS over to the shared geography contract and reconcile existing observations.
- [ ] GEO-006 — Enable exact-code resolution for PEP, CDC, USDA NASS, and FBI.

## Decision

Census Gazetteer and TIGER/cartographic-boundary data will be ingested by an independent, shared geography pipeline. PEP and other observation pipelines will resolve their provider geography codes to this shared reference data; they will not own competing city, county, or state dimensions.

The supported product hierarchy is:

```text
United States
└── state
    ├── county or county equivalent
    └── place (the canonical level displayed as city when appropriate)
```

A Census place is not always contained by exactly one county. County and place are therefore sibling branches beneath state, with a versioned many-to-many relationship when a place intersects one or more counties. The system must not encode `county -> city` as a universally strict parent-child relationship.

`city` is a serving label, not a universal source geography type. The canonical type is `place`, with legal/statistical attributes distinguishing incorporated places, census-designated places, consolidated cities, and other Census classifications. FBI agency geography remains `agency` until an effective-dated, evidence-backed mapping to a Census place exists.

## Required geography depth by pipeline

| Pipeline | National | State | County | City/place | Lowest required canonical geography |
| --- | --- | --- | --- | --- | --- |
| Shared Census geography | Yes | Yes | Yes | Yes | `place` |
| Census ACS currently implemented | Yes | Yes | Yes | Future-compatible | `county` today |
| Census PEP planned | Yes | Yes | Yes | Yes, incorporated place | `place` |
| CDC illness/disease planned | Yes | Yes | Yes | No in initial product scope | `county` |
| FBI crime planned | Yes | Yes | Yes | Yes, only through validated agency/place mapping or provider-published city geography | `place`/`agency` |
| USDA NASS crops planned | Yes | Yes | Yes | No | `county` |
| BLS currently implemented | Yes | Yes where supported | Yes where supported | No | `county` |
| FRED currently implemented | Yes, plus source-specific regions | Where deterministically supported | Where deterministically supported | No | Source-dependent |

Source coverage is not permission to synthesize missing levels. State and national values must come from provider-published observations unless an explicitly reviewed derived product documents aggregation eligibility and completeness.

## Existing implementation

### Runtime and ownership

- `dags/silver_ref_dag.py` owns a monthly `silver_ref` DAG and invokes `sync_geo_dim()` independently from the observation DAGs.
- `src/data_ingestion_toolbox/silver_ref/geography.py` downloads Census Gazetteer state/county files and Census cartographic boundary shapefiles.
- `src/data_ingestion_toolbox/silver_ref/DDL/silver_ref.sql` creates the shared `silver_ref.dim_geo` table and a PostGIS GIST index.
- `src/data_ingestion_toolbox/census_acs/geography.py` is a second, legacy Gazetteer implementation that writes `raw_census.geo_dim`; `acs_ingest_dag.py` still invokes it. This duplicates shared-reference ownership and must be retired during cutover.

### Current table grain

`silver_ref.dim_geo` has one row per `(geo_level, geo_id)` and currently stores:

- surrogate key and canonical `geo_id`;
- state/county FIPS components;
- current names;
- representative latitude/longitude;
- one multipolygon geometry;
- `source_year`, `first_seen_year`, and `last_seen_year`; and
- current-looking `is_active`, source, and ingestion timestamp fields.

The implemented canonical identifiers are:

```text
us:1
state:SS
state:SS|county:CCC
```

The ACS mapper rejects any level other than U.S., state, or county. There is no place code, place type, generic Census GEOID/GEOIDFQ, geographic version, or relationship bridge.

### Current refresh behavior

The loader probes Gazetteer years back to its configured minimum, reads available state and county files, and groups all years by `(geo_level, geo_id)`. It keeps the latest attributes and only the minimum/maximum source years. Consequently:

- it is not a 14-year row-level geography history even though the README describes it that way;
- historical names, coordinates, area values, status, and boundary versions cannot be reconstructed;
- `is_active` is always loaded as true and missing entities are not explicitly retired;
- historical geometry is not retained;
- the latest cartographic boundary is attached while processing every Gazetteer year, so geometry is not aligned to the Gazetteer version;
- Gazetteer `ALAND`, `AWATER`, `GEOIDFQ`, USPS, LSAD, and functional-status fields are not retained;
- only state and county Gazetteer products are downloaded; and
- raw provider files are parsed before a lossless capture is committed.

Unit and integration tests currently verify legacy county parsing, canonical padding, polygon validity/SRID, idempotent upsert, and the current gold serving refresh. They do not verify place geography, version retention, retirement, cross-county place relationships, or offline replay from immutable captures.

## Authoritative source roles

| Source | Role | Must not be used as |
| --- | --- | --- |
| Census Gazetteer | Entity listings, codes, names, areas, representative coordinates, yearly reference snapshots | Polygon geometry or population observations |
| Census TIGER/Line or cartographic boundaries | Versioned polygon/multipolygon geometry and source relationship evidence | Population estimates |
| Census geographic relationship/change files | Cross-geography intersections, boundary/name/code changes when available | Guessed containment based only on names |
| Observation providers | Source codes and observations at their published geography | Owners of the canonical shared geography dimension |

Census documents Gazetteer files as reference listings containing identifiers, names, area measurements, and representative coordinates, with separate products for counties, county subdivisions, places, and other geography types. Population and housing counts are not part of Gazetteer files.

## Target data model

Names are provisional until migration design review, but the grains are required.

### `silver_ref.dim_geo_type`

One row per supported canonical geography type.

| Column | Purpose |
| --- | --- |
| `geo_type` | Stable key such as `nation`, `state`, `county`, `place`, `agency` |
| `display_label` | Provider-neutral label |
| `canonical_code_length` | Validation metadata where fixed |
| `is_census_geography` | Distinguishes Census entities from provider jurisdiction entities |
| `product_rank` | National/state/county/city navigation order, not a containment claim |

### `silver_ref.dim_geo_entity`

One stable identity row per canonical entity, independent of yearly attribute changes.

| Column | Purpose |
| --- | --- |
| `geo_sk` | Warehouse surrogate key |
| `geo_id` | Stable canonical key |
| `geo_type` | FK to `dim_geo_type` |
| `census_geoid` | Census concatenated code when applicable |
| `state_fips`, `county_fips`, `place_fips` | Typed code components; nullable by level |
| `first_seen_version`, `last_seen_version` | Discovery bounds, not substitutes for history |
| `created_at`, `updated_at` | Warehouse lineage |

Canonical IDs for the required hierarchy are:

```text
us:1
state:SS
state:SS|county:CCC
state:SS|place:PPPPP
```

County subdivision may be retained for source fidelity as `state:SS|cousub:CCCCC`, but it is not labeled city and is outside the initial public national/state/county/city hierarchy.

### `silver_ref.dim_geo_entity_version`

One row per `(geo_sk, geography_vintage, source_snapshot_id)`.

Required attributes include source year/vintage, `GEOIDFQ`, name, USPS, LSAD, functional status, legal/statistical classification, land/water area in square meters, representative latitude/longitude, active/retired status, and source capture lineage. A deterministic uniqueness rule must prevent duplicate versions while retaining changed source captures.

### `silver_ref.dim_geo_geometry_version`

One row per entity, boundary vintage, geometry source, and resolution.

Required attributes include geometry, SRID, source product, source resolution, source capture, validity result, and geometry checksum. Keeping geometry separate prevents repeated large values in entity snapshots and allows Gazetteer and boundary vintages to be selected independently and explicitly.

### `silver_ref.bridge_geo_relationship_version`

One row per effective-dated relationship between two entities.

Required fields include parent/related `geo_sk`, relationship type, geography vintage, overlap/weight fields when source-supported, evidence source, and capture lineage. Initial relationship types are:

- nation contains state;
- state contains county;
- state contains place;
- place intersects county; and
- provider agency serves/intersects place or county, only when validated.

No equal-allocation or name-matching weights may be invented.

### Current projections

- `silver_ref.dim_geo_current` selects the approved current entity version and approved current geometry version.
- `gold.dim_geo_latest` remains a serving projection until consumers migrate.
- A compatibility projection may temporarily expose the old `silver_ref.dim_geo` columns during the beta cutover, but the old table must not remain a second owner.

## Source geography resolution contract

Every observation silver transform must produce or quarantine a resolution record containing:

- provider source and dataset;
- source geography type and exact source code components;
- exact source geography label where supplied;
- source geography/boundary vintage where supplied;
- canonical `geo_id` and `geo_sk` when resolved;
- resolution method (`exact_code`, `provider_crosswalk`, `effective_dated_bridge`);
- resolution evidence/capture lineage;
- status (`resolved`, `ambiguous`, `unmapped`, `unsupported`); and
- a reason code for non-resolved records.

Names are descriptive evidence, never primary join keys. Unresolved observations remain captured and quarantined; they are not silently dropped or assigned to a similarly named place.

## Ingestion and replay design

1. Discover the explicitly supported source year rather than assuming the current calendar year exists.
2. Create deterministic requests for each geography type and source version.
3. Store the downloaded ZIP/text/shapefile components in shared append-only `raw_capture` before parsing.
4. Record request/run/retry/watermark state in `control`.
5. Replay captures offline into source-shaped silver staging.
6. Validate required fields, codes, row counts, duplicate natural keys, coordinate ranges, and geometry validity.
7. Upsert stable entity identities without rewriting history.
8. Insert immutable/versioned attribute and geometry rows.
9. Reconcile appearance/disappearance against a complete successful snapshot; never retire entities from a partial capture.
10. Build source-backed relationships and refresh the current projection atomically.
11. Emit a publisher-ready event for independent glossary harvesting.

## Implementation phases

### GEO-001 — Freeze and test the canonical geography contract

- Define supported types and canonical key formatting.
- Add `place_fips` and generic `census_geoid` handling.
- Document the county/place sibling relationship.
- Add pure unit tests for nation, state, county, and place IDs, including leading zeroes.
- Add rejection tests for malformed and ambiguous codes.

**Acceptance:** All four requested product levels have deterministic identifiers and no code path constructs a place key from a name.

### GEO-002 — Add lossless reference-file capture and offline replay

- Register a stable source code such as `CENSUS_GEO`.
- Capture Gazetteer and boundary downloads with checksums and source version metadata.
- Move URL probing and file manifests into control state.
- Add representative county/place Gazetteer and boundary fixtures.
- Quarantine malformed archives after preserving them.

**Acceptance:** A network-disabled replay rebuilds identical parsed records from fixtures, and parser failure cannot erase the capture.

### GEO-003 — Create versioned identity and attribute dimensions

- Add ordered migration DDL for geo type, entity, and entity-version tables.
- Load nation, states, counties, and places.
- Preserve source-supported attributes instead of selecting a hand-written subset.
- Reconcile retirements only after a complete snapshot.

**Acceptance:** Two yearly snapshots with a renamed or dissolved test place retain both versions and select the expected current record.

### GEO-004 — Version geometry and relationships

- Load matching state, county, and place boundaries at a documented resolution.
- Add state/county/place relationships from authoritative files or geometric intersection where reviewed and reproducible.
- Validate SRID, geometry type, validity, duplicate identity, and relationship coverage.
- Record cross-county places as multiple relationships.

**Acceptance:** A fixture place crossing two counties is not forced under one county, and every published geometry identifies its boundary vintage.

### GEO-005 — Cut over shared consumers

- Update ACS and BLS resolution to use the shared current/version contract.
- Remove the ACS-owned Gazetteer loader and `raw_census.geo_dim` ownership.
- Update gold glossary refresh and Martin tile/API join contracts.
- Reconcile every currently published observation `geo_id` before cutover.
- Correct README row-count/history claims using measured post-load values.

**Acceptance:** One geography pipeline owns reference data, existing national/state/county APIs remain compatible, and no observation is lost because of an unreported geography miss.

### GEO-006 — Enable planned providers

- Add PEP nation/state/county/place resolution.
- Add CDC nation/state/county resolution.
- Add USDA NASS nation/state/county resolution.
- Add FBI agency identity and effective-dated agency/geography bridges without equating agency to city.

**Acceptance:** Each provider passes exact-code and unmapped/quarantine contracts at its documented lowest supported level.

## Validation and observability

- Capture count, checksum change, and parser-version metrics by source version and geography type.
- Expected/actual row counts and duplicate natural keys by type/version.
- New, changed, missing, reactivated, and retired entity counts.
- Null/invalid coordinate and area counts.
- Geometry validity, SRID, empty geometry, and unexpected geometry-type counts.
- Observation resolution success rate by provider, dataset, level, and vintage.
- Relationship coverage, including cross-county place counts.
- Current-projection freshness and source-version watermark.

Alerts must distinguish a legitimately changed Census snapshot from an incomplete download or parser regression.

## Cutover and rollback

This beta may use a clean database rebuild and re-ingestion. Before reset, export the set of existing `geo_id` values referenced by silver/gold facts and prove the replacement dimension resolves them. Apply checked-in migrations, replay geography captures, validate, then replay observation providers. Rollback means rebuilding the disposable environment from the prior checked-in schema and captures; no ad hoc console-only repair is permitted.

## Explicit non-goals

- Address geocoding or point-address storage.
- Treating USPS ZIP Codes as Census ZCTAs.
- Inferring a city from an FBI agency name.
- Treating every county subdivision as a city.
- Fabricating national/state observations by summing local observations.
- Supporting tract, block group, block, school district, congressional district, or ZCTA in the first implementation slice.

## Primary references

- [Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
- [Census Gazetteer File Record Layouts](https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/gaz-record-layouts.html)
- [Census Data API geography guidance](https://www.census.gov/data/developers/geography.html)
- [ADR-0001 data-layer ownership boundaries](../decisions/0001-data-layer-boundaries.md)
- [Adding a data source checklist](../reference/ADDING_A_DATA_SOURCE.md)
