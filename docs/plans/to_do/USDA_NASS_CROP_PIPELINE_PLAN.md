---
id: usda-crop
branch: feat/usda-crop
depends_on:
  - geography-reference
parallel_safe: true
complexity: high
verify:
  - make test-etl
  - make test-dags
  - make test-integration
---

# USDA NASS agricultural crop data pipeline plan

## Plan status

- **Status:** Proposed; no agricultural adapter is currently implemented
- **Last updated:** 2026-08-22
- **Source owner:** USDA National Agricultural Statistics Service (NASS)
- **Initial source:** NASS Quick Stats
- **Geography scope:** National, state, and county; county is the lowest level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-003 in the Census geography reference pipeline; resolve its current workflow location through the [plan index](../README.md)

## Implementation checkpoint

**Last updated:** 2026-08-22

**Current milestone:** Planning complete; implementation has not started

**Next pickup:** Complete NASS-001 by registering the first bounded crop basket and freezing its Quick Stats query contracts.

### Completed in the current slice

- [x] Defined the crop-focused Quick Stats scope and national/state/county geography boundary.
- [x] Defined registry-driven slicing, full classification identity, suppression, quality, revision, and aggregation contracts.
- [x] Split delivery into acceptance-tested discovery, capture/replay, silver, publication, and historical-bootstrap phases.

### Remaining

- [ ] NASS-001 — Freeze the initial product registry, bounded queries, source semantics, and fixtures.
- [ ] NASS-002 — Implement count-aware capture, deterministic slicing, quarantine, and offline replay.
- [ ] NASS-003 — Implement commodity/statistic/domain dimensions and crop observation silver data.
- [ ] NASS-004 — Implement gold products, glossary publisher, DAG, API, and integration coverage.
- [ ] NASS-005 — Backfill a bounded history and implement operational reconciliation.

## Objective

Publish source-transparent crop acreage, harvested acreage, yield, production, condition/progress, price, and value observations from USDA NASS while preserving commodity classification, statistic, unit, reference period, program, domain, suppression, revision, and geography.

The first product is crop-focused. Livestock, farm demographics, economics, environmental measures, and other Quick Stats domains may reuse the adapter later but require distinct registered dataset/measure contracts.

## Source-product scope

NASS Quick Stats is the primary observation source because it exposes agricultural statistics by commodity, location, and time and is the best NASS source for county-level data. Initial scope:

- `source_desc = SURVEY` crop acreage, yield, production, price/value, stocks, and progress/condition where available;
- `source_desc = CENSUS` crop measures from the Census of Agriculture where explicitly enabled; and
- aggregate levels `NATIONAL`, `STATE`, and `COUNTY` only.

Survey and Census of Agriculture observations remain distinct. The Census of Agriculture is periodic and provides uniformly defined detailed county data, while survey products have their own frequencies, coverage, forecasts/finals, and revisions.

Agricultural districts, regions, watersheds, ZIP Codes, and unspecified aggregates are retained only in raw captures until separately modeled. They must not be coerced into national/state/county.

## Geography contract

```text
us:1
state:SS
state:SS|county:CCC
```

- Resolve exact NASS state/county ANSI/FIPS components where supplied.
- Keep NASS aggregate-level and location fields as source evidence.
- Handle county equivalents through the shared Census geography version appropriate to the observation/source release.
- Do not join by state/county name.
- Do not synthesize a state or national observation by summing county rows unless a separately reviewed derivation proves the measure is additive, all required counties are present, suppression is absent, periods match, and the result is clearly labeled derived.
- Prefer provider-published national and state rows.

## Target package and runtime

```text
src/data_ingestion_toolbox/usda_nass/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_nass/
│   ├── dimensions.py
│   ├── values.py
│   └── transform.py
└── gold_nass/
    └── publisher.py

dags/usda_nass_crop_ingest_dag.py
sql/migrations/{sequence}_usda_nass_crop_pipeline.sql
tests/fixtures/usda_nass/
```

Use the required environment secret `USDA_NASS_API_KEY`. The deployment must
inject this named secret into every Airflow scheduler or worker Docker container
that can execute NASS ingestion when the container starts. The value must come
from the external stack's secret/environment configuration; it must not be baked
into an image or stored in a tracked environment file, Airflow DAG, database, or
capture. Read and validate it only when making an external request, and never
include it in request fingerprints, captured parameters/headers, logs, or
exception summaries.

## Query registry and slicing

Quick Stats has a large, multidimensional result space. Production ingestion must be allowlist/registry driven. Each registered product defines:

- source (`SURVEY` or `CENSUS`);
- sector/group/commodity/class/prodn-practice/util-practice/statistic category selections;
- national/state/county aggregate levels;
- year range and frequency/reference-period selections;
- domain/domain-category behavior;
- expected units and suppression symbols;
- request partitioning fields;
- parser/schema version; and
- release/finality expectations where source-supported.

Do not issue an unbounded “all Quick Stats” request. Partition by stable source dimensions and geography/time ranges, record every slice, and use provider count/discovery facilities where available before retrieving a slice.

## Capture and control design

- Capture exact JSON/CSV result payloads and relevant query metadata before parsing.
- Preserve every source field, including `short_desc`, `Value`, `CV (%)`, domain fields, location codes, period fields, reference period, load timestamp, and suppression/footnote representations.
- Store runs, registered queries, slices, retries, paging/count state, watermarks, and quarantine in `control`.
- Retain a changed provider response for the same logical query as a new capture/revision.
- Detect API truncation, row-limit responses, and partial slices before publication.
- Support offline replay from representative survey, census, county, suppression, and revision fixtures.
- Bulk downloadable files may be evaluated later for historical bootstrap, but must use the same raw-before-parse and conformed-silver contracts.

## Target silver model

### `silver_nass.dim_dataset_release`

One row per registered product/source release or extraction watermark, including program/source, release/load timestamps, query-contract version, final/forecast status when supported, and capture lineage.

### `silver_nass.dim_commodity`

Preserve the NASS commodity hierarchy and source labels/codes: sector, group, commodity, class, production practice, utilization practice, and related dimensions. Do not reduce identity to `commodity_desc` alone.

### `silver_nass.dim_statistic`

One source-backed statistic identity derived from exact Quick Stats classification fields and `short_desc`. Required attributes include statistic category, unit, value type, calculation basis, frequency, and whether additive behavior is explicitly known. Semantic interpretations that are not source-supported remain outside ETL.

### `silver_nass.dim_domain`

Preserve domain and domain category, including “TOTAL” categories as explicit source members. Domain categories often define subpopulations and cannot be discarded without changing the observation grain.

### `silver_nass.fact_crop_observation`

Proposed grain:

```text
dataset release/extraction
× full commodity classification
× statistic
× domain/category
× geo_id
× year/frequency/reference period
```

Required fields include exact source `Value`, parsed numeric value where valid, unit, CV text/numeric value where supplied, suppression/missing/status code, year, frequency, period, week-ending/reference date where supplied, source/load timestamp, program/source, aggregate level, source geography fields, `geo_sk`, source record key, capture ID, and transform version.

The exact source value remains available because NASS values can contain formatting, suppression, or other non-numeric representations. Parsing may not convert these to zero.

## Gold and serving products

- `gold_nass.crop_observation`: conformed crop facts with full classification and geography.
- `gold_nass.crop_series`: stable series identity for commodity/statistic/domain/geography/frequency combinations.
- `gold_nass.latest_release_observation`: latest validated source revision without deleting historical captures/releases.
- `gold_nass.measure_export`: provider-neutral glossary publisher contract.

The explorer/API must expose commodity, statistic, unit, source program, domain, geography, period/frequency, revision/load status, CV, and suppression. Acres, bushels, tons, dollars, percentages, and yield-per-acre values must never share an unlabeled metric.

## Data-quality and aggregation rules

- Exact uniqueness at the complete Quick Stats grain, not just commodity/geography/year.
- Validate configured units/statistics against the registry; quarantine unexpected schema/classification changes.
- Distinguish zero, suppressed, missing, not published, and non-numeric source values.
- Preserve CV/quality fields and do not rank close values without them when supplied.
- County codes must resolve to the shared geography version; unknown aggregate levels do not fall back to county.
- Forecast, preliminary, and final observations remain distinct where the source exposes that status.
- Survey and Census values remain separate even when labels and periods overlap.
- Yield, rate, price, percentage, and index measures are non-additive by default.
- Production/acreage counts are not automatically additive because suppression, coverage, estimation, and provider reconciliation can invalidate local sums.
- Partial slices cannot advance publication watermarks.

## Scheduling and revisions

NASS publishes products on multiple schedules, and Quick Stats downloadable data are updated frequently. The pipeline should:

- check registered product watermarks on business days or another evidence-based cadence;
- retrieve only changed/recent slices for regular operation;
- run periodic bounded historical reconciliation;
- preserve revised values and load timestamps;
- publish each registered product atomically; and
- emit publisher-ready state without waiting for glossary harvest.

The final cadence and incremental key must be proven during NASS-001 rather than inferred from the website update frequency alone.

## Implementation phases

### NASS-001 — Product/query contract discovery

- Register an initial crop basket such as corn, soybeans, wheat, and hay with acreage, yield, and production measures.
- Inventory exact Quick Stats fields, credentials, limits, count behavior, classifications, periods, suppression symbols, and revision/load semantics.
- Select national/state/county, survey/census, suppressed, CV, and revised fixtures.
- Define safe bounded query partitions.

**Acceptance:** Every request is generated from a reviewed registry entry and the expected observation grain is fully documented.

### NASS-002 — Capture, slicing, and replay

- Implement secret-safe requests, count/preflight checks, deterministic slices, payload capture, completeness checks, and quarantine.
- Add offline replay and changed-response retention.
- Prove an over-limit/truncated response cannot publish.

**Acceptance:** Representative queries replay without network access and all source values/quality markers are byte- or logically-equivalent to captures.

### NASS-003 — Silver dimensions and values

- Normalize full commodity, statistic, domain, time, and geography classifications.
- Parse numeric values without losing exact text or suppression.
- Resolve national/state/county geography.
- Retain source release/load/revision lineage.

**Acceptance:** Survey, Census, county, suppression, and CV fixtures retain distinct, exact identities and values.

### NASS-004 — Gold, glossary publisher, DAG, and API

- Publish crop observations/series and source-backed metric exports.
- Add filters for commodity, statistic, geography, period, source program, domain, and release.
- Add source notes for units, suppression, forecast/final status, and county coverage.

**Acceptance:** Consumers cannot aggregate incompatible units or mistake suppressed values for zero through the default contract.

### NASS-005 — Historical bootstrap and operational reconciliation

- Backfill a bounded reviewed history by registered product.
- Add per-slice row-count/checksum baselines and recent-versus-historical schedules.
- Exercise clean beta rebuild and full replay.

**Acceptance:** A fresh environment reproduces the selected history and repeated runs do not duplicate observations or erase revisions.

## Test plan

- Unit: query generation, bounds, classification identity, value/CV/suppression parsing, period normalization, FIPS mapping.
- Replay: survey/census and all three geography levels with networking disabled.
- Contract: raw-before-parse, secret redaction, no shared glossary DDL, no policy in gold.
- Integration: fresh bootstrap, truncation/partial-slice failure, rerun idempotency, changed revision retention, geography misses.
- Reconciliation: source counts and selected published totals only where source methodology supports comparison.
- API: multidimensional filters, exact units, suppression/CV exposure, latest/as-released behavior.

## Non-goals for the first release

- Farm-level or confidential respondent data.
- Livestock, operator demographics, or a universal all-NASS ingestion.
- City/place agricultural observations.
- Treating agricultural districts as counties.
- Interpolating suppressed values.
- Summing counties into provider-equivalent state/national values without a reviewed derivation contract.
- Commodity price forecasting or agronomic recommendations.

## Primary references

- [USDA NASS Quick Stats](https://www.nass.usda.gov/quick_stats/)
- [USDA NASS data and statistics](https://www.nass.usda.gov/data_and_statistics/index.php)
- [NASS Crops/Stocks survey](https://data.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Crops_Stocks/index.php)
- [NASS Census of Agriculture](https://www.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Census_of_Agriculture/)
- [ADR-0001 data-layer ownership boundaries](../../decisions/0001-data-layer-boundaries.md)
