# CDC illness and disease data pipeline plan

## Plan status

- **Status:** Proposed; no CDC adapter is currently implemented
- **Last updated:** 2026-08-22
- **Source owner:** Centers for Disease Control and Prevention
- **Initial products:** U.S. Chronic Disease Indicators (CDI) and PLACES county data
- **Geography scope:** National, state, and county; county is the lowest initial level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-003 in [GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md](./GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md)

## Implementation checkpoint

**Last updated:** 2026-08-22

**Current milestone:** Planning complete; implementation has not started

**Next pickup:** Complete CDC-001 by freezing the CDI and PLACES county asset IDs and their versioned source contracts.

### Completed in the current slice

- [x] Defined the initial CDI national/state and PLACES county product boundary.
- [x] Defined the source-transparent geography, release, measure, stratum, uncertainty, and suppression contracts.
- [x] Split delivery into acceptance-tested discovery, capture/replay, silver, and publication phases.

### Remaining

- [ ] CDC-001 — Freeze source asset, schema, paging, release, and methodology contracts.
- [ ] CDC-002 — Implement lossless capture, deterministic paging, quarantine, and offline replay.
- [ ] CDC-003 — Implement and reconcile CDI national/state silver data.
- [ ] CDC-004 — Implement and reconcile PLACES county silver data.
- [ ] CDC-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage.

## Objective

Publish source-transparent illness, chronic-disease, risk-factor, and health-outcome observations without presenting unlike surveillance products as one homogeneous measure set. The first release combines:

- **CDI:** provider-published national and state estimates; and
- **PLACES county:** model-based small-area county estimates.

The common gold/API contract may expose both products, but dataset, methodology, population basis, value type, adjustment status, confidence interval, and release must remain visible. County PLACES values must not be summed or averaged to manufacture state or national CDI values.

This plan intentionally stops at county even though PLACES also publishes place, tract, and ZCTA products. City/place health ingestion requires a later approved scope change and does not block the requested national/state/county product.

## Source and product boundaries

| Product | Geographic levels | Typical measure character | Required warning |
| --- | --- | --- | --- |
| CDC CDI | National and state/territory | Estimates compiled from multiple surveillance sources | Indicators can differ by source, population, periodicity, and stratification |
| CDC PLACES county | County; U.S. comparison values may also be present | Model-based small-area prevalence and related estimates | Local values are modeled, often for adults, and are not direct case counts |

Future infectious-disease or emergency-surveillance assets must be registered as separate datasets with separate parsers and release contracts. A shared CDC provider does not make all CDC assets semantically comparable.

## Geography contract

```text
us:1
state:SS
state:SS|county:CCC
```

- Resolve exact provider location codes to the shared versioned geography dimension.
- Retain territory and special-jurisdiction rows source-faithfully; do not coerce them into a state FIPS mapping when no supported canonical entity exists.
- Use the geography basis documented by each CDC release. PLACES release/boundary metadata must be retained because recent releases use 2020 Census geographies and county equivalents.
- Never use location names as primary joins.
- Do not infer suppressed or missing counties as zero.

## Target package and runtime

```text
src/data_ingestion_toolbox/cdc/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_cdc/
│   ├── cdi.py
│   ├── places_county.py
│   └── transform.py
└── gold_cdc/
    └── publisher.py

dags/cdc_ingest_dag.py
sql/migrations/{sequence}_cdc_pipeline.sql
tests/fixtures/cdc/
```

Use a registry entry per CDC asset ID, parser version, expected columns, geography levels, update cadence, and source documentation. Do not build a generic parser that guesses measure meaning from arbitrary Socrata columns.

Reserve `CDC_SOCRATA_APP_TOKEN` as the environment-secret name for the Socrata
application token used by CDC Open Data requests. This is the public-read app
token sent as `X-App-Token`; the OAuth secret token is not required for this
pipeline. The deployment must inject `CDC_SOCRATA_APP_TOKEN` into every Airflow
scheduler or worker Docker container that can execute CDC ingestion when the
container starts. The value must come from the external stack's
secret/environment configuration; it must not be baked into an image or stored
in a tracked environment file, Airflow DAG, database, or capture. The adapter
may permit an empty value only while the selected CDC API contract supports
anonymous reads, and it must read and validate a configured value only at
request execution.

## Capture and control design

- Use stable CDC Open Data/Socrata asset identifiers rather than mutable display titles.
- Capture dataset metadata/schema responses as well as observation pages.
- Page deterministically with an explicit stable ordering and recorded query parameters.
- Commit every successful response to `raw_capture` before parsing.
- Preserve exact value strings, footnotes, null/suppression representations, confidence bounds, and source record identifiers.
- Store requests, page cursors, retries, dataset watermarks, and quarantine state in `control`.
- Detect a dataset replacement, schema change, or backward-moving update watermark and stop publication until validated.
- `CDC_SOCRATA_APP_TOKEN`, when configured for higher limits, must be validated
  at request time and excluded from fingerprints, captures, selected headers,
  logs, and errors.

## Target silver model

### `silver_cdc.dim_dataset_release`

One row per CDC asset and published release/version, including asset ID, title, publisher/program, release/update timestamps, methodology URL, geography basis, parser contract version, and capture lineage.

### `silver_cdc.dim_measure`

One source-backed measure identity per dataset. Preserve CDC question/measure identifiers, topic, response category, unit, value type, population/universe, crude/age-adjusted status, and source label. Similar labels in CDI and PLACES remain different measure identities unless an explicit source crosswalk supports equivalence.

### `silver_cdc.dim_stratum`

Normalized source strata such as sex, age group, race/ethnicity, and overall population. Preserve exact source category codes and labels. `Overall` is a real stratum, not a null that can collide with missing metadata.

### `silver_cdc.fact_health_observation`

Proposed grain:

```text
dataset release
× measure
× geo_id
× period
× value type/adjustment
× stratum combination
```

Required fields include numeric value where parseable, exact source value text, unit, lower/upper confidence limits, numerator/denominator or sample size where published, suppression/missing flag, footnote code/text, source record identity, `geo_sk`, capture ID, and transformation version.

Rows with unparseable values remain represented with their source text and a typed status; they are not silently dropped.

## Gold and serving products

- `gold_cdc.health_observation`: data-derived observations with release, method, uncertainty, and geography.
- `gold_cdc.measure_export`: provider-neutral glossary publisher contract.
- `gold_cdc.latest_release_observation`: latest release selector that retains the observation period and never overwrites prior releases.
- Source-specific API filters for dataset, measure, geography, period, stratum, adjustment, and release.

Do not publish a single unlabeled `rate` or `prevalence` field. Values require their unit, denominator/population basis, adjustment status, and method.

## Data-quality rules

- Exact uniqueness at the documented observation grain.
- County codes must resolve at the expected geography vintage.
- Confidence bounds, when present, must bracket the estimate; violations quarantine the affected record or release according to severity.
- Percent/prevalence ranges are validated using the source unit, not a global assumption.
- Missing, suppressed, unreliable, and not-applicable states remain distinct.
- CDI national/state and PLACES county values are never conflated through label-only matching.
- Release row counts and per-geography/per-measure counts are compared with prior successful releases using thresholds that allow documented source change.
- Partial page ingestion cannot advance the published watermark.

## Scheduling and revisions

- Check metadata on a modest recurring schedule, then ingest only when the provider update/version changes.
- Retain each captured release and its parsed observations.
- Select latest releases in a projection, not by destructive replacement.
- Rebuild any release offline from captures after parser corrections.
- Emit a publisher-ready event only after the full release validates and commits.

## Implementation phases

### CDC-001 — Source-contract discovery

- Freeze initial CDI and current PLACES county asset IDs.
- Record columns, types, source keys, release semantics, paging behavior, and methodology.
- Choose representative overall, stratified, suppressed, missing, and confidence-interval fixtures.
- Document which national/state values are direct/provider-published and which county values are modeled.

**Acceptance:** The registry contains no inferred measure or geography semantics and has a versioned schema contract for both products.

### CDC-002 — Capture, paging, and replay

- Implement secret-safe Socrata requests, metadata capture, deterministic paging, and checksum-backed payload capture.
- Add offline replay and malformed-page quarantine.
- Prove reruns retain changed source responses and no parser executes before capture commit.

**Acceptance:** Complete representative releases replay with network disabled and incomplete page sets cannot publish.

### CDC-003 — Silver CDI national/state

- Normalize dataset release, measures, strata, periods, values, confidence bounds, and footnotes.
- Resolve national/state geography.
- Preserve source data type and adjustment dimensions.

**Acceptance:** National and state fixtures reconcile exactly to source rows, including non-numeric and suppressed observations.

### CDC-004 — Silver PLACES county

- Normalize county-level Open Data format at one measure per row.
- Resolve county GEOIDs against the matching geography basis.
- Preserve modeled-estimate and adult-population caveats as source metadata.

**Acceptance:** All supported fixture counties resolve, modeled values retain confidence intervals, and missing counties are not synthesized.

### CDC-005 — Gold, glossary publisher, DAG, and API

- Publish deterministic CDC facts and a provider-neutral metric export.
- Add release-level atomic publication and reconciliation metrics.
- Add source explorer/API contracts that expose uncertainty and methodology.

**Acceptance:** Consumers can distinguish CDI from PLACES, national/state from county, crude from age-adjusted, and direct/provider-published from modeled values.

## Test plan

- Unit: paging, schema validation, exact value preservation, suppression parsing, strata normalization, geography mapping.
- Replay: CDI and PLACES fixtures with network disabled.
- Contract: no secrets, raw-before-parse, no shared glossary DDL, no policy in gold.
- Integration: fresh bootstrap, full release commit, partial-page failure, rerun idempotency, changed release retention, geography misses.
- External: small metadata/schema checks only, marked and isolated from deterministic CI.
- API: filters, pagination, latest-release selection, confidence fields, and source notes.

## Non-goals for the first release

- Patient- or case-level restricted data.
- Clinical guidance, diagnosis, forecasting, or causal claims.
- Treating modeled prevalence as observed case incidence.
- City/place, tract, or ZCTA serving.
- Combining indicators into an unexplained health score.
- Filling suppressed/missing values or rolling county estimates into state/national values.

## Primary references

- [CDC U.S. Chronic Disease Indicators dataset](https://data.cdc.gov/Chronic-Disease-Indicators/U-S-Chronic-Disease-Indicators/hksd-2xuw)
- [CDC Chronic Disease Indicators overview](https://www.cdc.gov/cdi/about/index.html)
- [CDC PLACES overview and geography coverage](https://www.cdc.gov/places/about/index.html)
- [CDC PLACES data portal and API formats](https://www.cdc.gov/places/tools/explore-places-data-portal.html)
- [ADR-0001 data-layer ownership boundaries](../decisions/0001-data-layer-boundaries.md)
