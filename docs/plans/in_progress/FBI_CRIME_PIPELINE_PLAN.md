---
id: fbi-crime
branch: feat/fbi-crime
depends_on:
  - geography-reference
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 dags
  - ./tests/run.ps1 integration
---

# FBI crime data pipeline plan

## Plan status

- **Status:** In progress; claimed for implementation on `feat/fbi-crime`
- **Last updated:** 2026-08-27
- **Source owner:** FBI Uniform Crime Reporting Program / Crime Data Explorer
- **Geography scope:** Provider-published national and state results plus source-native agency observations; county is an agency relationship/filter, not an FBI observation grain
- **Depends on:** Completed new-source expansion gate, shared raw capture/control foundation, and versioned geography identity/relationship work in [GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md](../completed/GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md)

## Implementation checkpoint

**Last updated:** 2026-08-27

**Current milestone:** FBI-001 — freezing the first summarized-offense product contract from the official CDE API documentation.

**Next pickup:** Record the frozen request shape, parameters, authentication, schema, completeness rule, measure/count basis, participation join, and revision identity for the summarized violent-crime product, then implement FBI-002 capture and replay.

### Completed in the current slice

- [x] Defined the source-native agency model and qualified national/state/county/city-facing publication boundaries.
- [x] Defined participation, coverage, revision, and effective-dated agency/geography bridge requirements.
- [x] Split delivery into acceptance-tested discovery, capture/replay, reference, fact, and publication phases.
- [x] Confirmed the official CDE API server and mutable `/LATEST` base path.
- [x] Confirmed that documented observation endpoint families publish at national, state, and agency levels, while the Agency resource associates ORIs with county labels.
- [x] Froze county as an agency discovery/filter relationship rather than an FBI-published observation or aggregation grain.
- [x] Defined provider-published absolute totals as distinct measures from rates, percentages, trends, and categorical breakdown counts.

### Remaining

- [ ] FBI-001 — Freeze official source, program, offense, participation, revision, and suppression semantics.
- [ ] FBI-002 — Implement lossless capture, completeness checks, quarantine, and offline replay.
- [ ] FBI-003 — Implement agency identity and effective-dated geography relationships.
- [ ] FBI-004 — Implement crime and reporting-participation silver facts.
- [ ] FBI-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage, and register the FBI provider stub in `iter_provider_stubs` for orchestrated DAG execution.

## Objective

Ingest public FBI Uniform Crime Reporting (UCR) data while preserving program, reporting basis, coverage, revision, and agency jurisdiction. Publish provider-supplied national and state observations and source-native agency observations without presenting agency associations or local rollups as FBI-published county or city totals.

The pipeline must prevent three common analytical errors:

1. treating missing agency reports as zero crime;
2. equating a law-enforcement agency name or mailing city with a Census place; and
3. mixing Summary Reporting System (SRS), National Incident-Based Reporting System (NIBRS), arrests, offenses, estimates, or rates without explicit program and measure identity.

## Source discovery gate

FBI CDE delivery surfaces and available endpoints/files can change. Before implementation, FBI-001 must inventory the live official documentation and data products, record stable endpoint/file identities, authentication requirements, grains, coverage fields, revision behavior, and supported geographies. The production adapter may not be built from reverse-engineered web UI calls without a documented provider contract and representative captures.

Initial product families should be deliberately narrow:

- provider-published national and state absolute offense totals, with rates and percentages retained only as distinct measures;
- agency-level offenses and participation/coverage; and
- county-associated agency filtering without a county observation or default rollup; and
- city-facing agency filtering only after validated agency-to-place resolution.

NIBRS incident microdata, hate crime, LEOKA, use-of-force, arrests, and other UCR programs are separate future dataset contracts, even if delivered through the same provider.

## Confirmed official API surface

The current official CDE API documentation declares:

```text
server:    https://api.usa.gov/crime/fbi/cde
basePath: /LATEST
base URL:  https://api.usa.gov/crime/fbi/cde/LATEST
```

`LATEST` is a mutable provider alias, not a warehouse release identity. Every capture must therefore retain retrieval time, checksum, request fingerprint, and the provider freshness fields when present. Representative responses contain `cde_properties.max_data_date.UCR` and `cde_properties.last_refresh_date.UCR`; FBI-001 must confirm their scope and revision semantics before using either as a release key or completeness signal.

Confirmed documented endpoint families include:

| Family | Documented paths | Contract role |
| --- | --- | --- |
| Agency | `GET /agency/{query}/{value}` | Required ORI reference and agency-association source |
| Arrest | `GET /arrest/national/{offense}`, `/arrest/state/{state}/{offense}`, `/arrest/agency/{ori}/{offense}` | Separate arrest/citation/summons dataset contract |
| Expanded Homicide | `GET /shr/national`, `/shr/state/{state}`, `/shr/agency/{ori}` | Separate SRS/NIBRS-derived SHR contract |
| Hate Crime | `GET /hate-crime/national/{bias}`, `/hate-crime/state/{state}/{bias}`, `/hate-crime/agency/{ori}/{bias}` | Separate hate-crime contract |
| Expanded Property | Documented as additional detail beyond summarized UCR counts | Separate detailed-property contract; exact paths remain to be frozen |

The national/state/agency route pattern is a source contract, not an additive hierarchy. The first implementation slice still requires one summarized-offense family whose absolute-total, time, participation, and revision semantics are fully documented. Arrest, SHR, hate-crime, and expanded-property payloads must not be combined merely because they share national/state/agency routes.

### Discovery evidence retained as fixture requirements

- An Agency response for Wisconsin groups many ORIs under county-name labels while retaining each ORI as the identity. It includes city, county, university/college, tribal, state-police, and other-state-agency types.
- The same response includes one-to-many county labels and `NOT SPECIFIED`, proving that agency-to-county is optional and many-to-many.
- An Arrest response's sex and race breakdowns each summed to 27,940 while its offense-name/category/breakdown sections each summed to 27,964. The adapter must not infer an authoritative overall total by summing arbitrary categorical dictionaries.
- An Expanded Homicide response contains separate victim, offender, weapon, circumstance, and relationship marginals. Their sums describe different entities and must not be added or treated as interchangeable totals.

The checked-in fixtures must retain the raw provider values that demonstrate these contracts, together with redacted request URLs/parameters and documentation references. API keys and authorization material must not enter fixtures.

## Geography contract

### Canonical product levels

```text
us:1
state:SS
state:SS|county:CCC
state:SS|place:PPPPP
```

### Source-native agency level

```text
fbi_agency:<ORI>
```

An Originating Agency Identifier (ORI) identifies the reporting agency; it is not a Census city code. The reference model therefore needs:

- `silver_fbi.dim_agency`: ORI, official agency name/type, state, source status, and effective dates;
- `silver_ref.bridge_geo_relationship_version`: effective-dated agency-to-state/county/place relationships with evidence and relationship type; and
- an explicit resolution status and confidence class.

A city/place association is permitted only when the source publishes place geography or a reviewed crosswalk establishes the relationship for the relevant period. Countywide sheriffs, state police, tribal, university, transit, multi-jurisdiction, and federal agencies must not be mislabeled as city observations. Multiple agencies associated with one place remain separate unless a reviewed aggregation defines coverage and double-counting behavior.

### Agency-to-county relationship

County is not a supported FBI observation subject for the initial contract. It is an evidence-backed relationship from an ORI to zero, one, or many canonical counties:

```text
agency observation
    -> fbi_agency:<ORI>
    -> effective-dated agency/county relationship
    -> county discovery or filter
```

The source's county label is evidence, not a canonical county identifier. Canonical publication requires an authoritative county code supplied by the source or a reviewed, versioned crosswalk; name or coordinate guessing is prohibited. `NOT SPECIFIED` remains unresolved. Coordinates describe a source reference point and must not be treated as a jurisdiction boundary.

A county filter means "observations from agencies associated with this county." It must not be labeled or returned as a complete county crime total. Multi-county agencies remain one agency observation associated with multiple counties; a multi-county query deduplicates by observation identity and never copies the full agency value into each county as an additive fact.

## Target package and runtime

```text
src/data_ingestion_toolbox/fbi_ucr/
├── config.py
├── client.py
├── capture.py
├── registry.py
├── metadata.py
├── silver_fbi/
│   ├── agency.py
│   ├── participation.py
│   ├── offenses.py
│   └── transform.py
└── gold_fbi/
    └── publisher.py

dags/fbi_ucr_ingest_dag.py
sql/migrations/{sequence}_fbi_ucr_pipeline.sql
tests/fixtures/fbi_ucr/
```

Reserve `FBI_CDE_API_KEY` as the environment-secret name for FBI CDE access and
use it only if the documented delivery contract requires a key. When configured,
the deployment must inject this named secret into every Airflow scheduler or
worker Docker container that can execute FBI ingestion when the container starts.
The value must come from the external stack's secret/environment configuration;
it must not be baked into an image or stored in a tracked environment file,
Airflow DAG, database, or capture. Validate it at request execution; never
persist it in parameters, fingerprints, captures, selected headers, logs, or
error summaries.

## Capture and control design

- Capture provider metadata/reference responses, agency lists, participation/coverage data, and observation payloads before parsing.
- Preserve exact source values, nulls, revisions, program identifiers, estimate flags, population/coverage fields, and footnotes.
- Slice requests deterministically by program, time, geography, and provider-supported pagination.
- Keep runs, slices, retries, page state, release watermarks, and quarantine in `control`.
- A complete observation slice cannot publish without its required agency and participation reference slices.
- Retain changed responses for the same request fingerprint as new captures.
- Treat HTTP success with an error body or truncated page set as captured-but-quarantined, not successful publication.

## Target silver model

### `silver_fbi.dim_ucr_dataset_release`

One row per program/product/release with provider release identifier, covered period, publication/revision time, methodology/reference URLs, schema contract version, and capture lineage.

### `silver_fbi.dim_agency`

One row per stable agency identity, with versioned attributes held separately when they change. Required source fields include ORI, name, agency type, state, reported city/county labels, status, and provider effective dates where supplied. Labels are retained for evidence but are not canonical place joins.

### `silver_fbi.dim_offense_measure`

Source-backed program and offense identity. Required attributes include UCR program, offense code/category, count/rate/unit, incident/offense/arrest basis, estimate status, and source label. SRS and NIBRS measures remain distinct.

### `silver_fbi.fact_reporting_participation`

Proposed grain:

```text
dataset release × agency/geography × reporting period × UCR program
```

Preserve reporting months, population covered, participation/completeness indicators, estimate flags, and source footnotes. This table is a required analytical companion to crime observations.

### `silver_fbi.fact_crime_observation`

Proposed grain:

```text
dataset release
× UCR program
× offense measure
× source geography or agency
× reporting period
× provider-supported demographic/category dimensions
```

Required fields include exact source value text, parsed numeric value, unit, measure form (`absolute_total`, `rate`, `percentage`, `trend`, or `category_count`), counted-entity basis, reported/estimated status, population denominator when supplied, coverage linkage, source record identity, capture lineage, and canonical geography resolution status.

### Geography publication status

Each row records one of:

- `provider_geo_exact`: provider-published national or state observation;
- `agency_only`: valid agency observation with no approved Census-place mapping;
- `agency_county_bridged`: agency observation filterable by an evidence-backed county relationship but still published at agency grain;
- `agency_place_bridged`: approved effective-dated mapping suitable for place filtering but still labeled agency-reported;
- `ambiguous` or `unsupported`: retained in silver, withheld from geographic gold projection.

## Gold and serving products

- `gold_fbi.crime_observation`: provider-published or clearly labeled agency-reported observations.
- `gold_fbi.reporting_coverage`: participation and population/reporting completeness alongside observations.
- `gold_fbi.agency_geography`: evidence-backed, effective-dated agency geography projection.
- `gold_fbi.measure_export`: provider-neutral glossary publisher contract.

Every API result must expose UCR program, reported/estimated status, period, unit, measure form, counted-entity basis, reporting coverage, geography basis, and source release. A county-filtered result must say "agency-reported for agencies associated with this county." A city result must say "agency-reported for agencies mapped to this place" unless a future FBI contract explicitly publishes place-based observations.

## Aggregation rules

- Consume provider-published national and state totals directly from their respective endpoints; never reconstruct them by summing agencies.
- Preserve agency totals at ORI grain. County and place relationships support filtering and discovery, not a default geographic total.
- Do not publish a county value by summing associated agencies. If a future reviewed analytical product permits such a calculation, label it `associated_agency_sum`, not `county_total`, and document overlapping jurisdiction, completeness, multi-county allocation, and deduplication behavior.
- Never interpret no report as zero.
- Never add provider-published estimates to reporting-agency counts.
- Never combine SRS and NIBRS rows merely because offense labels look similar.
- A provider-published absolute total is distinct from a rate, percentage, trend, or categorical breakdown count. Do not derive the total by summing response dictionaries unless the provider documents a complete, mutually exclusive partition with the same counted-entity basis.
- Rates are not additive. Recalculation requires compatible absolute totals, denominator, coverage, and methodology.
- Local agency totals may overlap because jurisdictions and specialized agencies overlap; preserve that limitation.

## Data-quality rules

- Validate ORI format and exact uniqueness within the provider contract.
- Reconcile agency/reference versions before publishing observations.
- Validate reporting months and coverage ranges.
- Distinguish zero, null, not reported, not applicable, suppressed, and estimated.
- Validate that every numeric measure has an explicit measure form and counted-entity basis; do not describe arrest events as unique people without a provider contract.
- Reconcile provider totals only when the source documents that components should add to them.
- Detect duplicate records across pages and changed revisions.
- Quarantine geography mappings based solely on fuzzy names.
- Track observation and coverage row counts by program, period, geography, and release.

## Scheduling and revisions

The FBI states that UCR data are released monthly through CDE. A periodic metadata/release check should identify new or revised periods, but the exact schedule is chosen only after FBI-001 confirms the live source contract. Publication occurs per complete program/release slice. Historical revisions are retained and latest selection is a projection.

## Implementation phases

### FBI-001 — Official source and semantics inventory

- Record official endpoints/files, credentials, limits, schemas, stable keys, geography availability, release cadence, and revision behavior.
- Freeze the official base URL and treat `/LATEST` as mutable capture input rather than release identity.
- Choose the first summarized-offense program and period range; document its national, state, and agency absolute-total endpoints separately from rates, percentages, and categorical breakdowns.
- Confirm from the endpoint schema what each total counts and whether it is reported, estimated, or mixed.
- Record county as an agency relationship/filter for the initial contract; do not wait for or infer a county observation endpoint.
- Document participation/coverage fields required for responsible use.
- Store small representative payloads for national total, state total, city-like agency, countywide agency, multi-county agency, `NOT SPECIFIED` agency, missing report, reported zero, estimate, incompatible marginal totals, and revision cases.

**Acceptance:** The plan or a linked versioned registry contains the redacted request shape, parameters, authentication, schema, pagination/completeness rule, measure/count basis, participation join, and revision identity for the first summarized-offense product. Maintainers can state exactly which rows are provider national/state totals and which are agency observations; county filters retain agency grain, and no county/place mapping depends on an agency-name or coordinate guess.

### FBI-002 — Capture and offline replay

- Implement the documented authentication and deterministic request slices.
- Capture reference, participation, and observation responses before parsing.
- Add offline replay, schema drift detection, and malformed/truncated response quarantine.

**Acceptance:** A complete fixture release rebuilds with network disabled and cannot publish without coverage/reference dependencies.

### FBI-003 — Agency and geography reference model

- Load agency identities and versioned attributes.
- Resolve exact state/county fields where source-supported.
- Build reviewed effective-dated agency/place bridges.
- Represent one-to-many and unsupported jurisdictions explicitly.

**Acceptance:** Countywide and multi-jurisdiction fixtures remain agency observations, `NOT SPECIFIED` remains unresolved, county filtering deduplicates multi-county agencies by observation identity, and an exact incorporated-place mapping is queryable with evidence without creating a place total.

### FBI-004 — Crime and participation silver facts

- Normalize program, offense, period, value, estimate, denominator, and coverage fields.
- Preserve revisions and source record identities.
- Apply geography publication statuses.

**Acceptance:** Zero and non-reporting remain distinct and every published observation has a coverage interpretation.

### FBI-005 — Gold, glossary publisher, DAG, and API

- Publish explicit offense and coverage products.
- Add national/state/agency observations plus county/place agency-association filters without hiding or changing source grain.
- Add source notes and unsafe-comparison guards.
- Emit publisher-ready state after atomic release publication.
- Register a deterministic FBI provider stub in `iter_provider_stubs` in
  `tests/support/dag_pipeline.py` so the new DAG executes in the orchestrated
  DAG suite (`tests/dags/test_dag_pipeline_execution.py`). The suite's coverage
  assertion (DAG-015) fails for any DAG in `dags/` without a registered stub,
  and a passing `./tests/run.ps1 dag-pipeline` run is required evidence for the
  three-source review gate. The stub must answer the actual request (endpoint,
  parameters, pagination) at whatever scale the pipeline's own completeness
  guards demand; do not weaken a production guard to make the DAG pass.

**Acceptance:** API users cannot mistake county-filtered or place-filtered agency data for complete county/city crime, and program/measure/count basis/coverage/revision fields are always available. The FBI DAG completes a successful DagRun in the orchestrated suite with every task instance successful.

## Test plan

- Unit: ORI parsing, pagination, revision keys, missing/reporting states, program/measure/count identity, geography status, and multi-county deduplication.
- Replay: complete and partial release fixtures with networking disabled.
- Contract: raw-before-parse, secret redaction, no shared glossary DDL, no policy columns in gold.
- Integration: fresh bootstrap, atomic publication, reference dependency failure, changed revision retention, ambiguous geography quarantine.
- Orchestrated execution: the production DAG runs as a real DagRun against the disposable warehouse via the registered provider stub (DAG-015/DAG-016).
- Reconciliation: provider totals only for explicitly reconcilable products; categorical marginals with different denominators must not be summed into a total.
- API: geography filters retain agency grain, county filters cannot emit `county_total`, agency transparency, coverage fields, program isolation, latest/as-released behavior.

## Non-goals for the first release

- Restricted or personally identifying incident data.
- Predictive policing, risk scoring, causal claims, or neighborhood profiling.
- Fuzzy assignment of agencies to cities.
- Treating voluntary non-reporting as zero.
- Combining all UCR programs into one crime metric.
- Locally synthesizing national/state/county totals without a reviewed aggregation contract.
- Publishing an associated-agency sum under the label `county_total` or `city_total`.

## Primary references

- [FBI Uniform Crime Reporting Program](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr)
- [FBI UCR publications and Crime Data Explorer description](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr/publications)
- [FBI National Incident-Based Reporting System](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr/nibrs)
- [FBI Crime Data Explorer API documentation](https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi)
- [ADR-0001 data-layer ownership boundaries](../../decisions/0001-data-layer-boundaries.md)
