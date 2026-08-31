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

- **Status:** Accepted 2026-08-28; human review recorded in [FOUR_SOURCE_REVIEW_GATE.md](FOUR_SOURCE_REVIEW_GATE.md)
- **Post-acceptance amendment (2026-08-31):** Live internal-stack validation drove three changes. (1) The provider removed the mutable `/LATEST` base-path segment (requests including it now 404), so `CDE_BASE_PATH` is empty and the frozen base URL is the server root. (2) The registered `summarized_violent_crime` window was widened to `01-1990..06-2023` (warehouse-wide 1990 history floor; the CDE API serves the whole window in one documented from/to request per subject, verified live). To make a reviewed window change re-ingest instead of concluding the provider release is unchanged, the previous-release lookup is now scoped to the registered period window and accepts only `decision = 'ingest'` rows as evidence (an `unchanged` row stamps the current window without capturing it). (3) The API key now travels in the api.data.gov `X-Api-Key` header rather than the `API_KEY` query parameter, keeping the credential out of transport-level URL logging (header auth verified live: 200 with header, 403 without). Fixture-driven tests pin an explicit `01-2023..06-2023` product so the reviewed six-month fixtures stay valid. Live evidence: 11,084 observations spanning 1990-2023 (34 distinct years) published through silver; 125 unit tests pass.
- **Last updated:** 2026-08-31
- **Source owner:** FBI Uniform Crime Reporting Program / Crime Data Explorer
- **Geography scope:** Provider-published national and state results plus source-native agency observations; county is an agency relationship/filter, not an FBI observation grain
- **Depends on:** Completed new-source expansion gate, shared raw capture/control foundation, and versioned geography identity/relationship work in [GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md](../completed/GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md)

## Implementation status

All five phases are implemented, tested, and validated on `feat/fbi-crime`.
There is no remaining checklist item and no next pickup.

### Final validation checkpoint (2026-08-27)

- `./tests/run.ps1 etl`: **492 passed**.
- `./tests/run.ps1 dags`: **98 passed, 4 expected integration skips**; the
  FBI DAG structure tests passed.
- Focused disposable-PostGIS validation:
  `python -m pytest -m "integration and database"
  tests/integration/database/test_fbi_ucr_pipeline.py`: **12 passed**.
- Aggregation-boundary, catalog-evidence, and repository-hygiene focus:
  **8 passed**.
- Ruff on all changed Python files and `git diff --check`: **passed**.
- The broader `./tests/run.ps1 integration` command did not complete: after
  the disposable services were configured, failures occurred first in the
  unrelated API cache and legacy BLS integration tests, before the FBI file;
  the run was interrupted after the repository's single permitted poll. The
  FBI database file then passed independently in full against a fresh schema.
- `./tests/run.ps1 dag-pipeline` could not start a DagRun on the authoring
  host. The installed `apache-airflow` reported 2.11.2 while
  `apache-airflow-core` was 3.2.2; Airflow initialization loaded a 3.0
  migration and raised `ImportError: ignore_sqlite_value_error`. Production
  code was not changed to accommodate the contaminated local installation.
  **This is no longer outstanding** — see the orchestrated evidence below.

### Orchestrated evidence on the integration branch (2026-08-28)

The check recorded as blocked above was produced on `main` after the three
source branches merged, in the pinned environment the repository actually
targets rather than on the authoring host.

- `dag-parse` run 102 on `main` at `1f33b38`, Airflow 2.9.3 on Python 3.11
  against pinned PostGIS 16.14: **113 passed, 0 skipped, 0 errors** in
  134.35 s. The job sets `RUN_DAG_TESTS=1` and the `TEST_POSTGRES_*` service
  variables, and its command-line `-m dag` overrides the default marker filter
  in `pyproject.toml`, so `tests/dags/test_dag_pipeline_execution.py` was
  selected and executed rather than deselected. Zero skips and zero errors are
  what distinguish that from a filtered run.
- That module executes all ten production DAGs as real `DagRun`s in warehouse
  order and asserts each succeeded, `fbi_ucr_ingest` among them. It also
  asserts the executed set equals the DagBag, so the FBI DAG could not have
  been silently omitted.
- Every other workflow was green on the same commit, including
  `postgres-integration`, which is the tier the interrupted local `integration`
  run above could not finish.

This satisfies the machine-verifiable precondition of
[`FOUR_SOURCE_REVIEW_GATE.md`](FOUR_SOURCE_REVIEW_GATE.md).

### Live provider contract

`tests/external/test_fbi_source_contracts.py` covers the registered product
release identity, the period window, the actuals/rates separation, the
registered ORIs in the Wisconsin Agency directory, outage classification, and
credential handling. The `external-contract` workflow owns it on a daily
schedule and requires `FBI_CDE_API_KEY`.

**Not yet executed against the provider.** The module's live assertions have
never run: the repository has no `FBI_CDE_API_KEY` secret configured, and the
authoring environment's network policy blocks `api.usa.gov`. Its deterministic
assertions pass and its live assertions skip cleanly on the missing key. The
first credentialed `external-contract` run is what will close this.

### Delivered

- [x] Defined the source-native agency model and qualified national/state/county/city-facing publication boundaries.
- [x] Defined participation, coverage, revision, and effective-dated agency/geography bridge requirements.
- [x] Split delivery into acceptance-tested discovery, capture/replay, reference, fact, and publication phases.
- [x] Confirmed the official CDE API server and mutable `/LATEST` base path.
- [x] Confirmed that documented observation endpoint families publish at national, state, and agency levels, while the Agency resource associates ORIs with county labels.
- [x] Froze county as an agency discovery/filter relationship rather than an FBI-published observation or aggregation grain.
- [x] Defined provider-published absolute totals as distinct measures from rates, percentages, trends, and categorical breakdown counts.
- [x] FBI-001 — Froze official source, program, offense, participation, revision, and suppression semantics.
- [x] FBI-002 — Implemented lossless capture, completeness checks, quarantine, and offline replay.
- [x] FBI-003 — Implemented agency identity and effective-dated geography relationships.
- [x] FBI-004 — Implemented crime and reporting-participation silver facts.
- [x] FBI-005 — Implemented gold products, glossary publisher, DAG, and integration coverage, and registered the FBI provider stub in `iter_provider_stubs` for orchestrated DAG execution.

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

## Frozen source contract (FBI-001)

The first summarized-offense product is registered in
`src/data_ingestion_toolbox/fbi_ucr/registry.py` as
`summarized_violent_crime`. Its contract is frozen as follows; every value was
read from the official CDE API documentation page and confirmed against
representative live responses recorded in
`tests/fixtures/fbi_ucr/SOURCE_NOTES.md`.

| Contract element | Frozen value |
| --- | --- |
| Base URL | `https://api.usa.gov/crime/fbi/cde` + mutable base path `/LATEST` |
| Authentication | `API_KEY` query parameter, supplied at request execution from the `FBI_CDE_API_KEY` environment secret |
| Observation endpoints | `GET /summarized/national/{offense}`, `GET /summarized/state/{state}/{offense}`, `GET /summarized/agency/{ori}/{offense}` |
| Required parameters | `from` and `to`, both `mm-yyyy` matching `^(0[1-9]|1[0-2])-[0-9]{4}$` |
| Offense | `V` (Violent Crime) from the documented `summarized_offenses` enumeration |
| Period window | `01-2023` through `06-2023` |
| Reference endpoint | `GET /agency/byStateAbbr/{state}` from the documented `agency_query_types` enumeration |
| Program / reporting basis | SRS plus summarized NIBRS; reported, not estimated (the `/estimate/*` family is a separate contract) |
| Pagination | None; the period window is the slice, and every registered month must be present or explicitly not reported |
| Release identity | `cde_properties.last_refresh_date.UCR` (`mm/dd/yyyy`), stored as the ISO release key |
| Completeness signal | `cde_properties.max_data_date.UCR` (`mm/yyyy`); a window ending after it is quarantined |
| Absolute totals | `offenses.actuals["<subject label> Offenses"|"<subject label> Clearances"]` |
| Rates | `offenses.rates[...]`, per 100,000 population, a distinct measure form that is never derived from or added to a total |
| Counted entities | `Offenses` counts reported offenses; `Clearances` counts cleared offenses |
| Participation join | `populations.population[<subject label>]`, `populations.participated_population[<subject label>]`, and `tooltips["Percent of Population Coverage"][<subject label>]`, all keyed by the same `mm-yyyy` month |

### Which rows are provider totals and which are agency observations

A national or state row is consumed only from its own `/summarized/national/...`
or `/summarized/state/...` endpoint and is labelled `provider_geo_exact`. A
state or agency response also carries its parent geographies' comparison
series; those are never attributed to the requested subject, because a series
belongs to the subject named in its own label. An agency row keeps ORI grain
and one of `agency_only`, `agency_county_bridged`, or `agency_place_bridged`.
`gold_fbi.agency_observation_area_filter` exposes county and place filters that
carry `observation_grain = 'agency'` and a `result_label` stating that the rows
are agency-reported; no view in `gold_fbi` sums agencies into an area total.

### Participation contract note

The documented `/participation/*` endpoint family is scoped to the National
Use-of-Force collection, not to summarized UCR reporting. The summarized
payload itself publishes the covered population, the participating population,
and (for provider-published national and state subjects) the percentage of
population covered, so that payload is the participation join for this product.
Agency subjects publish population and participating population but no coverage
percentage; the absence is recorded as `coverage_basis =
'provider_population_only'` rather than filled in.

### Contract reconciliations recorded during implementation

- **Agency identity.** This plan names the source-native level
  `fbi_agency:<ORI>`, while the shared provider-neutral contract in
  `silver_ref/geography_contract.py` mints `agency:<ORI>`. Both are retained
  and queryable: `source_geo_level` on every FBI silver and gold row carries
  `fbi_agency:<ORI>`, and `silver_ref.dim_geo_entity.geo_id` carries
  `agency:<ORI>` so the shared reference model is not forked for one provider.
- **County resolution rule.** The provider publishes a county *name*, never a
  county code. A county relationship resolves only when the normalized label
  matches exactly one authoritative Census county name inside the agency's own
  resolved state; more than one match is recorded as `ambiguous` and zero
  matches as `unresolved`. The method is recorded as
  `reviewed_county_name_crosswalk` with `confidence_class = 'reviewed'`, it is
  never a fuzzy or coordinate match, and it never creates a county observation.
- **Place resolution rule.** A place relationship exists only where the
  reviewed, effective-dated crosswalk in
  `src/data_ingestion_toolbox/fbi_ucr/reference.py` covers the whole registered
  period. Countywide, campus, tribal, and state-police agencies deliberately
  have no entry.
- **ETL tier scope.** `tests/run.ps1 etl`, the `test-etl` Makefile target, and
  `.github/workflows/etl-unit.yml` now include `tests/unit/fbi_ucr`, so the
  plan's own verification command exercises this source's ETL unit tests.

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

**Met by:** [Frozen source contract (FBI-001)](#frozen-source-contract-fbi-001), `src/data_ingestion_toolbox/fbi_ucr/registry.py`, `tests/fixtures/fbi_ucr/SOURCE_NOTES.md`, and `tests/unit/fbi_ucr/test_fbi_registry.py`.

### FBI-002 — Capture and offline replay

- Implement the documented authentication and deterministic request slices.
- Capture reference, participation, and observation responses before parsing.
- Add offline replay, schema drift detection, and malformed/truncated response quarantine.

**Acceptance:** A complete fixture release rebuilds with network disabled and cannot publish without coverage/reference dependencies.

**Met by:** `client.py`, `capture.py`, and `silver_fbi/replay.py`; `tests/unit/fbi_ucr/test_fbi_replay.py::test_complete_release_replays_without_network_access`, `::test_missing_required_slice_blocks_the_release`, `::test_agency_observation_without_its_reference_slice_is_quarantined`, and `::test_changed_bytes_fail_the_checksum_before_parsing`; and `tests/integration/database/test_fbi_ucr_pipeline.py::test_reference_dependency_failure_blocks_the_release`.

### FBI-003 — Agency and geography reference model

- Load agency identities and versioned attributes.
- Resolve exact state/county fields where source-supported.
- Build reviewed effective-dated agency/place bridges.
- Represent one-to-many and unsupported jurisdictions explicitly.

**Acceptance:** Countywide and multi-jurisdiction fixtures remain agency observations, `NOT SPECIFIED` remains unresolved, county filtering deduplicates multi-county agencies by observation identity, and an exact incorporated-place mapping is queryable with evidence without creating a place total.

**Met by:** `silver_fbi/agency.py`, `reference.py`, and `silver_fbi/transform.py`; `tests/unit/fbi_ucr/test_fbi_agency.py`; and `tests/integration/database/test_fbi_ucr_pipeline.py::test_agency_geography_status_matches_its_reviewed_evidence`, `::test_county_filter_keeps_agency_grain_and_deduplicates`, and `::test_ambiguous_county_evidence_is_withheld_from_gold`.

### FBI-004 — Crime and participation silver facts

- Normalize program, offense, period, value, estimate, denominator, and coverage fields.
- Preserve revisions and source record identities.
- Apply geography publication statuses.

**Acceptance:** Zero and non-reporting remain distinct and every published observation has a coverage interpretation.

**Met by:** `silver_fbi/offenses.py`, `silver_fbi/participation.py`, and the `fact_crime_observation` foreign key to `fact_reporting_participation` in `sql/migrations/011_fbi_ucr_pipeline.sql`; `tests/unit/fbi_ucr/test_fbi_offenses.py::test_a_month_without_a_report_is_not_zero` and `::test_a_published_zero_stays_a_published_zero`; and `tests/integration/database/test_fbi_ucr_pipeline.py::test_missing_reports_stay_distinct_from_reported_zeros` and `::test_every_published_observation_has_a_coverage_interpretation`.

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
  four-source review gate. The stub must answer the actual request (endpoint,
  parameters, pagination) at whatever scale the pipeline's own completeness
  guards demand; do not weaken a production guard to make the DAG pass.

**Acceptance:** API users cannot mistake county-filtered or place-filtered agency data for complete county/city crime, and program/measure/count basis/coverage/revision fields are always available. The FBI DAG completes a successful DagRun in the orchestrated suite with every task instance successful.

**Met by:** the `gold_fbi` views in `sql/migrations/011_fbi_ucr_pipeline.sql`, `gold_fbi/publisher.py`, `dags/fbi_ucr_ingest_dag.py`, and the `fbi_ucr` stub registered in `iter_provider_stubs` in `tests/support/dag_pipeline.py`; `tests/dags/test_fbi_ucr_dag.py`; and `tests/integration/database/test_fbi_ucr_pipeline.py::test_provider_totals_and_agency_grain_stay_separable`, `::test_county_filter_keeps_agency_grain_and_deduplicates`, and `::test_publisher_contract_exposes_measure_identity`.

## Test plan

- Unit: ORI parsing, pagination, revision keys, missing/reporting states, program/measure/count identity, geography status, and multi-county deduplication.
- Replay: complete and partial release fixtures with networking disabled.
- Contract: raw-before-parse, secret redaction, no shared glossary DDL, no policy columns in gold.
- Integration: fresh bootstrap, atomic publication, reference dependency failure, changed revision retention, ambiguous geography quarantine.
- Orchestrated execution: the production DAG runs as a real DagRun against the disposable warehouse via the registered provider stub (DAG-015/DAG-016).
- Reconciliation: provider totals only for explicitly reconcilable products; categorical marginals with different denominators must not be summed into a total.
- API: geography filters retain agency grain, county filters cannot emit `county_total`, agency transparency, coverage fields, program isolation, latest/as-released behavior.

The FBI aggregation boundary is cataloged as ETL-042 in
`docs/reference/TESTING_CONTRACT.md` and guarded by
`tests/unit/fbi_ucr/test_fbi_aggregation_boundary.py` plus the database
integration suite.

## Implementation evidence

### Delivered code and warehouse objects

```text
src/data_ingestion_toolbox/fbi_ucr/
├── __init__.py
├── config.py                     frozen base URL, FBI_CDE_API_KEY handling, sizing
├── client.py                     secret-safe transport, retry budget, payload guards
├── capture.py                    run/request/capture orchestration and control state
├── registry.py                   frozen product, offense, state, and subject contract
├── metadata.py                   release identity, completeness, revision decisions
├── reference.py                  reviewed, effective-dated agency-to-place crosswalk
├── silver_fbi/
│   ├── models.py                 typed source-faithful outcomes
│   ├── agency.py                 Agency-resource parser and county-label evidence
│   ├── participation.py          coverage, population, and participation status
│   ├── offenses.py               measure identity, missing reports, reported zeros
│   ├── replay.py                 checksum-verified, complete-slice offline replay
│   └── transform.py              conformance, geography resolution, reconciliation
└── gold_fbi/
    └── publisher.py              atomic publication gate and publisher-ready emit

dags/fbi_ucr_ingest_dag.py
sql/migrations/011_fbi_ucr_pipeline.sql
tests/fixtures/fbi_ucr/           reviewed captures, derived scenarios, SOURCE_NOTES
```

`sql/migrations/011_fbi_ucr_pipeline.sql` owns `control.fbi_ucr_release`, the
`silver_fbi` revision/reference/fact model, and the `gold_fbi` publication
views (`crime_observation`, `reporting_coverage`, `agency_geography`,
`agency_observation_area_filter`, `latest_release_observation`,
`measure_export`, `metric_publisher`). It is registered in
`sql/bootstrap/warehouse_manifest.json` and mounted in the same order by
`infra/docker/docker-compose.test.yml`.

### Synchronized documentation, configuration, and manifests

- `docs/user-guides/FBI_UCR_PIPELINE_OPERATIONS.md` — deployment prerequisites,
  first-deployment test, quarantine and recovery, scope extension.
- `docs/reference/TESTING_CONTRACT.md` — DAG-002/005/007/008 rows and the test
  layout now include the FBI DAG, schedule, pool, retry contract, and suites.
- `docs/reference/BETA_RESET_REINGESTION.md` — pause, ingestion order, and
  completion checks include `fbi_ucr_ingest`.
- `sql/migrations/README.md` — migration sequence entry for 011.
- `README.md`, `infra/airflow/README.md`, and the three Compose files —
  `fbi_cde_api` pool and the `FBI_CDE_API_KEY` secret passthrough. Tracked
  environment examples keep no value.
- `tests/support/dag_pipeline.py` — `fbi_cde_api` pool, fixture credential,
  Wisconsin anchor geographies, and the `fbi_ucr` stub in
  `iter_provider_stubs`.
- `tests/support/build_fbi_fixtures.py` — reproducible fixture regeneration.

### Secret handling

`FBI_CDE_API_KEY` is read only inside `FbiUcrConfig.from_environment()`,
validated when a request executes, and merged into the outgoing query
parameters only. The redacted `request_parameters` written to
`control.ingestion_request` and `raw_capture.response_capture` carry the
documented `from`/`to` values alone, response headers are reduced to the shared
provenance allowlist, and configuration errors never echo the value. This is
asserted by `tests/unit/fbi_ucr/test_fbi_client.py` and
`tests/unit/fbi_ucr/test_fbi_capture.py`.

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
