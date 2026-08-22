# FBI crime data pipeline plan

## Plan status

- **Status:** Proposed; no FBI adapter is currently implemented
- **Last updated:** 2026-08-18
- **Source owner:** FBI Uniform Crime Reporting Program / Crime Data Explorer
- **Geography scope:** National, state, county, and city-facing results; agency is a required source-native geography
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and versioned geography identity/relationship work in [GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md](./GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md)

## Implementation checkpoint

**Last updated:** 2026-08-18

**Current milestone:** Planning complete; implementation has not started

**Next pickup:** Complete FBI-001 by inventorying official downloadable/API products and freezing the first UCR source contract.

### Completed in the current slice

- [x] Defined the source-native agency model and qualified national/state/county/city-facing publication boundaries.
- [x] Defined participation, coverage, revision, and effective-dated agency/geography bridge requirements.
- [x] Split delivery into acceptance-tested discovery, capture/replay, reference, fact, and publication phases.

### Remaining

- [ ] FBI-001 — Freeze official source, program, offense, participation, revision, and suppression semantics.
- [ ] FBI-002 — Implement lossless capture, completeness checks, quarantine, and offline replay.
- [ ] FBI-003 — Implement agency identity and effective-dated geography relationships.
- [ ] FBI-004 — Implement crime and reporting-participation silver facts.
- [ ] FBI-005 — Implement gold products, glossary publisher, DAG, API, and integration coverage.

## Objective

Ingest public FBI Uniform Crime Reporting (UCR) data while preserving program, reporting basis, coverage, revision, and agency jurisdiction. Publish national/state/county/city-facing products only where the FBI source or an approved, evidence-backed geography bridge supports that level.

The pipeline must prevent three common analytical errors:

1. treating missing agency reports as zero crime;
2. equating a law-enforcement agency name or mailing city with a Census place; and
3. mixing Summary Reporting System (SRS), National Incident-Based Reporting System (NIBRS), arrests, offenses, estimates, or rates without explicit program and measure identity.

## Source discovery gate

FBI CDE delivery surfaces and available endpoints/files can change. Before implementation, FBI-001 must inventory the live official documentation and data products, record stable endpoint/file identities, authentication requirements, grains, coverage fields, revision behavior, and supported geographies. The production adapter may not be built from reverse-engineered web UI calls without a documented provider contract and representative captures.

Initial product families should be deliberately narrow:

- provider-published national and state offense counts/rates;
- provider-published county aggregates where explicitly available;
- agency-level offenses and participation/coverage; and
- city-facing observations only after validated agency-to-place resolution.

NIBRS incident microdata, hate crime, LEOKA, use-of-force, arrests, and other UCR programs are separate future dataset contracts, even if delivered through the same provider.

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

A city/place projection is permitted only when the source publishes place geography or a reviewed crosswalk establishes the relationship for the relevant period. Countywide sheriffs, state police, tribal, university, transit, multi-jurisdiction, and federal agencies must not be mislabeled as city observations. Multiple agencies serving one place remain separate unless a reviewed aggregation defines coverage and double-counting behavior.

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

Use `FBI_CDE_API_KEY` only if the documented delivery contract requires one. Validate it at request execution; never persist it in parameters, fingerprints, captures, selected headers, logs, or error summaries.

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

Required fields include exact source value text, parsed numeric value, unit, reported/estimated status, population denominator when supplied, rate when provider-published, coverage linkage, source record identity, capture lineage, and canonical geography resolution status.

### Geography publication status

Each row records one of:

- `provider_geo_exact`: provider-published national/state/county/place;
- `agency_only`: valid agency observation with no approved Census-place mapping;
- `agency_place_bridged`: approved effective-dated mapping suitable for place filtering but still labeled agency-reported;
- `ambiguous` or `unsupported`: retained in silver, withheld from geographic gold projection.

## Gold and serving products

- `gold_fbi.crime_observation`: provider-published or clearly labeled agency-reported observations.
- `gold_fbi.reporting_coverage`: participation and population/reporting completeness alongside observations.
- `gold_fbi.agency_geography`: evidence-backed, effective-dated agency geography projection.
- `gold_fbi.measure_export`: provider-neutral glossary publisher contract.

Every API result must expose UCR program, reported/estimated status, period, unit, reporting coverage, geography basis, and source release. A city result must say “agency-reported for agencies mapped to this place” unless the FBI product itself is place-based.

## Aggregation rules

- Prefer provider-published national, state, and county aggregates.
- Do not calculate a state or county value by summing agencies unless a separately reviewed product proves mutually exclusive coverage, completeness, and compatible reporting basis.
- Never interpret no report as zero.
- Never add provider-published estimates to reporting-agency counts.
- Never combine SRS and NIBRS rows merely because offense labels look similar.
- Rates are not additive. Recalculation requires compatible counts, denominator, coverage, and methodology.
- Local agency totals may overlap because jurisdictions and specialized agencies overlap; preserve that limitation.

## Data-quality rules

- Validate ORI format and exact uniqueness within the provider contract.
- Reconcile agency/reference versions before publishing observations.
- Validate reporting months and coverage ranges.
- Distinguish zero, null, not reported, not applicable, suppressed, and estimated.
- Reconcile provider totals only when the source documents that components should add to them.
- Detect duplicate records across pages and changed revisions.
- Quarantine geography mappings based solely on fuzzy names.
- Track observation and coverage row counts by program, period, geography, and release.

## Scheduling and revisions

The FBI states that UCR data are released monthly through CDE. A periodic metadata/release check should identify new or revised periods, but the exact schedule is chosen only after FBI-001 confirms the live source contract. Publication occurs per complete program/release slice. Historical revisions are retained and latest selection is a projection.

## Implementation phases

### FBI-001 — Official source and semantics inventory

- Record official endpoints/files, credentials, limits, schemas, stable keys, geography availability, release cadence, and revision behavior.
- Choose the first offense program and period range.
- Identify provider-published county products, if any, separately from agency data.
- Document participation/coverage fields required for responsible use.
- Store small representative payloads for national, state, county, city-like agency, countywide agency, missing report, estimate, and revision cases.

**Acceptance:** Maintainers can state exactly which rows are provider aggregates and which are reporting-agency observations; no city mapping depends on an agency-name guess.

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

**Acceptance:** Countywide and multi-jurisdiction fixtures are not exposed as cities, while an exact incorporated-place mapping is queryable with evidence.

### FBI-004 — Crime and participation silver facts

- Normalize program, offense, period, value, estimate, denominator, and coverage fields.
- Preserve revisions and source record identities.
- Apply geography publication statuses.

**Acceptance:** Zero and non-reporting remain distinct and every published observation has a coverage interpretation.

### FBI-005 — Gold, glossary publisher, DAG, and API

- Publish explicit offense and coverage products.
- Add national/state/county/place-or-agency filters without hiding source grain.
- Add source notes and unsafe-comparison guards.
- Emit publisher-ready state after atomic release publication.

**Acceptance:** API users cannot mistake agency data for complete city crime, and program/coverage/revision fields are always available.

## Test plan

- Unit: ORI parsing, pagination, revision keys, missing/reporting states, program/measure identity, geography status.
- Replay: complete and partial release fixtures with networking disabled.
- Contract: raw-before-parse, secret redaction, no shared glossary DDL, no policy columns in gold.
- Integration: fresh bootstrap, atomic publication, reference dependency failure, changed revision retention, ambiguous geography quarantine.
- Reconciliation: provider totals only for explicitly reconcilable products.
- API: geography filters, agency transparency, coverage fields, program isolation, latest/as-released behavior.

## Non-goals for the first release

- Restricted or personally identifying incident data.
- Predictive policing, risk scoring, causal claims, or neighborhood profiling.
- Fuzzy assignment of agencies to cities.
- Treating voluntary non-reporting as zero.
- Combining all UCR programs into one crime metric.
- Locally synthesizing national/state/county totals without a reviewed aggregation contract.

## Primary references

- [FBI Uniform Crime Reporting Program](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr)
- [FBI UCR publications and Crime Data Explorer description](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr/publications)
- [FBI National Incident-Based Reporting System](https://www.fbi.gov/how-we-can-help-you/more-fbi-services-and-information/ucr/nibrs)
- [ADR-0001 data-layer ownership boundaries](../decisions/0001-data-layer-boundaries.md)
