---
id: usda-crop
branch: feat/usda-crop
depends_on:
  - geography-reference
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 etl
  - ./tests/run.ps1 dags
  - ./tests/run.ps1 integration
---

# USDA NASS agricultural crop data pipeline plan

## Plan status

- **Status:** Accepted 2026-08-28; human review recorded in [FOUR_SOURCE_REVIEW_GATE.md](FOUR_SOURCE_REVIEW_GATE.md)
- **Post-acceptance amendment (2026-08-31):** Live internal-stack validation widened `REGISTERED_YEAR_START` from 2022 to 1990 for the survey products (the Census of Agriculture keeps its single reviewed 2022 year) and drove three robustness changes. (1) Row-count drift is now computed only over the slice keys both contracts registered: a reviewed window expansion contributes new keys and a recent-mode run preflights a subset of a full-mode contract, and neither is provider drift (previously the 35-year preflight read as a ~10x row explosion and quarantined the release). (2) Quick Stats signals rate limiting with HTTP 403 alongside 429; the client now retries both with backoff (a genuinely bad key still fails terminally through retry exhaustion), default request spacing rose to 1s, and the `usda_nass_api` pool is serialized to 1 slot in the compose init blocks - sustained sweeps above ~1 req/s draw rolling 403s. (3) The steady-state recent-mode cadence was re-validated against the post-backfill contract. Live evidence: 1,086,806 crop observations spanning 1990-2024 at national/state/county levels; 159 unit tests pass. Known residual: aborted backfill iterations left non-terminal slice rows (`preflighted`/`over_limit`/`skipped`) in `control.usda_nass_slice`, which DQ-NASS-002 correctly flags; the design call (aborted runs finalize their slice rows vs. ledger rules assessing only the latest run per partition) is recorded in the warehouse data-quality plan.
- **Last updated:** 2026-08-31
- **Source owner:** USDA National Agricultural Statistics Service (NASS)
- **Initial source:** NASS Quick Stats
- **Geography scope:** National, state, and county; county is the lowest level
- **Depends on:** New-source expansion gate, shared raw capture/control foundation, and GEO-001 through GEO-003 in the Census geography reference pipeline; resolve its current workflow location through the [plan index](../README.md)

## Implementation checkpoint

**Last updated:** 2026-08-27

**Current milestone:** Complete. NASS-001 through NASS-005 are implemented,
tested, and validated on `feat/usda-crop`. The plan is ready for human review.

**Next pickup:** None.

### Implementation evidence

#### NASS-001 — Product/query contract discovery

- [x] `src/data_ingestion_toolbox/usda_nass/registry.py` freezes five products:
  corn, soybeans, wheat, and hay `SURVEY` acreage/yield/production, plus corn
  `CENSUS` harvested acreage and production. Each entry declares its source
  program, sector/group/commodity selections, statistic selections with their
  expected units, national/state/county aggregate levels, frequency, domain,
  registered year range, request partition fields, parser contract version,
  incremental field, and release expectation.
- [x] `QUICK_STATS_FIELDS` freezes the 39 consumed provider fields, and
  `SUPPRESSION_SYMBOLS` freezes the seven published value symbols.
- [x] `slice_query_parameters` refuses any partition, aggregate level, or year
  outside the registry, so a request cannot exist without a registry entry.
- [x] `tests/fixtures/usda_nass/SOURCE_NOTES.md` records the endpoints, the
  `key` query-parameter credential handling, the 50,000-record ceiling and its
  exact refusal text, the field contract, the symbol table with its NASS
  sources, the geography contract, the period fields, the **observation
  grain**, release identity and revision semantics, and the survey/census
  separation.
- [x] Eight reviewed fixture files cover national/state/county, survey/census,
  withheld/insufficient/not-applicable/not-available/below-rounding values, CV
  values and CV symbols, a revised extraction, and ten boundary records.
- **Acceptance met:** every request is generated from a reviewed registry entry
  (`test_slice_parameters_refuse_an_unregistered_partition`), and the expected
  observation grain is documented in `SOURCE_NOTES.md` and enforced by the
  `UNIQUE (product_id, release_watermark, source_record_id)` constraint.

#### NASS-002 — Capture, slicing, and replay

- [x] `client.py` keeps the registered selections and the transport query
  apart: `USDA_NASS_API_KEY` is validated at request execution, added only to
  the outgoing query, and refused if it ever appears in the captured parameters.
  Errors carry an endpoint path and a typed code, never a URL or query string.
- [x] `capture.py` preflights every registered slice through `get_counts`,
  captures the preflight and the retrieval separately, records every slice in
  `control.usda_nass_slice`, and marks a release incomplete when a retrieval
  disagrees with its own preflight.
- [x] An over-limit slice is refused before any data request; a response at the
  record ceiling is refused as truncated; a zero-count slice issues no data
  request.
- [x] `silver_nass/values.replay_slices` replays a complete slice set from
  capture bytes with checksum, row-count, preflight, and duplicate checks and
  no network access.
- [x] A changed provider response is retained as a new release: `decide_release`
  ingests a forward watermark and quarantines a backward one.
- **Acceptance met:** `tests/unit/usda_nass/test_nass_replay.py` and
  `tests/integration/database/test_usda_nass_pipeline.py` replay the reviewed
  captures without a network and reconcile every source value and quality
  marker; `test_an_over_limit_slice_cannot_replay_transform_or_publish` proves
  an over-limit release cannot replay, conform, or publish.

#### NASS-003 — Silver dimensions and values

- [x] `silver_nass/dimensions.py` derives commodity identity from the full
  classification, statistic identity from `source_desc`/`statisticcat_desc`/
  `short_desc`/`unit_desc`/`freq_desc` with the registry's declared value kind,
  calculation basis, and additivity, and domain identity including explicit
  `TOTAL` members.
- [x] Geography resolves only from exact ANSI/FIPS codes. Unsupported aggregate
  levels are retained with `geo_type = 'unsupported'` and no `geo_id`, and a
  county without an exact code is quarantined rather than name-matched.
- [x] `silver_nass/values.py` preserves the exact `Value` and `CV (%)` text,
  parses grouped and decimal numbers without loss, maps every registered symbol
  to an explicit non-numeric state, and refuses an unregistered symbol.
- [x] `sql/migrations/012_usda_nass_crop_pipeline.sql` defines
  `dim_dataset_release`, `dim_commodity`, `dim_statistic`, `dim_domain`, and
  `fact_crop_observation` with CHECK constraints that make a numeric value
  impossible for any non-`valid` state.
- **Acceptance met:** `test_nass_source_contracts.py` reconciles every reviewed
  fixture row against an independently declared expectation file, and
  `test_survey_and_census_statistics_never_share_an_identity` proves the two
  programs stay distinct where the data item label and unit agree.

#### NASS-004 — Gold, glossary publisher, DAG, and API

- [x] `gold_nass.crop_observation`, `crop_series`, `latest_release_observation`,
  `measure_export`, and `metric_publisher` publish only reconciled, published
  releases. The migration owns no `gold_glossary` object.
- [x] `dags/usda_nass_crop_ingest_dag.py` runs on business days at 10:00 UTC,
  validates shared geography, then captures, replays, and publishes each
  registered product through the `usda_nass_api` pool.
- [x] `stub_usda_nass_quick_stats` is registered in `iter_provider_stubs` in
  `tests/support/dag_pipeline.py`, and `usda_nass_crop_ingest` is registered in
  `ORDERED_PIPELINE_DAGS`. The stub answers the actual request: it resolves the
  product and aggregate level from the requested slice and reports a count that
  matches the rows it serves, so the pipeline's own preflight and completeness
  guards run unweakened.
- [x] `apps/api/routers/usda_nass.py` and
  `apps/api/services/usda_nass_service.py` expose observations, series,
  measures, and source notes with bound filters for commodity, class,
  statistic, unit, source program, domain, geography, period, release, and
  value status, plus `latest` versus as-released reads.
- [x] Source notes for units, suppression, release status, source program,
  county coverage, and aggregation are derived from the registry and the parser
  symbol table, so they cannot drift from the ingested contract.
- **Acceptance met (contract):** every observation, series, and measure row
  carries `unit_desc`; a statistic identity can never hold two units
  (`test_incompatible_units_never_share_an_unlabeled_metric`); and a suppressed
  value reaches consumers with a NULL numeric value, its exact source text, and
  its symbol (`test_a_suppressed_value_can_never_be_read_as_zero`).
- **Acceptance partially environment-limited (orchestrated DagRun):** DAG-015
  passes — no production DAG escapes the orchestrated suite, and the NASS DAG is
  covered. DAG-016 cannot execute on this machine; see *Environment-limited
  checks* below. As substitute evidence within this environment,
  `tests/integration/database/test_usda_nass_dag_tasks.py` drives the
  production DAG's own callables, through the registered provider stub and a
  real warehouse connection, from capture to gold for every registered product,
  and asserts slice, capture, and credential lineage.

#### NASS-005 — Historical bootstrap and operational reconciliation

- [x] The registered year range is frozen in the registry, so a fresh
  environment reproduces exactly the same slices and history. A run whose
  logical date falls on the first of the month sweeps the full registered
  history; other business days retrieve the bounded recent window
  (`resolve_slice_mode`).
- [x] Per-slice baselines: `control.usda_nass_slice` records the provider
  preflight count and the captured row count for every slice, and
  `control.usda_nass_release.slice_counts` is the baseline `decide_preflight`
  compares the next extraction against, with a configurable drift threshold.
  Per-capture checksums are verified on every replay.
- [x] `docs/user-guides/USDA_NASS_PIPELINE_OPERATIONS.md` documents deployment,
  the required secret, the first deployment test, the reconciliation queries,
  reset and re-ingestion, and a quarantine-decision troubleshooting table.
  `docs/reference/BETA_RESET_REINGESTION.md` now includes the DAG.
- **Acceptance met:** `test_reruns_are_idempotent_and_revisions_are_retained`
  re-runs replay, conformance, and publication over the same captured run with
  no duplicates, then publishes a revised extraction and proves both releases
  remain queryable with the formerly withheld value intact in the original.

### Validation

Run on 2026-08-27 from `feat/usda-crop`. Database-backed tiers ran against a
freshly provisioned pinned disposable PostGIS warehouse
(`docker compose -f infra/docker/docker-compose.test.yml up --detach --wait postgres`).

| Command | Result |
| --- | --- |
| `ruff format --check .` | pass (326 files) |
| `ruff check .` | pass |
| `./tests/run.ps1 etl` | exit 0 — 510 passed, 0 skipped |
| `./tests/run.ps1 api` | exit 0 — 110 passed, 0 skipped |
| `./tests/run.ps1 dags` | exit 0 — 100 passed, 4 skipped |
| `./tests/run.ps1 integration` | exit 0 — 67 passed, 8 skipped |
| `pytest tests/unit` | exit 0 — 883 passed |

The four `dags` skips are the database-backed DAG tests that require
`TEST_POSTGRES_*`. The eight `integration` skips are the Redis and Compose
tests that require `TEST_REDIS_URL` or `RUN_COMPOSE_TESTS`; they belong to the
`martin`, `redis-integration`, and `compose-smoke` tiers. No skip or xfail was
introduced by this work.

New coverage added by this plan: 140 unit tests in `tests/unit/usda_nass/`, 14
API unit tests in `tests/unit/api/test_usda_nass_api.py`, 5 DAG structure tests
in `tests/dags/test_usda_nass_dag.py`, 5 warehouse contract tests in
`tests/integration/database/test_usda_nass_pipeline.py`, 3 DAG-callable tests in
`tests/integration/database/test_usda_nass_dag_tasks.py`, and 4 API contract
tests in `tests/integration/api/test_usda_nass_api_contract.py`.

`tests/unit/usda_nass` was added to the ETL tier in `tests/run.ps1`, the
`test-etl` Makefile target, and `.github/workflows/etl-unit.yml` together, so
the tier definition stays synchronized across all three.

### Environment-limited checks

Two checks could not run on the authoring machine. Neither is caused by this
work, and neither was reported as passing at the time. The first has since been
produced in the pinned environment on the integration branch, as recorded
below; the second is a host permission quirk with a documented workaround.

1. **DAG-016 orchestrated DagRun.** `airflow.utils.db.initdb()` fails during
   test setup with `ImportError: cannot import name
   'ignore_sqlite_value_error' from 'airflow.migrations.utils'`. The installed
   environment is Airflow 2.11.2 on Python 3.13 whose
   `airflow/migrations/versions/` directory also holds 97 orphaned Airflow 3
   migration modules; alembic loads one of them and fails. The repository pins
   Airflow 2.9.3 on Python 3.11 (`.github/workflows/etl-unit.yml`,
   `.github/workflows/dag-parse.yml`, `infra/airflow/Dockerfile`).

   This affects `./tests/run.ps1 dag-pipeline`, and it also affects
   `./tests/run.ps1 dags` whenever `TEST_POSTGRES_*` is configured, because the
   orchestrated tests then stop skipping: that combination reports
   `102 passed, 2 errors`, both errors being the `ImportError` above in
   `tests/dags/test_dag_pipeline_execution.py`. Every other DAG test passes in
   both configurations.

   **Resolved on the integration branch.** The run was produced in the pinned
   environment after the source branches merged; see the orchestrated evidence
   below. DAG-015 — static coverage proving the new DAG does not escape the
   orchestrated suite — passed here as well.

2. **pytest temporary root.** `%LOCALAPPDATA%\Temp\pytest-of-synce` on this
   machine denies access to its owner, so `tmp_path_factory.mktemp` fails and
   every tier that uses a temporary path errors before collection. The
   directory cannot be read, renamed, or removed without elevation. **To
   resume:** delete that directory from an elevated shell, or set
   `PYTEST_DEBUG_TEMPROOT` to a writable directory. Every result in the table
   above was produced with `PYTEST_DEBUG_TEMPROOT` pointing at a writable
   scratch directory; nothing else about the runs was changed.

### Orchestrated evidence on the integration branch (2026-08-28)

- `dag-parse` run 102 on `main` at `1f33b38`, Airflow 2.9.3 on Python 3.11
  against pinned PostGIS 16.14: **113 passed, 0 skipped, 0 errors** in
  134.35 s. The job supplies `RUN_DAG_TESTS=1` and the `TEST_POSTGRES_*`
  service variables, and its command-line `-m dag` overrides the default marker
  filter in `pyproject.toml`, so the orchestrated module was selected and
  executed. Zero skips and zero errors are what separate that from the
  `102 passed, 2 errors` recorded on the authoring host above.
- The module executes all ten production DAGs as real `DagRun`s in warehouse
  order and asserts each succeeded, `usda_nass_crop_ingest` among them.
- Every other workflow was green on the same commit.

This satisfies DAG-016 and the machine-verifiable precondition of
[`FOUR_SOURCE_REVIEW_GATE.md`](FOUR_SOURCE_REVIEW_GATE.md).

### Live provider contract

`tests/external/test_nass_source_contracts.py` covers the registered slice
preflight against the provider's 50,000-record ceiling, the survival of every
registered classification selection in the provider's own `get_param_values`
domain, outage classification, and credential handling. The
`external-contract` workflow owns it on a daily schedule and requires
`USDA_NASS_API_KEY`.

**Not yet executed against the provider.** The module's live assertions have
never run: the repository has no `USDA_NASS_API_KEY` secret configured, and the
authoring environment's network policy blocks `quickstats.nass.usda.gov`. Its
deterministic assertions pass and its live assertions skip cleanly on the
missing key. The first credentialed `external-contract` run is what will close
this.

### Observations outside this plan's scope

- `tests/integration/database/test_source_capture_cutovers.py::test_census_ingest_captures_array_and_bypasses_legacy_raw`
  leaves a `silver_census.observation_revision` row for `state:55|county:001`
  without a matching geography and never removes it. Re-running
  `./tests/run.ps1 integration` against the *same* database therefore fails
  `test_census_silver_flow.py::test_census_raw_rows_transform_to_exact_silver_keys`
  on the historical-geography guard. The tier passes on a freshly provisioned
  warehouse, which is how CI runs it. This predates this plan and is untouched
  here.
- Moving this plan out of `to_do/` left one stale relative link in the
  data-product E2E coverage plan, which that execution was bounded from
  repairing. It was repaired on 2026-08-31 when that plan was implemented and
  accepted into `completed/`; the link now resolves through `../completed/`.

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
- Register a deterministic Quick Stats provider stub in `iter_provider_stubs`
  in `tests/support/dag_pipeline.py` so the new DAG executes in the
  orchestrated DAG suite (`tests/dags/test_dag_pipeline_execution.py`). The
  suite's coverage assertion (DAG-015) fails for any DAG in `dags/` without a
  registered stub, and a passing `./tests/run.ps1 dag-pipeline` run is required
  evidence for the four-source review gate. The stub must answer the actual
  request (registered slice parameters, counts, pagination) at whatever scale
  the pipeline's own completeness guards demand; do not weaken a production
  guard to make the DAG pass.

**Acceptance:** Consumers cannot aggregate incompatible units or mistake suppressed values for zero through the default contract. The NASS DAG completes a successful DagRun in the orchestrated suite with every task instance successful.

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
- Orchestrated execution: the production DAG runs as a real DagRun against the disposable warehouse via the registered provider stub (DAG-015/DAG-016).
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
