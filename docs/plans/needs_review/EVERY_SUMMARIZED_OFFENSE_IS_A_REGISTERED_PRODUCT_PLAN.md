---
id: every-summarized-offense-registered
branch: claude/every-summarized-offense-registered
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/fbi_ucr tests/unit/api/test_catalog_discovery.py tests/unit/api/test_neutral_observations.py -q
  - python -m pytest tests/dags -q
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database/test_fbi_ucr_pipeline.py -q
  - ruff check .
---

# Every summarized offense is a registered product

## Plan status

- **Status:** **Ready for review.** Implemented on branch `claude/plans-iteration-2026-09-20`; every work item and acceptance criterion has evidence below, including the live run.
- **Last updated:** 2026-09-23
- **Dependencies:** none. The FBI UCR pipeline this extends is accepted
  ([`docs/plans/completed/FBI_CRIME_PIPELINE_PLAN.md`](../completed/FBI_CRIME_PIPELINE_PLAN.md)).
- **External requirement:** a working `FBI_CDE_API_KEY` to capture the new
  provider fixtures (FBI-X01). Without the key, Milestone 1 stops after the
  registry and test scaffolding and the plan stays in `in_progress/`.
- **Next pickup:** none -- awaiting human review.

## Why

The Data Catalog lists one FBI UCR dataset, `summarized_violent_crime`, because
`ALL_PRODUCTS` in
[`registry.py`](../../../src/data_ingestion_toolbox/fbi_ucr/registry.py) holds
one product, and the catalog publishes one `source_dataset` per `product_id`
(`gold_fbi.sql`, `measure.product_id AS source_dataset`). A reader reasonably
expects burglary, homicide, robbery, and the other offenses the FBI publishes,
and finds only the violent-crime aggregate.

The registry already carries what is needed to fix this. `SUMMARIZED_OFFENSES`
lists all ten offense codes documented for the `summarized` endpoint
(`V`, `ASS`, `BUR`, `LAR`, `MVT`, `HOM`, `RPE`, `ROB`, `ARS`, `P`), and the
route pattern, parser contract, measure forms, and counted-entity bases are
shared. The first release left nine of the ten unregistered on purpose, as
its "deliberately narrow" scope. This plan registers the remaining nine, then
widens state coverage.

## Scope

**In scope**

1. **Milestone 1: nine new summarized-offense products.** One `FbiUcrProduct`
   per remaining offense code, using the same subject scope and period window
   as `summarized_violent_crime`: national, Wisconsin, the six reviewed
   Wisconsin ORIs, and `01-1990`..`06-2023`.
2. **Milestone 2: every documented state.** State scope widens from `("WI",)`
   to every code in `STATE_CODE_CONTRACT` for all ten products. `FS` and `GM`
   stay unsupported. Agency scope stays at the six reviewed ORIs.

**Out of scope**, and each is a separate future dataset contract per the
original plan:

- Agencies beyond the six reviewed ORIs. That is thousands of ORIs times ten
  offenses, and it needs its own request-budget and place-resolution plan.
- Expanded Homicide (`/shr/...`), arrests, NIBRS incident data, hate crime,
  LEOKA, use of force.
- Any locally derived total. `V` and `P` stay provider-published aggregates.
  They are never computed by summing component offenses, and component offenses
  are never checked against them as an additive identity (see Decisions).
- Renaming `summarized_violent_crime` or any of its measure identities. Web
  templates (`apps/web/lib/productTemplates.ts`) and saved analyses reference
  `FBI_UCR:summarized_violent_crime:V:*`, and v1 identities are promised.

## Decisions

These are settled here so the implementer does not re-open them. Each one
records its reason.

- **One product per offense, not one product with many offenses.** The
  registry docstring requires each product to freeze its own offense identity.
  `offense_code` is a scalar field, and the release, coverage, and measure
  tables key on `product_id`. Keeping one product per offense needs no schema
  change and gives each offense its own catalog dataset.
- **Product ids** follow the existing form, `summarized_<offense>`:
  `summarized_assault`, `summarized_burglary`, `summarized_larceny`,
  `summarized_motor_vehicle_theft`, `summarized_homicide`, `summarized_rape`,
  `summarized_robbery`, `summarized_arson`, `summarized_property_crime`. They
  are frozen once published.
- **Aggregates are not identities.** In UCR, violent crime is murder and
  nonnegligent manslaughter, rape, robbery, and aggravated assault. Property
  crime is burglary, larceny-theft, and motor vehicle theft, and it excludes
  arson. Agencies report these values unevenly, and the rape definition changed
  in 2013. Provider aggregates therefore need not equal the sum of their
  components. No quality rule, test, or API field may assert or present such a
  sum.
- **Per-offense measure availability is discovered, not assumed.** Each offense
  publishes whatever `actuals`/`rates` × `Offenses`/`Clearances` series the
  provider returns. If a series is absent for an offense (arson rates are a
  plausible case), no measure is registered for it. It is never published as
  zero or null-filled.
- **Shared scope constants.** The six reviewed ORIs and the state scope move to
  module-level constants that every product uses. This keeps ten products from
  drifting apart.

## Open question to settle in FBI-X04 (Milestone 2)

`FbiUcrProduct.reference_states` includes every state in `state_scope`, not
only the states of agency-scope ORIs. `capture.py` captures the Agency directory
per product for every reference state, and `replay.py` requires each of those
directories. With all states and ten products, that is about 520 directory
requests per run, all duplicates. It also makes state observations wait on
directories they do not use.

The implementer must pick one of these options and record the evidence:

- **(a)** derive `reference_states` from `agency_scope` only, as the method's
  own docstring describes ("derived from the agency scope"). This keeps
  Wisconsin as the only directory and needs a check that no state-observation
  path reads the directory. Preferred if that check holds.
- **(b)** keep the directory per state, and capture it once per run as a shared
  reference slice instead of once per product.

Neither option may weaken the rule that an agency observation cannot publish
without its reference slice.

## Work items

### Milestone 1: nine offenses

- [x] **FBI-X01: capture provider evidence.** For each of the nine codes, fetch
  the national, `WI`, and six-ORI `summarized` responses live. Trim them to the
  fixture window with `tests/support/build_fbi_fixtures.py` (it currently
  hard-codes the `_V` suffix at line 170 and must take the offense code). Store
  them as `summarized_{national|state_WI|agency_<ORI>}_<CODE>.json`. Record in
  `tests/fixtures/fbi_ucr/SOURCE_NOTES.md`, for each offense:
  - which series containers and suffixes it publishes;
  - how the provider labels each series (for example "Rape" legacy vs revised);
  - any series absent at any grain.

  Redact the key as the existing notes require.
- [x] **FBI-X02: register the products.** Add the nine `FbiUcrProduct` entries
  and extract the shared scope constants. Extend `ALL_PRODUCTS` in
  `SUMMARIZED_OFFENSES` order. Registry tests assert the following:
  - ten products with unique ids, one per documented offense code;
  - `get_product` round-trips every id;
  - `summarized_violent_crime` is byte-identical in every contract field.
- [x] **FBI-X03: prove each product end to end on fixtures.** Parametrize the
  capture, replay, metadata, and aggregation-boundary unit tests over the
  registered products rather than the `PRODUCT = SUMMARIZED_VIOLENT_CRIME`
  constant, and check that each one:
  - emits exactly the measures its fixtures publish (failure path: a missing
    series registers no measure and writes no zero);
  - uses the offense code in every `measure_id`;
  - never mixes one product's series into another's.
- [x] **FBI-X03a: downstream registers.** Update the following:
  - `tests/unit/api/test_catalog_discovery.py:346`: the dataset list becomes
    the ten ids in registry order;
  - `tests/support/product_coverage.py`: coverage entries or datasets for the
    new products;
  - `tests/fixtures/api/viz_coverage.json`;
  - `tests/unit/api/test_neutral_observations.py`, where it assumes a single
    FBI dataset;
  - `docs/user-guides/FBI_UCR_PIPELINE_OPERATIONS.md`, which says "the first
    registered product" and must list all ten;
  - any `TESTING_CONTRACT.md` behavioral-catalog rows the new tests add. Keep
    the register total and its table in sync.

  The FBI DAG already builds one capture → replay → publish chain per
  `enabled_products()`. Assert that the DAG structure test sees ten chains.
- [x] **FBI-X03b: warehouse proof.** Run the FBI database integration file
  against the pinned PostGIS container. It must publish all ten products and
  keep one release row per product. It must also replay idempotently: a second
  replay writes no new rows.

### Milestone 2: every state

- [x] **FBI-X04: settle the directory question** above with a failing-first test
  of the chosen behavior.
- [x] **FBI-X05: widen the state scope** to every `STATE_CODE_CONTRACT` code.
  The request budget is about (1 national + 52 states + 6 agencies) × 10
  offenses, roughly 590 observation requests per run at the 0.25 s minimum
  spacing, plus whatever directory captures FBI-X04 keeps. Confirm this against
  the api.data.gov hourly limit for the key in use and record the result. If it
  does not fit, record the pool, spacing, or schedule change instead of quietly
  narrowing the scope.
  - Fixtures: one additional captured state is enough, preferably one with known
    partial agency participation, to prove that state resolution does not depend
    on Wisconsin. Every other state is covered by registry-level tests of
    `canonical_state_fips` and endpoint construction, not by 50 fixtures.
  - The `VI` (FIPS `78`) state subject must resolve to the territory geography.
    Show it has a geography row, or record that it lands in the
    unresolved-geography path by contract.
- [x] **FBI-X06: live run.** Run the ingest DAG, or the capture → replay →
  publish chain, against the internal stack with the key. Record rows per
  product and per state, plus any provider error bodies. Make sure no subject
  that did not report is published as zero.

## Acceptance criteria

1. The Data Catalog lists ten FBI UCR datasets, one per documented summarized
   offense, each with only the measures its provider responses publish.
2. `summarized_violent_crime` and its four measure identities are unchanged,
   and existing web templates still resolve.
3. No test, rule, or response presents a component-offense sum as, or
   reconciles one against, `V` or `P`.
4. After Milestone 2, national and all 52 `STATE_CODE_CONTRACT` state subjects
   publish for every product. `FS`/`GM` remain unsupported, and agency scope is
   still the six reviewed ORIs.
5. The directory-capture behavior is decided, tested, and documented. The
   request volume is measured against the key's rate limit.
6. Every `verify` command passes, and the live run (FBI-X06) is recorded with
   row counts.

## Evidence record

### Decisions taken during implementation

- **FBI-X04: option (a).** `reference_states` derives from the agency scope
  only. Evidence: `subject_label` labels a state subject from
  `STATE_CODE_CONTRACT`, never from the directory, and no transform, gold, or
  quality path reads the directory for a state subject. Failing-first tests:
  `test_fbi_registry.py::test_reference_states_come_from_the_agency_scope_only`
  and `test_fbi_replay.py::test_state_observations_replay_without_any_agency_directory`
  (the latter raised `missing required capture slices: /agency/byStateAbbr/WI`
  before the change). The agency rule is unchanged:
  `test_agency_observation_without_its_reference_slice_is_quarantined` and
  `test_missing_required_slice_blocks_the_release` still hold.
- **The provider's `-1` rate is a sentinel, and it stays quarantined.** Live
  agency (and Virgin Islands state) responses publish a rate of `-1` where the
  covered population is zero. Replay already quarantined it as
  `negative_measure_value`, and the live violent-crime pipeline had been doing
  so (1,804 rows in the 2026-08-15 release); the derived V agency fixtures
  simply never showed it. Tests now count those quarantines exactly instead of
  assuming none, and assert no published value is negative.
- **Fixture-driven tests replay a fixture-scoped product.** Every product
  registers 52 states; fixtures exist for WI, PA, and VI. `fixture_scoped()`
  (unit `conftest.py`, `tests/support/fbi_release.py`) narrows the state scope
  for replay; the full scope is proved at registry level
  (`test_every_product_covers_every_documented_state`).
- **Defect found and fixed: the previous release.**
  `load_latest_accepted_release` counted a release that was captured but never
  published, and ignored the subject scope. On the development warehouse the
  violent-crime 2026-09-15 release, captured by the failed 2026-09-07 run, had
  been judged `unchanged` by every later run and never published; and the
  widened state scope would have been judged `unchanged` and never captured.
  The previous release must now be `published` and match `subject_scope`
  (`test_only_a_published_release_of_the_same_scope_is_the_previous_one`,
  failing first).

### FBI-X01 evidence

72 live requests on 2026-09-23 (nine offenses × national, WI, six ORIs, window
`01-1990`..`06-2023`), every one `200`; plus 20 for PA and VI across all ten
offenses. Every offense publishes all four series at every grain for all 402
months; nothing is absent, arson publishes rates, and rape has one label.
Recorded in `tests/fixtures/fbi_ucr/SOURCE_NOTES.md`. The key was checked
absent from every fixture and capture.

### FBI-X05: request budget

A run is 600 requests: per product 1 national + 52 states + 6 agencies + 1
directory (FBI-X04 removed the 51 other directory captures per product, which
would have made it 1,110). api.data.gov documents a default of 1,000 requests
per hour (`https://api.data.gov/docs/developer-manual/`), so 600 fits one run
per hour with room. The `api.usa.gov` gateway's own headers did not confirm
the hourly figure: they report `x-ratelimit-limit: 10` with
`x-ratelimit-remaining: 9` constant across eight requests spread over 70 s,
after ~110 requests from this key in the preceding half hour -- the behaviour
of a short-window limit, not an hourly counter. Spacing is 0.25 s per client
and the `fbi_cde_api` pool is 2, so at most ~8 requests per second leave the
host, under a 10-per-window limit. No pool, spacing, or schedule change was
needed; the live run (FBI-X06) is the measurement.

### Validation so far

| Command | Result |
| --- | --- |
| `pytest tests/unit -q` | 2111 passed (before Milestone 2); FBI suite 283 passed after |
| `pytest tests/unit/fbi_ucr tests/unit/api/test_catalog_discovery.py tests/unit/api/test_neutral_observations.py -q` | passed |
| DAG tier in `docker-airflow-scheduler-1` (`pytest -m dag tests/dags`) | 145 passed, 5 skipped (DB-backed DAG tests needing `TEST_POSTGRES_*` in the container; unrelated to FBI) |
| `RUN_INTEGRATION_TESTS=1 pytest -m "integration and database" tests/integration/database/test_fbi_ucr_pipeline.py` (pinned PostGIS, port 55532) | 16 passed |
| `RUN_E2E_TESTS=1 pytest -m e2e tests/e2e/test_fbi_ucr_pipeline.py` | 1 passed |
| `pytest tests/unit -q` (after every change) | 2133 passed |
| `npm --prefix apps/web run test:unit` (templates addressing `summarized_violent_crime`) | 681 passed |
| `RUN_EXTERNAL_TESTS=1 pytest -m external tests/external/test_fbi_source_contracts.py` (live, with the key) | 26 passed |
| `test_only_a_published_release_of_the_same_scope_is_the_previous_one` (added after the 16-test file run) | passed |
| `ruff format --check . ; ruff check .` | clean |

### FBI-X06: live run on the internal stack

`fbi_ucr_ingest` run `manual__fbi_x06_2026-09-23`, 18:49:52–18:58:00 UTC,
**success**, all 32 tasks. Every product decided `ingest` for refresh date
2026-09-15 and published, with 59 observation slices and 1 directory slice each
-- 600 provider requests in about 8 minutes, no `429` and no retry, which is the
measured confirmation of the FBI-X05 budget. Violent crime's 2026-09-15
release, stuck unpublished since 2026-09-18, published in the same run.

Rows in `gold_fbi.crime_observation` for release 2026-09-15:

| Product | Rows | Null (`not_reported`) | States | Measures | Quarantined `-1` rates |
| --- | --- | --- | --- | --- | --- |
| `summarized_violent_crime` | 92,826 | 1,932 | 52 | 4 | 3,826 |
| `summarized_assault` | 92,826 | 1,932 | 52 | 4 | 2,046 |
| `summarized_burglary` | 92,830 | 1,932 | 52 | 4 | 2,042 |
| `summarized_larceny` | 92,830 | 1,932 | 52 | 4 | 2,042 |
| `summarized_motor_vehicle_theft` | 92,830 | 1,932 | 52 | 4 | 2,042 |
| `summarized_homicide` | 92,828 | 1,932 | 52 | 4 | 2,044 |
| `summarized_rape` | 92,830 | 1,932 | 52 | 4 | 2,042 |
| `summarized_robbery` | 92,828 | 1,932 | 52 | 4 | 2,044 |
| `summarized_arson` | 92,828 | 1,932 | 52 | 4 | 2,044 |
| `summarized_property_crime` | 92,830 | 1,932 | 52 | 4 | 2,042 |

(V's quarantine count includes both of its 2026-09-15 captures: the failed
2026-09-07 run's and this one's.)

Per state (burglary shown; every product has the same shape): each of the 52
state subjects publishes 1,608 rows (402 months × 4 measures) with
`geography_status = provider_geo_exact` on its canonical `state:<FIPS>`,
except the Virgin Islands, which publishes 1,346 rows on `state:78` -- 1,036 of
them null -- because 262 of its rates are the `-1` sentinel and were
quarantined. Kentucky (36) and Montana (92) are the only other states with
null months. No provider error body was returned.

**No subject that did not report is published as zero.** Every zero in the
release carries the provider's own source text `0` (0 rows otherwise); months
the provider left empty are null (`unknown` participation: 11,640 state and
7,680 agency rows, none zero). Zeros under `no_participation` are
provider-published zeros -- for example the Wisconsin State Patrol, which has no
resident population yet reports counts -- and the contract keeps a published
zero as published.

After `docker restart docker-api-1` and a `glossary_harvest` run,
`/api/v1/catalog/capabilities` lists the ten datasets in registry order and
`/api/v1/catalog/metrics?source_code=FBI_UCR` answers 40 metrics, four per
dataset.

### Acceptance criteria

1. Ten datasets, each with the four measures its responses publish -- live
   catalog above; `test_each_product_emits_exactly_the_measures_its_provider_publishes`.
2. Violent crime unchanged -- `test_violent_crime_contract_is_unchanged_by_the_new_products`;
   web unit tests pass.
3. No component sum -- `test_no_offense_is_named_outside_the_registry`; no rule,
   test, or response adds offenses.
4. National plus 52 states for every product, `FS`/`GM` unsupported, six ORIs --
   `test_every_product_covers_every_documented_state` and the live run.
5. Directory behaviour decided and tested (FBI-X04); request volume measured
   (FBI-X05, FBI-X06).
6. Every `verify` command passes; live run recorded above.
