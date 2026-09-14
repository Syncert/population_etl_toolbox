---
id: cdc-sweep-fixture
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - source-route-code-sweep
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit/shared -q
---

# The catalog sweeps reach the third identity strategy

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/integration/api/test_catalog_serving_agreement.py`

## Context

DB-025 and DB-028 sweep every registered source's current catalog codes and
published grains. They are only as strong as the warehouse under test: a
source that publishes no catalog row contributes nothing, and the fixtures
publish Census ACS, FRED, and — since DB-030 — Census PEP.

DB-030 is what that gap costs. Seeding Census PEP was what revealed that its
entire source-scoped route family answered no rows for any catalog code; the
sweep had been passing for years without ever asking about the source.

Three sources remain unseeded, and one of them exercises a strategy nothing
else does. The registry identifies a metric's serving rows three ways:

| Strategy | Sources | Seeded |
|---|---|---|
| `metric_code_column` | BLS, Census ACS, FRED | ACS, FRED |
| `lineage_key_column` | Census ACS, Census PEP | both |
| `identity_columns` | **CDC, FBI UCR, USDA NASS** | **none** |

`identity_columns` is the strategy where the service binds
`lineage.get(field)` for each declared column and refuses the metric if the
lineage publishes no such key. Nothing has ever asked a source that uses it
to answer a code the catalog published. CDC is the one to seed: its silver
schema is small, its publisher derives grains from rows, and it is the
stratified source whose rows carry a stratum and an adjustment status, so a
grain sweep over it exercises more of the envelope than a single-series
source does.

## Acceptance criteria

1. One CDC metric is published end to end — silver rows, the real gold views,
   the real glossary harvest — so DB-025 and DB-028 actually ask about a
   source identified by `identity_columns`.
2. The code that answers is the catalog's own, composed by the publisher and
   the harvest rather than written in the fixture.
3. The fixture removes exactly what it created, registration side effects
   included, in foreign-key order.
4. The grain the catalog publishes for it is the vocabulary word derived from
   its rows, and it answers.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Seeding FBI UCR and USDA NASS. They share CDC's strategy; the first one
  makes the strategy non-vacuous, and the remaining two are their own work.
- Changing what any sweep asks. DB-025 and DB-028 already ask the right
  questions; this gives them a source to ask about. Each gains the fixture and
  one non-vacuity assertion — that CDC was among the sources exercised —
  exactly as they already assert for Census ACS and FRED.
- Wiring the fixture into DB-030 and DB-031. Those sweep `SERVING_CONTRACTS`,
  the source-scoped route family, and CDC has no source-scoped route: it is
  registered in `OBSERVATION_DISPATCH` only. Adding it there asserts a route
  that does not exist. (Tried, reverted; see Validation.)

## Validation

Run against a local PostgreSQL 16.13 + PostGIS 3.4 (`population_etl_test`),
not rendered SQL.

**Green.** `tests/integration/api/test_catalog_serving_agreement.py`,
`-m "integration and database and not slow"` — 8 passed, up from 7. The new
row's test is
`test_a_source_identified_by_its_lineage_columns_answers_its_catalog_code`.

**Failing-first.** Mutating the reviewed registry so CDC's `identity_columns`
names a column its lineage does not publish
(`("asset_id", "measure_id", "value_type_id", "topic")`) makes the service
refuse every CDC metric:

| File under test | Result under the mutation |
|---|---|
| At `HEAD` (no CDC fixture) | **7 passed** — the strategy was never asked |
| With this change | **3 failed** — DB-025, DB-028, DB-032 |

That table is the finding. DB-028's own docstring names CDC first, for a
grain-vocabulary defect CDC actually had, and the guard written for it could
not see a defect in CDC's identity strategy because nothing published a CDC
metric for it to ask about.

**Register.** `python -m tests.support.catalog_evidence` renders 335 rows;
`tests/unit/shared/test_repository_hygiene.py` and
`tests/unit/shared/test_catalog_evidence.py` — 16 passed.

**Unit tier.** `tests/unit` — 1441 passed (run while diagnosing the mutations
above; the registry was restored before the green run).

### What the schema required

The fixture had to satisfy the real constraints, which is the point of
publishing through the real relations:

- `asset_id` is constrained to `'cdi'` or `'places_county'`, so every cleanup
  delete names this fixture's own release, measure or stratum rather than the
  asset alone.
- `release_watermark` is ordered as `::BIGINT` by both the publisher view and
  `gold_cdc.latest_release_observation`, so it is Socrata epoch seconds, not a
  date.
- `stratum_id` and `source_record_id` are both `^[0-9a-f]{64}$` — content
  digests, per the schema.
- `period_start`/`period_end` are integer years.
- `reconciled_at` is required unless the release is `replaying`, and
  `source_run_id` is a foreign key to the run the capture was recorded under.

### Not reproducible under this harness

Mutating `gold_cdc.metric_publisher`'s `physical_lineage` key in the database
directly proves nothing: the integration harness reapplies the migrations at
session start, so the view is restored before the first test runs. The
registry mutation above is the equivalent disagreement, expressed on the side
the harness does not rebuild.

## Remaining work

- None. FBI UCR and USDA NASS remain unseeded by design (see Non-goals).
