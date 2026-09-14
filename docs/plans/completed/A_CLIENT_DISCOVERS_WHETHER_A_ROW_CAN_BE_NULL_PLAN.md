---
id: a-client-discovers-whether-a-row-can-be-null
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A client can discover whether a row of a source can be null

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/schemas/catalog.py`,
  `apps/api/services/catalog_service.py`,
  `docs/reference/API_CONSUMER_GUIDE.md`

## Context

Found by checking a guide promise against the warehouse. The guide said:

> `value` is `null` whenever the source did not publish a usable number, and
> `value_status` says why in the source's own vocabulary … `value_status` is
> `null` when the source publishes no status vocabulary at all — which is
> distinguishable from a published `valid`.

The first half is true of three sources. The second half is not true of the
other four, and the difference is not about vocabularies — it is about which
rows are served at all.

| source | served `value_status` | why |
| --- | --- | --- |
| CDC, FBI UCR, USDA NASS | published | the fact carries a value state and the serving views project it |
| BLS | absent | `gold_bls.fact_bls_observation` selects `WHERE s.value IS NOT NULL` |
| FRED | absent | `gold_fred.fact_fred_observation` selects `WHERE s.is_missing = FALSE` |
| Census ACS | absent | `gold_census.fact_acs_observation` selects `WHERE s.estimate_value IS NOT NULL` |
| Census PEP | absent | `silver_pep.fact_population_estimate.value` is `NOT NULL`, so a non-numeric row never reaches the fact |

Each of those four **does** publish a status vocabulary in silver — BLS, FRED
and ACS have a `value_status` CHECK, FRED an `is_missing` flag besides. So the
reason a served row carries no status is that the serving relation carries
only published numbers.

The consequence is a caller-visible difference in row shape that nothing in
the contract declared. A period BLS or FRED published without a usable number
is **absent from the served series**, not present and marked. A client
charting a monthly history draws a straight line across the gap, exactly as
if the period had never existed — and it had no way to know the two shapes
differ, because the flag did not exist and the guide said the opposite. That
is the "infer the shape from a row you happened to read" that
`observation_dimensions` was added to prevent (API-109).

## What was changed

- `SourceCapability` and `MetricCapability` publish
  `publishes_value_status`, derived from the dispatch entry's
  `value_status_column` — the column the neutral read actually projects — so
  it is one declaration rather than a second list. On both resources for the
  reason API-119 records: a client that searched the catalog should not have
  to enumerate sources to learn the shape of its own rows.
- The guide replaces the wrong sentence with the two shapes, names which
  sources are which, and says plainly that a gap in a series from the second
  kind is a gap: do not draw across it.
- Two additive fields in the reviewed OpenAPI snapshot, which is the whole
  served-contract change.

## Validation

Unit, in `tests/unit/api/test_catalog_discovery.py`:

- `test_the_value_state_capability_is_derived_from_the_dispatch` — per
  source, the flag equals `value_status_column is not None`, and **both**
  shapes must be represented or the rule proves only one of them.
- `test_a_metric_declares_whether_its_own_rows_can_be_null` — the metric
  resource agrees with its source.

Integration, in `tests/integration/api/test_catalog_serving_agreement.py`:

- `test_a_source_that_publishes_no_value_state_serves_only_numbers` — the
  warehouse is held to the declaration in both directions, so
  `publishes_value_status: false` is a fact about the rows rather than about
  a projection.
- `test_a_metric_carries_the_same_value_state_declaration_as_its_source` —
  across a sample of published metrics against the served catalog.

## Deliberately not done

- **The four sources are not changed to serve their unpublished periods.**
  Including them would be a different contract — every `total`, every page,
  every map and every chart in the application would change, and a row with
  a null value has to be given a status vocabulary in the serving relation to
  be worth serving. It may well be the better contract, and this records the
  choice rather than making it: what a client needs first is to know which
  shape it is reading.
- **`publishes_value_status` is not a filter.** A client that wants only
  published numbers already has them from those four sources, and for the
  three that publish a state the filter belongs on the source-scoped route
  where the vocabulary is the provider's (`/usda-nass/observations` has
  `value_status`, API-124).
