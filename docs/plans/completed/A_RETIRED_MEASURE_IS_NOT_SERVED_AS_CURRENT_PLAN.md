---
id: a-retired-measure-is-not-served-as-current
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
---

# A retired measure is neither a valid stored configuration nor a served capability

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row API-119.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/services/saved_analysis_service.py`,
  `apps/api/services/evidence_packet_service.py`,
  `apps/api/services/catalog_service.py`

## Context

The guide: a retired LAUS series "still resolves through
`GET /catalog/metrics/{metric_code}` and reports `freshness_state:
"retired"` ... It no longer answers observations." A stored block "whose
measure was retired after it was stored is reported the same way", and
`validation` "reports whether the document still matches live
capabilities."

Two services read only existence:

- `_require_metric` (`saved_analysis_service.py:76-82`), reused by the
  evidence-packet write and read paths, raises only when
  `resolve_metric(...)` is `None`. `freshness_state` is projected by
  `catalog_queries.py` and carried on the model and never read.
- `get_metric_capability` (`catalog_service.py:190-216`) copies
  `served_by_neutral_routes` and `observation_routes` from the source's
  discovery entry, so a retired row is published with six routes that
  answer it `total: 0`. The same response omits `observation_dimensions`,
  which API-109 put on the source resource and not on the metric's.

## Findings

- `test_saved_analysis.py:557` and `test_evidence_packets.py:593` simulate
  retirement by deleting the glossary row and assert on "not a published
  metric". The contract's retirement -- row present, state `retired` -- is
  never exercised, and both pass for the wrong reason.
- `test_catalog_discovery.py:219` asserts the route superset for a current
  metric only.

## Acceptance criteria

1. A configuration or packet block naming a retired metric is refused on
   write and reported invalid on read, with a reason that says "retired",
   the document returned unmodified.
2. A retired metric's capability resource reports no observation routes and
   `served_by_neutral_routes: false`, and every metric resource carries
   `observation_dimensions` for its source.
3. Failing-first unit tests for both, with the row present and
   `freshness_state='retired'`; the two existing tests are corrected to
   exercise that state rather than deletion.
4. The OpenAPI snapshot and the guide carry the new metric field.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-113 at authoring time).

## Non-goals

- Serving retired observations. The guide says they are not answered.

## Validation

- `apps/api/services/metric_freshness.py` is the one statement of what the
  harvested `freshness_state` means to the served API: `is_retired` (value,
  not row — its two readers hold a `RowMapping` and a validated
  `MetricCapability`, compared case-insensitively, with an absent or unknown
  state *not* retirement) and `retirement_refusal`, the single sentence every
  refusing surface serves.
- `_require_metric` (`saved_analysis_service.py`) now reads the state after
  existence, so both the configuration and the evidence-packet write and read
  paths refuse a retired measure from one change.
- `get_metric_capability` (`catalog_service.py`) publishes
  `observation_dimensions` for every metric, and for a retired one returns
  before copying the source's routes: `served_by_neutral_routes: false`,
  `observation_routes: []`, `observation_filters: []`. The dimensions stay —
  they describe the rows the warehouse published, which retirement does not
  withdraw.
- New nodes:
  - `test_catalog_discovery.py::test_a_retired_metric_advertises_no_route_that_will_not_answer_it`
  - `test_catalog_discovery.py::test_every_metric_declares_the_dimensions_its_rows_carry`,
    which compares the metric resource's declaration against
    `list_source_capabilities` rather than a copy of it
  - `test_saved_analysis.py::test_a_retired_measure_cannot_be_stored_as_a_configuration`
  - `test_evidence_packets.py::test_a_retired_measure_is_refused_at_write_naming_the_block`
- Corrected nodes: `test_stale_configuration_is_reported_on_read_not_repaired`
  and `test_retired_measure_is_reported_on_its_block_not_repaired` retired the
  measure by *deleting* the glossary row. They now leave the row in place with
  `freshness_state = 'retired'` — the contract's retirement — and assert the
  reason says `retired`. The deleted-row path stays asserted where it belongs,
  on the write of an unknown `metric_code`. Both metric fixtures carry
  `freshness_state` because the projected row does.
- Break-test: neutralising the two reads (`retirement_refusal` never
  consulted, the retired branch never taken, the dimensions never copied)
  leaves `6 failed, 114 passed` — every new and corrected node — and the fix
  restored, `120 passed`.
- `MetricCapability.observation_dimensions` is in the regenerated OpenAPI
  snapshot (39 operations, 56 schemas; one added field). The guide states the
  retirement behaviour under the retired-LAUS-rows paragraph and the
  two-resource declaration under `observation_filters`.
- Tiers: `pytest tests/unit` 1548 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 157 passed, 2 skipped,
  14 deselected; `ruff format --check .` and `ruff check .` clean.

## Remaining work

- None. The dependent plan `an-envelope-records-its-reduction` is unblocked.
