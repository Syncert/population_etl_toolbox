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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect.**
- **Last updated:** 2026-09-13
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
   identifier; API-112 at authoring time).

## Non-goals

- Serving retired observations. The guide says they are not answered.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
