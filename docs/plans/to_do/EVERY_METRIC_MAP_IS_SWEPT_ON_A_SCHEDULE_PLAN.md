---
id: full-map-sweep-scheduled
branch: claude/full-map-sweep-scheduled
depends_on:
  - every-map-proves-it-displays-its-data
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
---

# Every metric's map is swept on a schedule

## Plan status

- **Status:** Unclaimed.
- **Last updated:** 2026-09-24
- **Dependencies:** `every-map-proves-it-displays-its-data`.
- **Next pickup:** SWEEP-1.

## Why

The map-display sweep (`tests/frontend/smoke/map-display.smoke.test.js`,
WEB-118) reads at most 40 metrics per source by default: an even,
deterministic spread. On the development warehouse that is every FBI, CDC,
FRED, PEP and NASS metric, but only 40 of about 13,300 BLS and 40 of 4,447 ACS
metrics. A defect confined to one ACS table or one BLS program can sit
outside the spread indefinitely; the NASS combined-counties collision was
found only because 24 of the sampled NASS metrics happened to carry it.
`MAP_SWEEP_ALL=1` reads everything, but nothing runs it.

## Work items

- [ ] **SWEEP-1: measure a full run** on the development stack
  (`MAP_SWEEP_ALL=1 SMOKE_BASE_URL=http://localhost:3001`), recording wall
  time per source and the API's latency under it. The sweep is sequential, so
  expect hours.
- [ ] **SWEEP-2: make it resumable and bounded.** Add
  `MAP_SWEEP_OFFSET`/`MAP_SWEEP_LIMIT`, or a per-source shard, so a scheduled
  run can cover the catalog across several nights, and write a machine-readable
  report (JSON per metric and grain: verdict, rows, coloured, problem).
- [ ] **SWEEP-3: schedule it** against the live-deployment smoke target
  (`live-deployment-smoke.yml` already runs against a deployment) or the
  internal stack, weekly, with the report as an artifact and any FAIL as a
  red run.
- [ ] **SWEEP-4: triage the first full report** and file a plan per defect
  class it finds, as the NASS findings were.

## Acceptance criteria

1. Every published metric's map is graded at least once a week.
2. A failure names the metric, grain, and reason, and turns the run red.
3. The run's report is kept as an artifact.
