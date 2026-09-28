---
id: full-map-sweep-scheduled
branch: codex/analytics-backlog-2026-09-28
depends_on:
  - every-map-proves-it-displays-its-data
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
---

# Every metric's map is swept on a schedule

## Plan status

- **Status:** In progress on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28
- **Dependencies:** `every-map-proves-it-displays-its-data`.
- **Checkpoint:** SWEEP-2's stable per-source offset/limit and JSON verdict
  report are implemented. The focused selection/report tests and composed
  smoke tier pass. Inspect a retained report outside Playwright's
  `test-results` directory, then continue with SWEEP-1/3/4 when a populated
  target exists.
- **Blocker:** SWEEP-1 needs the populated development warehouse at
  `http://localhost:3001`, which is unavailable here. SWEEP-3/4 need a
  reachable deployment origin or equivalent populated internal stack; no
  deployment target is configured. A disposable smoke fixture cannot establish
  weekly coverage of the full published catalog.

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
- [x] **SWEEP-2: make it resumable and bounded.** Add
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

1. Every published metric's map is graded at least once a week against a
   reachable deployment or populated internal stack.
2. A failure names the metric, grain, and reason, and turns the run red.
3. The run's report is kept as an artifact.

## Evidence and remaining work

- `MAP_SWEEP_OFFSET`/`MAP_SWEEP_LIMIT` select a contiguous slice of each
  source's sorted active catalog. `MAP_SWEEP_ALL=1` and the ordinary even
  sample still work. Invalid bounds fail explicitly. Empty-grain declarations
  continue to be checked across the entire fetched catalog.
- `MAP_SWEEP_REPORT_PATH` writes JSON with source catalog totals, selected
  metric codes, and every graded metric/grain verdict, row count, coloured
  count, and problem. The report's `complete` flag is false if the sweep aborts.
  The retained fixture report at the requested temporary path showed seven
  source catalogs, six drawable metric/grain results, `complete: true`, and
  `fail: 0`; FRED is national-only and has no drawable map.
- `npm --prefix apps/web run test:unit -- map-sweep-selection.test.js`: 4 passed.
  `./tests/run.ps1 web-smoke` with offset 0 and limit 1: 27 smoke tests and
  6 live map paint tests passed against the disposable composed fixture.
- `npm --prefix apps/web run test:unit`: 707 passed. `npm --prefix apps/web
  run lint`, `ruff check tests/support/plan_environments.py`, and
  `git diff --check` passed. The plan-environment and dispatcher graph tests
  passed (23).
- Still required: measure the full populated catalog, schedule bounded shards
  with durable artifacts against a real target, and triage its first complete
  report. These are not satisfied by the fixture's six selected metrics.
