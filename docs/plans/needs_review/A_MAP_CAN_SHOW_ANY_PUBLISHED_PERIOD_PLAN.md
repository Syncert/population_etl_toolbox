---
id: map-shows-any-published-period
branch: codex/analytics-backlog-2026-09-28
depends_on:
  - selected-geography-shows-painted-period
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit -q
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -q
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
---

# A map can show any published period

## Plan status

- **Status:** Ready for human review on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28
- **Dependencies:** `selected-geography-shows-painted-period` (the panel must
  already agree with the painted row before the painted row can move).
- **Next pickup:** Review the exact-period API and explorer control against the evidence below.
- **Origin:** time-granularity planning session, 2026-09-25. The user chose
  this as the **first wave** of time work: select a published period, no
  derivation. Rollups are the later wave in
  [`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](../in_progress/TIME_WINDOWS_AND_ROLLUPS_PLAN.md).

## Why

Every map, legend, tooltip and selected-geography card answers **the newest
published period per geography** and nothing else. The explorer caption says
so ("The publication spans 402 periods; each geography is coloured by its
newest one"), but a reader cannot look at June 2020, or at the year before a
policy change. The rows exist — FBI UCR's latest publication is the whole
402-month series — they simply cannot be chosen.

This wave adds no derived value. Each painted value is a provider-published
row for the chosen period, so ADR-0001's rule that aggregation is serving/
semantic policy, and the workbench plan's "nothing is rolled up", are
untouched.

## Current state (evidence, 2026-09-25)

- `GET /api/v1/observations` filters time only by `year_from`/`year_to`
  (`apps/api/routers/observations.py:77-86`). There is no exact-period filter
  and no "newest at or before" reduction; `newest_per_geography` always means
  newest overall.
- Non-reducing sources (FBI UCR, CDC, NASS) are loaded whole and reduced in
  the client (`observationAccess.ts` `newestPerGeography`). Reducing sources
  (BLS, ACS, FRED, PEP) are loaded already reduced to newest, so the client
  cannot choose a period for them without a new request.
- `/distribution/bins` measures one snapshot (the newest); legend bins for
  another period need the same period parameter or they describe a
  different period than the map (see completed
  `DISTRIBUTION_ONE_SNAPSHOT_PLAN.md`, `DISTRIBUTION_PERIOD_HONESTY_PLAN.md`).
- There is no route listing a metric's published periods; the catalog carries
  `valid_time_grains` but not the period list or span.
- Period columns differ per source (BLS `observation_date` = period end, FRED
  = period start, ACS Jan 1, PEP Jul 1, FBI `MM-YYYY` + dates, NASS `year`).
  The neutral route already projects `period_start`/`period_end` through each
  source's registry expression (`apps/api/registry.py`); this plan uses only
  those.

## Decisions to take in PP-1 (record in the API consumer guide)

1. **Parameter shape.** Recommended: `period_start=YYYY-MM-DD` (exact match on
   the served `period_start`), valid with `scope=latest` and with
   `newest_per_geography` omitted. Alternative: `period_on_or_before=` with
   newest-at-or-before semantics — more forgiving across mixed grains, but it
   silently shows a *different* period for a geography missing the chosen one,
   which the explorer would then have to disclose per geography. Default to
   exact; a geography with no row for that period is "No observation", and a
   withheld row is "Value not published" as today.
2. **Period list.** Recommended: a new additive route
   `GET /api/v1/observations/periods?metric_code=…&scope=latest` returning the
   distinct `(period_start, period_end)` pairs served, newest first, with row
   counts — paged and bounded like every list route.
3. **Default.** No `period_start` keeps today's behaviour exactly (newest per
   geography), so existing links and clients are unchanged.

### PP-1 decision (2026-09-28)

The existing neutral row publishes `period_start` as text. CDC and USDA NASS
can publish a bare year (`2021`), while the other sources publish an ISO date.
The parameter therefore matches that **served string exactly**, accepting
`YYYY` or a real calendar `YYYY-MM-DD`; a malformed value is a 422. Requiring
only a full date would silently make the year-only sources unselectable, which
contradicts this plan's every-source acceptance criterion. No date is
invented for a year-only publication. A period pin applies to `scope=latest`,
with or without `newest_per_geography`; `scope=as_released`, including a pinned
release, refuses the pin because the periods selector describes the latest
publication only. The additive periods route lists distinct non-null served
`(period_start, period_end)` pairs from that latest relation, newest first,
with counts, bounded paging, and the same public caching policy. Omission of
the new parameter leaves the existing query path untouched.

## Work items

- [x] **PP-1: decisions above,** recorded in `docs/reference/API_CONSUMER_GUIDE.md`
  (observations and ordering sections) before code.
- [x] **PP-2: API period filter.** Failing-first unit tests in
  `tests/unit/api/test_neutral_observations.py` for each source family
  (reducing and non-reducing): exact filter, unknown period → empty page not
  error, malformed date → 422, combination with `newest_per_geography` /
  `as_released` / pinned `release` per the recorded decision, and deterministic
  order. Capability declared in `/catalog/capabilities`.
- [x] **PP-3: periods route.** Unit + integration tests; bounded paging;
  cache headers per the existing policy.
- [x] **PP-4: distribution for a period.** `/distribution/bins` accepts the
  same parameter so the legend measures the painted period.
- [x] **PP-5: explorer period control.** A "Period" select (newest by default,
  labelled "Newest published (per geography)"), populated from PP-3; map,
  tooltip, legend, selected-geography card and caption all read the chosen
  period; the caption states the chosen period instead of "coloured by its
  newest". Shared explorer links carry the period and reopen it
  (`A_SHARED_EXPLORER_LINK_REOPENS_ITS_VIEW_PLAN.md` contract).
- [x] **PP-6: browser evidence.** Selecting an older period repaints the map
  from that period's rows (map oracle), legend and panel agree, and a
  geography missing that period reads "No observation".
- [x] **PP-7: contracts.** TESTING_CONTRACT catalog entries (API, WEB),
  CI_EVIDENCE_MAP, and the explorer notes.

## Implementation and validation (2026-09-28)

- `/observations` accepts a bound exact `period_start` over the latest
  publication before any geography reduction. `/observations/periods`
  discovers the source's non-null served start/end pairs with counts, paging,
  and the public cache policy. `scope=as_released` is refused for a period
  pin and for the latest-only period listing. `/distribution/bins` applies
  the same pin before binning. The served OpenAPI and visualization coverage
  snapshots and API consumer guide carry the additive contracts.
- The explorer offers provider-published period starts, preserves the pin in
  a shared URL, and filters map rows, the local/API legend, caption and
  selected panel. Several end dates for one start become one selectable
  start with a combined count, matching the API filter. A missing geography
  says "No observation" even when its history contains a value for a
  different period. The default sends no period pin.
- Focused tests covered all seven dispatch sources, year-only
  CDC/NASS, malformed dates, unknown periods, release-scope refusal, periods
  paging/order/cache, and distribution filtering. Real seeded FRED SQL
  exercised the three routes: `test_real_latest_period_list_filter_and_bins`
  passed on disposable PostGIS 16.
- `python -m pytest tests/unit -q --basetemp=.pytest_tmp_period`: 2,158
  passed before the final additive scope-refusal case; its focused API run
  then passed 80 tests. `npm --prefix apps/web run test:unit`: 701 passed.
  `npm --prefix apps/web run lint`, `ruff check .`, Next production build,
  OpenAPI snapshot tests, and `git -c core.whitespace=cr-at-eol diff --check`
  passed. `npm --prefix apps/web run test:browser`: 170 passed; the focused
  period browser case passed again after the final control and fixture edits.
  `tests/integration/api -m 'integration and not external'`: 178 passed,
  four unrelated skips. The database tier excluding `legacy/`: 250 passed,
  one skip. Skipped cases are not counted as evidence.
- The all-inclusive `tests/integration/database` command was attempted on a
  disposable PostGIS database but stalled in the unrelated legacy BLS
  metadata case. It was interrupted once; an interrupted run polluted its
  test database, so the container was recreated. The isolated FBI test then
  passed and the complete nonlegacy database tier passed from a fresh
  instance. The legacy BLS case remains unverified by this branch's local
  run. No external-source, live deployment, or browser-against-live-warehouse
  check was run for this change.

## Acceptance criteria

- A client can list a metric's published periods and request the rows for one
  of them, for every source, through `/api/v1` additive changes only.
- The explorer can paint any published period; map, legend, tooltip, panel and
  shared link agree on it.
- No value shown is derived; a geography without that period is never filled
  from another period.
- Omitting the period reproduces current behaviour byte-for-byte in responses.

## Out of scope

Rollups, trailing windows, YTD, and change-over-period — all derived; see
[`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](../in_progress/TIME_WINDOWS_AND_ROLLUPS_PLAN.md).
A time slider/animation is a later presentation of the same control.
