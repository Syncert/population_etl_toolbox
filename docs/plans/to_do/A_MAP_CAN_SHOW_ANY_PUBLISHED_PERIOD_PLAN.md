---
id: map-shows-any-published-period
branch: claude/map-shows-any-published-period
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

- **Status:** Unclaimed.
- **Last updated:** 2026-09-25
- **Dependencies:** `selected-geography-shows-painted-period` (the panel must
  already agree with the painted row before the painted row can move).
- **Next pickup:** PP-1.
- **Origin:** time-granularity planning session, 2026-09-25. The user chose
  this as the **first wave** of time work: select a published period, no
  derivation. Rollups are the later wave in
  [`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](TIME_WINDOWS_AND_ROLLUPS_PLAN.md).

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

## Work items

- [ ] **PP-1: decisions above,** recorded in `docs/reference/API_CONSUMER_GUIDE.md`
  (observations and ordering sections) before code.
- [ ] **PP-2: API period filter.** Failing-first unit tests in
  `tests/unit/api/test_neutral_observations.py` for each source family
  (reducing and non-reducing): exact filter, unknown period → empty page not
  error, malformed date → 422, combination with `newest_per_geography` /
  `as_released` / pinned `release` per the recorded decision, and deterministic
  order. Capability declared in `/catalog/capabilities`.
- [ ] **PP-3: periods route.** Unit + integration tests; bounded paging;
  cache headers per the existing policy.
- [ ] **PP-4: distribution for a period.** `/distribution/bins` accepts the
  same parameter so the legend measures the painted period.
- [ ] **PP-5: explorer period control.** A "Period" select (newest by default,
  labelled "Newest published (per geography)"), populated from PP-3; map,
  tooltip, legend, selected-geography card and caption all read the chosen
  period; the caption states the chosen period instead of "coloured by its
  newest". Shared explorer links carry the period and reopen it
  (`A_SHARED_EXPLORER_LINK_REOPENS_ITS_VIEW_PLAN.md` contract).
- [ ] **PP-6: browser evidence.** Selecting an older period repaints the map
  from that period's rows (map oracle), legend and panel agree, and a
  geography missing that period reads "No observation".
- [ ] **PP-7: contracts.** TESTING_CONTRACT catalog entries (API, WEB),
  CI_EVIDENCE_MAP, and the explorer notes.

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
[`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`](TIME_WINDOWS_AND_ROLLUPS_PLAN.md).
A time slider/animation is a later presentation of the same control.
