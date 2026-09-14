---
id: analytics-workbench
branch: feat/analytics-workbench
depends_on: []
parallel_safe: true
complexity: high
verify:
  - ./tests/run.ps1 api
  - ./tests/run.ps1 web-unit
  - ./tests/run.ps1 web-browser
  - ./tests/run.ps1 web-build
---

# Build-your-own analytics: the workbench

## Plan status

- **Status:** Ready for review. Claimed 2026-09-13, all seven phases complete
  2026-09-14. Authored 2026-09-13 from a product request and an inspection of
  the comparison workspace, the explorer, the analysis routes, and the
  saved-analysis contract.
- **Last updated:** 2026-09-14
- **Phase order taken:** WB-3, WB-4, WB-1, WB-2, WB-5, WB-6, WB-7. The plan
  leaves WB-1 and WB-3 in either order; the API-first order is taken because
  `AGENTS.md` builds frontend behaviour against stable API contracts, and
  because WB-5 needs WB-4 while WB-2 ships an interim refusal until it lands.
- **Progress:** see "Evidence record" below.
- **Owner surfaces:** `apps/web/app/workbench/` (new), `apps/web/lib/workbench.ts`
  (new), `apps/web/components/{LineChart,BarChart,HeatmapChart}.tsx` (new),
  `apps/api/routers/comparison.py`, `apps/api/services/comparison_service.py`,
  `apps/api/schemas/saved_analysis.py`, `docs/reference/API_CONSUMER_GUIDE.md`,
  `docs/decisions/0003-saved-analysis-authentication-and-persistence.md`.
- **Depends on:** nothing in `to_do/` or `in_progress/`. It builds on the
  first-wave web foundation (`WEB_ANALYTICS_FIRST_WAVE_PLAN.md`, in
  `needs_review/`) and the analysis routes delivered by
  `API_DEVELOPMENT_PLAN.md` (completed). Phases WB-3 and WB-4 add API
  behaviour and must land before the web phases that consume them; the
  phase table below states the order.
- **Next pickup:** none. See "Plan close-out" at the end of this document for
  what was delivered, the two recorded corrections to the criteria, and the
  environment-limited checks that were not run.

## The request, and how this plan reads it

The request was for "a build-your-own analytics page that allows selecting,
mixing and matching any of the API endpoints and graphing them on a line, bar,
scatter plot or heatmap, with the option to check correlation — complicated
by the grain of county, state, national."

Four adjustments, each with a reason:

1. **You mix measures, not endpoints.** The endpoints are transport. What a
   reader combines is a *published measure* — a catalog `metric_code` with its
   `source_code`, `units`, `valid_time_grains`, `valid_geo_grains`, scope and
   release. Every measure is reachable through whichever access shape its
   `/catalog/capabilities` entry declares, which `lib/explorerSources.ts` and
   `lib/observationAccess.ts` already resolve. So the picker is the catalog,
   filtered by capability, and the chart never names a route. Catalog, health,
   freshness, saved-analysis and evidence-packet routes are not chartable and
   are not offered.

2. **"Heatmap" is two different charts.** A *matrix heatmap* is one measure
   laid out as geographies × periods (states down, years across) and needs
   only `/observations`. A *correlation heatmap* is measures × measures at one
   grain and needs an API-derived statistic. They are separate deliverables
   (WB-2 and WB-4) because one is a rendering of published values and the
   other is a derived analysis with caveats.

3. **Correlation is computed by the API, never in the browser.** The
   first-wave invariants forbid a client-authored composite or "any
   client-authored definition that could be mistaken for a provider fact",
   and `/distribution/bins` is the precedent: an analysis the API derives,
   labelled `derived: true`, with the caveats it could not carry. A Pearson
   coefficient over an inner join of two newest-per-geography reductions is
   exactly such an analysis. It goes behind `/comparison/preflight`, is served
   only where the pair is comparable, and travels with its own caveats —
   above all that association is not causation, which
   `docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md` already makes a product
   rule.

4. **The grain is resolved by a small set of rules, not worked around.**
   They are written out in "The grain contract" below. The short form: a
   cross-sectional chart has exactly one grain; a time chart has one
   geography per series; nothing is ever rolled up from a finer grain in the
   client; and a grain a measure does not publish is stated, not synthesised.

And one naming decision: `/builder` is the evidence packet composer and keeps
its name. The new page is `/workbench`, labelled **Workbench** in the site
navigation, so "build" keeps meaning "compose a document" and "workbench"
means "compose a chart".

## Context

Three surfaces already do most of what a workbench needs, and the plan reuses
them rather than duplicating:

- **The explorer** (`SourceExplorerPage.tsx`) resolves a source's access
  shape, builds observation requests bounded by declared filters, reports
  stratification instead of collapsing it, and draws one measure's history in
  `TimeSeriesChart.js` with time on the horizontal axis.
- **The comparison workspace** (`ComparisonWorkspace.tsx`, `lib/comparison.ts`)
  asks `/comparison/preflight`, presents the three-valued verdict without
  reinterpreting it, requests `/comparison` only for a comparable pair, offers
  the grains both sides publish (WEB-074), and draws the pair as
  `ScatterChart.tsx`, outlining non-contemporaneous points (WEB-049).
- **Saved analyses and packets** persist intent, not data, to the account when
  a token is held and to the browser otherwise, through one destination
  decision (`lib/savedAnalysis.saveDestination`).

What is missing is the combination: more than one measure on one time axis;
more than two measures at one grain; bar and heatmap presentations; a
correlation statistic; and a saved document that describes such a
composition. The API side is missing exactly one thing the client is not
allowed to compute — the correlation — and one thing it cannot do
efficiently — align more than two measures in one answer.

## Objective

A reader opens `/workbench`, adds measures from any completed source, pins
each to a geography or chooses one grain for all of them, picks a
presentation the selection can answer, sees the chart with every value's
provenance, asks for a correlation where the API will serve one, and saves or
shares the composition so it reopens the same way tomorrow against the live
publication.

## The grain contract

Every rule below is enforced in `lib/workbench.ts` from published evidence,
and a chart is offered only where these rules leave it answerable — the same
"a presentation is offered only where it can answer" invariant as
`lib/viewModes.ts`.

1. **Two chart families, two grain rules.**
   - *Longitudinal* (line, bar-over-time, geography × period heatmap): each
     series is one measure at **one geography**. Series may sit at different
     grains — US unemployment beside one county's population is a legitimate
     chart — and the legend names each series' grain and geography. The
     horizontal axis is time (WEB-042 rule; `TimeSeriesChart` is the
     precedent).
   - *Cross-sectional* (scatter, bar ranking, correlation heatmap): every
     measure is read at **one shared grain**, one newest value per geography,
     and the horizontal axis or matrix key is the geography. The grain offer
     is the intersection of every selected measure's `valid_geo_grains`, in
     `GEO_GRAIN_ORDER`, with the same attribution to the publishers that
     WEB-074 gives; a measure declaring no grains does not narrow the offer,
     because unknown is not none.

2. **Nothing is rolled up.** The client never sums, averages or otherwise
   derives a coarser grain from finer rows. The compatibility policy says it
   in the API's words — "derived values must not be summed across
   geographies" — and a county-only measure asked for at STATE is answered
   "not published at STATE by <source>", never synthesised. This holds even
   where the measure's aggregation characteristic would make a sum
   arithmetically valid, because the workbench draws published values.

3. **A national series is a reference line, not a point.** On a
   cross-sectional chart at STATE or COUNTY, a measure whose only grain is
   NATIONAL (every FRED series) may be added as a horizontal reference line,
   labelled with its own period and unit, and only where its unit matches
   the axis it is drawn on. It is never plotted as one of the geographies and
   never enters a correlation.

4. **Stratified and agency-grain sources join time charts only with their
   dimensions pinned.** CDC, USDA NASS and FBI UCR are declined by the analysis
   routes for a reason the API states; the workbench inherits the refusal for
   cross-sectional charts and correlation, presenting the API's own
   `analysis_refusal` text. On a longitudinal chart such a measure joins as a
   series only once every declared dimension (`stratum_id`, `domain_desc`,
   `subject_code`, and so on, read from `observation_filters`) is pinned to
   one value, so the series is one published line; an unpinned answer is
   reported as N series and left unplotted, as WEB-014 does.

5. **Units decide axes.** A longitudinal chart gets one vertical axis per
   distinct published unit, at most two (left and right, each labelled with
   its unit and the measures on it). A third unit turns the chart into small
   multiples rather than a third axis. A measure publishing no unit (Census
   ACS) is drawn on its own axis labelled "unit not published". Nothing is
   normalised, indexed or rescaled to share an axis.

6. **Periods are shown, not aligned.** The analysis routes reduce each side
   to its own newest value and say so on every row. The workbench keeps that:
   scatter points and correlation pairs whose periods differ are outlined and
   counted (WEB-049), a cross-sectional bar carries its period in the tooltip
   and export, and the correlation answer (WB-3) reports how many of its
   pairs were contemporaneous. An optional `year` on the correlation route
   lets a reader ask for a same-year answer explicitly, at the cost of
   coverage the answer then reports.

7. **A grain the boundary cannot draw still charts.** PLACE and AGENCY have
   no tile geometry (`lib/tileGrains.ts`) and no map; they are ordinary on a
   scatter, a bar or a heatmap. The workbench has no map at all — the explorer
   and comparison workspace own maps, and a link from a workbench series to
   the explorer view of the same measure is the path to one.

## Design decisions

- **Hand-rolled SVG, no charting dependency.** `ScatterChart.tsx` and
  `TimeSeriesChart.js` are inline SVG with `role="img"`, an accessible label
  that states what is and is not plotted, data attributes for tests, and
  data-driven `style` attributes only where the CSP allows them. Line, bar
  and heatmap follow the same pattern. A charting library would add to the
  bundle budget (`check:bundle`), bring inline-style behaviour the CSP does
  not admit, and hide the honesty rules (no zero for an unpublished value, a
  gap where the data has one) inside a dependency. Revisit only if a phase
  needs an interaction the SVG pattern cannot give.
- **Correlation lives on the comparison resource.** `GET
  /api/v1/comparison/correlation` takes exactly the parameters `/comparison`
  takes, runs the same preflight and the same per-side reduction, and returns
  the statistic over the same inner join, so a reader who has a comparison
  on screen can ask for its coefficient and be answered about the same rows.
  Pearson and Spearman are both returned: Pearson because it is what readers
  expect, Spearman because published economic measures are routinely skewed
  and a rank correlation is the honest companion. Both are `derived: true`.
- **An N-measure alignment is a new resource, not a widening of
  `/comparison`.** `/comparison` is a pair by contract, in its schema, its
  preflight and its saved kind. `GET /api/v1/comparison/matrix` (WB-4) takes
  `metric_codes` (2–8), runs preflight pairwise, and returns the pairwise
  verdicts, the pairwise correlations for comparable pairs, and the aligned
  wide rows. Incomparable pairs are reported in the matrix as declined with
  their failed rules, not as an error for the whole request — a 6×6 matrix
  with two declined cells is still an answer.
- **A saved workbench is a new configuration kind.** `ConfigurationKind`
  gains `"workbench"`, whose document is a list of `series` (each the same
  fields an `observations` document carries) plus a `presentation` block the
  API validates only for shape, and an optional `alignment` block naming the
  shared grain for a cross-sectional presentation. Each series is validated
  exactly as an `observations` document is today, so a stored series cannot
  encode a request the route refuses (API-082 line). This is an amendment to
  ADR-0003 and is recorded there.
- **The URL carries the composition.** Series, geography pins, grain,
  presentation and the correlation toggle serialise through `lib/urlState.ts`
  so a shared link reopens the same chart. Never a value, never a
  configuration id, never a token — the privacy boundary in
  `WEB_FIRST_WAVE_HANDOFF.md` holds. The URL has a stated ceiling (eight
  series); beyond it the share control explains why the link cannot be made
  and points at saving.

## Phases

| Phase | Layer | Delivers | Needs |
| --- | --- | --- | --- |
| WB-1 | Web | Workbench shell, measure picker, longitudinal line and bar charts, per-series geography pin, unit-driven axes, URL state | — |
| WB-2 | Web | Cross-sectional scatter and bar ranking for a comparable pair over `/comparison`; geography × period heatmap for one measure over `/observations` | WB-1 |
| WB-3 | API | `GET /comparison/correlation` with Pearson, Spearman, pairing coverage, contemporaneity count and caveats; consumer guide section | — |
| WB-4 | API | `GET /comparison/matrix` for 2–8 measures: pairwise verdicts, pairwise correlations, aligned wide rows | WB-3 |
| WB-5 | Web | Correlation panel and correlation heatmap over WB-3/WB-4; N-measure scatter-matrix selection | WB-2, WB-4 |
| WB-6 | API + Web | Saved `workbench` kind (ADR-0003 amendment), account/browser save, load, conflict handling; workbench charts as evidence-packet block sources | WB-5 |
| WB-7 | Web + Docs | CSV export with envelope, accessibility pass, browser tier, bundle budget, `TESTING_CONTRACT.md` rows, handoff update | WB-6 |

### WB-1 — Shell, picker, longitudinal charts

Acceptance criteria:

1. `/workbench` renders under the site header with a **Workbench** navigation
   entry between Compare and Profiles, and the page is reachable signed out.
2. The measure picker is capability-driven: sources come from
   `lib/explorerSources.ts`, measures from `/catalog/metrics` paged
   deterministically as the catalog page does (WEB-015), and a source with no
   declared observation route is absent with its reason.
3. A series is `{ sourceCode, metricCode, scope, release?, geoLevel, geoId,
   filters }`. The geography pin uses `lib/geographyPicker.ts`; a filter is
   sent only where the source declares it and is never dropped where it does
   (the WEB-014 rule). A stratified source's series is added only once every
   declared dimension is pinned to one value; otherwise the control names
   the unpinned dimension and refuses to add the series.
4. Requests go through `lib/observationAccess.ts` with
   `newest_release_per_period=true` on the neutral resource, so a series is a
   settled history (the `explorer-settled-history` rule), and are bounded by
   `fetchCollectionPages`; a series whose history was truncated at the page
   bound says so beside its legend entry.
5. `LineChart.tsx` and `BarChart.tsx` draw up to eight series with time on
   the horizontal axis positioned by date, one vertical axis per unit (at most
   two; a third unit switches the presentation to small multiples, stated on
   screen), no coercion of an unpublished value, a visible gap where a period
   is absent, and a legend naming each series' source, measure, grain,
   geography, unit and scope/release. Each chart has `role="img"` and an
   accessible label that says how many series and how many dropped periods
   it carries; a data table below the chart lists every plotted value.
6. Line and bar are offered only where every series has at least two
   periods (line) or at least one (bar); an unanswerable presentation is
   listed with its reason, and the presentation the reader chose is kept
   across a transient unavailability (WEB-017 pattern).
7. The composition round-trips through the URL: parse drops an invalid
   parameter rather than sending it, serialize omits defaults, and a link
   copied from the page reopens the same series, pins and presentation.
8. Unit tests cover the series model, the axis assignment, the gap rendering,
   the picker's refusal of an unpinned stratified series, and the URL
   round-trip. A browser test adds two series from two sources at two grains
   and reads the legend and table.

### WB-2 — Cross-sectional pair and the matrix heatmap

Acceptance criteria:

1. Switching to a cross-sectional presentation offers the grains in the
   intersection of the selected measures' `valid_geo_grains`, in the
   published order, and says which publisher removed each absent grain.
2. With exactly two measures selected, the workbench asks
   `/comparison/preflight`, presents the verdict through `describePreflight`
   unchanged, and requests `/comparison` only where `mayRequestComparison`
   says so — the same code path as the comparison workspace, not a copy.
   The scatter is `ScatterChart.tsx` reused; the bar ranking is `BarChart`
   over geographies, sorted by the chosen side, carrying both periods.
3. Reading a full county grain pages `/comparison` to its declared limit
   (1000 rows per page) with the bounded reader, and reports
   `geographies_a`/`geographies_b` against `total` so a partial pairing is
   visible (the `comparison-coverage-surfaced` rule).
4. With more than two measures selected, the cross-sectional presentations
   are listed as unavailable with the reason "an aligned answer for more than
   two measures needs the matrix route" until WB-5 lands; the plan is
   explicit that this interim state ships.
5. The geography × period heatmap takes one measure at one grain, optionally
   narrowed by state, reads its settled history through `/observations`, and
   draws geographies down and periods across with a sequential colour scale
   whose legend is `ChoroplethLegend.tsx` reused, a distinct "not published"
   cell colour that is never a colour on the scale, and a tooltip per cell
   naming geography, period, value, unit and release. It is capped at 60
   geographies × 60 periods and says so when a state or a year range would
   be needed to fit.
6. A national reference line is offered on a cross-sectional bar only for a
   NATIONAL-only measure whose unit equals the bar's unit, labelled with its
   period, and is never sent to `/comparison`.
7. Unit tests cover the grain intersection, the interim refusal for three or
   more measures, the heatmap cell model including the not-published state,
   and the reference-line eligibility rule. A browser test draws a pair at
   STATE and the heatmap for one measure.

### WB-3 — `GET /api/v1/comparison/correlation`

Contract:

```text
GET /api/v1/comparison/correlation
    ?metric_code_a=…&metric_code_b=…
    [&geo_level=…][&state_fips=…][&year=YYYY]
```

Response:

```json
{
  "derived": true,
  "metric_code_a": "…", "metric_code_b": "…",
  "geo_level": "COUNTY", "state_fips": null, "year": null,
  "n": 3016,
  "geographies_a": 3143, "geographies_b": 3016,
  "contemporaneous_pairs": 3016,
  "pearson_r": 0.412, "spearman_rho": 0.377,
  "period_a": "2023", "period_b": null, "periods_differ": true,
  "derivations": ["pearson_r", "spearman_rho"],
  "caveats": ["…association, not causation…", "…margin of error…"]
}
```

Acceptance criteria:

1. The route runs `evaluate_comparison` first and answers `422` with the
   failed rules for an incomparable pair, `404` for an unknown metric, and
   `422` with the source's `analysis_refusal` for CDC, USDA NASS and FBI UCR —
   the same three refusals `/comparison` gives, tested against the same
   fixtures.
2. The pairs are the rows `/comparison` would return under the same filters,
   unpaged: the same per-side newest-per-geography reduction, the same inner
   join, the same tie rule (`reduction-tie-determinism`). `n` is the count of
   pairs where both values are published numbers; a pair with a null,
   suppressed or non-numeric side is excluded and never zero.
3. `pearson_r` and `spearman_rho` are `null`, with a caveat, when `n < 3` or
   when either side is constant; they are never `NaN`, never `0` by
   default.
4. `year`, when given, reduces each side to its newest release for that year
   instead of its newest overall, so a same-year answer is available on
   request; `contemporaneous_pairs` counts pairs whose `period_a` and
   `period_b` are equal, and `period_a`/`period_b`/`periods_differ` follow
   the `/distribution/bins` convention (a single period when every pair
   shares one, `null` otherwise).
5. `caveats` always carries the association-not-causation sentence, the
   uncertainty caveat where a side publishes one (`uncertainty_caveat`), the
   preflight's `unknown` rules, a coverage caveat when `n` is below either
   side's geography count, and a contemporaneity caveat when
   `contemporaneous_pairs < n`.
6. The route is publicly cacheable under the same key discipline as
   `/comparison` and is declared under the analytical rate-limit class.
7. The consumer guide's Analysis section documents the route, its parameters,
   its nulls and its caveats. The saved-analysis validator accepts a
   `comparison` document unchanged (no new kind is needed for a pair).
8. `TESTING_CONTRACT.md` gains rows for: the refusal parity with
   `/comparison`; the reduction parity (a fixture where paging `/comparison`
   and reading the correlation see the same pairs); the `n < 3` and
   constant-side nulls; the `year` pin; and the caveat set.

### WB-4 — `GET /api/v1/comparison/matrix`

Contract:

```text
GET /api/v1/comparison/matrix
    ?metric_codes=a,b,c[,…]        (2–8 distinct codes)
    [&geo_level=…][&state_fips=…][&year=YYYY]
    [&limit=…][&offset=…]          (paging the wide rows only)
```

Response: `metrics` (each code's summary and its declared grains),
`pairs` (every unordered pair: `comparable`, its rules and caveats, and, when
comparable, the same statistic block WB-3 returns), and `items` — wide rows
keyed by `geo_id` and `geo_level` with, per metric, `value`, `period` and
`release`, `null` where a geography has no published value for that metric,
paged over the union of geographies with a declared total order.

Acceptance criteria:

1. A pair declined by preflight is a declined cell with its failed rules;
   the request still answers `200` as long as at least one pair is
   comparable. All pairs declined answers `422` naming every failure.
2. A metric belonging to an analysis-refused source, or unknown, answers
   `422`/`404` for the whole request, because a matrix with a hole for a
   source the API has declined is not the honest shape — the refusal is
   about the source, not the pair.
3. Every derived statistic is `derived: true`; every published value on a
   wide row carries its own period and release.
4. `items` pages over a declared total order (`geo_level, geo_id`) so the
   `every-paged-read-declares-its-order` rule holds; the statistics are
   computed over the whole join, not the page.
5. The consumer guide documents the route, the 2–8 bound and the cell
   semantics. Catalog rows in `TESTING_CONTRACT.md` cover the partial-decline
   answer, the whole-request refusals, and paging determinism.

### WB-5 — Correlation on screen

Acceptance criteria:

1. A "Check correlation" control appears only where the current selection is
   a comparable pair at a cross-sectional grain (WB-2) and, after WB-4, for
   any 2–8 measures whose sources the analysis routes serve. It is absent,
   with the API's reason, for stratified sources and for a longitudinal
   composition — a correlation between two histories of one geography is a
   different statistic this plan does not offer, and the control says so
   rather than computing it.
2. The correlation panel shows `n`, both coefficients, coverage against each
   side's geography count, the contemporaneous count, and every caveat, with
   the association-not-causation sentence first and not collapsible. A null
   coefficient shows its reason. The coefficients are labelled **API-derived**
   wherever they appear, including the export.
3. The correlation heatmap (`HeatmapChart.tsx` reused with a diverging
   scale) draws the WB-4 pairwise matrix with a declined cell rendered in the
   not-published colour and its failed rule in the tooltip; the legend is
   `ChoroplethLegend.tsx`. A three-or-more-measure scatter is offered as a
   pair chooser over the matrix rather than a scatter-matrix grid, keeping
   one chart on screen.
4. The `year` pin is a control beside the grain control, off by default,
   and its effect on coverage is shown from the answer, not predicted.
5. Unit tests cover the control's eligibility, the null presentations, the
   derived labelling, and the heatmap's declined cell. A browser test asks
   for a correlation on the WB-2 pair and reads the caveats.

### WB-6 — Saving a workbench

Acceptance criteria:

1. `AnalysisDocument` gains `kind: "workbench"` with `series: list[SeriesDocument]`
   (1–8; each carries the fields an `observations` document carries today,
   validated by the same code path), `presentation: {type, options}`
   (validated for shape and a closed `type` vocabulary; `options` opaque), and
   `alignment: {geo_level, state_fips?, year?} | null`. A document naming a
   grain no series publishes, or a filter its route refuses, is rejected on
   write with the same explanation the live route gives
   (`storage-is-not-a-back-door-for-a-refused-value`).
2. ADR-0003 is amended with the new kind and the reason the series are
   validated individually; the consumer guide's saved-analysis section lists
   the kind.
3. The workbench saves through `lib/savedAnalysis.saveDestination`, states
   the destination on the control and on the outcome, reports a refused
   account save without redirecting it, and loads a saved workbench from
   `/saved` with its `validation` shown unrepaired when stale.
4. A saved workbench chart appears in the evidence packet composer's chart
   source (`saved-charts:v1` for the browser store; the account library for
   the API store), carrying an envelope per series so the packet's
   completeness rule sees every series' source, measure, grain, geography,
   period, release and caveats.
5. Tests: API validation success and each refusal; web save/load round-trip
   against a fixture; a stale document shown unrepaired.

### WB-7 — Export, accessibility, gates, documentation

Acceptance criteria:

1. CSV export carries one row per plotted value with the full envelope
   columns `lib/observationExport.ts` writes, plus a `derived` column that is
   `true` only for correlation statistics; a partial read is stated in the
   export as it is on screen (`an-exported-prefix-says-so`).
2. Every chart passes the browser tier's accessibility checks: `role="img"`
   with a complete label, keyboard-reachable tooltips or an equivalent table,
   and colour never the only carrier of a distinction (outlined points,
   hatched or labelled not-published cells).
3. `npm run check:bundle` passes without raising the budget; if the three
   new charts cannot fit, the plan records the measured cost and the
   decision, and does not silently update the budget.
4. `TESTING_CONTRACT.md` gains one frontend row per WB-1, WB-2, WB-5 and WB-6
   behaviour listed above, and `CI_EVIDENCE_MAP.md` names the jobs that run
   them. `WEB_FIRST_WAVE_HANDOFF.md` lists `lib/workbench.ts` and the three
   charts among the reusable modules.
5. The README's route list names the two new API routes and the Workbench
   page.

## Non-goals

- A map in the workbench. The explorer and comparison workspace own maps;
  the workbench links to them.
- Regression lines, trend fits, seasonal adjustment, indexing to a base
  period, per-capita derivation, or any client-side transform of a published
  value. A per-capita measure is a warehouse or publisher product, not a
  chart option.
- Correlation between two time series of one geography (autocorrelation,
  lagged or otherwise). Named as absent on screen; a later plan may add it
  as an API-derived analysis with its own caveats.
- Any roll-up of finer-grain rows to a coarser grain, in the API or the
  client.
- Publishing or sharing a saved workbench beyond the existing link and
  packet mechanisms.

## Validation

Record commands and outcomes here as phases land.

```text
./tests/run.ps1 api          # WB-3, WB-4, WB-6 API behaviour
./tests/run.ps1 web-unit     # WB-1, WB-2, WB-5, WB-6 view models
./tests/run.ps1 web-browser  # WB-1, WB-2, WB-5, WB-7 flows
./tests/run.ps1 web-build    # lint, typecheck, build, bundle budget, CSP
```

Integration checks that need a warehouse (the reduction-parity fixture in
WB-3) run under `./tests/run.ps1 integration`; if that environment is
unavailable, record the exact command and keep the plan in `in_progress/`.

## Assessment at claim (2026-09-13)

The plan was read against the surfaces it names before any work began. It
holds, with these confirmations and one correction:

- **Confirmed.** `apps/api/routers/comparison.py` and
  `apps/api/services/comparison_service.py` already expose `ranked_latest_cte`
  as a shared reduction — `distribution_service` imports it — so WB-3 can
  reuse the same per-side newest-per-geography ranking rather than restate it,
  which is what "the pairs are the rows `/comparison` would return" requires.
- **Confirmed.** `evaluate_comparison` in `apps/api/services/compatibility.py`
  returns the whole three-valued verdict including the `source_analysis_ready`
  rule that carries each refused source's `analysis_refusal()`, so WB-3's
  refusal parity with `/comparison` is a matter of calling the same function,
  not of restating three source names.
- **Confirmed.** `uncertainty_caveat` is already shared by `/comparison` and
  `/distribution/bins`; WB-3's caveat set extends it rather than forking it.
- **Correction to WB-3 criterion 6.** The plan asks the route to be "declared
  under the analytical rate-limit class" and "publicly cacheable under the
  same key discipline as `/comparison`". Both are already true by
  construction and neither is a declaration to add: `main.CACHEABLE_ROUTERS`
  derives `PUBLIC_CACHE_TARGETS` from the routers' own paths (API-076), and
  `RateLimitMiddleware._classify` puts everything that is not a catalog path
  in the `analysis` bucket. The criterion is therefore discharged by a test
  asserting both, not by an edit to a list. Recorded here so a reviewer does
  not look for a list entry that should not exist.
- **Confirmed.** The three grain-vocabulary helpers the plan leans on
  (`normalize_geo_level`, `_filter_conditions`'s normalisation, and
  `GEO_GRAIN_ORDER` on the web side) exist and are single-sourced, so the
  grain contract's rule 1 intersection can be computed from published
  evidence rather than from a second copy of the vocabulary.

## Evidence record

### WB-3 — `GET /api/v1/comparison/correlation`

Status: **complete**, 2026-09-13.

Implementation:

- `apps/api/services/compatibility.py` — `CORRELATION_DERIVATIONS` and
  `CORRELATION_CAUSATION_CAVEAT`. The caveat sentence is written once because
  the route, the consumer guide and (at WB-5) the web panel all present it,
  and three wordings of one product rule read as three rules.
- `apps/api/schemas/analysis.py` — `ComparisonCorrelationResponse`.
- `apps/api/services/comparison_service.py` — `metric_correlation`,
  `_year_pin_condition`, `_correlation_caveats`, `MINIMUM_CORRELATION_PAIRS`.
- `apps/api/routers/comparison.py` — the route, with `/comparison`'s three
  exception translations unchanged.
- `tests/unit/api/test_comparison_correlation.py` — 21 tests.
- `docs/reference/TESTING_CONTRACT.md` — API-130, API-131, and the register
  totals beside them (432 → 434); `tests/support/catalog_evidence.py`'s
  audited API count 129 → 131.
- `docs/reference/API_CONSUMER_GUIDE.md` — the route's Analysis section
  paragraphs; "both comparison routes" → "all three" in the caching bullet.
- `README.md` — the route in the API surface list.
- `tests/fixtures/api/openapi_contract.json` — regenerated; the diff is
  purely additive (one operation, one schema), no existing entry moved.

Decisions taken while implementing, beyond what the plan wrote:

1. **Spearman is Pearson over average ranks, computed in SQL.** The average
   rank is `RANK()` plus half its tie group's excess. `RANK()` alone is the
   minimum rank, which deflates the coefficient wherever a measure ties — and
   published measures tie constantly (a rate rounded to one decimal, a count
   of zero in a small county). Computing it in the same statement as
   everything else is what makes API-084's one-snapshot rule hold for it.
2. **The whole answer is one statement.** The pair count, both geography
   counts, the contemporaneity count, the two distinct-value counts that
   decide whether a coefficient exists, and both coefficients come from one
   evaluation of the two reductions. Measuring them separately would let a
   serving refresh land between them and leave the coefficient describing
   rows the counts beside it no longer measure — API-084 and API-087, whose
   recorded defects are exactly this.
3. **The null decision is taken from the counts, not inherited from `corr`.**
   Postgres already answers null for a constant side, but the answer and the
   caveat explaining it must be made by one rule, and `corr` would happily
   answer ±1 over two points. `MINIMUM_CORRELATION_PAIRS = 3` is enforced
   here.
4. **The `year` pin constrains the period expression the reduction ranks on**
   (`SUBSTRING(<period_start_expression> FROM 1 FOR 4)`), not a per-source
   declared `year` filter. The analysis-ready sources do not all declare one —
   `year_from`/`year_to` belong to the union family — so reading it from
   `filter_conditions` would serve a same-year correlation for some sources
   and refuse it for others with no difference a caller could see. And the
   pin means "rank within this year", which is a statement about the
   reduction; writing it against the ranking expression makes those one
   sentence. Recorded because it is the one place this phase touches SQL the
   plan did not specify.
5. **`source_code_*` and `units_*` are on the response** though the plan's
   example JSON omitted them. Every other analysis response carries its
   inputs' identity, a coefficient is unit-free while the measures behind it
   are not, and the alternative is a client re-fetching the catalog to label
   its own chart.
6. **Criterion 6 needed no edit, only evidence** — see the assessment above.
   `test_the_route_is_a_public_cache_target_and_costs_analysis_budget`
   asserts both against the derived target set and the limiter's classifier.
7. **Reduction parity is proved at the unit tier, not only at integration.**
   The plan put the parity fixture behind a warehouse. A stronger and cheaper
   assertion was available: render `ranked_latest_cte` from the same dispatch
   entry and conditions both routes build from, and assert the text appears
   in *both* routes' statements. Two reductions that rank alike but break
   ties differently answer different published rows (API-083), and equal text
   makes that a failing test rather than a discrepancy someone notices in two
   answers. The warehouse fixture is still worth having and is recorded as
   not run below.

Validation run:

```text
python -m pytest tests/unit -q          # 1671 passed
python -m pytest tests/unit/api/test_comparison_correlation.py -q   # 21 passed
ruff check .                            # All checks passed
python -m tests.support.regenerate_openapi_contract   # 40 operations, 58 schemas
```

Not run, with the reason:

- `./tests/run.ps1 integration` (the warehouse reduction-parity fixture, and
  every other integration node). No PostgreSQL/PostGIS is reachable from this
  environment. The unit-tier parity assertion above covers the same property
  at the level of the emitted statement; what remains unproven is that the
  two statements return identical rows from a real relation.
- `python -m pytest tests/dags -q` — Airflow is not installed, and installing
  `.[airflow-dev]` pins SQLAlchemy 1.4 against the API's 2.x, which is why CI
  runs those tiers in separate jobs. Untouched by this phase.

### WB-4 — `GET /api/v1/comparison/matrix`

Status: **complete**, 2026-09-13.

Implementation:

- `apps/api/services/comparison_matrix_service.py` (new) — `metric_matrix`,
  `parse_metric_codes`, and the four SQL builders the statements are composed
  from.
- `apps/api/schemas/analysis.py` — `ComparisonMatrixResponse`,
  `MatrixMetricSummary`, `MatrixPair`, `MatrixRow`, `MatrixCell`, and
  `CorrelationStatistic`, which is the per-cell shape WB-5 reads.
- `apps/api/services/comparison_service.py` — `ranked_latest_cte` gains
  `include_release`; `_correlation_caveats` becomes public
  `correlation_caveats` with `include_causation_lead`.
- `apps/api/routers/comparison.py` — the route.
- `tests/unit/api/test_comparison_matrix.py` — 16 tests.
- `docs/reference/TESTING_CONTRACT.md` — API-132, API-133, totals 434 → 436;
  `tests/support/catalog_evidence.py` audited API count 131 → 133.
- `docs/reference/API_CONSUMER_GUIDE.md` — the route's prose, its row in the
  paging-order table (the gate `test_every_paged_read_declares_what_orders_it`
  demanded it, which is criterion 4 enforcing itself), "all three" → "all
  four" comparison routes.
- `README.md`, `tests/fixtures/api/openapi_contract.json` (additive again:
  one operation, six schemas).

Decisions taken while implementing, beyond what the plan wrote:

1. **The wide rows are a union built from a `keys` CTE, keyed on `geo_id`
   alone.** Keying on `(geo_id, geo_level)` would turn a single disagreement
   about a grain word into two half-empty rows for one geography; the grain is
   coalesced across the sides instead, which answers the word every side that
   published the geography agrees on. Each side then `LEFT JOIN`s onto the
   keys, which is obviously correct in a way a chain of `FULL OUTER JOIN`s
   with coalesced keys is not.
2. **Spearman is ranked inside each pair's own non-null subset.** Ranking once
   over the wide relation would rank every geography including those the other
   measure did not publish — a different statistic wearing the same name. Each
   comparable pair therefore gets three generated CTEs (`_rows`, `_ranked`,
   the aggregate), evaluated once each.
3. **Every number is one labelled union in one statement.** A `kind` column
   (`total` / `metric` / `pair`) rather than a column per pair, because the
   result shape must not grow quadratic in the measure count: eight measures
   is twenty-eight pairs. The page of wide rows is the second statement, which
   is the shape `/comparison` already has.
4. **A repeated code is refused, not de-duplicated** — de-duplicating answers
   a two-measure matrix for a three-code request, and a measure against itself
   correlates 1 with no information in it.
5. **The requested order is kept, not sorted.** It is the order the reader's
   own legend will be in, and sorting would make `metrics` disagree with the
   request that produced it for no gain.
6. **`ranked_latest_cte`'s default rendering is unchanged character for
   character** under the new `include_release` flag, because API-130 asserts
   the comparison and distribution reduction *is that text*. A reduction that
   differs by a column is still a different reduction to explain.
7. **The causation sentence is response-level, not per cell.** Repeating a
   40-word rule in each of up to twenty-eight cells makes it scenery;
   `correlation_caveats(include_causation_lead=False)` builds the cell's own
   caveats and the response carries the sentence once, first.

Validation run:

```text
python -m pytest tests/unit -q                                   # 1687 passed
python -m pytest tests/unit/api/test_comparison_matrix.py -q      # 16 passed
ruff check .                                                      # passed
python -m tests.support.regenerate_openapi_contract    # 41 operations, 64 schemas
```

The generated SQL was additionally parse-checked against the PostgreSQL
grammar (`sqlglot.parse_one(..., dialect="postgres")`) for the matrix's two
statements and the correlation's one, with bind parameters substituted. All
three parse. `sqlglot` was installed in this environment for the check only
and is deliberately **not** added to the project's dependencies: the
repository already proves SQL acceptance where it matters, against a real
warehouse, in `tests/integration/api/test_dispatch_expressions_execute.py`.
A parser agreeing is weaker evidence than PostgreSQL agreeing; it is recorded
because it is the strongest check this environment can run.

Not run, with the reason: the integration tier, as for WB-3 — no PostgreSQL is
reachable here, so `corr`, `RANK()`'s tie arithmetic, and `width_bucket`-style
planner behaviour over the generated statements are unproven against a real
database. `./tests/run.ps1 integration` is the command.

### WB-1 — Shell, picker, longitudinal charts

Status: **complete**, 2026-09-13.

Implementation:

- `apps/web/lib/workbench.ts` (new) — the whole rule set as pure functions:
  `seriesKey`/`sameSeries`, `unpinnedDimensions`, `admitSeries`,
  `assignValueAxes`, `presentationOffer`, `buildPlottedSeries`,
  `describeSeries`, `describeChart`, and the presentation vocabulary.
- `apps/web/lib/urlState.ts` — `parseWorkbenchState`,
  `serializeWorkbenchState`, `workbenchHref`, `workbenchLinkCeiling`.
- `apps/web/components/LineChart.tsx`, `BarChart.tsx` (new), inline SVG with
  `role="img"`, a complete accessible label, and `data-*` hooks for tests.
- `apps/web/components/WorkbenchPage.tsx`, `apps/web/app/workbench/page.js`
  (new); `SiteHeader.js` gains the **Workbench** entry between Compare and
  Profiles.
- `apps/web/app/styles/profiles.css` — `.chart-legend`, shared by both charts
  and the series list so a swatch means one thing in all three.
- `apps/web/scripts/bundle-budgets.json` — one entry, for the new route.
- Tests: `tests/frontend/unit/workbench.test.js` (23),
  `workbench-charts.test.jsx` (11), nine cases appended to
  `url-state.test.js`, and `tests/frontend/browser/workbench.spec.js` (4).
- `docs/reference/TESTING_CONTRACT.md` — WEB-082…WEB-086 and the register
  totals (436 → 441); `tests/support/catalog_evidence.py` WEB count 81 → 86.

Two defects the tests caught, both worth recording:

1. **A hand-edited link could smuggle a request parameter in as a dimension
   pin.** `isCarriableDimension` screens against the *explorer's* reserved
   names, which are that page's short spellings (`metric`, `geo`, `state`),
   not the API's. So `s=...;metric_code:OTHER` passed the screen and would
   have been sent as a dimension filter — refused by the resource as
   undeclared, or overriding the measure the rest of the link named.
   `SERIES_RESERVED_KEYS` now covers the API's own parameter names.
2. **A literal `|` in a TESTING_CONTRACT cell silently split the row.**
   WEB-085's pass metric wrote an absolute value with pipes; the register
   built 440 rows against a declared 441 and the gate caught it. Written out
   in words instead.

Decisions taken while implementing, beyond what the plan wrote:

1. **A stratified series is refused at the picker, not reported unplotted.**
   The plan says the control "names the unpinned dimension and refuses to add
   the series", and this records why that differs from the explorer's
   WEB-014 behaviour: the explorer is reading one measure and must show what
   it got, while a composition can decline an ambiguous member outright.
2. **The link ceiling is asked before a link is made**, rather than a URL
   produced and hoped for. A link silently truncated by a chat client reopens
   as a different composition, which is worse than no link.
3. **Each named measure's catalog row is read individually.** The picker's
   own read covers only the selected source, so a restored composition would
   have had no unit for its other series — and a series with no unit lands on
   the "unit not published" axis, which would be this application inventing a
   fact about the publication.
4. **The reader's presentation choice survives a transient unavailability**
   (the WEB-017 pattern): `effectivePresentation` falls back only while the
   chosen one cannot answer, and `presentation` itself is untouched.
5. **`--update` was not used on the bundle budget.** It rewrites all fourteen
   existing budgets upward against the current build, which is the silent
   loosening the gate exists to prevent. One entry was added by hand, at the
   figure the tool's own formula gives. Measured: `/workbench/page` 411.5 kB
   against a 474 kB budget, and every other route unchanged.
6. **The cross-sectional and heatmap presentations ship listed-with-a-reason,
   as the plan says WB-2's interim state does.** They are not hidden; the
   reason names the phase.

Validation run:

```text
npm --prefix apps/web run test:unit     # 30 files, 449 passed
npm --prefix apps/web run lint          # passed
npm --prefix apps/web run typecheck     # passed
npm --prefix apps/web run build         # passed; /workbench 8.91 kB, 126 kB First Load JS
npm --prefix apps/web run check:bundle  # every route within its declared budget
npm --prefix apps/web run check:csp     # passed
npx playwright test                     # 99 passed (the whole browser tier)
python -m pytest tests/unit -q          # 1687 passed
ruff check .                            # passed
```

The browser tier ran here against the pre-installed Chromium
(`PLAYWRIGHT_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium-1194/chrome-linux/chrome`,
which `playwright.config.mjs` already reads); the four workbench specs and
the ninety-five that preceded them all pass.

### WB-2 — Cross-sectional pair and the matrix heatmap

Status: **complete**, 2026-09-14.

Implementation:

- `apps/web/lib/comparison.ts` — `sharedGrainOffer` (new, N-ary) with
  `comparisonGrainOffer` rewritten to delegate to it, keeping the pair's own
  wording. `AbsentGrain`/`SharedGrainOffer` types.
- `apps/web/lib/workbench.ts` — `crossSectionalRefusal`, `crossSectionalPair`,
  `referenceLineOffer`, `heatmapModel`, and the 60×60 caps.
- `apps/web/components/HeatmapChart.tsx` (new), reusing `ChoroplethLegend` and
  the choropleth palette and withheld colour.
- `apps/web/components/WorkbenchPage.tsx` — the shared-grain control, the
  state scope, the preflight, the bounded `/comparison` read, the scatter
  (`ScatterChart` reused), the ranking (`BarChart` with
  `orientation="geography"`), the reference-line list, and the heatmap.
- `apps/web/app/styles/profiles.css` — `.line-chart .map-legend` flows.
- Tests: `tests/frontend/unit/workbench-cross-section.test.js` (25) and four
  browser cases appended to `tests/frontend/browser/workbench.spec.js`.
- `docs/reference/TESTING_CONTRACT.md` — WEB-087…WEB-090, totals 441 → 445;
  `tests/support/catalog_evidence.py` WEB count 86 → 90.

Two defects the browser tier caught, neither visible at the unit tier:

1. **The colour legend rendered outside the flow.** `ChoroplethLegend` uses
   `.map-legend`, which is `position: absolute` because on a map it overlays
   the canvas. Inside a chart figure there is no canvas, so it was positioned
   against an unrelated ancestor and Playwright correctly reported it as not
   visible. `.line-chart .map-legend` now flows; the swatch, rows and type
   stay the choropleth's, so a colour still means the same on both surfaces.
2. **The compatibility verdict was only rendered while a cross-sectional
   presentation was on screen.** An incomparable pair makes those
   presentations unavailable, so the pair that most needed explaining showed
   no verdict at all — the reason was in the unavailable-presentations list
   and nowhere else. The verdict, the blocking rules and the alternatives now
   render whenever there is a pair.

Decisions taken while implementing, beyond what the plan wrote:

1. **`comparisonGrainOffer` was rewritten to delegate rather than the
   workbench getting its own intersection.** Criterion 1 asks for "the same
   attribution to the publishers that WEB-074 gives"; two implementations of
   that rule is how the two screens come to disagree about one publication.
   The workspace keeps its own sentences — a screen about a pair should not
   start talking about a set — and `comparison.test.js`'s 35 cases pass
   unchanged, which is the evidence the refactor is behaviour-preserving.
2. **`absent` names, per unoffered grain, the measures that removed it.** The
   plan asks the screen to say "which publisher removed each absent grain";
   the offer had no field for it, so one was added rather than the page
   recomputing it. A measure declaring no grains is never named, because
   unknown is not none.
3. **The preflight is asked whenever there is a pair**, not lazily when a
   cross-sectional presentation is selected — the verdict is what decides
   whether those presentations may be offered, so asking it lazily would
   leave the control enabled until a request came back and refused.
4. **The heatmap lays out the first selected series' measure.** The plan says
   "one measure at one grain"; the composition's own order picks which,
   rather than this page choosing for the reader, and the caption names it.
5. **Heatmap cells are keyed through a nested map, not a joined string.** A
   geography identity is `state:55|county:025` — it contains both of the
   separators a joined key would plausibly use.
6. **The reference lines are offered from the composition itself**, not from
   a search: a reference line is a measure already on the chart, drawn
   differently because the cross-sectional axis cannot hold it as a geography.
7. **The bundle budget did not move.** `/workbench/page` grew from 411.5 kB to
   440.3 kB against its declared 474 kB, so the three new charts fit inside
   the budget WB-1 declared. Recorded because WB-7 criterion 3 asks for the
   measurement either way.

Validation run:

```text
npm --prefix apps/web run test:unit     # 31 files, 474 passed
npm --prefix apps/web run lint          # passed
npm --prefix apps/web run typecheck     # passed
npm --prefix apps/web run build         # passed; /workbench 13.3 kB, 135 kB First Load JS
npm --prefix apps/web run check:bundle  # /workbench/page 440.3 kB / 474 kB; every route within budget
npm --prefix apps/web run check:csp     # passed
npx playwright test                     # 103 passed (the whole browser tier)
python -m pytest tests/unit -q          # 1687 passed
ruff check .                            # passed
```

Not run: the integration tier, as for WB-3 and WB-4 — no PostgreSQL is
reachable here. `./tests/run.ps1 integration` is the command.

### WB-5 — Correlation on screen

Status: **complete**, 2026-09-14.

Implementation:

- `apps/web/lib/api/types.ts` — `CorrelationStatistic`,
  `ComparisonCorrelation`, `ComparisonMatrix` and the matrix's own row, cell,
  pair and metric-summary shapes.
- `apps/web/lib/api/client.ts` — `getComparisonCorrelation`,
  `getComparisonMatrix`.
- `apps/web/lib/workbench.ts` — `correlationEligibility`,
  `CORRELATION_IS_ACROSS_GEOGRAPHIES`, `correlationReadings`,
  `formatCoefficient`, `correlationMatrixModel`, `selectablePairs`; the >2
  refusal became a pair chooser now that the matrix route exists, and
  `presentationOffer` gained a separate `correlationReason`.
- `apps/web/components/CorrelationPanel.tsx`,
  `CorrelationMatrixChart.tsx` (new).
- `apps/web/components/WorkbenchPage.tsx` — the coefficient control, the
  same-year pin, the pair chooser, both requests, and the panel and matrix.
- Tests: `tests/frontend/unit/workbench-correlation.test.js` (23), four
  browser cases, and one WB-1 case updated for the deliberate split.
- `docs/reference/TESTING_CONTRACT.md` — WEB-091…WEB-094, totals 445 → 449;
  `tests/support/catalog_evidence.py` WEB count 90 → 94.

**A correction to criterion 1, and the reason for it.** The criterion says
the control "is absent, with the API's reason … for a longitudinal
composition". Read literally that makes the control unreachable: the
correlation *is* one of the presentations a reader selects, so it would be
refused while a line is on screen, and a line is on screen until it stops
being refused. The browser tier found it as a 30-second timeout waiting for a
button that could never appear.

The statistic the plan declines to offer is still not offered — and is now
*named* rather than silently absent. `CORRELATION_IS_ACROSS_GEOGRAPHIES`
rides the panel and tells a reader arriving from a line chart that this
coefficient is measured across geographies at the shared grain, one newest
value per geography, and is not a correlation of the two histories they were
just looking at, a shared time trend being able to produce a coefficient on
its own. The eligibility check no longer reads the presentation at all.

Two smaller defects the browser tier caught, both test-side:

1. A `not.toContainText("—")` assertion about a blank coefficient matched the
   em dash inside the new scope sentence. Rewritten to assert about the two
   coefficient readings themselves.
2. Playwright resolves the most recently registered matching route first, so a
   blanket correlation stub registered after a year-aware one shadowed it and
   the year-pin test read the unpinned answer. Order corrected, with the rule
   written down beside it.

Other decisions beyond what the plan wrote:

1. **The `>2` refusal became a pair chooser.** WB-2 shipped
   "an aligned answer for more than two measures needs the matrix route" as a
   deliberate interim; with WB-4 landed, a scatter and a ranking are each
   about one pair, so the screen offers every unordered pair and draws one
   chart — criterion 3's "pair chooser over the matrix rather than a
   scatter-matrix grid". A choice naming a measure since removed is not
   honoured.
2. **The correlation's reason is separate from the scatter's and ranking's.**
   Those read `/comparison` for one chosen pair; the correlation reads
   `/comparison/matrix` for up to eight at once. One reason for both would
   have to be the stricter, which would withhold a correlation the API serves.
3. **The diagonal carries no coefficient and the scale is fixed to ±1.** A
   grid whose diagonal reads 1.000 invites calibrating every other cell
   against a number nothing measured; a scale rescaled per answer paints a
   matrix whose strongest pair is 0.3 exactly as one whose strongest is 0.95.
4. **The null-coefficient reason is lifted from the answer's caveats**, not
   composed here from `n` — that would be this screen re-deriving a rule the
   API owns and could change.
5. **The pinnable years come from the periods the measures published**, so
   the control cannot ask for a year no side has rows in, which would answer
   an empty correlation and report it as a coverage problem.
6. **The bundle budget still did not move.** `/workbench/page` is 454.4 kB
   against the 474 kB WB-1 declared.

Validation run:

```text
npm --prefix apps/web run test:unit     # 32 files, 498 passed
npm --prefix apps/web run lint          # passed
npm --prefix apps/web run typecheck     # passed
npm --prefix apps/web run build         # passed; /workbench 17.1 kB, 139 kB First Load JS
npm --prefix apps/web run check:bundle  # /workbench/page 454.4 kB / 474 kB; every route within budget
npm --prefix apps/web run check:csp     # passed
npx playwright test                     # 107 passed (the whole browser tier)
python -m pytest tests/unit -q          # 1687 passed
ruff check .                            # passed
```

Not run: the integration tier, as for the phases before it — no PostgreSQL is
reachable here. `./tests/run.ps1 integration` is the command.

### WB-6 — Saving a workbench

Status: **complete**, 2026-09-14.

Implementation:

- `apps/api/schemas/saved_analysis.py` — `SeriesDocument`,
  `PresentationDocument`, `AlignmentDocument`, `WorkbenchPresentationType`,
  and `AnalysisDocument`'s three new fields; `ConfigurationKind` gains
  `"workbench"`.
- `apps/api/registry.py` — the three per-kind tables gain a workbench row.
- `apps/api/services/saved_analysis_service.py` —
  `_require_consistent_observation_read` extracted so a series and an
  observations document run the same checks, and `_validate_workbench`.
- `docs/decisions/0003-…md` — the amendment.
- `docs/reference/API_CONSUMER_GUIDE.md` — the kind, its shape and its rules.
- `apps/web/lib/api/types.ts`, `lib/savedAnalysis.ts` —
  `workbenchDocument`, and `reopenHref`/`describeDocument` for the new kind.
- `apps/web/components/WorkbenchPage.tsx` — the save control and handler.
- Tests: 15 API cases in `test_saved_analysis.py`, 15 web cases in
  `workbench-saved.test.js`, 2 browser cases.
- `docs/reference/TESTING_CONTRACT.md` — API-134, API-135, WEB-095…WEB-097;
  totals 449 → 454; `catalog_evidence.py` API 133 → 135, WEB 94 → 97.

**The API-112 gate was extended, not exempted.** `test_the_fields_each_kind_carries_are_the_ones_its_route_declares`
reads the per-kind field table against the served contract, and a workbench's
three fields are containers rather than query parameters, so the literal check
failed. Weakening it to skip the kind would have left the new nesting
unguarded. Instead the check moved inside: every field a `SeriesDocument`
carries must be a parameter `/observations` declares, and for this kind what
the top level withholds must be *exactly* what a `SeriesDocument` carries —
so a field in neither place fails. Nothing was lost; it moved down a level,
and the gate now says so.

Decisions taken while implementing, beyond what the plan wrote:

1. **`_require_consistent_observation_read` was extracted rather than
   duplicated.** A series is an observations request, so the contradictions
   `/observations` refuses are checked by the code that checks them for an
   observations document. Two copies would drift the first time one moved,
   which is how API-091, API-117 and API-122 each arrived.
2. **Every series refusal names the series by position.** "series 2: release
   can only be combined with scope=as_released" is actionable; "a series" is
   not.
3. **A top-level `filters` is refused, not ignored.** A workbench's filters
   belong to its series; one at the top has nowhere to be replayed, which is
   API-112's defect one level up.
4. **A measure declaring no grains does not block an alignment.** Unknown is
   not none — the rule the composing screen applies, applied again at write
   because storage must not be a back door for a value the screen refused,
   nor stricter than it.
5. **The reopen link carries the measure's own prefix as the source key.** A
   stored series names its measure but not the route segment the workbench
   resolves an access shape from; the page re-derives the source from the
   capability list on open, which it already does, so a source that changed
   segments since the save still reopens.
6. **The browser store gets one envelope per series**, carrying each one's
   source, grain, unit, newest period, release, dropped-period count and
   truncation — so the evidence packet's completeness rule (criterion 4) sees
   every series rather than one envelope for a composition of eight.

**One thing to watch, recorded for WB-7.** `/workbench/page` is now 465.2 kB
against the 474 kB budget WB-1 declared — 8.8 kB of headroom. WB-7 adds the
CSV export; if it does not fit, WB-7 records the measured cost and the
decision rather than silently raising the budget, which is what its criterion
3 asks for.

Validation run:

```text
python -m pytest tests/unit -q              # 1702 passed
python -m pytest tests/unit/api/test_saved_analysis.py -q   # 95 passed
ruff check .                                # passed
python -m tests.support.regenerate_openapi_contract   # 41 operations, 67 schemas; diff purely additive
npm --prefix apps/web run test:unit         # 33 files, 513 passed
npm --prefix apps/web run lint              # passed
npm --prefix apps/web run typecheck         # passed
npm --prefix apps/web run build             # passed; /workbench 18 kB, 143 kB First Load JS
npm --prefix apps/web run check:bundle      # /workbench/page 465.2 kB / 474 kB
npm --prefix apps/web run check:csp         # passed
npx playwright test                         # 109 passed (the whole browser tier)
```

Not run: the integration tier. The real-schema saved-analysis contract
(`tests/integration/api/test_saved_analysis_contract.py`) is the tier that
would exercise a workbench document against the actual `app_api` JSONB
column; no PostgreSQL is reachable here. `./tests/run.ps1 integration` is the
command.

### WB-7 — Export, accessibility, gates, documentation

Status: **complete**, 2026-09-14.

Implementation:

- `apps/web/lib/observationExport.ts` — `workbenchExport`,
  `workbenchExportFilename`.
- `apps/web/components/WorkbenchPage.tsx` — the export control and its note.
- `docs/reference/CI_EVIDENCE_MAP.md` — one row naming every owning path and
  the three jobs that run them.
- `docs/reference/WEB_FIRST_WAVE_HANDOFF.md` — the surface is marked
  delivered and its reusable modules are listed.
- `README.md` — the analytical pages, including `/workbench` and why
  `/builder` keeps its name.
- Tests: `tests/frontend/unit/workbench-export.test.js` (13) and three
  browser cases.
- `docs/reference/TESTING_CONTRACT.md` — WEB-098, WEB-099; totals 454 → 456;
  `catalog_evidence.py` WEB 97 → 99.

**Criterion 3, measured.** `npm run check:bundle` passes **without raising the
budget**. `/workbench/page` finished at **469.4 kB against the 474 kB** WB-1
declared — 4.6 kB of headroom. The progression across the phases, for a later
reader deciding whether something else fits:

| After | Route bundle | Budget |
| --- | --- | --- |
| WB-1 (shell, line, bar) | 411.5 kB | 474 kB |
| WB-2 (+ scatter, ranking, heatmap) | 440.3 kB | 474 kB |
| WB-5 (+ correlation panel, matrix) | 454.4 kB | 474 kB |
| WB-6 (+ save) | 465.2 kB | 474 kB |
| WB-7 (+ export) | 469.4 kB | 474 kB |
| Post-review fixes (WEB-100, WEB-101) | 471.4 kB | 474 kB |

The three new charts fit, so the plan's contingency ("if the three new charts
cannot fit, the plan records the measured cost and the decision") did not
arise.

**The headroom is now 2.6 kB, and the next addition to this route needs a
budget decision.** That decision should be deliberate and recorded — not a
`--update` run, which rewrites *all* fourteen other routes' budgets upward
against the current build, as WB-1 recorded. The honest options when it comes
are: raise this one route's number with the measurement beside it, or split
the correlation surface out of the workbench route so the composing page does
not carry the matrix chart for readers who never ask for one.

## Post-review corrections (2026-09-14)

Found reviewing the delivered code rather than by a failing gate, and fixed
with the test that would have caught each:

1. **WEB-100 — a derived answer outlived the selection it described.** Every
   effect used `createRequestTracker`, which stops a stale *response* being
   committed; nothing stopped a stale *answer* being kept. The correlation
   effect returned early for an ineligible selection and left the previous
   answer in state — invisible while the presentation fell back, and rendered
   again unchanged the moment the selection became eligible, or while a
   narrowed read was in flight. A reader narrowing from every state to one saw
   the fifty-two-state coefficient under the loading note. The aligned rows had
   the same shape of defect. Both are now dropped before the effect decides
   whether to fetch. The browser test holds the second answer open, which is
   the only way to see this: with both answers instant, the stale one is
   replaced before anyone can read it and the defect stays in the code.

2. **API-136 — four API corrections, one of them serious.**

   - **The `year` pin meant two different things on the two sides.** It read
     the first four characters of `period_start_expression`, on the claim
     (written into its own docstring) that those are always the calendar year.
     For Census ACS they are not: an `acs5` row's `duration_start` is
     `estimate_year - 4`, because a five-year estimate covers a window. So
     `year=2023` pinned FRED to 2023 and ACS to the estimate whose window
     *opens* in 2023 — a vintage that does not exist. The join came back empty
     and the answer said "0 paired geographies is fewer than the 3 a
     correlation needs", presenting *there was not enough data* for *the pin
     meant two different things*; once a 2027 vintage lands it would have
     silently correlated the wrong one. The pin is now each entry's own
     declared `year_from`/`year_to`, so it asks a side exactly what
     `/observations?year_from=Y&year_to=Y` asks it, and the test reads all
     four analysis-ready entries rather than asserting about them.
   - **`/comparison/matrix` answered 500 where its siblings answer 422.** The
     glossary can publish a metric whose source has no reviewed dispatch entry
     — warehouse work lands before API registry work by design. `/comparison`
     and `/comparison/correlation` evaluate the pair first, so
     `compatibility._source_finding` composes a 422; the matrix resolved the
     dispatch first and `observation_dispatch` raised.
   - **A stored alignment's `state_fips` had a length bound and no shape
     rule.** `ZZ` is two characters; it stored clean, reported `valid: true`,
     and replayed as a 422 its owner never saw. The test asserts a
     *correspondence* with `closed_value_refusal` rather than a list of
     values, so the two cannot drift.
   - **The coverage caveat named one of two causes.** The gap between `n` and
     each side's published count is a geography one side does not publish *or*
     one where a side published a row without a number, and the sentence
     blamed only the first — sending a reader looking for a coverage
     difference when what they had was suppression.

   None of these were caught by the existing tests, because the unit tier
   drives a session double that never executes SQL and always resolves a
   metric to a known source. That is a real limit of the tier, recorded here:
   the SQL's *shape* is asserted, its *meaning against a real warehouse* is
   the integration tier's, and that tier does not run in this environment.

3. **WEB-101 — the heatmap laid out one geography.** It reused the loaded rows
   of the series it draws, and a series is one measure at *one geography* by
   definition, so its read pins `geo_id`. Laying those rows out as geographies
   × periods produces a grid exactly one row tall — which renders, carries a
   legend and a colour scale, and is a single line. `buildSettledSurfaceRequest`
   now reads every geography at the grain with no geography pinned, and the
   heatmap has its own optional state scope. The browser fixture had hidden
   it by answering a metric's whole set regardless of `geo_id`; the new test
   watches the requests the page actually makes, which is the only tier that
   can see it.

Decisions taken while implementing, beyond what the plan wrote:

1. **The export adds four columns, not one.** The plan asks for `derived`;
   a composition also needs `series` (eight measures' rows in one file must be
   separable), `geo_level` and the pinned `geo_id` (the rows come from several
   geographies at several grains, and a row's own attribution does not say
   which series it belongs to).
2. **The coefficients come last and carry no period or release.** A
   coefficient describes a set of pairs rather than a publication, and putting
   them last means a reader sorting by `derived` still has the published half
   of the file intact.
3. **The dimension columns are the union across the composition's sources.**
   WEB-061's rule for several sources: a file carrying a subset would be this
   client deciding which part of a source's published description a reader may
   have, and a composition has several descriptions in it.
4. **A composition is "partial" when any one series was truncated.** A file
   whose third series is a prefix is a partial file, whatever the other seven
   did.

Validation run:

```text
python -m pytest tests/unit -q              # 1702 passed
ruff check .                                # passed
npm --prefix apps/web run test:unit         # 34 files, 526 passed
npm --prefix apps/web run lint              # passed
npm --prefix apps/web run typecheck         # passed
npm --prefix apps/web run build             # passed
npm --prefix apps/web run check:bundle      # /workbench/page 469.4 kB / 474 kB; budget unchanged
npm --prefix apps/web run check:csp         # passed
npx playwright test                         # 112 passed (the whole browser tier)
```

## Plan close-out

All seven phases are complete. Delivered:

- **API**: `GET /comparison/correlation` (API-130, API-131),
  `GET /comparison/matrix` (API-132, API-133), and the saved `workbench` kind
  (API-134, API-135) with its ADR-0003 amendment.
- **Web**: `/workbench` with the measure picker, six presentations, the
  correlation panel and matrix, save to account or browser, a CSV export, and
  a shareable link — WEB-082 through WEB-099.

Two plan corrections are recorded above, in WB-3's and WB-5's entries: the
cacheability criterion needed a test rather than an edit, and the
correlation's "absent for a longitudinal composition" criterion would have
made the control unreachable and became a named statement on the panel
instead.

**The database integration tier was reached after all, and it found a gap.**

Earlier phases of this plan recorded the tier as unreachable. It was not: the
environment has PostgreSQL 16 and PostGIS could be installed, so the warehouse
bootstraps and the tier runs. Every entry above that says "not run: the
integration tier" is superseded by this:

```text
python -m pytest tests/integration -q -m "integration and database and not slow"
# 231 passed, 2 skipped, 1 failed
```

The one failure — `test_a_metric_carries_the_same_value_state_declaration_as_its_source`
— fails identically on this plan's base commit (`b4ea126`), verified in a
worktree. It asserts "the catalog published no metric", which is true of a
warehouse bootstrapped from DDL with no ingested content; it needs seeded
catalog data this environment has no source for. Not caused by, and not
touched by, this plan.

**What the tier caught that nothing else could.** `/comparison/correlation`
and `/comparison/matrix` both declare `geo_level`, and
`test_every_route_that_takes_a_grain_takes_the_same_grain_words` sweeps every
route that does, asserting each answers an alias exactly as it answers the
vocabulary word. Its own rule is that a route declaring `geo_level` with no
request in the sweep *fails* rather than skipping — so the two new routes
failed it. The guard was working; the gap was mine.

Closing it needed a real fixture change, because the matrix refuses a repeated
code (a measure against itself correlates 1 with no information), so it cannot
be swept with one measure against itself the way `/comparison` is, and no two
distinct fixture measures were comparable — PEP against FRED differs in units
and in time grain. `published_pep_metrics` is now parameterised on how many
measures to publish; two measures in one PEP dataset share their units, their
time grain and their geography grain, so the pair is comparable by the
policy's own rules rather than by a fixture asserting it.
`published_pep_metric` is a thin wrapper yielding the first, so every existing
caller reads unchanged.

The generated SQL was also executed directly against that PostgreSQL, not only
parse-checked: the correlation's statement returns a Pearson coefficient equal
to the definition computed independently in Python, ranks a monotone pair at
Spearman 1.0, excludes a pair whose side published no number while still
counting that geography in `geographies_a`, answers `null` rather than `0`
over an empty join, and honours the year pin on the right rows.

**Still not run:** `python -m pytest tests/dags -q`. Airflow is not installed,
and installing `.[airflow-dev]` pins SQLAlchemy 1.4 against the API's 2.x,
which is why CI runs those tiers in separate jobs. No phase touched the DAGs.

**Format drift, found and fixed 2026-09-14 after close-out.** Every
validation run above lists `ruff check .` and none lists
`ruff format --check .`, which the `lint` workflow runs as its own step
before the lint check. Eight files this plan wrote or touched --
`apps/api/schemas/saved_analysis.py`, the comparison, comparison-matrix and
saved-analysis services, and the four test modules for them -- carried
hand-wrapped lines the formatter would join. The gate would have failed on
the first push of this branch. `ruff format .` was applied to exactly those
eight files, with no change of behaviour, and the full run afterwards is:

```text
ruff format --check .                       # 463 files already formatted
ruff check .                                # All checks passed
python -m pytest tests/unit -q              # 1708 passed
python -m tests.support.catalog_evidence    # 459 rows, every one FULL
npm --prefix apps/web run test:unit         # 526 passed
npm --prefix apps/web run lint              # passed
npm --prefix apps/web run typecheck         # passed
npm --prefix apps/web run build             # passed
npm --prefix apps/web run check:bundle      # /workbench/page 471.4 kB / 474 kB
npm --prefix apps/web run check:csp         # passed
CI=1 npx playwright test                    # 114 passed, against the build
```
