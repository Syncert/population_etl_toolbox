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

- **Status:** In progress. Claimed 2026-09-13. Authored the same day from a
  product request and an inspection of the comparison workspace, the explorer,
  the analysis routes, and the saved-analysis contract.
- **Last updated:** 2026-09-13
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
- **Next pickup:** claim the plan, then start at WB-1. WB-1 and WB-3 are
  independent and may be worked in either order; WB-2 needs WB-1, WB-4 needs
  WB-3, WB-5 needs WB-2 and WB-4.

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
