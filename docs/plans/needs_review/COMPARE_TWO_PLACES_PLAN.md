---
id: compare-two-places
depends_on:
  - place-pages
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Compare two places

## Status

Ready for review, 2026-10-06, on branch `feat/compare-two-places`, stacked on
`feat/place-pages` (it reuses the chapter contract, address rules and trend),
so it merges after that one.

### Implementation evidence

- **Routes:** the path form, matching the place-page scheme:
  `/us/<state>/<county>/vs/us/<state>/<county>`, `/us/<state>/vs/us/<state>`,
  and the two mixed-grain forms, all rendered by
  `lib/comparePlacesRoute.js`. A segment outside the place vocabulary is a
  404; the title is built from the segments (`placeRouteTitle`). Pair pages
  are not listed in the sitemap: there is no finite set to list.
- **Verdicts:** `lib/placeComparison.ts` `compareMeasure` checks, in order,
  the preflight verdict (carried through unchanged, refused rules' reasons
  deduplicated), whether both places published a value, and whether they
  published it for the same newest period; anything else is listed under
  "Not comparable here" with the reason. The preflight is asked as the same
  measure on both sides (`metric_code_a = metric_code_b`), which is how the
  API's source-readiness, unit, grain and aggregation rules apply to a pair
  of places.
- **Rows and marks:** paired bars on one scale from zero, the parents' values
  for the same period as tick marks (state or states, and the nation), and
  the marks restated in text. No value is computed beyond formatting and bar
  length.
- **Trend:** each chapter's trend measure draws both places and the nation,
  through the place pages' `buildTrend` (counts indexed to a stated base).
- **Picker and swap:** "Compare ... with" searches the catalog's counties (or
  states) and navigates to the pair; "Swap sides" links the reversed pair.
  Neighbouring counties first waits for `nearby-and-related-places`.
- **Mixed grains:** a county against a state renders the explanation and
  offers the county's state against the other state.
- **Footnote:** no causal reading, no ranking, periods on every row, and a
  link to the comparison workspace.
- **Contract:** WEB-129, evidence-map row, budgets for the four routes.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: 51 files, 742 tests passed
  (`place-comparison.test.js`, 5).
- `lint`, `typecheck`, `build`, `check:csp`, `check:bundle`: passed.
- `npx playwright test`: 211 passed, including `compare-places.spec.js`
  (desktop and 390px with axe, a preflight refusal, a missing value, swap,
  mixed grains).

### Open items, decided

- Path segments, not a query parameter: shareable and consistent with the
  place pages.
- State pairs ship with county pairs, through the same route family; nation
  pairs do not exist (there is one nation).

## Why

"Is that normal" has a second answer: compared to where. Residents compare
their county to the one they moved from, the one next door, or the one in
the news. The page keeps the place page's chapter order, so it is the place
page folded in half, with the state and nation as reference marks on every
row. The existing comparison workspace decides what may be compared; this
page only offers pairs that pass its preflight.

## What exists

- `GET /api/v1/comparison/preflight` and `GET /api/v1/comparison` enforce
  same-source, same-period, same-grain comparability; the comparison
  workspace (`components/ComparisonWorkspace.tsx`) is the analyst surface.
- The chapter contract and three-level card from `place-pages`.

## Deliverables

1. **Route.** `/us/<state>/<county>/vs/us/<state>/<county>` (and the state
   and nation equivalents at matching grain). Both geographies resolve
   through the catalog; a pair at different grains renders an explanation
   and offers the parent of the finer one instead.
2. **Rows.** For each chapter, the measures both places publish for one
   shared period, as paired bars with state and nation ticks; a measure only
   one place publishes is listed under "Not comparable here" with the
   reason from preflight, never hidden.
3. **Shared trend.** One trend per chapter with both places as series and
   their parents as reference lines, indexed only where the unit allows and
   with the base stated.
4. **Picker.** "Compare with" offers neighbouring counties first when
   `nearby-and-related-places` has shipped, then catalog search; swap
   exchanges the two sides and the URL.
5. **Footnote.** What this comparison cannot say: no causal reading, no
   ranking, periods named on every row; a link to the comparison workspace
   carrying the current pair.

## Acceptance criteria

- The route renders for a county pair fixture with paired rows per chapter,
  parent ticks, and a shared trend; swapping the sides updates the URL and
  the rows.
- Every row shows one shared period; a measure failing preflight appears
  under "Not comparable here" with preflight's reason; the browser scenario
  covers one such measure.
- A pair at different grains renders the explanation and the parent offer,
  not a comparison.
- No value is computed client-side beyond formatting; the unit test asserts
  the view model carries preflight verdicts through unchanged.
- Route metadata, sitemap, bundle budget, `check:csp`, and the browser tier
  at desktop and 390px pass with WCAG AA.

## Open items to resolve during implementation

- URL shape for the pair (path segments as above versus a query parameter);
  the path form is shareable and matches the place-page scheme, but must
  stay within the route-title and sitemap contracts.
- Whether state-versus-state and nation-versus-state pairs ship in the first
  release or follow county pairs.

## Checkpoint

Implementation complete; awaiting human review.
