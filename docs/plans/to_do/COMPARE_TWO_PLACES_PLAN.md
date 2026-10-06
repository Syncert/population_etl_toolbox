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

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

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

Next pickup: read `lib/comparison.ts`, write the failing unit test that a
preflight failure becomes a "Not comparable here" row, then the route.
