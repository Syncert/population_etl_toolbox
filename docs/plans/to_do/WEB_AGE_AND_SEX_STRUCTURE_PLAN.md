---
id: web-age-and-sex-structure
depends_on:
  - api-age-and-sex-structure
  - place-pages
  - web-cross-level-compare
parallel_safe: true
complexity: high
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# A place page draws who lives there by age and sex, against any level it sits in

## Status

To do, 2026-10-31. The web side of
[`AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md`](AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md)
under [ADR-0013](../../decisions/0013-age-and-sex-structure.md). It reads the
contract in [`API_AGE_AND_SEX_STRUCTURE_PLAN.md`](API_AGE_AND_SEX_STRUCTURE_PLAN.md)
and can be built against stubbed answers in that shape while the API is
built; the live check waits for the API to merge.

Look and behaviour: the reviewed mock,
<https://claude.ai/artifact/Pt7U3yxUU5EX4MZcMZyaE4> (Madison and Dane County,
illustrative numbers).

## Deliverables

1. `lib/ageStructure.ts`: the fetchers (`/age-structure`, `/history`,
   `/cohorts`), the toggle model (which reference is shown, from
   `default_reference_geo_id` and the `places` order), formatting. Computes
   nothing the API serves.
2. `components/AgePyramid.tsx`: SVG, hand-drawn like `BarChart.tsx`; no new
   chart library. Eighteen bands, men left, women right, bars as shares,
   MOE whiskers, unreliable bands (`reliable: false`) hatched and named,
   absent bands drawn absent with their reason, the reference as an outline,
   an optional earlier-release outline (from `/history`), hover and keyboard
   focus per bar, and a table view.
3. **The level toggle** (Nick, 2026-10-31): a segmented control of the
   references the API returned, in its order, next level up selected, plus
   "None" (city: County · State · Nation · None; county: State · Nation ·
   None). One choice drives the pyramid outline, the summary cards'
   comparison values and the difference bars. Switching makes no request.
   A city's county option names its overlap when below 100 %.
4. `components/AgeDifferenceBars.tsx`: per band, the place's share minus the
   selected reference's, men and women.
5. `components/AgeSummaryCards.tsx`: median age (all, men, women, the
   provider's), shares under 18, 65+, 85+, the three dependency ratios and
   the sex ratio, each with its MOE and the selected reference's value; the
   draft mark while the scheme is `draft`.
6. `components/AgeStructureHistory.tsx`: broad-group shares as lines across
   the releases `/history` returns, and the "earlier release" outline
   option on the pyramid.
7. `components/CohortChart.tsx`: per band, the count five years earlier
   beside the count now, one band up, with the API's caveat and refusals in
   words; sentences limited to ADR-0011's verbs.
8. Place pages: an "Age and sex" section in the People chapter
   (`lib/placeChapters.ts`) on nation, state, county, city, metro and tract
   pages; the compare page draws both places' pyramids on one axis
   (`compare_geo_ids`).
9. The one-measure map stops offering single B01001 lines as maps (they stay
   readable in the explorer). Age indicators on the map come with API part 2.
10. Plain labels for every band and indicator; raw codes only inside
    `data-provenance`.

## Acceptance criteria

1. A city page's section shows County, State, Nation and None, County
   selected; a county page State, Nation and None. Switching redraws the
   outline, the card comparisons and the difference bars, and the browser
   tier counts no new request.
2. The section makes one `/age-structure` request on load and one
   `/history` request only when the reader opens the history or earlier
   outline.
3. A band the API marks unreliable is hatched and says so; an absent band is
   drawn absent with its reason, never as a zero-length bar.
4. Keyboard focus reaches every bar and announces band, sex, share, count and
   MOE; the table view holds the same numbers; axe passes; it reads at
   390 px in light and dark.
5. The colours are the site's two categorical slots for men and women (not
   blue and pink by convention, and both pass the colour-vision checks).
6. `/layout`, `/explore` and `/map` stay within their bundle budgets
   (507 / 550 / 505 kB); the age components load only on place and compare
   routes.
7. No raw variable code appears outside `data-provenance`.
8. Live check on OFFICE_LAB after the API merges: Madison, Dane County,
   Wisconsin and a Dane County tract render with real values; screenshots
   recorded.

## Checkpoint

Next pickup: stub `/age-structure` answers for Madison and Dane County in the
shape the API plan fixes, then build `AgePyramid` and the toggle against
them.
