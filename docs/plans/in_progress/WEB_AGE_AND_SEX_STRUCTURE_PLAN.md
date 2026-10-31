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

In progress, 2026-10-31: built, merged and checked live for a city, a county
and a state; blocked only on the tract page (D-4) for the tract check. The web side of
[`AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md`](../to_do/AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md)
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

## Evidence (2026-10-31)

Built:

- `lib/ageStructure.ts` (fetchers, toggle model, formatting; reads, never
  recomputes), `lib/ageLines.ts` (which B01001 lines are single age lines).
- `components/AgePyramid.tsx` (+ table view), `AgeDifferenceBars.tsx`,
  `AgeSummaryCards.tsx`, `AgeStructureHistory.tsx` (small multiples, one per
  broad group, each on its own scale, plus a table), `CohortChart.tsx`,
  `AgeAndSexSection.tsx` (the People chapter section and the toggle),
  `ComparePyramids.tsx` (compare page, one shared axis, one request with
  `compare_geo_ids`), `AgeLineNotMapped.tsx` (`/map/<single age line>`).
- All age components are loaded with `next/dynamic` from the place and
  compare pages; `/layout`, `/explore` and `/map` are unchanged in size
  (`check:bundle`: every route within budget).
- Colours: the site's first two categorical slots (`seriesStroke(0)`,
  `seriesStroke(1)`: #0b6b57 men, #8c3b17 women). The dataviz validator passes
  lightness, CVD separation (worst ΔE 8.4, deutan), normal-vision floor (19.0)
  and contrast; it flags #0b6b57's chroma (0.087) as low. Kept as the site's
  slot; sex is also carried by side, legend and every label.
- The site has no dark theme, so "light and dark" is light only here.
- Tracts have no page yet (D-4) and the compare page addresses no tract, so
  unreliable and absent bands are proved in the unit tier on a tract
  fixture; the section will appear on tract and metro pages through the same
  component when those pages exist.

Validation (local, fixtures):

- `npm run typecheck`, `npm run lint`: clean.
- `npm run test:unit`: 73 files, 956 tests passed (16 in
  `tests/frontend/unit/age-structure.test.jsx`).
- `npm run build`, `npm run check:bundle`: every route within budget.
- `tests/frontend/browser/age-structure.spec.js`: 9 passed, against a scratch
  copy of the OpenAPI snapshot carrying the three age routes. The committed
  snapshot comes from the API PR; the served-contract check fails every place
  page's age request until it lands, so this PR merges after it.
- Catalog: WEB-152, recounted to 679.

Remaining: merge dev after the API PR, rerun the full browser tier against
the real snapshot, then the live check on OFFICE_LAB (acceptance 8).

## Live check (OFFICE_LAB, 2026-10-31, batch 425ee9eb)

Madison, Dane County and Wisconsin render with real values: Madison's 20 to
24 band sits well outside the Dane County outline, median age 31.8, sex ratio
100.1, margin whiskers drawn. Screenshots: project files `age_madison.png`,
`age_dane.png`, `age_wisconsin.png`. Found and fixed in the follow-up PR:

- The nation read "Nation: Us:1" and its legend "us:1, 2020–2024": the catalog
  names the nation by its id. The API fixes it at the source (#230); the web
  also names every place for a reader where an answer arrives
  (`readableName`), so an id is never shown.
- A county page asked `/age-structure` for the state while the county was
  still resolving; the section now mounts only for the page's own resolved
  place (as #228 did for migration flows).

A Dane County tract was not checked live because tracts have no page yet
(D-4); the section is the same component and will be checked when that page
exists.

## Checkpoint

Blocked on D-4 (tract page), 2026-10-31. Everything else is built, merged
(#223, #231) and checked live. The one open acceptance item is the live
check of a Dane County tract, which needs a tract page to exist. When D-4
lands: open a tract page, confirm the section renders with real values
(including hatched and absent bands where the tract has them), record a
screenshot here, and move this plan to `needs_review/`.
