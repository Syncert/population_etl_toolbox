# Website user experience review, November 2026

An adversarial review of the web app (`apps/web`) as an ordinary resident
meets it. The reader assumed here is not an analyst: they arrive with a
question about where they live, on a phone as often as on a desktop, and
they leave when the page stops making sense.

## How it was reviewed

- A live capture of the deployed site (OFFICE_LAB, dev `62875ee3`+) on
  2026-11-15: 20 pages at 1400 px and 390 px, first viewport after 2 s,
  full page, the page's text as rendered, and per-page measurements
  (`ux_review/capture.js`, results in `ux_capture_desktop.zip` and
  `ux_capture_mobile.zip` in the project Library).
- A read of the components and styles behind what the capture showed.
- Nick's standing rules were treated as fixed: minimise cognitive load, no
  subjective words, no extrapolation, true to source with coverage notes,
  "Not loaded into this site yet" for empty cards. No finding below asks for
  a number the sources do not publish; almost all of them are about how a
  published number is worded, ordered and laid out.

## What the capture measured

| Page | Height at 1400 px | Height at 390 px | Words | Raw ISO dates | Links and controls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Home | 3,280 px | 4,331 px | 531 | 0 | 62 |
| Nation (`/us`) | 20,691 px | 31,244 px | 6,132 | | 177 |
| State (Wisconsin) | 26,774 px | 41,407 px | 8,546 | | 246 |
| County (Dane) | 34,316 px | 52,044 px | 11,476 | 206 | 373 |
| City (Madison) | 16,144 px | 27,249 px | 5,954 | | 115 |
| Map of one measure | 4,231 px | 7,381 px | 745 | | 124 |
| Explorer | 1,360 px | 3,067 px | 16,024 | | 105 |

A phone screen is 844 px tall, so the Dane County page is about 62 phone
screens and roughly 45 minutes of reading. Every page settled in 8 to 17 s
in the capture (which waits 6 s after the network goes quiet, so the true
figure is 2 to 11 s); the nation page was slowest because
`/api/v1/where-money-goes?geo_id=us:1` took 9.8 s.

## What already works

These are worth protecting while fixing the rest.

- The home page asks one question and offers one search box. That is the
  right first screen.
- The dot strips (place, state, nation on one scale with margin whiskers)
  and the one-sentence reading under them ("Dane County was lower than
  Wisconsin and the United States at the published margins of error") do
  the comparing for the reader.
- "What stands out" answers "what is unusual about my place?" before the
  reader has to look for it.
- "What the record shows" is phrased as questions people ask.
- "Starting a family" translates prices into hours of work with the formula
  stated.
- Gaps are stated, never filled.

## Findings

Severity: **S1** a resident is stopped or misled; **S2** a resident is slowed
or has to translate; **S3** polish. Effort: **S** under a day, **M** a few
days, **L** a design decision plus a week or more.

### A. The first screen of a place page

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| A1 | Three status pills under the title speak to the developer: "Place: state:55\|county:025", "Measures: 165 of 166 candidate measures are published", "Observations: 195 published series read". They sit above every figure on the page, the third one in a warning colour while loading. | `county.desktop.first.png` | S1 | S |
| A2 | The opening paragraph describes the site's layout ("The same chapters in the same order on every place page...") and not the place. On a phone, the title, breadcrumb, paragraph, pills and chapter links fill the first 620 px; no figure is visible without scrolling. | `county.mobile.first.png` | S2 | S |
| A3 | On a phone the site header takes 160 px (wordmark, a bare "Sign in" button on its own row, two rows of navigation) before the page title. | `county.mobile.first.png`, `home.mobile.full.png` | S1 | S |
| A4 | "Sign in" renders as an unstyled browser button. | every capture | S3 | S |

### B. Words a resident has to translate

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| B1 | Periods are printed as database dates: "2020-01-01 – 2024-12-31", "2026-09-01 – 2026-09-30", "index, 1971-07-01 = 100", "period beginning 2020-01-01". The Dane page has 206 of them. The site already says "2020–2024" and "September 2026" in sparklines and the record section, so the same period is written two ways within one card. | `county.desktop.txt` | S1 | S |
| B2 | Each chapter footer lists every period on the card as raw dates and names sources by warehouse code: "Period: 2020-01-01 – 2024-12-31; 2024-01-01 – 2024-12-31; 2026-09-01 – 2026-09-30; ... · Source: CENSUS_ACS, BEA, BLS, BLS_QCEW, CENSUS_SAIPE_SAHIE". | `county.desktop.txt` | S1 | S |
| B3 | Money and percentages are written as a number followed by a unit word: "89,975 Dollars", "9.9 percent", "1,631 dollars per week", "590,375 persons", "35.7 Years". Starting a family on the same page writes "$394,800" and "$1,486 a week". | `county.desktop.txt` | S2 | S |
| B4 | A card's small line repeats its heading: "Resident population estimate" / "Resident population estimate · 2025-07-01 · persons". | `cd_00.png` | S3 | S |
| B5 | Under "Among the lowest" the sentence reads "Higher than 1 of 71 Wisconsin counties", so the reader has to subtract to learn what "lowest" means. | `county.desktop.first.png` | S2 | S |
| B6 | Survey vocabulary is left in the reading line: "Universe: people 16 and over" on every small tile, "at county grain", "state context", "candidate measures", "Population Estimates Program vintage". | `county.desktop.txt` | S2 | M |
| B7 | "DRAFT" badges sit beside seven age figures ("People under 18 DRAFT") and on view toggles. To a resident "draft" reads as "this number may be wrong", when it means the grouping has not had its internal review. | `cd_01.png` | S2 | decision |
| B8 | Source notes in bold green look like links and carry table codes: "BEA: the county, CAINC35 transfer receipts (released November 16, 2023) over CAINC1 personal income...". | `p_dane.png` | S2 | M |
| B9 | Table rows show uncertainty three ways at once: "margin of error ±2,947 (3.22%) · confidence lower 88532 · confidence upper 94426" (the last two without thousands separators). | `county.desktop.txt` | S3 | S |
| B10 | GDP is shown as "51,263,648 Thousands of chained 2017 dollars". | `cd_01.png` | S2 | S, needs a rounding rule |

### C. Length and finding your way

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| C1 | A county page is 34,000 px (52,000 px on a phone) and 11,500 words, all open at once. The chapter links scroll away after the first screen, so from the middle of the page there is no way to tell where you are or to jump elsewhere. | measurements above | S1 | S for a sticky chapter bar; L for collapsing chapters |
| C2 | The chapter links leave out "Cost of living" and "Nearby and related", and "What the record shows" appears only after loading, shifting the row. | `county.desktop.first.png` vs headings | S2 | S |
| C3 | The same explanatory note is repeated in full under each chart or card: the five-year-estimate note under every ACS trend, the fiscal-year note three times and the place-of-performance note twice in "Where the money goes". | `where_money_dane_378.png` | S2 | M |
| C4 | Each chapter mixes three depths (headline cards, trend charts, a grid of 10 to 15 small tiles, industry bars, then special sections) with no visual break between "the answer" and "the detail". | `cd_00.png`, `cd_03.png` | S2 | L |
| C5 | Work and Money alone holds 8 headline cards, 2 trends, 13 tiles, 2 industry charts, Taxes and public money, and Where the money goes. | headings list | S2 | L |

### D. Charts

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| D1 | Trend charts label the axis with the data's own minimum, middle and maximum: "200.53", "150.1", "99.66". Round reference values (100, 150, 200) are what a reader can hold. | `cd_00.png` | S2 | S |
| D2 | On a phone the trend chart is scaled down as one image, so its axis text is about 4 px tall and cannot be read. | `cm_01.png` | S1 | M |
| D3 | The index chart needs two sentences to explain ("Each line is its own value divided by its value in 1971-07-01, times 100"). For most headline measures the published values themselves on a plain axis would need none. | `cd_00.png` | S2 | M |
| D4 | "Where the money goes" prints "0.0%" beside $17.8 million. | `where_money_dane_378.png` | S3 | S |
| D5 | The one-measure map lists its five colour bands twice (legend, then a bullet list) and prints "persons" on all 50 table rows. | `map_population.desktop.full.png` | S3 | S |

### E. The home page

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| E1 | The county map's legend reads "Counties, unpainted: no briefing measure is published yet" and "Not painted". The map is only a way to pick a county; the legend describes an unbuilt feature. | `home.desktop.full.png` | S2 | S |
| E2 | Below the search the page turns to analysts: "11 population matches", a "Featured analysis" that links to an empty Articles page, and a "National snapshot" whose headline is the raw catalog name "Population (persons) 1/ (CAINC1, BEA number of persons)". | `home.desktop.full.png` | S1 | S |
| E3 | The sources block leaves the left half of the page empty and lists "NOAA National Centers for Environmental Information" three times. | `home.desktop.full.png` | S3 | S |
| E4 | The site is called "Economic Data Studio", which describes a tool for analysts and not a place to look up where you live. | every page | S2 | decision |

### F. Navigation and the analyst tools

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| F1 | "Find your place" opens the United States page, which has no search box. The search exists only on the home page. | `SiteHeader.js` | S1 | S |
| F2 | The explorer is wider than the screen: its row of 37 source tabs pushes the page to 3,302 px at desktop width and 3,302 px at 390 px, so the whole page scrolls sideways. | `report.json` (`horizontalOverflow` 1,902 and 2,912) | S1 | S |
| F3 | The explorer's tabs are warehouse codes ("BLS_QCEW", "CENSUS_SAIPE_SAHIE"), its intro says "validate data availability for this MVP", and its pills print tile-server internals ("chosen_layer=counties; healthy_tile=true"). | `explore.desktop.first.png` | S2 | M |
| F4 | The compare workspace overflows a phone screen by 130 px. | `report.json` | S2 | S |
| F5 | "Use cases" is the first navigation item and opens a two-level menu of 20 numbered analyst pages. For a resident the place page already is the product. | `home.desktop.use-case-menu.png` | S2 | decision |
| F6 | An unknown place address answers HTTP 200 with a "not found" page. Search engines will index it. | `report.json` | S3 | M |
| F7 | Every page load logs a failed request (`401 /api/v1/auth/refresh`) for readers who are not signed in. Harmless, but it is noise in every error report. | `report.json` | S3 | S |
| F8 | "Where the numbers come from" prints coverage as codes ("census_division, provider_area") and dates as "11/6/2026". | `data.desktop.txt` | S3 | S |

### G. Accessibility and touch

| # | Finding | Evidence | Sev | Effort |
| --- | --- | --- | --- | --- |
| G1 | The county page has about 200 text elements under 12 px (the stylesheets use sizes from 0.66rem to 0.78rem in 40 places). | `report.json` (`smallText`) | S2 | M |
| G2 | About 290 links and controls on the county page are under 24 px in one dimension at phone width (the "compare" links, chapter links and "Show as a table" toggles). | `report.json` (`smallTapTargets`) | S2 | M |

## Triage

**Fix now** (high value, small, no decision needed). These are S1 or S2
findings where the fix is wording or layout and nothing about the data
changes.

1. Place page in plain words: A1, A2, B1, B2, B4, B5. Plan:
   `PLACE_PAGE_PLAIN_WORDS_PLAN.md`.
2. Shell and home for residents: A3, A4, C2 and the sticky chapter bar from
   C1, E1, E2, E3, F1, F2, F4. Plan:
   `SITE_SHELL_AND_HOME_FOR_RESIDENTS_PLAN.md`.

**Fix next** (clear value, a few days each, no decision needed).

3. Money and percentages written as "$89,975" and "9.9%": B3. Held out of
   the first fix because an earlier UAT change deliberately made the cards
   say "Dollars" where the survey serves no unit, and four browser tests pin
   that wording; it needs one rule for every card, tile and table.
4. Trend charts a phone can read: D1, D2, D4, D5, then D3.
5. Say each note once: C3, B8, B9.
6. Type and touch sizes: G1, G2.
7. Explorer wording: F3, F8, F7.

**Needs Nick's decision** before any work.

- **C1/C4/C5, page length.** The structural fix is to show each chapter's
  headline cards open and fold its trends, small tiles and special sections
  behind one "More about ..." control per chapter (with every fold opened by
  a link that targets something inside it, and by the browser's find). That
  would cut the first read of a county page from about 34,000 px to roughly
  8,000 px without removing anything. It changes how the whole page reads,
  so it is yours to call.
- **B7, DRAFT badges.** Options: keep them; move the review status into the
  "Words used here" notes; or approve the five-year age grouping and the
  pairings so the badges go away.
- **E4, the site's name**, and **F5, whether "Use cases" stays first in the
  navigation** or moves under Tools.
- **B10**, a rounding rule for very large dollar figures ("$51.3 billion in
  2017 dollars" beside the published figure).

**Leave.** F6 (correct status codes need server-side place resolution, which
belongs with the serving-layer plan).
