---
id: starting-a-family-collection
depends_on:
  - county-childcare-prices
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/semantics tests/unit/api -q
  - ruff format --check . ; ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every place page shows what starting a family there looks like

## Status

Proposed 2026-11-10, awaiting Nick's approval. Nick, 2026-11-10 12:35Z:
"would it be interesting to have a 'starting a family looks like ...'
collection of metrics such as childcare, house price etc that indicate
blockers or favorable situations to encourage child rearing". Nothing is
built until the open questions below are answered.

## Why

A family deciding where to have and raise children weighs a handful of
things at once: what care costs, whether a home with room is affordable,
what work pays, and what the surroundings are like for a child. The site
already holds each of these, but spread over five chapters (Cost of living,
Work and Money, Housing, Health, People), so the reader has to assemble the
picture. A collection puts them in one place, each still a published value
with its own source, period and comparison.

## Rules this plan keeps

- **No score.** Measures are never combined into an index, average or
  "family-friendliness" rank. Each row stands alone, as
  `distinctive_service.py` already guarantees for ranks.
- **True to source, no extrapolation.** A figure the source publishes only
  for a larger area is shown as that area's figure, labelled with its name
  (a city shows its county's childcare price as "Dane County"). A grain the
  source does not publish says so; nothing is estimated down or summed up.
- **Derived figures** (hours of work, shares, students per teacher) show the
  plain published inputs and the formula once (Nick, 2026-11-08).
- **Free sources already in the warehouse.** Slice 1 adds no source. New
  sources are listed separately below as an optional slice 2.
- **Minimise cognitive load** (Nick, 2026-11-04): one compact list on a
  shared visual grammar, built with the dataviz skill.

## What the collection shows (slice 1, all already loaded)

Eight rows in four short groups. Every row: the plain value, its year, a
place / state / nation level strip (the existing `PlaceLevelStrip`), and
where the distinctive service ranks it, "lower than N of Wisconsin's 72
counties".

| Group | Row | Source and code | Newest (to confirm on OFFICE_LAB) | County | City | State / nation |
|---|---|---|---|---|---|---|
| Paying for care | Infant care at a center, per week, and hours of work | DOL Women's Bureau NDCP `DOL_CHILDCARE:center_infant`; existing `minutes_of_work_childcare_center_infant` | 2022 | yes | county figure, labelled | "Published by county" |
| A home with room | Fair Market Rent, 3 bedrooms, per month, and hours of work | HUD `HUD_FMR_IL:fmr_3br`; **new** time-cost pairing `minutes_of_work_rent_3br` | FY 2027 | HUD area figure, labelled with the area name | county's HUD area, labelled | "Set by HUD area" |
| A home with room | Median home value | ACS `B25077_001` | 2020–2024 survey | yes | yes | yes |
| Money coming in | Average weekly wage | QCEW `annual_avg_weekly_wage:10:0` | newest annual | yes | county figure, labelled | yes |
| Money coming in | Children under 18 in poverty | SAIPE `SAEPOVRT0_17` | 2024 | yes | county figure, labelled | yes |
| A child's surroundings | Children under 19 without health insurance | ACS share, existing `uninsured_under_19_share` | 2020–2024 survey | yes | yes | yes |
| A child's surroundings | Students per teacher in public schools | NCES CCD `student_membership` / `teacher_fte`; **new** ratio pairing `students_per_teacher` | newest school year | yes, only where both sums are `complete` | county figure, labelled | yes |
| A child's surroundings | Households with children under 18 | ACS `B11005_002` / `B11005_001`; **new** share pairing `households_with_children_share` | 2020–2024 survey | yes | yes | yes |

"More for families" (a fold under the list, same row format, no extra
reads until opened): births per 1,000 people (PEP, existing
`birth_rate`), violent crime rate (FBI, state context at county as the
Safety chapter already does), workers commuting 90 minutes or more
(existing share), PM2.5 annual mean (EPA), and the other seven childcare
prices (link to the Cost of living table, not repeated).

Years differ by source and are stated per row, never aligned by moving a
figure to another year. The childcare row says "2022, the newest year the
Women's Bureau has published" so it does not read as current.

## How it reads

A chapter titled **Starting a family**, in the chapter rail after Cost of
living, opening with one sentence: "What having and raising children here
costs and looks like, from published figures. Each line stands alone; they
are not added up." Then the four groups. Example, Dane County:

> **Paying for care**
> Infant care at a center · **$293 a week** (2022) · 8 h 42 min of work
> at the county's average wage · ●──── state ──── nation
> Higher than N of Wisconsin's 72 counties

(The price and hours are Dane's loaded 2022 values; the rank is a
placeholder until the distinctive service ranks the measure.)

The hours line repeats the existing childcare arithmetic once at the foot
of the chapter: "Hours of work = weekly price ÷ (county average weekly wage
÷ 40)", with the county's wage, its year and its source filled in. Each row links to the card
it came from in its home chapter.

**Favourable or a blocker** (open question 1): the recommended reading is
one plain word per row comparing the place with its state, "easier than the
state" or "harder than the state", from a direction fixed per row (lower
care cost, rent, poverty, uninsured share and class size are easier;
higher wage is easier), shown only when the two values are the same period
and, for survey values, the difference is beyond both margins. Otherwise
"about the same as the state". Two rows have no direction and get no word:
home value (a higher value is a cost to buyers and wealth to owners) and
households with children (context, not a condition).

## Data and API work (slice 1)

1. `docs/semantics/derived_ratios.json`, three new reviewed pairings:
   - `households_with_children_share`, `share`, ACS `B11005_002` /
     `B11005_001`, same table (ADR-0009 rule 1).
   - `students_per_teacher`, `ratio`, `NCES_CCD:student_membership` /
     `NCES_CCD:teacher_fte`, same program; served only where both rows
     are `completeness = complete` (a partial sum is not divided).
   - `minutes_of_work_rent_3br`, `time_cost`, `HUD_FMR_IL:fmr_3br` over the
     county's own QCEW wage. Needs `HUD_FMR_IL: "county"` in
     `TIME_COST_PRICES` and a one-paragraph ADR-0009 amendment: the price
     is the HUD area's, the wage is the county's own, and the newest-wage
     rule for prices (11-06 amendment) applies because FMRs run a fiscal
     year ahead of QCEW. Shown as hours per month.
2. `DISTINCTIVE_MEASURES` gains `DOL_CHILDCARE:center_infant`,
   `HUD_FMR_IL:fmr_3br`, `BLS_QCEW:annual_avg_weekly_wage:10:0` and
   `CENSUS_SAIPE_SAHIE:saipe:SAEPOVRT0_17`, so each row can say where the
   county sits among its state's counties. HUD areas repeat one value over
   several counties; those are counted as ties, which the service already
   reports apart.
3. No new metrics, so no glossary or `dim_metric` work: every code above is
   already in the catalog. No warehouse DDL, no DAG change, no load.

## Web work (slice 1)

- `lib/familyCollection.ts`: the eight rows, their groups, direction and
  grain rules, as data, unit-tested.
- `components/StartingAFamily.tsx`, lazy-loaded like the other chapters'
  extras. Reads: it reuses the answers the page already holds for codes
  other chapters read (home value, wage, child poverty, uninsured share),
  and sends one newest-values batch for the rest plus one derived read for
  the three pairings. The page keeps "each value read once".
- Empty rows say "Not loaded into this site yet"; a year a source did not
  publish says "not published that year"; an API null is "missing".
- Place-page byte budget: measured in the PR; expected well under the
  current headroom because the component is lazy.
- A use-case directory entry "Starting a family" (group Place profiles)
  pointing at the chapter, so the collection is findable from the field
  guide.

## Slice 2 (optional, new ACS tables; needs Nick's go separately)

All free, all Census ACS through the existing `census_acs` DAG using the
`added_tables` mechanism (only the new tables, on the next scheduled run).
Cloud cannot reach census.gov, so the first run is on OFFICE_LAB.

| Table | Would add | Why it helps |
|---|---|---|
| B23008 | Children under 6 with all parents in the labor force | how many families need paid care |
| B19125 | Median family income, families with own children under 18 | income of families with children, not all households |
| B13002 | Women 15–50 with a birth in the past 12 months | a survey fertility rate beside PEP births |

Not recommended: the Women's Bureau state-level NDCP estimates (only on
www.dol.gov as xlsx, which refuses our client; the DOL API dataset is
county only, to be confirmed by one OFFICE_LAB probe if wanted), CDC
WONDER natality (county data needs an interactive agreement, not
automatable), state paid-leave and child-tax-credit laws (no free
machine-readable official source; would be hand-sourced).

## Acceptance criteria

1. A county page shows the Starting a family chapter with the eight rows,
   each with value, year, level strip, rank where served, and a link to
   its home card.
2. A city page shows county-only rows as the county's figure, named; a
   state and the nation page say "Published by county" / "Set by HUD area"
   for those rows. No row shows a value for a grain its source does not
   publish.
3. The three new pairings pass the registry checks, the time-cost row
   shows dollars first and states its arithmetic once.
4. No row is combined with another; no total, score or overall rank exists
   in the API response or the page.
5. The place page sends no more than one extra batch and one derived read
   for the chapter; speed gate as for #339 (no API read over 2 s).

## Open questions for Nick

1. **Favourable / blocker cue:** a plain "easier / harder / about the same
   as the state" word per row (recommended), or neutral marks only?
2. **The eight rows:** keep as above, or swap any (for example rent burden,
   commute or crime into the main list)?
3. **3-bedroom rent in hours of work:** extend minutes-of-work to HUD Fair
   Market Rents (needs the ADR-0009 amendment above)?
4. **Slice 2:** add the three ACS tables now, later, or not at all?

## Checkpoint

2026-11-10: plan written; no code.
