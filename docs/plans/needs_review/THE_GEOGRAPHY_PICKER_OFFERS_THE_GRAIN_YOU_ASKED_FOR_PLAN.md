---
id: geography-picker-offers-the-grain-you-asked-for
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The geography picker offers the grain you asked for

## Plan status

- **Status:** Implemented; awaiting review. Claimed and completed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/geographyPicker.ts` (new),
  `apps/web/components/SourceExplorerPage.tsx`

## Context

WEB-038 put the whole published grain vocabulary in the explorer's grain
selector, with the reason recorded in the component:

> PLACE is Census PEP's and AGENCY is FBI UCR's; with only the spatial three
> here, a measure declaring either offered no levels at all and was queried
> at a grain it does not publish.

The geography picker beside it was not extended. It still answers two grains
and falls through to a third:

```tsx
{(selectedGeoLevel === "COUNTY" ? counties : states).map((county) => (
  <option value={county.geo_id} key={county.geo_id}>
    {selectedGeoLevel === "COUNTY" ? county.county_name : county.state_name}
```

So at `PLACE` — Census PEP's own grain, and the only grain the API publishes
`place_fips`/`place_name` for — the picker offers **states**, labelled by
their state name, as the places to choose from. At `AGENCY` it does the same.
Three things follow, none of them stated to the reader:

1. The options are another grain's geographies. Picking one sends a `geo_id`
   that cannot exist at the selected grain.
2. The control will not hold the choice. Its value is
   `allGeographies.some(...) ? selectedGeoId : ""`, and `allGeographies` is
   `[]` for any grain but `STATE` and `COUNTY`, so the selection is made,
   stored, and rendered as blank.
3. The history panel then asks for that geography at that grain — a request
   that answers nothing — and reports it as no published history.

Meanwhile `/catalog/geographies` publishes `place_fips` and `place_name` on
every row (they are declared in the reviewed contract), takes `geo_level` and
`state_fips`, and the client's `GeographySummary` declares neither field: a
sweep of the served response schemas against the web source found them among
the fields the application never names.

`AGENCY` is a different answer from `PLACE`, and honestly so: the geography
projection is `gold_glossary.dim_geo_latest`, whose grains come from
`dim_geo_current.geo_level` (`us`/`state`/`county`/`place`), so it carries no
agency identity at all. The picker must say that rather than offer states.

## Acceptance criteria

1. The picker's options are the selected grain's geographies, never another
   grain's, for every grain in the published vocabulary.
2. `PLACE` is offered from the projection, labelled by its published
   `place_name`, and bounded the way counties already are: a state first,
   then that state's places. An unbounded read of some 32k places for a
   `<select>` is not the fix.
3. A grain the projection publishes nothing for says so — in the picker's own
   empty option — rather than offering a list that is not of that grain.
   The statement is derived from the empty answer, not from a client-side
   list of which grains have geographies.
4. "Select a state first" keeps meaning what it says: it is what `COUNTY`
   shows today, and `PLACE` shows it for the same reason.
5. The control holds a choice at every grain it offers one for.
6. The decision — which list, what the empty option says, whether the control
   is disabled — is a pure function with unit tests, not a chain of ternaries
   in the markup.
7. `GeographySummary` declares `place_fips` and `place_name`.
8. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-064), with the
   browser tier asserting what a reader sees at `PLACE` and at `AGENCY`.

## Non-goals

- Loading every place in the country. The bound is the point of criterion 2.
- Offering FBI agencies. That needs the warehouse to project agency
  identities into the geography dimension; until it does, the honest client
  answer is that none are published.
- Touching the map. `DRAWABLE_TILE_GRAINS` (WEB-062) already answers which
  grains can be drawn, and `PLACE` is not one of them; the picker is a filter
  control, not a map control.

## What changed

- `apps/web/lib/geographyPicker.ts` (new) holds the decision:
  `geographyPickerState(grain, {geographies, stateSelected, read})` answers
  the label, the options, the placeholder and whether the control is
  disabled, and `geographyName(row, grain)` names a row by the field that
  attributes its own grain. `GEO_GRAIN_ORDER` is `GEO_LEVELS`, so the
  vocabulary is not spelled twice — the component's own copy is gone.
- `read` is the part that matters for honesty: "no places are published" and
  "no places have arrived" are different statements, and only the first is
  about the warehouse. The component tracks both reads (`geographiesRead`,
  `grainGeographiesRead`) and the picker says "Loading …" until one answers.
- A grain the eager state/county read does not cover is read for the grain
  actually selected, narrowed by `state_fips` where a state is chosen.
- `GeographySummary` declares `place_fips`/`place_name`.
- The browser fixture answered the county row for *every* grain, which is
  what let the defect survive its own suite. It now answers per grain, with
  `AGENCY` empty for the reason the projection is: `dim_geo_latest` takes its
  grains from `dim_geo_current.geo_level`, which is us/state/county/place.
  The PEP fixture metric declares `PLACE`, which the real source publishes.

## What this does not reach, and why

With today's declared capabilities, **PLACE with a state selected is not
reachable in the shipped app**: the state control is disabled unless the
source declares `state_fips` as an observation filter, and Census PEP — the
only source publishing places — does not, because
`gold_pep.population_estimate_latest` carries no fips columns. The same gate
already made the *county* picker unusable on PEP before this plan. That is a
separate defect with a design question attached (what the observations status
line should say when a state narrows the map but not the rows), recorded as
`docs/plans/to_do/THE_STATE_CONTROL_NARROWS_WHAT_IT_CAN_NARROW_PLAN.md`
rather than folded in here.

So the browser tier asserts what a reader can actually see: at PLACE and at
COUNTY the picker says "Select a state first" and offers neither Wisconsin
nor Dane County; at AGENCY it says nothing is published. The state-selected
paths are asserted against the pure function, whose contract is over its
inputs — and which becomes reachable the moment either the follow-up plan or
a PEP relation carrying `state_fips` lands, with no client change.

## Validation

- `npm --prefix apps/web run test:unit` — **347 passed** (334 before: +13 in
  a new `geography-picker.test.js`).
- `npm --prefix apps/web run test:browser` — **82 passed** (80 before: +2).
  Both new nodes fail against the old picker, which offered Wisconsin as a
  place and Dane County as an agency.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `python -m tests.support.catalog_evidence` renders WEB-064 `FULL`; the
  register is 373 rows.

## Remaining work

- None. Review is the remaining step.
