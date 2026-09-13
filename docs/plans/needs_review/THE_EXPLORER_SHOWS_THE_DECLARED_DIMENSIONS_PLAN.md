---
id: the-explorer-shows-the-declared-dimensions
branch: claude/iterate-plans-improvements-ir885c
depends_on: [the-capability-map-names-the-dimensions]
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The explorer shows the dimensions the source declares, not the filterable ones

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/explorerSources.ts`,
  `apps/web/components/SourceExplorerPage.tsx`

## Context

The explorer's observation table and its CSV both take their dimension
columns from `dimensionFilters` — the source's filterable names, derived by
subtracting the universal and shared filters from the route's parameter list.
That is the right list for the filter *controls*. It is an accident as the
list of what a row publishes, and the two sets barely overlap:

| Source | Columns shown | Declared dimensions | Not shown anywhere |
|---|---|---|---|
| BLS | — | 2 | `series_id`, `seasonal_adjustment_status` |
| Census ACS | — | 2 | `dataset_code`, `variable_code` |
| Census PEP | — | 4 | `dataset_code`, `product_code`, `summary_level`, `value_source` |
| FRED | — | 2 | `series_id`, `seasonal_adjustment_status` |
| CDC | `adjustment_status`, `stratum_id` | 14 | `footnote_code`, `footnote_text`, `estimate_method`, `population_basis`, `strata`, `dataset_title`, … |
| FBI UCR | `subject_code`, `subject_type` | 12 | `offense_code`, `offense_label`, `counted_entity_basis`, `geography_basis`, `measure_form`, … |
| USDA NASS | `domain_desc`, `domaincat_desc` | 14 | `commodity_desc`, `class_desc`, `prodn_practice_desc`, `freq_desc`, `suppression_code`, `week_ending`, … |

Four of seven sources show **no** dimension at all, and the fields missed are
not incidental. `footnote_text` is how CDC qualifies an estimate.
`suppression_code` is how NASS says why a value is absent. `offense_label`
is what an FBI count counts. And the guide says of BLS:

> The BLS series id is still on every row, under `dimensions.series_id`, so
> lineage back to the provider's series is never lost.

It is lost here — the explorer never shows it.

This is the defect WEB-051 and WEB-053 named, with a different deciding rule:
"a file that carried a subset would be this client deciding which part of a
source's participation basis a reader may have". Here the subset is decided
by which fields happen to be filterable.

API-109 published the declared set as `observation_dimensions` on
`/catalog/capabilities`, so there is now a contract to read instead of an
accident to infer from.

## Acceptance criteria

1. The export carries one column per **declared** dimension. A file is for
   completeness; that is WEB-051's rule and it decides this outright.
2. The table keeps a column for each filterable dimension — those are what
   the reader is filtering on — and carries the remaining declared fields in
   one cell as `name value` pairs, which is the presentation the repository
   already chose for the seven uncertainty fields
   (`observationUncertaintyLabel`) rather than seven columns.
3. The filter controls are unchanged: they are driven by what the route
   accepts, which is `dimensionFilters`.
4. A source that declares no dimensions grows no empty column and no empty
   cell.
5. The declared set is read from the capability map, never re-derived from a
   loaded row: a declared dimension a page happens not to publish still
   appears, empty, rather than vanishing.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-061).

## Non-goals

- Fourteen more table columns. See criterion 2.
- Changing what `dimensions` carries. That is the API's review, published by
  API-109 and read here.

## Validation

**Failing first** at the unit tier (the declared set does not reach
`ExplorerSource`; the label helper does not exist) and at the browser tier
(no `dimensions-…` cell).

**The browser tier found something bigger, and it is not this plan's
behaviour.** The new test asserted `stratum id` was a column and got zero. So
did a control assertion on `Value` — a column that certainly exists. The
table panel is `display: none` until its tab is selected, so it is not in the
accessibility tree, and **every `getByRole("columnheader", …)` assertion in
this spec was running against a hidden panel**. That makes two pre-existing
guards vacuous:

- `a source that publishes no participation grows no column for it` (WEB-051)
- `a source that publishes no uncertainty grows no column for it` (WEB-053)

Both assert `toHaveCount(0)` and both passed by seeing *nothing at all*.
Neither could have failed. They now click the table tab and assert a control
column first, so the zero means the column is absent rather than that the
table is. That is WEB-043's lesson applied to an assertion instead of a
fixture, and it needs no new catalog row: it is the two existing rows
becoming true.

**The CDC row fixture was carrying the filterable subset too**, which is the
same drift its neighbour already warns about in a comment ("A fixture
without them models a weaker contract than the one that ships"). It now
publishes the declared set, so both halves of the new browser test are
load-bearing: without the capability's `observation_dimensions` the cell does
not render, and without the row's fields it renders empty.

**The split between table and export is deliberate.** The file gets one
column per declared dimension, because WEB-051 settled that outright for
files. The table gets a column per *filterable* dimension — those are what
the reader is filtering on — and one `Dimensions` cell for the rest, which is
what this repository already chose for the seven uncertainty fields rather
than seven columns. Fourteen more columns for CDC and USDA NASS would have
been a different and worse answer.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | **325 passed** (was 320) |
| Frontend browser | `npm --prefix apps/web run test:browser` | **80 passed** (was 78), 2.0m |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit` | 1488 passed |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**Register.** 366 rows.

## Remaining work

- None.
