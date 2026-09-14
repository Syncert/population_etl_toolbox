---
id: the-state-control-narrows-what-it-can-narrow
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The state control narrows what it can narrow

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Implemented 2026-09-13 as catalog row WEB-075.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`

## Context

The explorer's state control is disabled unless the selected source declares
`state_fips` as an observation filter:

```tsx
disabled={selectedGeoLevel === "NATIONAL" || !supportsStateFilter}
```

`selectedStateFips` has two other jobs, though. It narrows the geography
picker — the county picker is `"Select a state first"` until one is chosen,
and after WEB-064 the place picker is too — and it narrows the map, which is
what `tileFilterForSelection` exists for ("a selected state is the whole
map"). Neither depends on the source's observation filters.

Census PEP is the source where this bites. Read from the registry:

| Source | `/observations` filters it declares |
| --- | --- |
| `CENSUS_ACS` | county_fips, geo_id, geo_level, state_fips, year_from, year_to |
| `BLS`, `FRED` | county_fips, geo_id, geo_level, state_fips, year_from, year_to |
| `USDA_NASS` | county_fips, domain_desc, domaincat_desc, geo_id, geo_level, state_fips, … |
| **`CENSUS_PEP`** | **geo_id, geo_level, year_from, year_to** |
| `CDC` | adjustment_status, geo_id, geo_level, stratum_id, … |
| `FBI_UCR` | geo_id, geo_level, subject_code, subject_type, … |

PEP's omission is not an oversight to fix in the API:
`gold_pep.population_estimate_latest`, the relation the neutral route reads,
carries `geo_id` and `geo_type` and no fips columns at all. (Its
source-scoped relation `gold_pep.mv_pep_latest` does carry them, which is why
`/pep/observations/latest` declares `state_fips` — the same source narrows by
state through one route and not the other, and the capability resource
declares exactly that, correctly.)

So on Census PEP the state control is disabled at every grain, and therefore:

- the county picker says "Select a state first" and can never be given one,
  so a PEP county's history is unreachable from the picker (the map is the
  only way in, by clicking a county);
- after WEB-064 the place picker says the same, on the one source that
  publishes places.

The instruction is the problem: the control tells the reader to do something
the screen does not let them do.

**Since WEB-066 the dead end is total.** A shared link used to apply a state
unconditionally, so `?source=pep&state=55` was the one way to give the
picker a state — at the cost of a state the reader could not see or clear, a
map and legend narrowed while the rows stayed national, and a save the API
refused over a filter nobody chose. WEB-066 closed that, correctly and for
the reason the requested scope was already gated. The picker now has no route
to a state on Census PEP at all, which makes this plan the only remaining
answer.

## Open question, which is why this is not folded into WEB-064

Enabling the control would send nothing undeclared — `declaredOnly` in
`buildLatestObservationRequest` already drops `state_fips` for a source that
does not declare it, and `/distribution/bins` takes `state_fips` for every
source as a route parameter. But then, on PEP, a selected state would narrow
the map and the picker and **not** the rows, while
`describeObservationLoad` says "… records published for this selection".
That sentence would become false.

So the work is two decisions, not one:

1. Does the state control narrow the picker and the map on a source that
   cannot narrow its rows?
2. If it does, what does the observations status line say, so that a reader
   looking at one state's map and a nation's rows is told which is which?

## Acceptance criteria

1. No control instructs an action the screen cannot perform. Either the
   state control is usable wherever the picker needs it, or the picker says
   what actually blocks it instead of "Select a state first".
2. If a state narrows the map and the picker without narrowing the rows, the
   observations status line says so, in the reader's terms, rather than
   describing a national answer as "this selection".
3. A PEP county's history is reachable without clicking the map.
4. The register row records what a reader sees, on the source it was found
   on.

## Non-goals

- Adding `state_fips` to PEP's neutral filter set. The relation has no such
  column; inventing one in the API would be the client's convenience written
  into the contract.

## Validation

**The two decisions the plan asked for, answered:**

1. **Yes** — the state control narrows the picker and the map on a source
   that cannot narrow its rows. It is now disabled only at NATIONAL, on every
   source. A state narrows three things and only one of them depends on the
   source's observation filters; gating the control on that one left the
   other two unreachable, and the screen instructed an action it did not
   allow.
2. The observations status line says what the state did not narrow.
   `stateScopeNote` in `observationAccess.ts` is the sentence — "… declares
   no state filter for its observations, so these rows are national: the
   selected state narrows the map and the geography list only" — appended by
   `describeObservationLoad` to both its branches, so "0 records published for
   this selection" cannot read as one state's zero either. It is derived from
   `params.state_fips` on the request the effect **issued**, not from the
   selection: `buildLatestObservationRequest` drops a filter the source does
   not declare, so the request is the only place that knows.

**A third thing the plan did not foresee, and had to be settled here.**
Enabling the control makes a state selectable on Census PEP, and
`explorerDocument` wrote `filters.state_fips` from the selection — so saving
such a view would have been a 422 (API-117: a stored document may carry only
filters its own route accepts). The document now records the state the
request carried, which is `""` for PEP. This is WEB-066's third reason, kept.

**WEB-066's link gate is lifted, and its row says so.** That row gated a
link's `state=` on the source declaring `state_fips` for three reasons; two
were the disabled control itself (a state the reader could not see or clear,
and a narrowed map beside national rows with nothing said), and WEB-075
answers both. The link and the control now agree: a link's state applies
wherever the control can hold one. The WEB-066 row is annotated as
superseded in part, names WEB-075 as where the current behaviour lives, and
keeps its surviving rule; the WEB-075 row records the lift.

- The standing `state-filter-note` now says what the control actually
  narrows (the map *and* the geography list) and points at the observations
  line.
- New tests:
  - `tests/frontend/unit/observation-access.test.js` — `stateScopeNote` says
    the rows are national when the state did not reach them, says nothing
    when there is nothing to qualify, and keeps a real subject without a
    source title.
  - `explorer.spec.js` > "a state narrows what it can narrow, and says what
    it did not": on `?source=pep` the state control is **enabled**, selecting
    Wisconsin leaves every PEP request without `state_fips`, the observations
    line says the rows are national, and the county picker — which said
    "select a state first" with no way to be given one — is then usable, so a
    PEP county's history is reachable without the map (criterion 3).
  - `explorer.spec.js` > "a shared link asks for a state, and the view saves
    only what it sent" replaces WEB-066's node: the link is honoured on PEP,
    the control is clearable, and the saved document carries no `state_fips`
    while a Census ACS save carries `55`.
- Break-test: restoring `|| !supportsStateFilter` on the control fails the
  new node with `expect(locator).toBeEnabled() … unexpected value
  "disabled"`.
- Tiers: frontend units 374 passed; `pytest tests/unit` 1562 passed;
  `npm run lint` and `tsc --noEmit` clean; browser tier 94 passed in 1.1m
  against a fresh production build.

## Remaining work

- None.
