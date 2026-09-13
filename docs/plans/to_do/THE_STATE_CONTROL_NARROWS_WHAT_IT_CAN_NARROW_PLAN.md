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

- **Status:** To do. Found and recorded 2026-09-13 while implementing
  WEB-064; **not** fixed there, because fixing it honestly means answering a
  question about the observations status line (see Open question).
- **Last updated:** 2026-09-13
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
