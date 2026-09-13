---
id: a-link-asks-for-a-state-only-where-one-can-be-honoured
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A shared link asks for a state only where one can be honoured

## Plan status

- **Status:** Implemented; awaiting review. Claimed and completed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`

## Context

The explorer applies a shared link's requested selections, and it already
knows this needs care. Three lines apart, in the same function:

```tsx
if (requested?.stateFips) setSelectedStateFips(requested.stateFips);
if (requested?.geoId) setSelectedGeoId(requested.geoId);
// The requested scope is applied only where the source declares it;
// a link asking for an as-released read of a source that publishes
// none resolves to the latest publication rather than a 422.
if (requested?.scope === SCOPE_AS_RELEASED && servesAsReleased(source)) {
```

The scope is gated on what the source declares. The state is not — and
`state_fips` is exactly as per-source as `scope` is. Read from the registry,
Census PEP's `/observations` dispatch declares `geo_id`, `geo_level`,
`year_from`, `year_to` and no `state_fips`, because
`gold_pep.population_estimate_latest` carries no fips columns at all. (Every
other county-publishing source declares it; `geo_id` is declared by all
seven, so the line below is sound.)

So `/explore?source=pep&state=06` puts a state into a screen that cannot
honour one, with four consequences, none of them stated to the reader:

1. **The control holds it and cannot be cleared.** The state `<select>` is
   disabled unless the source declares the filter, and its value is
   `selectedStateFips` — so the link sets a state the reader has no control
   to remove.
2. **The map narrows and the rows do not.** `tileFilterForSelection` draws
   one state; `buildLatestObservationRequest` drops the undeclared filter
   through `declaredOnly`, so the table holds every published county. The
   status line says "loaded N county records" and nothing about the
   difference.
3. **The distribution narrows.** `/distribution/bins` takes `state_fips` as a
   route parameter for every source, so the legend describes one state while
   the table describes the nation.
4. **Saving the view is refused.** `explorerDocument` writes
   `filters.state_fips` whenever a state is set, and the API's saved-analysis
   validation refuses a filter the source does not declare — so the reader is
   told their save failed over a filter they never chose. This is precisely
   the "rather than a 422" the scope's own comment exists to prevent.

## Correction to WEB-064

WEB-064's plan says PLACE-with-a-state "is not reachable in the shipped app".
That is right for the *controls* and wrong for a *link*: `?state=06` reaches
it, which is how the county picker on Census PEP is escapable today at all.
Closing this hole closes that route too, so the picker dead end on PEP
becomes total — which is what
`docs/plans/to_do/THE_STATE_CONTROL_NARROWS_WHAT_IT_CAN_NARROW_PLAN.md`
already records, and this plan updates it to say so.

## Acceptance criteria

1. A requested state is applied only where the active source declares
   `state_fips`, exactly as the requested scope is applied only where the
   source declares as-released reads, with the same reason recorded.
2. Nothing else about link handling changes: `geo_id` is declared by every
   source and keeps being applied, and a state on a source that does declare
   the filter keeps working exactly as it does today.
3. A link carrying a state a source cannot honour leaves no state selected,
   so the control is clearable, the map and the rows describe the same
   scope, and saving the view is not refused for a filter the reader never
   chose.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-066), with the
   browser tier asserting what a reader sees after following such a link.

## Non-goals

- Making the state control usable on a source that cannot narrow its rows by
  state. That is the deeper question, with an honesty decision attached about
  what the observations status line then says, and it stays in the to_do plan
  that records it.

## What changed

- The requested state is gated on `sourceSupportsParameter(source,
  "state_fips")`, with the reason recorded beside the scope's own.
- The dashboard publishes `data-selected-state`, beside
  `data-selected-geo-id`. The browser assertion needs it: the control is
  disabled on such a source *and* its option list is empty until the
  projection answers, so it renders `""` whether or not a state was applied.
  The state that was applied is what narrowed the map and broke the save, so
  that is what the test reads.

## Validation

- `npm --prefix apps/web run test:browser` — the new node passes, and
  **fails without the gate**: removing the condition leaves
  `expect(page).not.toHaveURL(/state=55/)` failing, which also shows the
  unhonourable state reaching the shareable URL.
  `apps/web/components/SourceExplorerPage.tsx` was restored byte-for-byte
  after that check.
- Full tiers: see the commit; unit, browser, tsc and eslint all clean.
- `python -m tests.support.catalog_evidence` renders WEB-066 `FULL`.

## Remaining work

- None. Review is the remaining step.
