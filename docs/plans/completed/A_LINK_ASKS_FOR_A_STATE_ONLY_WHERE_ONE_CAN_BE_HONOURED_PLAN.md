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

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Claimed and completed 2026-09-13. **Superseded in part on 2026-09-13 by WEB-075**, the same day — see "What WEB-075 lifted, and what survives" below. The surviving rule is the saved-document one; criterion 1 and the link-gating half of criterion 3 no longer describe the repository.)
- **Last updated:** 2026-09-14
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
[`THE_STATE_CONTROL_NARROWS_WHAT_IT_CAN_NARROW_PLAN.md`](THE_STATE_CONTROL_NARROWS_WHAT_IT_CAN_NARROW_PLAN.md)
already records, and this plan updates it to say so.

## What WEB-075 lifted, and what survives

Recorded 2026-09-14, reviewing the delivered code rather than the plan text.

WEB-075 — the plan linked just above — lifted this plan's gate on the same
day it landed, for a reason it records: the state control became usable on
every source, so a requested state is honourable everywhere and refusing to
apply it was the defect rather than the fix. At head
`SourceExplorerPage.tsx:800-813` applies `requested.stateFips`
unconditionally, with a comment naming WEB-075, and the `WEB-066` row in
`docs/reference/TESTING_CONTRACT.md` is annotated "Superseded in part by
WEB-075".

- **Lifted:** criterion 1 (a requested state applied only where the source
  declares `state_fips`) and the link-gating half of criterion 3. The
  browser node at `tests/frontend/browser/explorer.spec.js:1204` is now
  shared, "Covers: WEB-066, WEB-075", and asserts the opposite of what this
  plan's Validation section describes: `toHaveURL(/state=55/)` and the state
  control enabled.
- **Survives, and is still the rule:** a saved document carries `state_fips`
  only where the request that produced it carried one
  (`SourceExplorerPage.tsx:1755-1759`), asserted by the same node —
  `filters.state_fips` undefined for Census PEP and `"55"` for Census ACS.
  That is the half this plan was really about: a stored analysis whose
  replay would be refused over a filter the reader never chose.

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

- `npm --prefix apps/web run test:browser` — the new node passed when this
  plan landed, and **failed without the gate**: removing the condition left
  `expect(page).not.toHaveURL(/state=55/)` failing, which also showed the
  unhonourable state reaching the shareable URL.
  `apps/web/components/SourceExplorerPage.tsx` was restored byte-for-byte
  after that check. **That assertion is no longer in the suite**: WEB-075
  replaced it the same day, for the reason recorded above, and the node now
  asserts the state *is* applied while still asserting the saved document
  carries `state_fips` only where the request did.
- Full tiers: see the commit; unit, browser, tsc and eslint all clean.
- `python -m tests.support.catalog_evidence` renders WEB-066 `FULL`.

## Remaining work

- None. Review is the remaining step.
