---
id: explorer-saved-reduction
branch: feat/web-saved-reduction
depends_on:
  - saved-view-records-its-reduction
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A saved explorer view reopens as the view

## Plan status

- **Status:** Ready to claim. Authored 2026-09-13 as the consumer half of
  API-082, which is in `needs_review/`.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/savedAnalysis.ts`,
  `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** `saved-view-records-its-reduction` (API-082). Its two
  document fields must be served before this can store them.
- **Next pickup:** claim it, then start at "Acceptance criteria".

## Context

`explorerDocument` builds what the explorer saves: the kind, the metric, the
scope, a release under `as_released`, and the filters. It records no
reduction, because until API-082 the document had nowhere to put one.

So a saved map view does not reopen as the map. The explorer's map asks
`/observations` for `newest_per_geography=true`; the saved document replays
as `scope=latest` with filters only. For a source whose latest publication is
a series — Census PEP publishes every estimated year of the current vintage —
that is a different set of rows, and a map drawn from them colours whichever
row arrived last per polygon.

API-082 gave the document `newest_per_geography` and
`newest_release_per_period`, both defaulting to false and validated against
the same contradictions the live route refuses.

## Acceptance criteria

1. `explorerDocument` records `newest_per_geography` when the view asked for
   it, and `newest_release_per_period` when the view read a settled history.
2. It never records a contradiction the API would refuse — the document it
   builds is one the live route would serve, which is testable without a
   server.
3. Reopening a saved view replays the reduction: the request the explorer
   issues after loading a configuration carries what the document recorded.
4. A document stored before API-082, carrying neither field, reopens exactly
   as it does today.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing what the explorer saves besides the reduction.
- The comparison and article documents, whose routes serve no reduction.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything. This plan is unclaimed.
