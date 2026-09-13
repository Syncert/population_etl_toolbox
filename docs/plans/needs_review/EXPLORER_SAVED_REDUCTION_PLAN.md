---
id: explorer-saved-reduction
branch: claude/iterate-plans-improvements-ir885c
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

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as the consumer half of API-082, which is in `needs_review/`.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/savedAnalysis.ts`,
  `apps/web/components/SourceExplorerPage.tsx`
- **Depends on:** `saved-view-records-its-reduction` (API-082). Its two
  document fields must be served before this can store them.
- **Next pickup:** none. Delivered as catalog row WEB-047.

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

## What was built

`AnalysisDocument` gained the two optional fields API-082 serves, and
`explorerDocument` now records them. Each reduction is kept only under the
scope that accepts it, and a pinned release is dropped alongside a settled
history: the client refuses the same four contradictions the live route
does, so the document it builds is always one the API would serve.

`SourceExplorerPage` reads the claim back from the request the map issues
rather than asserting it. The `latestQuery` memo asks for
`newest_per_geography` unconditionally; `buildLatestObservationRequest`
drops it where the source's capability entry does not declare the
parameter. Hoisting the built request into a `latestRequest` memo — which
the displayed API query already needed — lets the save record the question
that was actually sent. A source that cannot reduce is therefore never
saved as though it had been.

One assertion moved. "An explorer selection saves its query, never its
values" proved that nothing observation-shaped is stored by searching the
serialized document for the substrings `value` and `period`, which read the
new configuration key `newest_release_per_period` as a stored datum. It now
walks the document's own keys for the names an observation arrives under,
which is what the assertion meant.

## What this does not do

The explorer never stores `newest_release_per_period`, and that is correct
rather than missing. Its settled history (WEB-046) is a fallback *under*
`scope=latest`, taken when a source's latest relation holds one row per
geography; the reduction can only travel with `scope=as_released`, which the
API enforces and `explorerDocument` mirrors. The field is recorded and
tested at the document level because the contract serves it and other
producers of a document — an as-released view that reads a settled history —
can set it; the explorer's own map has no such view today.

The legacy browser-chart import (`migrationCandidates`) still stores neither
reduction. It knows a metric code and nothing about the source that publishes
it, so claiming a reduction there would be the client asserting a capability
it never read. Those documents reopen exactly as they do today, which is
criterion 4.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Web unit | `npm --prefix apps/web run test:unit` | 257 passed (21 files) |
| Web browser | `npm --prefix apps/web run test:browser` | 64 passed |
| Lint | `npm --prefix apps/web run lint` | clean |
| Types | `npm --prefix apps/web run typecheck` | clean |
| Build | `npm --prefix apps/web run build` | succeeded |
| Bundle budget | `npm --prefix apps/web run check:bundle` | every route within budget |
| Catalog guards | `pytest tests/unit/shared/test_repository_hygiene.py` | 12 passed |

The browser test was confirmed failing-first: with the save's
`newestPerGeography` argument removed, "a saved map view records the
reduction the map asked for" fails on
`expect(document.newest_per_geography).toBe(askedForTheReduction)` —
`Expected: true, Received: false` — which is the defect this plan names.

The browser tier needs `PLAYWRIGHT_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium`
in this environment; the default headless-shell path Playwright resolves is
not the browser that is installed here.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`, `make test-web-smoke`) need a
container runtime this environment does not provide. Nothing in this change
touches a migration, a compose service, or a served response body.

## Acceptance criteria, as delivered

1. **Met**, with the scope limit recorded above: the explorer records
   `newest_per_geography` from the request it issued;
   `newest_release_per_period` is recorded by `explorerDocument` and proved
   by unit test, and the explorer has no as-released view that asks for it.
2. **Met.** Four contradictions are dropped rather than stored, each proved
   without a server in `tests/frontend/unit/saved-analysis.test.js`.
3. **Met.** `reopenHref` carries the document's metric, filters, scope, and
   release into the explorer, which rebuilds the same capability-bounded
   request; the browser test asserts the stored reduction equals the one the
   map's own `/observations` request carried, so the two cannot drift.
4. **Met.** Both fields default to `false`, and a document carrying neither
   reopens through the unchanged path.
5. **Met.** `WEB-047` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `frontend` CI job, with `AUDITED_COUNTS["WEB"]` raised to 47.

## Remaining work

- None.
