---
id: a-packet-period-is-one-the-source-published
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A packet block's period is one the source published, never the moment the view was saved

## Plan status

- **Status:** Needs review. Investigated and authored 2026-09-13;
  delivered 2026-09-13 (`5669a3d`). The defect was present on every
  composed block; see "What changed" and "Validation".
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/evidencePackets.ts`, the three saved-chart
  producers

## Context

`evidencePackets.ts:271`:

```ts
period: text(chart.period) || text(chart.savedAt),
```

No producer writes `period` -- `SourceExplorerPage.tsx:1670-1692`,
`ComparisonWorkspace.tsx:515-531`, `ProfileProduct.tsx:346-356` all write
`savedAt: new Date().toISOString()` and nothing else -- so every analytical
block attached in the builder gets a period like
`2026-09-13T12:41:03.117Z`. The module's own docstring says "a field the
view never captured stays empty so `packetIssues` can report it rather than
a guess filling it in". Line 271 is the guess.

## Findings

- Three consequences: the envelope states a period no source published;
  `packetIssues` never reports `period` missing, so the packet reports
  itself complete; and `packetToDocument` ships the timestamp to
  `POST /evidence-packets` as `envelope.period`.
- `evidence-packets.test.js:29` and `evidence-packet.spec.js:22-36` fixtures
  hand-write `period: "2023"` onto a saved chart, a shape no code path
  produces: a WEB-043-class fixture on the local store, which is why the
  `savedAt` branch has never been exercised.

## Acceptance criteria

1. The fallback is removed; a view that captured no period yields an empty
   envelope period, and `packetIssues` names it.
2. The producers that know the period (the explorer's selected period, the
   profile's answered row) write it; the ones that do not leave it empty.
3. Fixtures model what the producers write; a unit test builds the saved
   chart through the producer's own helper rather than by hand.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-064 at authoring time).

## Non-goals

- Deriving a period from the rows a block displayed.

## What changed

- The fallback is gone. `period: text(chart.period)`, with the reason
  recorded where the guess used to be.
- `sharedObservationPeriod` (new, in `observationAccess`) answers the one
  period a set of rows describes, or `""` where they differ. It reads
  `observationPeriodLabel`, so a bounded period compares as its range and
  two different ranges do not share a period.
- The explorer writes it over the rows it loaded; the profile writes it over
  its answered measures' rows. The comparison writes none, by design: it
  carries one period per side, which is what WEB-049 exists to keep visible.

## On criterion 3

There is no shared chart-building helper to build a fixture "through": each
of the three producers writes its literal inline, and extracting three
component-local object literals into lib functions is a refactor this plan
did not ask for. What the defect was about — the period — is now produced by
one helper, and that helper is tested directly. The fixtures that hand-wrote
`period: "2023"` are left as they are because they now model exactly what
the explorer writes, and the browser fixture gains a third saved view — a
comparison, which legitimately captures no period — so the `savedAt` branch's
case is exercised end to end for the first time.

## Validation

- `npm --prefix apps/web run test:unit` — **358 passed** (354 before: +4).
- `npm --prefix apps/web run test:browser` — 84 passed before this plan's
  node; the new node passes and **both new nodes fail on the old fallback**:
  restoring `|| text(chart.savedAt)` fails the unit node and the browser
  node (`data-complete` reads `true` and the envelope carries the
  timestamp). `apps/web/lib/evidencePackets.ts` was restored byte-for-byte
  after each check.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `python -m tests.support.catalog_evidence` renders WEB-069 `FULL`.

## Remaining work

- None. Review is the remaining step.
