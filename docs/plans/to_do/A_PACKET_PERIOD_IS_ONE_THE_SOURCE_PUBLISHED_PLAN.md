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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect on every composed block.**
- **Last updated:** 2026-09-13
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

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
