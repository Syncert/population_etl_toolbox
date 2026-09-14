---
id: the-profile-card-says-its-read-was-bounded
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The profile card says when its value came from a bounded read

## Plan status

- **Status:** Needs review. Investigated and authored 2026-09-13;
  delivered 2026-09-13 (`0b2f6e4`). WEB-036/WEB-056's rule was
  unapplied on the product screen; see "What changed" and "Validation".
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/ProfileProduct.tsx`

## Context

`ProfileProduct.tsx:258-278` computes, for a page the bound truncated,
`state: "warn"` and the message "read N of M published rows; the page bound
cut the answer short, so this may not be the newest". The card renders the
message only when there is no row:

```tsx
{answer && !row ? (<small ...>{answer.message}</small>) : null}   // :555-557
```

The case the message exists for -- a row arrived, from a truncated page --
shows the number with no qualifier. The exported CSV writes the same string
into its `availability` column (`productTemplates.ts:436`), so the file
says what the screen does not.

## Findings

- A source whose capability entry does not declare `newest_per_geography`
  falls back to `limit=1000` (`observationAccess.ts:344-359`); with
  `total > rows.length` the card shows "568,203 persons" and nothing else.
- `profiles.spec.js:234-236` asserts `measure-answer-*` only for the no-row
  case; the warn path is unrendered and untested.

## Acceptance criteria

1. The card renders the answer's state and message whenever the state is
   not `ok`, beside the value.
2. A failing-first browser test serves a truncated page and asserts the
   qualifier is shown beside the number, and that the CSV and the card
   carry the same wording.
3. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `WEB-`
   identifier; WEB-065 at authoring time).

## Non-goals

- Changing the request. Whether the bound is reached is the API's answer.

## What changed

- The card's condition is `answer.state !== "ok"` rather than `!row`, with
  the reason recorded: the message exists for the case where a row *did*
  arrive from a truncated page, and that was the one case it was not shown
  for.

## Reaching the case at all

The bounded path needs a source whose `/observations` route declares no
`newest_per_geography`: where it is declared, `buildNewestValueRequest` asks
for one row and the answer is `ok` by construction. Every source in the
profiles fixture shares the real served parameter list, so the warn path was
not merely untested — it was unreachable from that fixture.

The browser fixture now has a `truncate` mode that declares the neutral
route through `servedParametersWithout("/api/v1/observations",
["newest_per_geography"])`, the helper WEB-043 left for exactly this: a
deployment serving an older contract, which ADR-0002 makes a real state
because an additive parameter lands in `v1` and a client meets deployments
on both sides of one. In that mode the card pages with `limit=1000` and
reduces locally, which is when "this may not be the newest" is true.

## Validation

- `npm --prefix apps/web run test:browser` — the new node passes and
  **fails on the old rendering**: restoring `answer && !row` leaves it
  unable to find `measure-answer-total-population`.
  `ProfileProduct.tsx` was restored byte-for-byte afterwards.
- The node reads the downloaded CSV's bytes as well as the card, so the two
  are asserted to carry the same wording rather than assumed to.
- `npx tsc --noEmit` (apps/web), `npm --prefix apps/web run lint`, and
  `npm --prefix apps/web run test:unit` (358 passed) — clean.
- `python -m tests.support.catalog_evidence` renders WEB-070 `FULL`.

## Remaining work

- None. Review is the remaining step.
