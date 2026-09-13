---
id: an-exported-prefix-says-so
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A CSV of a bounded read says it is a prefix

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`

## Context

The explorer reads observations through `fetchCollectionPages` and is honest
about the bound on screen:

```
loaded 40000 of 51234 county records; the page bound cut the answer short,
so the map is incomplete
```

The status pill turns `bad`, so a reader looking at the page cannot mistake
it. Then `exportCsv` writes those rows to a file whose name is
`{metric}-{grain}-{scope}.csv` and whose columns carry, by design, "its own
reproducibility envelope: which scope and release answered, and each row's
own published release identity". The one thing the envelope does not record
is that the answer was cut short — the fact the screen it came from led with.

A file outlives the screen that produced it. That is the whole argument
WEB-058 just made for the evidence packet's replay verdict, and it applies
here for the same reason: the person who opens this CSV next week, or the
person it is sent to, has only the file.

The bound is 8 pages of 5,000, so nothing in the catalog today reaches it —
the same standing as WEB-056, which refused to hand back a prefix from
`fetchAllPages` on exactly that basis: the signal is being discarded, not a
present miscount, and it is discarded in the one place that computed it.

## Acceptance criteria

1. The export knows whether the read it is writing was complete, and how many
   records the API reported. The loader already computes both and threw them
   away after rendering the status line.
2. A complete read's filename is unchanged, so nothing about today's exports
   moves.
3. A prefix names itself in the filename — what it holds and what the API
   reported — because the filename is the one part of a CSV that survives
   being opened in a spreadsheet, renamed columns and all.
4. The export is never refused. A reader may legitimately want the rows they
   have; the fix is that the file says what it is. (WEB-056's non-goals left
   this per-screen decision open, and this is the decision.)
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-059).

## Non-goals

- Raising the page bound, or paging further on export. 40,000 records is
  already past any list a person reads in a spreadsheet; a larger answer is a
  different request, not a bigger file.
- A per-row column. That the file is a prefix is a fact about the file, not
  about any row in it.

## Validation

**Failing first**, three nodes — the complete case included, because "a
complete read's filename is unchanged" is half the claim and a new naming
rule that quietly renamed every export would be a worse defect than the one
being fixed:

```
FAILED a complete read's filename is unchanged
FAILED a prefix names what it holds and what the API reported
FAILED a prefix of an unreported total still says it is a prefix
```

The naming rule was lifted out of the component into
`observationExportFilename` so it can be asserted at the unit tier rather
than only through a download in a browser.

**A mistake worth recording.** The first edit replaced
`scope: observationScope, release: selectedRelease,` with the gated
`asReleased ? "as_released" : "latest"` — and that pair occurs twice, so it
also clobbered the *request* built for `latestQuery`, silently changing what
the explorer asks the API for. `tsc` caught it (`Type 'string' is not
assignable to type 'ObservationScope'`), and the baseline was re-checked by
stashing to confirm the error was mine and not pre-existing. The request is
restored verbatim; only the filename is gated, which is what the old
`scopeSuffix` did.

**The gate matters.** The old name used `asReleased`, which is
`observationScope === SCOPE_AS_RELEASED && releasesDeclared` — not the raw
scope. A source that has no declared releases must not get a release-pinned
filename, and the new call passes the same gated value, so no export's name
changes except a prefix's.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | **307 passed** (was 304) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 77 passed (2.0m) |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit/shared` | 205 passed |

The browser tier passing unchanged is the rest of the "nothing moves" claim:
every export the suites exercise is a complete read, and every one keeps the
name it had.

**Register.** 359 rows.

## Remaining work

- None.
