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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  defect; WEB-036/WEB-056's rule unapplied on the product screen.**
- **Last updated:** 2026-09-13
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

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
