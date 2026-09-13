---
id: comparison-coverage-surfaced
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - comparison-geography-coverage
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The comparison screen says what its geographies are an intersection of

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row WEB-050.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/comparison.ts`,
  `apps/web/components/ComparisonWorkspace.tsx`,
  `apps/web/lib/api/types.ts`
- **Depends on:** API-087, which serves `geographies_a` and `geographies_b`.

## Context

`/comparison` joins its two reduced sides on geography identity with an inner
join, so `total` is the size of the intersection. API-087 made the route say
what that is an intersection of. Nothing reads it yet.

The screen currently reports:

```ts
message: `${pages.items.length} aligned geographies`
```

which is the same sentence the API used to answer with, and it reads as the
universe. A pair of a measure covering 3,143 counties against one covering
500 reports "500 aligned geographies", in green, with a map of 500 polygons
and a scatter of 500 points — and no indication that 2,643 counties publish
one of the two measures and are not here.

This is the consumer half of the defect API-087 closed, and the same shape as
WEB-036: a correct answer to a narrower question than the reader asked,
presented as the answer to theirs.

## Acceptance criteria

1. Where either side published more geographies than the comparison paired,
   the screen says so, naming both counts and both measures.
2. Where both sides published exactly what was paired, the screen says
   nothing extra — the note is a fact about this answer, not a standing
   disclaimer.
3. An API that publishes neither count — an older deployment — is not
   reported as having dropped anything.
4. The note is distinct from the page-bound shortfall WEB-039 reports: they
   are different facts with different fixes, and a comparison can have both.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the request or the paging.
- Listing the dropped geographies, which the API deliberately does not serve.

## What was built

`describeComparisonCoverage` reads the two counts API-087 serves and returns
one sentence naming both, or `""`. `ComparisonWorkspace` renders it under the
status pills, where it sits beside the count it qualifies rather than inside
it.

Three emptiness cases are deliberate and tested. Both sides publishing
exactly what was paired says nothing, so the note describes this answer
rather than standing on every comparison. An API publishing neither count —
an older deployment — says nothing either: `publishedCount` returns `null`
for an absent field rather than folding it to zero, which would have reported
every geography as dropped. And a malformed or negative count is treated as
absent for the same reason.

The note is kept separate from the WEB-039 page-bound shortfall, which lives
in the status pill. They are different facts with different fixes — one is
"this is an intersection", the other is "this is a prefix" — and a comparison
can have both at once.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Web unit | `npm --prefix apps/web run test:unit` | 271 passed (21 files) |
| Web browser | `npm --prefix apps/web run test:browser` | 69 passed |
| Lint | `npm --prefix apps/web run lint` | clean |
| Types | `npm --prefix apps/web run typecheck` | clean |
| Build | `npm --prefix apps/web run build` | succeeded |
| Bundle budget | `npm --prefix apps/web run check:bundle` | every route within budget |
| Register | `python -m pytest tests/unit -q` | WEB-050 is `FULL` |

Failing-first on both tiers: the three unit tests failed with
`describeComparisonCoverage is not a function`, and removing only the
rendered note fails the browser test on the missing element while everything
else still passes.

Not run, and why: the Docker-gated tiers (`make test-compose-smoke`,
`make test-web-smoke`, `make test-e2e`) need a container runtime this
environment does not provide.

## Acceptance criteria, as delivered

1. **Met.** Both counts and both measure codes, in one sentence.
2. **Met.** Asserted on both tiers.
3. **Met.** `test("an API that publishes neither count reports no shortfall")`.
4. **Met.** The browser test checks the status pill still reads as it did
   while the note is shown.
5. **Met.** `WEB-050` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `frontend` CI job, with `AUDITED_COUNTS["WEB"]` raised to 50.

## Remaining work

- None.
