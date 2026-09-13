---
id: distribution-caveats
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - distribution-period-honesty
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - npm --prefix apps/web run test:browser
---

# A distribution says what it could not carry

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/distribution_service.py`,
  `apps/api/schemas/analysis.py`, `apps/web/components/SourceExplorerPage.tsx`

## Context

API-096 gave the comparison a caveat naming the uncertainty it cannot carry:
Census ACS publishes `margin_of_error` and `margin_of_error_pct`, an aligned
comparison carries neither, and a `difference` is not more precise than the
estimates behind it. That was possible because the comparison response
already had a `caveats` array.

The distribution reads the same rows through the same reduction and has no
such array. `/distribution/bins` bins ACS county estimates into a histogram,
and the explorer paints a choropleth from it — bin boundaries drawn to the
value and nothing saying each value carries a margin that can straddle them.
API-097 has just given that response the other half of its own honesty, the
period its bins describe; this is the half API-096 established one route
over.

Naming it there and not here is the inconsistency: one analysis says what it
dropped and the other, reading the same published figures, does not.

## Acceptance criteria

1. `/distribution/bins` publishes a `caveats` array, and it carries the
   uncertainty note when the metric's source publishes one.
2. The note is the same one the comparison publishes, from the same helper
   and the same registry, so the two analyses cannot drift into describing
   the same source differently.
3. The field is additive — no existing field changes meaning — and the
   reviewed OpenAPI snapshot is regenerated deliberately.
4. The explorer presents the caveats beside the map the bins paint, in the
   caution treatment the other analytical notes use, and presents nothing
   when there are none.
5. Two `TESTING_CONTRACT.md` catalog rows: API-098 for the answer, WEB-055
   for the surface.

## Non-goals

- Widening the bins by a margin, or drawing an uncertainty band. Propagating
  a margin through an equal-width binning is a methodological decision this
  API does not make.
- Caveats about anything else. The compatibility rules are a comparison's
  business — a distribution has one metric, so there is nothing to reconcile.

## Validation

**Failing first**, on both sides. API-098: two unit nodes, one asserting the
published note is exactly the comparison's — read from the shared helper
rather than written into the test — and one that a source publishing no
uncertainty carries no caveat. WEB-055: with the explorer reverted, the
browser node fails on the note that is not rendered.

**One helper, two analyses.** `_uncertainty_caveat` became
`uncertainty_caveat` and is now called by both routes. The unit test asserts
`response.json()["caveats"] == [uncertainty_caveat(metric, "metric_code")]`,
so a change to the wording changes both or fails.

**Additive.** The regenerated snapshot changed by one line:

```diff
         "bin_count": "integer",
+        "caveats": "array<string>",
```

**Published as published.** `distributionCaveats` filters to non-empty
strings and renders them unchanged. The API names the source and the fields;
composing a sentence in the client would be restating a qualifier it did not
derive. The browser fixture carries the served note, so the tier models an
analysis that says what it dropped.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1459 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| Frontend units | `npm --prefix apps/web run test:unit` | 292 passed |
| Frontend browser | `npm --prefix apps/web run test:browser` | 77 passed |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 348 rows.

### Corrected after delivery

The shared note read "an aligned comparison carries neither … a difference or
a ratio", which is the wrong analysis on this route: a distribution derives
bins. The helper now names the source and "a derived value", which is true of
both, and the same-source duplication it caused on the comparison is gone.
Verified live on all three routes.

## Remaining work

- None.
