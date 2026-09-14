---
id: the-profile-shows-what-qualifies-its-values
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The profile product shows every field that qualifies its values

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/components/ProfileProduct.tsx`

## Context

WEB-051 and WEB-053 named this exact defect and fixed it in one screen. The
explorer's export comment states the rule it learned:

> Every published coverage field travels, the way `margin_of_error` already
> does whether or not the source publishes one: a file that carried a subset
> would be this client deciding which part of a source's participation basis
> a reader may have (WEB-051). The same rule, and the same reason, for every
> field `ObservationUncertainty` publishes: the export carried
> `margin_of_error` alone, so CDC's confidence bounds and USDA NASS's
> coefficient of variation — the figure NASS publishes a symbol for precisely
> to say an estimate is unreliable — were dropped from every file (WEB-053).

The profile product is still carrying `margin_of_error` alone. Both halves:

- **The screen.** Each measure card ends with
  `<small>Margin of error: {marginOfErrorText(row)}</small>` and nothing else.
  A CDC measure publishing `confidence_lower`/`confidence_upper`, or a NASS
  measure publishing the CV trio, renders "Margin of error: Not published" —
  which is true, and reads as "this value carries no published uncertainty",
  which is false.
- **The export.** `headings` carries one uncertainty column,
  `margin_of_error`, and no coverage column at all. It reads
  `row?.margin_of_error` directly rather than through
  `observationUncertaintyValue`, so it sees only what
  `normalizeObservationRows` happens to lift — which is `margin_of_error` and
  `margin_of_error_pct`, two of seven, and only for a neutral-shaped source.

This is not hypothetical for this screen. The shipped template configures a
CDC slot: the browser spec asserts `measure-reason-cdc-indicator` names
`CDC:cdi:ALC1_1:crude`. The slot is unfilled in the fixture, so the day the
catalog publishes it the profile shows a CDC value with its interval
suppressed — on a product screen, which is the polished surface a
non-specialist reads.

## Acceptance criteria

1. A measure card shows every uncertainty field the row published, not
   `margin_of_error` alone.
2. `marginOfErrorText` stays where it is used. It is not redundant with a
   generic label: it decodes the Census sentinel margins
   (`-555555555` → "0 (Census-controlled estimate)", `-222222222`,
   `-333333333`), which a field-value join cannot.
3. The fields beyond the margin are derived from
   `OBSERVATION_UNCERTAINTY_FIELDS`, not listed again, so a field added there
   reaches this screen without an edit — the rule `publishesUncertainty`
   already follows.
4. The export carries every field `ObservationUncertainty` and
   `ObservationCoverage` publish, read through the accessors that look in the
   nested envelope, exactly as the explorer's export does.
5. A row that published no uncertainty grows no new line, and a source that
   publishes none grows no empty columns beyond the declared ones.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-060).

## Non-goals

- Changing `marginOfErrorText`. Its sentinel handling is the reason it exists.
- A per-source list of which measures publish what. Read from the answer, as
  `publishesUncertainty` and `publishesCoverage` both do.

## Validation

**Failing first, on both tiers, and checked against the old code rather than
assumed.**

The unit nodes were written after the extraction, so the old columns were
restored inside `profileExport` and the suite re-run to prove they fail on
them:

```
FAILED a confidence interval reaches the file
FAILED the coefficient of variation and its unreliability flag reach the file
FAILED every published coverage field travels too
      Tests  3 failed | 10 passed (13)
```

The fourth node — an unfilled slot exporting its stated reason with an empty
value — passes on both, which is what makes it a regression guard rather than
evidence.

For the screen, the new card line was removed and the browser spec re-run:

```
1 failed
  a filled CDC slot shows the interval CDC published
```

**The scenario is the shipped template's, not an invented one.** The existing
spec asserts `measure-reason-cdc-indicator` names `CDC:cdi:ALC1_1:crude` —
the unfilled path. The new test publishes that identity and asserts the
filled one, so both halves of the slot's life are covered. It had to declare
CDC in `/catalog/capabilities` as well as publish it: a source the capability
map does not name cannot be asked at all, which is itself the product's
design working.

**`marginOfErrorText` was left alone deliberately.** It decodes the Census
sentinel margins — `-555555555` is a controlled estimate, not a negative
interval — which a field-value join cannot, so the card presents the margin
through it and everything else through
`OBSERVATION_UNCERTAINTY_BEYOND_MARGIN`, derived from the one field list by
filtering rather than written out again.

**Why the export moved out of the component.** Neither this export's rows nor
the explorer's were asserted anywhere — the only export coverage in the suite
is `expect(getByTestId("export-csv")).toBeEnabled()`. That is how a defect
named and fixed in one screen survived in the other. `profileExport` is now a
pure function beside `packetExport`, which was already this shape.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | **313 passed** (was 307) |
| Frontend browser | `npm --prefix apps/web run test:browser` | **78 passed** (was 77), 2.0m |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit/shared` | 205 passed |

**Register.** 360 rows.

## Remaining work

- None. One observation for a future agent: the explorer's export is still
  built inline in `SourceExplorerPage` and its rows are still asserted
  nowhere. Extracting it the way `profileExport` was extracted would put
  WEB-051 and WEB-053 under test rather than under review.
