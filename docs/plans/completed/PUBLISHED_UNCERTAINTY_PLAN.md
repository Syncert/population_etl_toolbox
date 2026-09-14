---
id: published-uncertainty
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A published uncertainty travels with the value it qualifies

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/observationAccess.ts`,
  `apps/web/components/SourceExplorerPage.tsx`

## Context

WEB-051 established the rule for one of the neutral envelope's two qualifier
objects: a published coverage qualifier travels with the value it qualifies,
into the table and verbatim into the export, because "an agency covering
61.9% of its population showing an offence count that reads as the whole
jurisdiction" is a number the reader cannot correct for.

The other qualifier object never arrives at all. `ObservationUncertainty`
publishes seven fields across three sources:

| Source | Published | Reaches the explorer |
|---|---|---|
| Census ACS | `margin_of_error`, `margin_of_error_pct` | `margin_of_error` only |
| CDC | `confidence_lower`, `confidence_upper` | no |
| USDA NASS | `cv_value`, `cv_status`, `cv_symbol` | no |

`normalizeObservationRows` lifts exactly two names out of the nested object —
the two the legacy source-scoped shape already had — and the other five stay
inside `row.uncertainty`, which neither the table nor `exportCsv` reads. The
explorer's table has no uncertainty column at all.

So a CDC prevalence estimate is shown and exported as a point estimate with
its published interval dropped, and a USDA NASS estimate is shown without the
`cv_symbol` NASS publishes precisely to say the estimate is unreliable. That
is the same defect WEB-051 named, with a sharper edge: a coverage percent
qualifies how much of a population a count covers, and a CV symbol says
whether the number should be used at all.

## Acceptance criteria

1. Every field `ObservationUncertainty` publishes reaches the CSV export,
   verbatim, in the order the envelope declares them — not a subset this
   client chose.
2. The explorer's table shows a row's published uncertainty beside its value,
   naming each field rather than composing a number the source did not
   publish.
3. The column appears only when a loaded row actually published an
   uncertainty, read from the answer rather than from a list of sources, so a
   source that publishes none grows no empty column.
4. A field a source does not publish stays absent, never a zero or a dash in
   the exported data.
5. The field list is read from the reviewed contract in the tests, so a field
   added to the envelope fails them rather than being silently dropped.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-053).

## Non-goals

- Rendering an interval or a margin as a formatted range (`12.1 – 13.9`,
  `± 1.5`). The three sources' fields are not interchangeable, and composing
  them into one notation is this client deciding what the numbers mean.
- Charting the uncertainty. A band on the series is a real feature and its
  own work; this is about not losing what the API publishes.
- The comparison workspace and evidence packets. They read the same envelope
  and deserve the same rule; the explorer is where the export lives.

## Validation

**Failing first.** With the explorer reverted and the tests in place, the
browser tier reports the column that does not exist:

```
Error: expect(locator).toContainText(expected) failed
Locator: getByTestId('uncertainty-state:55|county:025').first()
  - waiting for getByTestId('uncertainty-state:55|county:025')
1 failed
19 passed
```

**What it carries now.** Seven fields, in the order the envelope declares
them, into the export and into a column that appears only when a row
published one:

| Source | Published | Exported before | Exported now |
|---|---|---|---|
| Census ACS | `margin_of_error`, `margin_of_error_pct` | `margin_of_error` | both |
| CDC | `confidence_lower`, `confidence_upper` | neither | both |
| USDA NASS | `cv_value`, `cv_status`, `cv_symbol` | none | all three |

The table names each field (`cv value 14.7 · cv status unreliable ·
cv symbol (D)`) rather than composing a notation. A margin, an interval and a
coefficient of variation are not interchangeable, and rendering them as one
would be this client deciding what three sources' numbers mean.

**The list is read from the contract**, not written here:
`servedSchemaFields("ObservationUncertainty")` reads the reviewed snapshot, so
a field added to the envelope fails the unit tier rather than being dropped
from every export. The same assertion is made for `ObservationCoverage`,
which strengthens WEB-051's list the same way.

**The fixture was part of the defect.** `cdcRow` published no `uncertainty`
at all, so the browser tier modelled a CDC that publishes no interval — the
exact failure mode WEB-043 exists to name. It now carries the bounds CDC's
dispatch entry declares, `null` on the suppressed row.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | 286 passed (was 280) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 73 passed (was 71) |
| Frontend lint | `npm --prefix apps/web run lint` | clean |
| Frontend typecheck | `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit` | 1452 passed |

**Register.** 343 rows.

## Remaining work

- None. The comparison workspace and the evidence packet builder read the
  same envelope and are named under Non-goals; applying the rule there is its
  own work.
