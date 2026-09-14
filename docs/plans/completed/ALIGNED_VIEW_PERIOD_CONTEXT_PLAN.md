---
id: aligned-view-period-context
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The aligned views say when a pair is not contemporaneous

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row WEB-049.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/comparison.ts`,
  `apps/web/components/ScatterChart.tsx`,
  `apps/web/components/ComparisonWorkspace.tsx`

## Context

The comparison route does not align its two sides to a shared period. It says
so in its own docstring, in the schema, and in the row it returns:

> The join is on geography identity; each row carries `period_a` and
> `period_b` so differing as-of context is visible, never implied away.

The table honours that. `periodsDiffer(row)` marks every row whose two sides
describe different periods, and the panel above it explains that the API
combines each side's newest value rather than aligning them.

The two *aligned views* do not. `periodsDiffer` has exactly one call site,
inside the table body. A geography pairing a 2023 value with a 2019 one is
drawn in the scatter as a point like any other, and coloured on the map by a
`difference` computed across those four years, with nothing on either panel
saying so.

Both panels are otherwise careful — the scatter says the axes assert no
shared unit, the map says the colour is API-derived and that a geography
with one side missing stays uncoloured — which makes the omission a real
gap rather than a general absence of care. The chart is also the artifact a
reader looks at instead of the table, so it is where an implied alignment
does the most work.

The map is the sharper half. It colours one number per polygon, and that
number is a subtraction between two publications that may be years apart;
"coloured by difference" reads as a difference *at a time*.

## Acceptance criteria

1. `comparisonScatterModel` reports how many plotted points pair values from
   different periods, and marks each point, so the chart draws from the same
   fact the table shows.
2. The scatter distinguishes those points visually and states the count in
   its caption and in its accessible description — it never drops them, since
   the values are published and real.
3. The comparison map's note states how many of the coloured geographies
   combine values from different periods.
4. A comparison whose rows all share a period says nothing extra: the note
   appears because a mismatch exists, not as a standing disclaimer.
5. A row missing one or both periods is not counted as differing — an absent
   period is not a mismatch — and stays plotted as it is today.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Excluding mismatched pairs from either view. Both values are published;
  hiding them would answer a narrower question than the one asked.
- Aligning the two sides to a shared period, which is the API's decision and
  a different question from the one `/comparison` answers.

## What was built

`ScatterPoint` carries `periodA`, `periodB`, and `periodsDiffer`, and
`ScatterModel` carries `differingPeriods`. Both come from `periodsDiffer`,
the same predicate the table already used, so the chart and the table cannot
say different things about the same row.

`ScatterChart` draws a non-contemporaneous point hollow — outlined in the
same colour rather than given a second one, because the difference is about
what the pair *is*, not about where it sits on either scale — puts both
periods in the point's title, states the count in the caption, and names it
in the SVG's accessible description so the fact is not carried by colour
alone.

`mapPeriodMismatchNote` counts only the geographies the map actually
coloured, because a geography whose derived field is null is not coloured
and saying it "combines values" would describe a polygon the reader cannot
see. It returns `""` when nothing differs and when the map is not drawn, so
the note is a fact about this answer rather than a standing disclaimer.

A row publishing one period and not the other is not a mismatch. `periodsDiffer`
already required both, and a test now pins that: an absent period is
incompleteness, and asserting a mismatch from it would state something the
row does not.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Web unit | `npm --prefix apps/web run test:unit` | 268 passed (21 files) |
| Web browser | `npm --prefix apps/web run test:browser` | 67 passed |
| Lint | `npm --prefix apps/web run lint` | clean |
| Types | `npm --prefix apps/web run typecheck` | clean |
| Build | `npm --prefix apps/web run build` | succeeded |
| Bundle budget | `npm --prefix apps/web run check:bundle` | every route within budget |
| Register | `python -m pytest tests/unit -q` | 1408 passed; WEB-049 is `FULL` |

Failing-first was confirmed on both tiers. On the unit tier the three model
assertions failed with `expected undefined to be 1` and
`mapPeriodMismatchNote is not a function`. On the browser tier, removing the
hollow marker and the two notes while leaving everything else in place fails
the new test on `toHaveCount(1)` for the marked point — the chart drawing a
2023-with-2024 pair indistinguishably from a contemporaneous one, which is
the defect.

One existing assertion moved with the model: "the scatter plots each
geography's own published pair" compared a point against a four-key literal.
It now carries the three new fields, with the fixture's own periods, which
also documents that the fixture pair is itself not contemporaneous.

Not run, and why: the Docker-gated tiers (`make test-compose-smoke`,
`make test-web-smoke`, `make test-e2e`) need a container runtime this
environment does not provide. Nothing here changes a served response or a
deployment surface.

## Acceptance criteria, as delivered

1. **Met.** `differingPeriods` on the model, `periodsDiffer` on each point,
   both from the predicate the table uses.
2. **Met.** Hollow marker, caption count, and `aria-label` text; the point is
   plotted either way.
3. **Met.** `mapPeriodMismatchNote`, rendered as `map-period-note`.
4. **Met.** Both notes are absent for a contemporaneous comparison, asserted
   on both tiers.
5. **Met.** `test("an absent period is not a mismatch")`.
6. **Met.** `WEB-049` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `frontend` CI job, with `AUDITED_COUNTS["WEB"]` raised to 49.

## Remaining work

- None.
