---
id: published-coverage-surfaced
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A published coverage qualifier travels with the value it qualifies

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row WEB-051.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/observationAccess.ts`,
  `apps/web/components/SourceExplorerPage.tsx`

## Context

FBI UCR is the one source the API refuses to serve through the per-source row
shape, and the registry says why:

> FBI UCR is deliberately absent: it publishes agency-level facts with a
> participation basis that this row shape cannot represent honestly. Its
> observation surface is the registry-dispatched neutral resource, whose
> envelope carries the participation and coverage semantics this shape
> cannot.

The neutral envelope carries six published fields for it — `population`,
`participated_population`, `coverage_percent`, `coverage_basis`,
`participation_status`, `population_denominator` — under `coverage`, and the
schema states their purpose: "a not-reported subject keeps `null` values;
coverage context explains the gap instead of the API inventing a zero."

The web reads none of them. `normalizeObservationRows` maps `uncertainty`
onto the row and leaves `coverage` behind; no table, chart, panel, or export
mentions it. The only place the word appears is `dataQuality.ts`, and only to
point at where the evidence lives — "the coverage object on FBI UCR
observation rows" — rather than to show it.

So the explorer renders an agency's offence count with no indication of the
participation it rests on. An agency covering part of its population reports
offences for that part, and the number without `coverage_percent` reads as
the whole jurisdiction; an agency that did not report keeps a null value
whose explanation sits in a field nothing displays. That is the defect the
API restructured a whole source to avoid, arriving one layer later.

## Acceptance criteria

1. The neutral envelope's `coverage` survives normalization onto the row
   shape the explorer's view models read, the way `uncertainty` already does.
2. The table shows the published participation for a source that publishes
   one, and does not grow an empty column for a source that does not — the
   presence is read from the loaded rows, not from a list of sources.
3. The CSV export carries every published coverage field verbatim, the way it
   already carries `margin_of_error` whether or not the source publishes one.
4. Nothing is computed, inferred, or reworded: a field the source did not
   publish stays absent rather than becoming a zero or a dash in the data.
5. The browser fixture models the served contract for FBI UCR, coverage
   included, so the client is exercised against the envelope the API actually
   serves (WEB-043).
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Judging the coverage. Whether 62% participation makes a number usable is
  the reader's call; this puts the published number in front of them.
- Adding coverage to the map or the chart. The value is what those present;
  the qualifier belongs with the row, where the reader can read both.

## What was built

`OBSERVATION_COVERAGE_FIELDS` names the six the envelope publishes, in the
order it declares them — not a client-authored shortlist, because an export
carrying a subset would be this client deciding which part of a source's
participation basis a reader may have. `observationCoverageValue` reads one,
and `publishesCoverage` answers whether any loaded row published a
participation.

The explorer's table gains a **Participation** column showing the published
status and, where one was published, the covered percentage. The column
appears only when a loaded row actually carries a participation, read from
the answer rather than from a list of sources, so a source that begins
publishing coverage is shown it without an edit here and one that does not
grows no empty column. The CSV export carries all six fields, always, the way
it already carries `margin_of_error` whether or not the source publishes one.

The browser fixture for FBI UCR now carries the coverage object the served
envelope carries. A fixture without it models a weaker contract than the one
that ships, and the client then goes untested for the field it is missing —
the WEB-043 lesson.

## A correction worth recording

The first version of this change also copied `coverage` across inside
`normalizeObservationRows`, beside the `uncertainty` mapping. Removing that
line and re-running the browser test showed it still passed: normalization
spreads the row it was given, so the envelope's `coverage` already survived
it, and the line was a no-op dressed as a fix. It was removed.

The defect was never that the field was dropped in transit. It was that
nothing read it. Failing-first was therefore re-established against the
change that actually matters — with the table cell removed and everything
else in place, the browser test fails on `element(s) not found` for the
participation cell — and the unit test that pins normalization now says
plainly that it holds by construction rather than by a change it needed.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Web unit | `npm --prefix apps/web run test:unit` | 275 passed (21 files) |
| Web browser | `npm --prefix apps/web run test:browser` | 71 passed |
| Lint | `npm --prefix apps/web run lint` | clean |
| Types | `npm --prefix apps/web run typecheck` | clean |
| Build | `npm --prefix apps/web run build` | succeeded |
| Bundle budget | `npm --prefix apps/web run check:bundle` | every route within budget |
| Register | `python -m pytest tests/unit -q` | 1433 passed; WEB-051 is `FULL` |

Not run, and why: the Docker-gated tiers (`make test-compose-smoke`,
`make test-web-smoke`, `make test-e2e`) need a container runtime this
environment does not provide. Nothing here changes a request, a served
response, or a deployment surface.

## Acceptance criteria, as delivered

1. **Met**, and found to hold already — see the correction above.
2. **Met.** `publishesCoverage(observations)` drives the column;
   `test("a source that publishes no participation grows no column for it")`
   covers the negative on the browser tier.
3. **Met.** All six fields, in the envelope's own order.
4. **Met.** `test("a source that publishes no coverage publishes none")` and
   `test("a not-reported agency keeps its explanation")`, which also pins
   that a published `0` stays a published `0`.
5. **Met.** The FBI fixture carries the coverage object.
6. **Met.** `WEB-051` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `frontend` CI job, with `AUDITED_COUNTS["WEB"]` raised to 51.

## Remaining work

- None.
