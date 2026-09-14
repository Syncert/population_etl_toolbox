---
id: envelope-qualifiers-documented
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - published-uncertainty
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
---

# The guide names every field that qualifies a value

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/API_CONSUMER_GUIDE.md`

## Context

WEB-053 found the explorer dropping five of the seven fields
`ObservationUncertainty` publishes — CDC's confidence bounds and USDA NASS's
coefficient of variation with the status and symbol that qualify it. The
guide drops the same five, in prose:

> `uncertainty` is `null` when the source publishes none; otherwise margins
> of error, confidence bounds, or the CV trio.
> `coverage` carries FBI UCR participation context — a month nobody reported
> is `not_reported` with `null` value and a `participation_status`, not zero
> crime.

Measured against the reviewed snapshot, that names two of seven uncertainty
fields and two of six coverage fields. A consumer cannot code against "the CV
trio": `cv_symbol` is the flag USDA NASS publishes to say an estimate is
unreliable, and a reader who does not know it exists reads the estimate as
usable.

These two objects are the only ones the neutral envelope nests, and they
exist for one purpose: to say what a source published about a number so it
can be read honestly. Naming them field by field is not padding; it is the
document's own rule, "Reading a row honestly", applied to the fields that
make a row honest.

## Acceptance criteria

1. Every field of every object the neutral observation envelope nests is
   named in the guide, with which sources publish it.
2. The guard derives those objects from the reviewed snapshot — the schemas
   `NeutralObservation`'s own properties reference — so a qualifier object
   added later is covered without an edit, and it fails on a field the guide
   does not name.
3. No other schema is swept. The source-explorer rows pass a provider's whole
   classification through by design, and the guide says so once rather than
   listing sixty column names.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-102).

## Non-goals

- Documenting every response field of every route. Measured, that is 82 of
  224 field names undocumented, most of them provider passthrough columns the
  guide deliberately describes as a whole.
- Restating the sources' own definitions of their statistics. The guide says
  which field carries what a source published and where it came from; what a
  coefficient of variation means is the source's documentation.

## Validation

**Failing first**, with the gap measured rather than asserted:

```
AssertionError: the consumer guide does not name these published qualifier
fields: {'ObservationCoverage': ['coverage_basis', 'coverage_percent',
'participated_population', 'population_denominator'],
'ObservationUncertainty': ['confidence_lower', 'confidence_upper',
'cv_status', 'cv_symbol', 'cv_value']}
```

Nine fields, four of six and five of seven.

**Derived, not listed.** The guard walks `NeutralObservation`'s own
properties in the reviewed snapshot and takes whatever schema names they
reference — today `ObservationUncertainty` and `ObservationCoverage`. A
qualifier object added to the envelope later is swept without an edit here.

**Scoped deliberately.** Measured across every schema the observation and
analysis responses reach, 82 of 224 field names are undocumented, and most
are provider passthrough columns on the source-explorer rows, which the guide
describes as a whole on purpose. Sweeping those would pad the guide with
sixty column names and teach a reader nothing. The envelope's nested objects
are different in kind: they exist to say what a source published about a
number, which is the guide's own "Reading a row honestly".

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1467 passed |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 352 rows.

## Remaining work

- None. The 82 undocumented passthrough fields are recorded above as measured
  and deliberately out of scope, not as an oversight.
