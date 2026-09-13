---
id: packet-geography-contradiction
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A packet block cannot name one geography and query another

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/evidence_packet_service.py`

## Context

The evidence packet service states one rule and derives every check from it:

> **refuse contradictions, report incompleteness.** … a block whose envelope
> names a measure its own query does not ask for would display one measure's
> name over another measure's numbers — the precise failure the envelope
> exists to prevent, invisible to the client, and never made legitimate by
> later editing.

The contradiction table applies that to three fields — `metric_codes`,
`scope`, `release` — and `ReproducibilityEnvelope` says why:

> Only the fields that duplicate the block's query (`metric_codes`, `scope`,
> `release`) are cross-checked against it. The rest are observations about
> what the source published when the block was composed.

`geo_id` and `geo_level` are on the wrong side of that line. They are not
observations about what a source published — `period` and `units` are. They
are request parameters, the same names the block's own `document.filters`
carries and the same ones `/observations` accepts. So a packet can store, and
a reader can read, a block whose envelope says `state:06` over numbers its
query asked for `state:55`: one geography's name over another geography's
numbers, which is the failure the module's opening rule names, one identity
over.

`geo_level` brings its own wrinkle. API-092 promised the vocabulary's older
words keep answering, so an envelope holding `NATION` and a query asking
`NATIONAL` name the same grain and must not be a contradiction — the
comparison is the one `normalize_geo_level` defines.

## Acceptance criteria

1. A block whose envelope and query both name a geography, and name
   different ones, is refused at write, naming the block — as the measure,
   scope and release disagreements already are.
2. A block that records one and not the other is not refused. That is
   incompleteness, which this module reports on read and never repairs.
3. `geo_level` is compared through `normalize_geo_level`, so an alias and its
   vocabulary word are the same grain.
4. The envelope's docstring says which fields are cross-checked and why, and
   stays true.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-099).

## Non-goals

- Cross-checking `period`, `units`, `source_codes`, `transformation` or
  `caveats`. Those are what the composer saw, and the API substituting its
  present view for them is what the envelope exists to prevent.
- Refusing an envelope that names a geography its query does not filter to.
  A block can legitimately narrate one row of a many-geography answer; the
  contradiction is two different answers to the same question, which is how
  the scope and release checks already read.

## Validation

**Failing first.** Two parametrized cases, on the two fields:

```
FAILED …[envelope-names-another-geography]   envelope state:06, query state:55
FAILED …[envelope-names-another-grain]       envelope COUNTY,   query STATE
```

Both stored a `201` before the change, and the test asserts `storage.rows ==
[]` as well as the `422`, so a refusal that arrives after the write would not
pass either.

**And two that must keep passing**, which is the half that decides where the
line sits:

| Case | Verdict |
|---|---|
| envelope `NATION`, query `NATIONAL` | stored — one grain, through `normalize_geo_level` |
| envelope names a geography, query filters to none | stored — incompleteness, reported on read |

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1463 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 133 passed, 2 skipped |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**The docstring stayed true.** `ReproducibilityEnvelope` said only
`metric_codes`, `scope` and `release` are cross-checked "because the rest are
observations about what the source published". It now names the geography
among the cross-checked, and names the rest -- `period`, `units`,
`source_codes`, `transformation`, `caveats` -- as the observations they are.
That sentence is what made the gap findable, and leaving it stale would have
hidden the next one.

**Register.** 349 rows.

## Remaining work

- None.
