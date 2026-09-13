---
id: a-packets-sources-are-the-sources-it-read
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and (redis or database) and not slow" -q
---

# A packet's stated sources are the sources its query read

## Plan status

- **Status:** Implemented; awaiting review. Claimed and completed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/evidence_packet_service.py`,
  `apps/api/services/saved_analysis_service.py`

## Context

`_contradiction` in the packet service opens on the rule it exists to
enforce, and crosses the envelope against the query field by field: a
measure the envelope names that the query does not ask for, a scope or a
release they disagree on, a geography or a grain they disagree on. Its own
comment states the stake:

> Left uncrossed, a packet could store one geography's name over another
> geography's numbers — the failure this module's opening rule names, one
> identity over (API-099).

`source_codes` is not crossed, and it is exactly the same kind of field. The
sources a query reads are not composer opinion: they are the owning sources
of the metrics it asks for, which the glossary resolves and this function's
own caller already resolves for every document it validates.

Read off `validate_packet`, with the metric resolution stubbed:

```
ACCEPTED  envelope names a source the query never read   (source_codes=["BLS","CDC"])
ACCEPTED  envelope names no source at all                (source_codes=[])
ACCEPTED  envelope names the right source
refused   envelope names a measure the query does not ask for
```

The first line is a stored packet whose every number came from FRED and
whose envelope says the evidence came from BLS and the CDC.
`EvidenceEnvelope.tsx` renders `envelope.sourceCodes` to the reader as
"Sources", and `packetExport` writes a `source_codes` column, so the wrong
provenance reaches both the screen and the file a packet is handed to
someone else as. ADR-0004's whole premise is that a packet is a document you
hand to someone else.

The second line is not the same defect: an empty `source_codes` is
*incompleteness*, and `_REQUIRED_ENVELOPE_FIELDS` already names it so the
read reports the field as missing. It stays stored and reported.

## Acceptance criteria

1. An analytical block whose envelope names a source outside the owning
   sources of the measures its query asks for is refused at write, naming the
   block and the stray source, the way the measure check already reads.
2. The owning sources are resolved from the published glossary, not from the
   spelling of a metric code, and resolved once per distinct document — the
   caching the document verdicts already use, because a packet reuses three
   or four measures across a dozen blocks.
3. An envelope that names nothing, or a block still missing its query or its
   envelope, is unchanged: incompleteness is reported on read, never refused.
4. A source spelled in another case is one source, not a contradiction. The
   catalog publishes upper-case codes and a composer's record of the same
   source is the same fact.
5. The consumer guide's packet section says the envelope's sources are
   checked against the query's measures.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-113).

## Non-goals

- Cross-checking `units`. A block may have transformed the value — the
  envelope carries `transformation` beside `units` for that reason — so a
  unit that differs from the measure's published one is not a contradiction.
- Cross-checking `api_query` against the document. It is the strongest
  reproducibility statement in the envelope, but a legitimate recording can
  differ in parameter order, in defaults spelled out, and in parameters the
  document does not model. Refusing on a string comparison would refuse
  correct packets.

## What changed

- `_stray_sources` crosses the envelope's `source_codes` against the sources
  the block's query reads, refusing a stray one by name. Upper-cased on both
  sides, and `None` for a block with no envelope.
- Where the sources come from mattered. A first pass resolved each metric
  again through `resolve_metric`, and `test_repeated_measures_are_resolved_once_per_request`
  caught it immediately: that node pins two lookups of the one distinct query
  across twelve blocks, and a third appeared. So `validate_document` now
  *answers* the sources it already resolved (`_owning_sources`), and
  `validate_packet` caches that per distinct document beside the verdict. The
  crossing costs no lookup at all.
- The guide's contradiction bullet names the source check, says why the
  sources are not the composer's field to decide, and says case is not a
  contradiction.

## Validation

- `pytest tests/unit` — **1506 passed** (1502 before: +4 nodes, two of them
  parametrised contradiction cases).
- **The tests fail without the crossing.** Commenting out the two lines in
  `validate_packet` leaves `2 failed, 29 passed` in the packet module.
- `pytest tests/integration -m "integration and (redis or database) and not
  slow"` — 145 passed, 2 skipped, 14 deselected.
- `ruff format --check .` / `ruff check .` — clean (442 files).
- `python -m tests.support.catalog_evidence` renders API-113 `FULL`; the
  register is 372 rows.

### Ground truth

Before the change, against `validate_packet` with the metric resolution
stubbed:

```
ACCEPTED  envelope names a source the query never read   (["BLS","CDC"])
ACCEPTED  envelope names no source at all
ACCEPTED  envelope names the right source
refused   envelope names a measure the query does not ask for
```

After:

```
refused   envelope names a source the query never read: block 'b1' names
          source(s) BLS, CDC in its envelope that its query does not read
ACCEPTED  envelope names no source at all
ACCEPTED  envelope names the right source
```

The shipped client composes `sourceCodes` from catalog source codes —
`selectedMetricMeta.source_code` in the explorer, `comparison.source_code_a`
in the comparison workspace — so nothing it writes is refused.

## Remaining work

- None. Review is the remaining step.
