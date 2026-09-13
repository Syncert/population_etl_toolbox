---
id: an-envelope-records-its-reduction
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-retired-measure-is-not-served-as-current]
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api/test_evidence_packets.py -q
  - npm --prefix apps/web run test:unit
---

# A packet block's envelope records the reduction its query was viewed with

## Plan status

- **Status:** Needs review. Implemented 2026-09-13 as catalog rows API-120
  and WEB-071.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/schemas/evidence_packet.py`,
  `apps/api/services/evidence_packet_service.py`,
  `apps/web/lib/evidencePackets.ts`

## Context

API-082 and WEB-047 made a saved view record `newest_per_geography` /
`newest_release_per_period`, because "a view saved without one replays as
the whole publication, which for a source whose latest publication is a
series is a different set of rows". A packet block carries the same
document, and its `ReproducibilityEnvelope` (`extra="forbid"`) carries
`scope` and `release` but no reduction (`evidence_packet.py:39-64`). The
write-time cross-check compares `scope`, `release`, and geography
(`evidence_packet_service.py:115-155`) and never the reduction.

## Findings

- A map block composed from `/observations?...&newest_per_geography=true`
  records `period = "2024-07-01"` (one row per county). Stored with the
  document's reduction at its `False` default, the packet is 201 and
  valid, and replays as every estimated year under an envelope that
  declares one period.
- The guide's contradiction rule -- 422 naming the block "when an
  analytical block's envelope ... records a scope or release its query
  does not" -- lists the two duplicated request parameters the envelope
  had; the reduction is the third.

## Acceptance criteria

1. The envelope carries the reduction; a block whose envelope and document
   disagree on it is refused at write naming the block, and reported at
   read like the other contradictions.
2. The web builder writes the reduction into the envelope from the saved
   view (`evidencePackets.ts`), and the fixtures model what the app
   produces (WEB-043's rule).
3. Failing-first tests beside `test_a_block_cannot_name_one_geography_and_query_another`.
4. The guide and the OpenAPI snapshot carry the field.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (next free `API-`
   identifier; API-116 at authoring time).

## Non-goals

- Inferring a reduction from the row count.

## Validation

- `ReproducibilityEnvelope` carries `newest_per_geography` and
  `newest_release_per_period`, and `_contradiction` crosses them against the
  block's document the same way `scope` and `release` are crossed, in one
  loop over the two names.
- `_block_state` now calls `_contradiction` as well. A contradiction cannot
  be written, but it can be *stored before the field existed*: a row written
  while the envelope carried only `scope` and `release` keeps the reduction at
  its default beside a query that asks for one. It is reported before the
  warehouse is asked anything, because it is a fact about the document itself,
  and the composer's document is returned unmodified.
- Web: `envelopeFromSavedChart` records the reduction from the saved view, and
  `documentFromSavedChart` now takes it *from that envelope* rather than
  reading the chart a second time — the one way the two sides the API
  cross-checks could disagree. `envelopeToApi`/`envelopeFromApi` and
  `normalizeEnvelope` carry it in both directions. `reductionLabel` is the one
  wording: shown on screen beside the publication it narrows
  (`EvidenceEnvelope`) and written into the exported file as a `reduction`
  column, because the envelope's single `period` only reads correctly beside
  it.
- Fixtures model what the app produces (WEB-043): both round-trip envelopes
  carry the reduction, one of them `true`, with the block's query agreeing.
- New nodes:
  - three parametrisations of
    `test_evidence_packets.py::test_contradictions_are_refused_at_write_naming_the_block`
    — envelope-records-a-reduction-the-query-does-not-ask-for,
    query-asks-for-a-reduction-the-envelope-does-not-record, and the
    settled-history case under `scope=as_released`
  - `test_evidence_packets.py::test_a_reduction_a_stored_block_did_not_record_is_reported_on_read`,
    which builds the pre-rule row by popping the two fields from the stored
    envelope
  - `tests/frontend/unit/evidence-packets.test.js` > "the envelope records the
    reduction its document asks for"
  - the export column asserted in `evidence-packet-account.test.js`
- Break-test: leaving `envelopeFromSavedChart` at the defaults and emptying
  the crossed-field loop leaves `4 failed, 32 passed` in
  `test_evidence_packets.py` and `2 failed | 357 passed` in the frontend
  units.
- Tiers: `pytest tests/unit` 1552 passed; `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 157 passed, 2 skipped,
  14 deselected; frontend units 359 passed; browser tier 86 passed in 54.9s
  against a fresh `npm run build` (WEB-068 made the tier grade the build, so
  a local run after a source edit needs the rebuild first — run against the
  stale build, the two newest specs failed and nothing else did);
  `ruff format --check .`, `ruff check .`, `npm run lint` and `tsc --noEmit`
  clean.
- OpenAPI snapshot regenerated: `ReproducibilityEnvelope` gains the two
  booleans (39 operations, 56 schemas). The guide's contradiction rule now
  lists the reduction and says why, and states that a contradiction stored
  before the field existed is read rather than repaired.

## Remaining work

- None.
