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

- **Status:** To do. Investigated and authored 2026-09-13. **Present
  gap; the API-082/WEB-047 rule stopped one layer short.**
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

To be recorded by the agent that claims this.

## Remaining work

- Everything.
