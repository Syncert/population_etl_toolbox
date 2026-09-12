---
id: evidence-packet-persistence
branch: feat/evidence-packet-api
depends_on: []
parallel_safe: false
complexity: high
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 web-unit
  - ./tests/run.ps1 web-browser
  - ./tests/run.ps1 web-build
---

# Evidence packet persistence (ADR-0004)

## Plan status

- **Status:** Claimed 2026-09-12, the day ADR-0004 was accepted. Created
  directly in `in_progress/` because acceptance of the ADR was the approval
  the `to_do/` state would have recorded.
- **Last updated:** 2026-09-12
- **Owner surface:** `apps/api/schemas/evidence_packet.py`,
  `apps/api/services/evidence_packet_service.py`,
  `apps/api/routers/evidence_packets.py`, `apps/api/middleware.py`,
  `sql/bootstrap/002_app_api.sql`, `apps/web/lib/evidencePackets.ts`,
  `apps/web/components/EvidencePacketBuilder.tsx`,
  `apps/web/components/ComposedArticle.tsx`
- **Depends on:** [ADR-0004](../../decisions/0004-evidence-packet-persistence.md)
  accepted — **satisfied 2026-09-12**. No warehouse change: `app_api` is not
  warehouse content and validation only reads the glossary.
- **Unblocks:** item 1 under "Remaining before this plan is done" in
  `WEB_ANALYTICS_FIRST_WAVE_PLAN.md`.

## Implementation checkpoint

**Last updated:** 2026-09-12

**Current milestone:** EP-001 (this plan and the ADR's acceptance). Next is
EP-002, the shared request-body bound, because its exposure exists today
independent of packets.

**Next pickup:** EP-002.

- [ ] EP-001 ADR-0004 marked Accepted; plan claimed
- [ ] EP-002 shared request-body bound on the authenticated write paths
- [ ] EP-003 `/api/v1/evidence-packets`: schema, service, router, DDL
- [ ] EP-004 API evidence: unit denial paths, real-schema contract, guide,
      catalog, bootstrap docs
- [ ] EP-005 web: client, encode/decode, builder and articles on the account,
      local-draft migration, catalog row
- [ ] EP-006 close the WEB plan's item 1 and the handoff follow-on

## Objective

Give the evidence packet composer and the composed-article reader an
account-backed destination, so the last two screens persisting user
analytical state to `localStorage` move behind an authenticated, validated,
versioned API contract — and close the request-body exposure the ADR found on
the way.

The design is fixed by ADR-0004. This plan does not re-decide it; where an
implementation detail is not in the ADR, the ADR's rule decides it:
**refuse contradictions, report incompleteness.**

## Phases

### EP-002 — Shared request-body bound

There is no body-size limit anywhere in `apps/api`, and
`AnalysisDocument.filters`/`.visualization` are unbounded, so an
authenticated user can already store an arbitrarily large JSONB document.

- A pure-ASGI `RequestBodyLimitMiddleware` beside `SecurityHeadersMiddleware`,
  applied to every request, bounding the body by `content-length` and by
  bytes actually received (a chunked body with no declared length is bounded
  as it streams). Over the bound answers `413 {"detail": ...}` with a stable
  sanitized body, before any parsing.
- One bound, `API_MAX_REQUEST_BODY_BYTES`, default 262144 (256 KB): the
  ADR's packet cap, and far above any configuration. Public GETs carry no
  body and are unaffected.
- Sits inside the cache and the limiter — a rejected body must not be cached,
  and a refused write still spends analysis budget, which is right because
  the limiter exists to protect the database and a 413 never reaches it.

**Acceptance:** an oversize `POST`/`PUT` to either authenticated write
resource answers 413 without touching storage; a body exactly at the bound
is accepted; a chunked oversize body is refused as it streams. Catalog row
added.

### EP-003 — The resource

Exactly the ADR's schema, storage, and routes.

- `apps/api/schemas/evidence_packet.py`: `ReproducibilityEnvelope`,
  `PacketBlock`, `EvidencePacketDocument` (`schema_version`, not `version`),
  `BlockValidation`, `PacketValidation`, summary/list/detail/create/update
  models. `AnalysisDocument` reused unchanged for a block's query.
- `apps/api/services/evidence_packet_service.py`: owner-scoped SQL mirroring
  `saved_analysis_service` statement for statement; `validate_packet` refuses
  the ADR's contradiction table with a 422 naming the `block_id`; per-block
  read-time validation distinguishes *incomplete* (missing fields named)
  from *stale* (the reused `validate_document`'s reason). Metric codes are
  deduplicated across blocks and resolved once per request.
- `apps/api/routers/evidence_packets.py`: the five routes, `private,
  no-store`, 404 for another owner's id, `expected_version` → 409, mounted in
  `PUBLIC_ROUTERS` under the versioned root.
- `sql/bootstrap/002_app_api.sql`: `app_api.evidence_packet` above the GRANT
  block; re-running the file is the migration.
- The list summary carries `block_count` and `analytical_block_count` and
  **no validation**.

**Acceptance:** the ADR's condition table holds row by row under unit tests;
the real DDL round-trips create/read/list/update/delete with owner scoping
and concurrency enforced by PostgreSQL.

### EP-004 — API evidence and documentation

- Unit: `tests/unit/api/test_evidence_packets.py` — ownership and
  non-enumeration, every contradiction row, incomplete-stored-and-reported,
  stale-reported-not-repaired per block, concurrency, private no-store and
  outside the cacheable prefixes, list carries no validation, the body bound.
- Integration: `tests/integration/api/test_evidence_packet_contract.py` —
  the checked-in DDL against PostgreSQL, lifecycle, cascade on account
  delete.
- `API_CONSUMER_GUIDE.md`: a "Saved evidence packets" section naming every
  route (API-065 parses them), the contradiction/incompleteness split, the
  explicit statement that a list summary carries no verdict, and the 413.
- `TESTING_CONTRACT.md`: API-068 (body bound), API-069 (packet ownership and
  non-enumeration), API-070 (contradictions refused, incompleteness
  reported), API-071 (packet concurrency, privacy, list without verdict),
  API-072 (real-schema packet contract); DB-027 (packet storage, cascade);
  `AUDITED_COUNTS` and the register bumped; summary table corrected.
- `BETA_RESET_REINGESTION.md`: the one re-run step. `CI_EVIDENCE_MAP.md`:
  the new nodes under the existing API and database rows.

### EP-005 — Web

- `lib/api/types.ts` and `lib/api/client.ts`: packet types and the five
  calls, token only as a header, mirroring the configuration calls.
- `lib/evidencePackets.ts`: `packetToDocument`/`documentToPacket` at the
  snake/camel boundary; `readComposedPacket` unchanged for the local draft.
- `EvidencePacketBuilder`: one destination decision through
  `saveDestination`; the account whenever a token is held, the browser
  otherwise; destination stated on the control before and on the outcome
  after; a refused account save reported, never rewritten to the browser.
  Reopening an account packet loads it by id into the composer; updates send
  `expected_version` and a 409 is surfaced, not merged.
- `ComposedArticle`: signed in, lists the account's packets and renders the
  selected one with the API's per-block validation beside `packetIssues`;
  signed out, the local draft as today. **Nothing about a packet reaches the
  address bar.**
- `planLocalMigration`-style bridge for `builder-draft:v1`: a block the
  contract cannot describe is skipped with its reason; the local store is
  never cleared.
- `TESTING_CONTRACT.md`: WEB-031. Unit and browser specs for the builder and
  the reader on the account.

### EP-006 — Close the loop

Update `WEB_ANALYTICS_FIRST_WAVE_PLAN.md` item 1 from blocked to closed with
a pointer here, and the handoff's follow-on list. Move this plan to
`needs_review/`.

## Definition of done

- Every route in ADR-0004 is served under `/api/v1`, documented in the
  consumer guide, and covered by unit and real-schema evidence.
- Every row of the ADR's contradiction/incompleteness table has a test.
- The request-body bound protects both authenticated write resources.
- The builder and the reader save to and read from the account when a token
  is held, state their destination, and keep every privacy boundary in
  `WEB_FIRST_WAVE_HANDOFF.md`.
- Catalog, register, CI map, bootstrap docs, guide, and ADR are synchronized.
- All verify tiers pass with no unexpected skips; composed-service tiers that
  cannot run here are recorded as not run.

## Non-goals

Sharing, public packets, approval workflow, server-side rendering or export,
and any change to `/analysis-configurations`' document shape beyond the
shared body bound.
