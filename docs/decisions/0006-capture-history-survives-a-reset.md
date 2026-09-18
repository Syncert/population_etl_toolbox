# ADR-0006: Capture history survives a beta reset

- **Status:** Accepted
- **Date:** 2026-09-16
- **Accepted:** 2026-09-16
- **Decision owners:** Repository owner (recorded in the
  [raw-capture retention plan](../plans/completed/WHAT_HAPPENS_TO_RAW_CAPTURE_WHEN_THE_WAREHOUSE_IS_RESET_PLAN.md))
- **Amends:** [ADR-0001](0001-data-layer-boundaries.md), the "Legacy transition" paragraph on beta resets

## Context

ADR-0001 says two things that were only compatible by accident.

It makes captures append-only and protects them with statement triggers
(`sql/migrations/001_raw_capture_control_foundation.sql`, DB-021): "a changed
checksum is a distinct source response and must not erase previous content".

It also says a full warehouse reset and re-ingestion "is an acceptable and
preferred cutover", and that append-only "does not prohibit intentionally
destroying and rebuilding a beta environment".

Those hold together only if a reset re-captures the same evidence. It does
not. `docs/reference/BETA_RESET_REINGESTION.md` re-ingests **current**
provider data, and providers do not serve their past:

- FRED publishes vintages through `realtime_start`/`realtime_end`; a capture
  taken today cannot reproduce what the series said last March.
- CDC supersedes releases, NASS publishes revisions, and FBI refreshes.
- A provider that changed a value between two captures is precisely the case
  the append-only rule exists to record, and precisely the case a reset
  erases.

`raw_capture` is therefore the one layer a reset cannot reproduce. Silver and
gold are deterministic functions of it. There was no backup or export path
anywhere in the repository, and the deployment keeps one Docker volume, so the
evidence and the database it lives in were lost or kept together.

## Decision

**Capture history survives a beta reset.** A reset destroys silver and gold,
which are reproducible, and never `raw_capture.*` or the `control` rows that
identify those captures.

Concretely:

1. An export path copies `raw_capture.payload_blob`,
   `raw_capture.response_capture`, and the `control.ingestion_run` and
   `control.ingestion_request` rows those captures reference, to a location
   **outside the database volume**, configured by the deployment. There is no
   default location: a default would be a path inside the container, and an
   export inside the container is destroyed by the reset it exists for.
2. The reset procedure restores that export **before** re-ingestion, so the
   first ingestion after a reset extends the capture history rather than
   restarting it.
3. The restore inserts and never updates. The append-only triggers stay in
   place for the whole restore: a load that has to disable them is a load that
   could rewrite history, which is the property being restored.
4. Every payload is verified against its own sha256 on export and again on
   restore. A payload file is named by its checksum, so the name is the
   verification.

### What is deliberately not carried forward

- **`control.capture_quarantine`.** It records a *parser's* failure against a
  capture, and it references captures rather than being referenced by them.
  Replaying restored captures through the current parser produces current
  quarantine state, which is the state worth having; restoring the old rows
  would reinstate a claim about a parser version that is no longer running.
- **The slice ledgers** (`control.*_ingestion_slices`). They are watermarks
  for planning work, not evidence of a response, and a reset intends to
  re-plan.
- **Silver and gold.** Reproducible by design; that is what makes a reset
  acceptable at all.

## Consequences

- ADR-0001's beta-reset paragraph is narrowed: "destroying and rebuilding a
  beta environment" means destroying silver, gold and the serving projections.
  It no longer includes the captures.
- The export is the largest artifact this repository produces --
  `payload_blob.payload` is `BYTEA` and holds every response body ever
  captured -- so it is written by a scheduled maintenance DAG rather than by
  hand, and one export per run rather than one growing file.
- A deployment that does not configure an export path gets a failing task
  saying so, rather than an export nobody can find. That is the intended
  behaviour: silently exporting nowhere is the failure this ADR is about.
- The round trip is a tested path, not a described one: the integration tier
  exports fixture captures, re-bootstraps the warehouse, restores, and
  verifies every restored capture under `DQ-SHARED-001`.

## Alternatives considered

- **Accept the loss and say so.** Honest, and it was the status quo by
  omission rather than by decision. Rejected: the repository's invariants ask
  to preserve revision history, and a reset that silently discards a
  provider's superseded values makes every downstream claim about revisions
  unfalsifiable.
- **Back up the whole database volume.** Simpler to operate, and it would also
  carry silver, gold and the serving tables -- which are reproducible, are the
  bulk of the data, and are exactly what a reset is trying to rebuild. It also
  makes the restore an all-or-nothing volume swap rather than a load that the
  append-only triggers can police.
- **Never reset.** Rejected in ADR-0001 for reasons this ADR does not reopen.
