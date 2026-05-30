# RAW Layer Ingestion Documentation

## Purpose
The RAW layer captures source API payloads in minimally transformed, append-safe tables and records each ingestion slice in a source ledger table.

This layer is the system-of-record for replay and idempotent reruns.

## Shared RAW Pattern
All three sources implement the same pattern:

1. Raw long fact table (source-grain observations).
2. Source metadata tables (datasets, variables, series, optional geo lookups).
3. Ingestion slices ledger table for planning, execution tracking, and rerun safety.

## Slice Ledger Contract
Each source has an ingestion slices table that tracks:

- slice identity (source-specific dimensions such as dataset/year/geo or domain/date)
- hash fingerprint for source scope
- status lifecycle (planned, running, success, empty, failed, skipped)
- row counts and timestamps
- error message fields for triage

## Idempotency and Replay
Idempotency is achieved with:

- uniqueness constraints on raw fact natural keys
- conflict-safe writes during ingest
- hash-based skip logic in DAG planning

Replay is supported by re-planning or re-running slices with the same key dimensions.

## Retry and Error Handling
Source ingest clients use retryable exception classes and bounded retries with backoff.

Non-retryable validation/config errors fail fast and are written to ledger status.

## Concurrency and Pools
Airflow pools should be used per API:

- census_api
- bls_api
- fred_api

Additional pool(s) may be used for downstream merge/serving steps.

## Configuration-Agnostic Policy
- Document contracts (tables, keys, constraints, ledger semantics) as stable behavior.
- Treat dataset/program/series selectors as optional scope configuration.
- Describe selected scope behavior without assuming any fixed list is mandatory.
- Keep examples illustrative and refer to source config modules for active profiles.

## Source Docs
- See census_acs_ingestion.md for Census ACS RAW behavior.
- See bls_ingestion.md for BLS RAW behavior.
- See fred_ingestion.md for FRED RAW behavior.
