# SILVER Layer Transformation Documentation

## Purpose
The SILVER layer converts RAW source payloads into conformed analytic-ready fact tables keyed to shared reference dimensions.

SILVER is where schema normalization, duration derivation, dimensional joins, and quality instrumentation are enforced.

## Shared SILVER Pattern
Across Census ACS, BLS, and FRED:

1. Read staged RAW slices.
2. Parse temporal semantics into observation windows.
3. Normalize key fields and source metadata.
4. Join to silver_ref dimensions (time and optionally geography).
5. Track quality metrics (input rows, join misses, dedupe, write counts).
6. Write via idempotent merge strategy.

## Conformance Rules
- time_sk comes from silver_ref.dim_time.
- geo_sk comes from silver_ref.dim_geo for geography-aware sources.
- geo_id grammar is canonical and stable across layers.
- each SILVER fact table has a documented natural uniqueness key.

## Quality Instrumentation
Transforms should log:

- input and output row counts
- missing time/geo dimension counts
- deduplicated row counts
- load batch metadata

## Configuration-Agnostic Policy
- SILVER docs define transformation contracts independent of selected ingest scope.
- Selector profiles can change row coverage, but not table contracts.
- Document algorithmic behavior (join rules, duration mapping, idempotency), not fixed selector sets.
- Reference configuration modules for active scope selection.

## Source Docs
- See silver_ref_dimensions.md for shared reference dimensions.
- See census_acs_ingestion.md for ACS SILVER behavior.
- See bls_ingestion.md for BLS SILVER behavior.
- See fred_ingestion.md for FRED SILVER behavior.
