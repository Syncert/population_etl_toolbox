# Configuration Guide

## Purpose
This document explains how configuration works across the pipeline without assuming any fixed dataset, measure code, or series selection.

Configuration controls scope and behavior, while schema contracts remain stable.

## Principles
1. Documentation should describe contracts, not a single selector profile.
2. Source selection is optional and environment-specific.
3. Config values should change ingestion scope, not table structure.
4. Runtime controls (concurrency, chunking, pool behavior) are distinct from selector controls.

## Configuration Layers

### Required Runtime Configuration
- Database connection id
- API credentials (where applicable)
- Environment and execution context

### Optional Scope Configuration
- ACS: selected datasets, selected table/variable scope, selected geography levels
- BLS: selected programs, selected selectors by program
- FRED: selected series/domain scope

### Optional Performance Configuration
- API concurrency limits
- API spacing/backoff knobs
- Chunk sizes
- Task concurrency controls

## Source Configuration Modules
- census_acs/config.py
- bls/config.py
- fred/config.py

Treat examples in those files as profiles, not hard requirements.

## Contract vs Scope

### Stable Contract (Document in Layer Docs)
- table names
- column definitions
- keys and constraints
- refresh and idempotency behavior

### Variable Scope (Document as Optional)
- which datasets/tables/series are selected
- which geographies are enabled
- which programs are enabled

## Selector and Measure Configuration by Source

### Census ACS (census_acs/config.py)
Selector model:
- datasets controls dataset type (for example acs1, acs5)
- curated_tables controls selected table groups
- geo_levels controls geography scope

How to add ACS scope safely:
1. Add or remove dataset codes in datasets.
2. Add table identifiers in curated_tables (for example B01003, B19013).
3. Confirm selected tables are valid for selected datasets and years.
4. Run metadata sync and a small ingestion test before full backfill.

Important:
- ACS selector unit is table scope, not BLS-style measure code.
- Variable expansion happens from metadata for selected tables.

### BLS (bls/config.py)
Selector model:
- programs enables source program families (la, ln, ce, cu, jt)
- curated_by_program stores per-program selectors

How to add BLS selectors safely:
1. Ensure target program is enabled in programs.
2. Add selector values under curated_by_program[program].
3. Respect program-specific selector grammar:
	- LAUS (la): selector values are measure codes (for example 03, 04, 05)
	- LN/CE/CU/JT: selector values are full series identifiers
4. Validate unit semantics and comparability before promoting to dashboard-facing metrics.

Important:
- BLS does not use one universal selector format across programs.
- Treat each program as a separate instrument class.

### FRED (fred/config.py)
Selector model:
- curated_series_ids is the canonical selected series list
- curated_by_domain is an optional organizational grouping
- domains defines logical groups, not separate schema contracts

How to add FRED series safely:
1. Add series identifiers to curated_series_ids.
2. Optionally add grouping in curated_by_domain.
3. Validate series availability and frequency semantics.
4. Run a bounded date-window ingestion test before full historical refresh.

Important:
- FRED selector unit is series id.
- Domain grouping is organizational and should not change table contracts.

## Recommended Documentation Wording
- Use: "selected scope", "configured scope", "optional selectors"
- Avoid: language that implies fixed "curated" lists are mandatory for pipeline correctness

## Operational Guidance
1. Validate configuration before running ingestion.
2. Keep selector changes versioned and reviewed.
3. Run README validation after config or DAG changes.
4. Re-run source docs drift checks when selector strategy changes.

## Related Docs
- README.md
- documentation/README_VALIDATION.md
- documentation/raw/README.md
- documentation/silver/README.md