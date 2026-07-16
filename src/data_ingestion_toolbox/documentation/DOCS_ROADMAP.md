# Documentation Rollout Roadmap

## Objective
Complete end-to-end documentation for RAW, SILVER, REF, and GOLD layers across Census ACS, BLS, and FRED.

## Current Status
Completed in this implementation pass:

- documentation/raw/README.md
- documentation/raw/census_acs_ingestion.md
- documentation/raw/bls_ingestion.md
- documentation/raw/fred_ingestion.md
- documentation/silver/README.md
- documentation/silver/silver_ref_dimensions.md
- documentation/silver/census_acs_ingestion.md
- documentation/silver/bls_ingestion.md
- documentation/silver/fred_ingestion.md
- documentation/CONFIGURATION.md
- documentation/README_VALIDATION.md
- scripts/check_readme_alignment.py

## Remaining Work

### Phase 2: Deepen Source-Level Technical Detail
- add explicit table/column contract tables per source file
- add concrete retry/exception matrices per source
- add DAG task flow diagrams and rerun procedures

### Phase 3: GOLD Integration Docs
- keep documentation/GOLD_SCHEMA_DOCUMENTATION.md as canonical
- add optional source appendices under documentation/gold if needed
- cross-link each SILVER source doc to its GOLD serving path

### Phase 4: Quality and Operations Docs
Create:

- documentation/quality/README.md
- documentation/quality/validation_rules.md
- documentation/quality/sla_freshness.md
- documentation/operations/ingestion_runbook.md
- documentation/operations/troubleshooting.md
- documentation/operations/ownership_matrix.md

### Phase 5: Verification Gate
- validate all documented object names against DDL
- validate all procedure names against gold SQL
- validate all sample SQL snippets against current table signatures
- perform terminology and cross-link consistency pass

## Authoring Standard
Each source-layer file should include:

1. Scope and target objects
2. Grain and natural key
3. Core transformation/ingestion steps
4. Idempotency and write strategy
5. Quality instrumentation and checks
6. Troubleshooting and runbook actions

Use configuration-agnostic wording for selector scope: selected/optional/configurable, not fixed mandatory lists.

## Maintenance Cadence
- update docs on any DDL, DAG, or config behavior change
- quarterly review for drift and broken links
- assign source owners for ACS, BLS, and FRED docs
