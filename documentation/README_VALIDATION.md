# README Validation

## Purpose
README validation checks that key README references stay aligned with the current codebase.

This prevents stale command examples, outdated DAG names, and broken documentation pointers.

## Validation Script
Run:

```bash
python scripts/check_readme_alignment.py
```

Behavior:
- exit code 0: all checks pass
- exit code 1: one or more checks failed

## What Is Checked
1. Required DAG files exist:
   - dags/acs_ingest_dag.py
   - dags/bls_ingest_dag.py
   - dags/fred_ingest_dag.py
   - dags/silver_ref_dag.py
2. README.md does not contain deprecated names:
   - acs_raw_ingest_dag
   - bls_raw_ingest_dag
   - fred_raw_ingest_dag
   - CONFIG.geographies
   - CONFIG.curated_variables
3. README.md contains expected current references:
   - acs_ingest_dag.py
   - bls_ingest_dag.py
   - fred_ingest_dag.py
   - documentation/CONFIGURATION.md
4. README.md includes a Last Updated line with year 2026.

## Recommended Usage
- Run locally before opening a PR that changes README, DAG names, or configuration docs.
- Run in CI to block drift.

## Common Failures
1. Renamed DAG file but README still uses old name.
2. Config key examples in README do not match source config modules.
3. Missing link to configuration documentation.