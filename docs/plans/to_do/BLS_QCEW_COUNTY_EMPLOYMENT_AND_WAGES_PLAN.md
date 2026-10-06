---
id: bls-qcew-county-wages
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/bls_qcew -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_bls_qcew_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# BLS QCEW: county employment and wages by industry

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

"What do people do here, and what does it pay" is the most asked question
the almanac cannot answer. LAUS says how many residents are unemployed; it
says nothing about jobs located in the county or what they pay. The
Quarterly Census of Employment and Wages publishes establishments,
employment, and total and average weekly wages by industry for every county,
quarterly, from unemployment-insurance records rather than a survey. The
existing BLS configuration already notes that QCEW must not be forced into
the series-identifier model used for LAUS and CES
(`bls/config.py`), so it is onboarded as its own adapter package.

## What exists

- The BLS adapter (LAUS county, CES, CPI, JOLTS national) with its
  program-aware series model; its documentation explicitly separates
  household measures from establishment measures.
- The source-adapter starter (`docs/templates/source-adapter/`) and the
  checklist in `docs/reference/ADDING_A_DATA_SOURCE.md`.
- Shared raw capture and control plane, geography resolution by county
  FIPS, and the glossary publisher contract.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/bls_qcew/` from the
   starter: config without import-time I/O; a client for the QCEW open-data
   CSV slices (one file per area and period, no credential expected; verify
   against the official QCEW open-data documentation during implementation);
   capture-first raw storage with checksum and request fingerprint; a
   versioned parser contract for the published CSV layout.
2. **Scope registry.** Explicit registration of ownership (private, and
   total covered), aggregation level (county, state, national), industry
   level (total, NAICS sector), and the period range; nothing is requested
   that is not registered.
3. **Silver.** Facts keyed to the shared time and geography dimensions with
   industry, ownership, and aggregation level as dimensions; disclosure
   suppression codes preserved as withheld values, never zero; the
   provider's annual-average records kept distinct from quarterly records.
4. **Gold.** Deterministic publication of establishments, employment by
   month, total quarterly wages, and average weekly wage, per (geography,
   industry, ownership, period), with units and the establishment-based
   observation basis stated in the publisher contract so it can never be
   confused with LAUS's household basis.
5. **Serving.** Discovery and observation dispatch entries in
   `apps/api/registry.py`; the consumer guide gains a section on reading a
   QCEW row (ownership, industry, basis, suppression); the OpenAPI snapshot
   is updated.
6. **Quality and operations.** Declared data-quality rules (suppression
   preserved, period continuity, county identity resolution), a DAG, an
   operations guide under `docs/user-guides/`, a live source-contract module
   under `tests/external/`, and bootstrap and reset instructions.
7. **Web, last.** The Work and Money chapter of a county page gains "Jobs
   located here" cards and an industry-mix table labeled with the
   establishment basis, beside the LAUS cards labeled with the household
   basis; the two are never summed or shown as one series.

## Acceptance criteria

- Configuration imports without network, database, or secret access; a
  checked-in county CSV fixture is captured byte-for-byte and replays
  offline into silver with suppressed cells preserved as withheld.
- A malformed fixture is captured and quarantined with a sanitized record.
- Gold publishes the four measures per (geography, industry, ownership,
  period) with units and the establishment basis in the publisher contract;
  the glossary contract test passes.
- Re-running a slice is idempotent; a changed provider file for the same
  slice retains both checksums.
- `/api/v1/observations` serves a QCEW metric for a county fixture with
  industry and ownership dimensions on the row; capabilities advertise the
  source; the consumer guide documents the row semantics.
- Quality rules run against the fixtures; the DAG parses; the external
  contract module is registered in the scheduled credentials map and the
  external-contract workflow.
- The county page shows QCEW beside LAUS with distinct basis labels and no
  combined figure; a browser scenario asserts both labels.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- The exact open-data slice URL pattern, the annual-average period code, and
  the published field list and layout version (official BLS QCEW
  documentation first).
- Whether to onboard NAICS sector level in the first release or total
  covered employment only; sector level is what the industry-mix table
  needs, so prefer it if slice volume allows.

## Checkpoint

Next pickup: copy the starter into `bls_qcew/`, record the verified slice
URL pattern and layout in `config.py`, and write the failing replay test
over one county fixture.
