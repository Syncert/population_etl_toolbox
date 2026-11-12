---
id: college-housing-capacity
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/nces_ipeds tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "ipeds" -q
  - python -m pytest tests/dags -m dag -q
---

# Every college shows its enrolment and on-campus housing capacity

## Status

To do. Step 5 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).
Madison's campus is the district's densest young-adult cluster.

## Source

NCES Integrated Postsecondary Education Data System (IPEDS) complete data
files, public domain, from the IPEDS Data Center: institutional
characteristics HD (name, address, county code, latitude, longitude), IC
(dormitory capacity `ROOMCAP`, whether housing is provided), and fall
enrolment EF (headcount by attendance). The probe on the internal stack
confirms the file URL pattern and variable names (the Data Center page could
not be read from the cloud session) and how many years are published.

## Design

- New adapter `nces_ipeds` (the NCES Common Core adapter covers schools, not
  colleges; it is a separate collection with its own files).
- Facts per institution and year, as published; county from the published
  county code, checked against the point (WARN on disagreement).
- Gold institution list per county and county sums of dormitory capacity
  and enrolment, labelled as sums of published figures.
- API, catalog, DQ (`DQ-IPEDS-001..003`), DAG yearly.

## Size

About 6,000 institutions a year, a directory row and up to four figures each: about 30,000 rows a year, about 0.5 GB for 2012 to 2024 at the measured 1,300 bytes per row.

## Acceptance criteria

1. UW-Madison and every other Dane institution shows published dormitory
   capacity and enrolment by year.
2. Speed gate under 500 ms.
