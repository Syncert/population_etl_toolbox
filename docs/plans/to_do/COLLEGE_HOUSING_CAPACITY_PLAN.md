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

Built 2026-11-12; the load follows the deploy. Step 5 of
[`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).

As built (probes 6 to 8 on the internal stack, 2026-11-12):

- Files: HD (directory, county code `COUNTYCD`, point), IC (`ROOM`, `ROOMCAP`)
  and EF A (`EFTOTLT` at the all, full-time and part-time levels). Registered
  from 2012: HD2008 has no county code or point and the 2002 files name the
  enrolment total `efrace24`; 2009 to 2011 were not checked.
- **2024 housing capacity has no located file.** IC2024 no longer carries
  `ROOM`/`ROOMCAP` (IPEDS 2024-25 moved them), the Data Center file list
  answers a plain client with a redirect, and names were not guessed further.
  Housing capacity is served through 2023 and labelled with its year.
- **Revised members:** a zip holding `<file>_rv.csv` beside the original is
  read from the revised member, NCES's current statement of that year (the
  institutions' corrections in the following collection). The capture keeps
  the whole zip, original included.
- DQ: `DQ-IPEDS-001` one row per institution (and level) per file, enforced;
  `-002` a figure marked not applicable or not reported carries no number,
  enforced; `-003` (WARN) institutions whose published point lies outside
  the county their code names.
- Dane County 2023: housing capacity 9,484 at 2 institutions (UW-Madison
  8,949, Edgewood 535); fall enrolment 69,371 at 8.
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
