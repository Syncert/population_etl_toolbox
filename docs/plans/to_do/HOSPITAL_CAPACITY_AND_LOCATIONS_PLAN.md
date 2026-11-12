---
id: hospital-capacity-and-locations
depends_on:
  - nursing-home-locations
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/cms_provider tests/unit/hhs_hospital_capacity tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cms_provider or hospital" -q
  - python -m pytest tests/dags -m dag -q
---

# Every hospital is on the map with its beds, and its weekly load during COVID-19

## Status

To do. Step 6 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).

## Sources

- **CMS Hospital General Information** (`xubh-q36u`), every
  Medicare-registered hospital, current list: CMS certification number,
  name, address, county name, hospital type, ownership, emergency services.
  A second dataset on the `cms_provider` adapter.
- **HHS COVID-19 Reported Patient Impact and Hospital Capacity by Facility**
  (healthdata.gov `anag-cw7u`), weekly per facility, 2020 to 2024-05-03,
  frozen, Public Domain U.S. Government. Kept: inpatient beds, staffed adult
  ICU beds, inpatient beds used, ICU beds used, COVID-19 inpatients, the
  facility's FIPS county code and geocoded address as published. The
  dataset writes small values as suppressed (-999999); that is kept as
  suppressed, never zero.

## Design

- The general list has no coordinates. Place each hospital by the HHS file's
  published geocode where the certification number matches; otherwise by its
  published county name within its state (`geography_status =
  'county_by_name'`). No address is geocoded by a third party.
- One frozen capture of the HHS file (paged Socrata reads with a per-page
  control table for resume); never re-requested once complete.
- Gold: hospital list per county; weekly series per hospital; county weekly
  sums of the published columns, with the count of reporting facilities and
  of suppressed cells beside each sum.

## Size

About 5,000 facilities x about 230 weeks x 8 columns: about 9 million facts,
about 3 GB with capture.

## Acceptance criteria

1. Dane hospitals listed with type and placement source; weekly beds and ICU
   use 2020 to 2024 as published, suppressed cells shown as suppressed.
2. Speed gate under 500 ms.
