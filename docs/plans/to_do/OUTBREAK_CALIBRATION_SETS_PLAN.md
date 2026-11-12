---
id: outbreak-calibration-sets
depends_on:
  - nursing-home-locations
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/cdc tests/unit/cms_provider tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cdc or cms_provider" -q
  - python -m pytest tests/dags -m dag -q
---

# Counties show recorded COVID-19 cases by month and age, and care homes their weekly cases

## Status

To do. Step 8 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md):
real outbreak curves to check a model against.

## Sources

1. **CDC COVID-19 Case Surveillance Public Use Data with Geography**
   (`n8mc-b4w4`), 106,219,500 case rows, 2020-01 to 2024-07, frozen, Public
   Domain U.S. Government. One row per case with county of residence, case
   month, age group, and hospitalisation and death fields. **Race and
   ethnicity fields are not requested.**
2. **CMS COVID-19 Nursing Home Data**, weekly resident and staff cases and
   deaths per facility, 2020 to 2024. The probe confirms its current
   dataset id, licence and API (the CMS page could not be read from the
   cloud session).
3. **CDC FluView ILINet**, weekly influenza-like illness by national, HHS
   region and state, with age groups. The probe confirms the download route.

The New York Times county series is **not** used (non-commercial licence).

## Design

- Case surveillance is read through the Socrata API's own grouping
  (`$select=res_state, res_county_fips, case_month, age_group, count(*)
  $group=...`), split by state, so the captured responses are counts of
  CDC's published records per county, month and age group (and the same with
  hospitalised = Yes and death = Yes). The full 106-million-row file is not
  captured. CDC suppresses records in small counties; a county-month CDC
  withholds is shown as withheld, and the coverage note says counts are of
  records CDC published, not all cases.
- CMS weekly nursing-home cases join the facilities of
  [`nursing-home-locations`](NURSING_HOME_LOCATIONS_PLAN.md) by
  certification number.
- ILINet weekly by state, as published, with the reporting-provider count.
- All three are frozen or seasonal; the DAG requests only weeks or months
  not yet loaded.

## Size

Case counts about 2 million rows, under 0.5 GB. CMS weekly about 3.5
million facility-weeks, about 3 GB. ILINet under 50 MB. About 3.5 GB in all.
Capturing the full case file instead would add about 15 to 20 GB; not
planned.

## Acceptance criteria

1. Dane County shows monthly case-record counts by age group 2020 to 2024
   with the suppression note; Dane nursing homes show weekly resident cases.
2. Wisconsin shows weekly ILINet by age group.
3. Speed gate under 500 ms.
