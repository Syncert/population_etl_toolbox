---
id: outbreak-calibration-sets
depends_on:
  - nursing-home-locations
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/cdc tests/unit/cms_ltc tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cdc or cms_ltc" -q
  - python -m pytest tests/dags -m dag -q
---

# Counties show recorded COVID-19 cases by month and age, and care homes their weekly cases

## Status

In progress. Step 8 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md):
real outbreak curves to check a model against.

- **Case surveillance: built** (`cdc_covid_cases`, ETL-135). Probes on
  2026-11-12: one whole state does not answer in one request (California
  timed out at 600 s), so the loader asks per state and month (about 2,900
  requests; California 2022-01 answered in 138 s). Each request groups by
  county, age group, `hosp_yn` and `death_yn`, so cases, hospitalised and
  deaths come from one capture, with `Unknown`/`Missing`/`NA` counted, not
  dropped. CDC writes `NA` for a withheld county.
- **CMS COVID-19 nursing-home weekly data: set aside.** The 2026-11-12 scan of
  `data.cms.gov/data.json` lists no dataset by that name on an answering path.
- **ILINet: route confirmed, to build.** FluView's own download service at
  `gis.cdc.gov/flu2/` answers the page's Download request. At state level CDC
  publishes unweighted ILI, ILI total, providers and patients; weighted ILI
  and the age bands are `X` (not published), so the age split is national
  and HHS-region only. National weekly back to 1997 week 40.

## Sources

1. **CDC COVID-19 Case Surveillance Public Use Data with Geography**
   (`n8mc-b4w4`), 106,219,500 case rows, 2020-01 to 2024-07, frozen, Public
   Domain U.S. Government. One row per case with county of residence, case
   month, age group, and hospitalisation and death fields. **Race and
   ethnicity fields are not requested.**
2. **CMS COVID-19 Nursing Home Data**, weekly resident and staff cases and
   deaths per facility, 2020 to 2024. Its page
   (`data.cms.gov/covid-19/covid-19-nursing-home-data`) is a script-only
   page and the `data.json` catalog lists no dataset by that name
   (2026-11-12). It is loaded only if CMS lists it on a path that answers
   an ordinary client (the main catalog's `data-api/v1` does); otherwise
   this part is set aside and said so.
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
  certification number (CCN).
- ILINet weekly by state, as published, with the reporting-provider count.
- All three are frozen or seasonal; the DAG requests only weeks or months
  not yet loaded.

## Size

Corrected to the measured 1,300 bytes per fact (first figures times about
6.5): case counts about 2 million rows, about 3 GB. CMS weekly about 3.5
million facility-weeks, about 20 GB. ILINet under 0.3 GB. About 23 GB in all.
Capturing the full case file instead would add about 100 to 130 GB; not
planned.

## Acceptance criteria

1. Dane County shows monthly case-record counts by age group 2020 to 2024
   with the suppression note; Dane nursing homes show weekly resident cases.
2. Wisconsin shows weekly ILINet as CDC publishes it for states (unweighted
   ILI, providers, patients); age groups at national and HHS-region level,
   since CDC marks state age bands `X`.
3. Speed gate under 500 ms.
