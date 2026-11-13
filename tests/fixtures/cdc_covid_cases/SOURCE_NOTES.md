# CDC COVID-19 case surveillance fixtures (n8mc-b4w4)

Captured from data.cdc.gov on 2026-11-12 by the read-only outbreak probes run
on the internal stack (toolbox user agent, no key).

- `n8mc_wi_2021-11.json` and `n8mc_ca_2022-01.json`: the loader's exact slice
  request (probe 10), `$select=county_fips_code, age_group, hosp_yn, death_yn,
  count(*) AS n`, grouped and ordered by the same columns, `$limit=50000`, for
  Wisconsin 2021-11 (1,506 groups, 82,251 records; Dane County 55025 3,651
  records, 83 hospitalised) and California 2022-01 (1,114 groups, 2,808,221
  records), as served.
- `n8mc_index.json`: the two slices' index rows, written from the totals of
  those two responses (the full index has about 2,900 rows); not a capture.
- `n8mc_view.json`: the view document's `columns`, as `/api/views/n8mc-b4w4/
  columns.json` served them (probe 2), wrapped with the dataset's id and name.

Race and ethnicity are never requested by the loader.
