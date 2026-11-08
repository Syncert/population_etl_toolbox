# Operating the DOL county childcare prices pipeline

The `dol_childcare_ingest` DAG captures and publishes the U.S. Department of
Labor Women's Bureau's National Database of Childcare Prices (NDCP): the
median weekly full-time price of childcare in each county, by type of care
(a childcare center, or family childcare in a provider's home) and age group
(infant, toddler, preschool, school age). Prices are published for counties
only; nothing is summed or averaged to a state or the nation.

## What the schedule reads

`src/data_ingestion_toolbox/dol_childcare/registry.py` registers each
release the Women's Bureau publishes as one workbook on `dol.gov`
(`https://www.dol.gov/sites/dolgov/files/WB/<file>`). One is registered:

| Release | File | Study years | Sheet |
| --- | --- | --- | --- |
| `ndcp2022` | `NDCP2022.xlsx` (about 92 MB, 48,308 rows, 370 columns) | 2008-2022 | `County_LevelNDCP_v8_update2008_` |

The earlier `nationaldatabaseofchildcareprices.xlsx` (2008-2018) is not
registered: `NDCP2022` restates every one of its years, and two copies would
be waste. The download needs no key. The client sends an honest
`User-Agent`, retries 429 and 5xx answers with backoff, and refuses a body
that is not a workbook (an HTML page answered in its place) before anything
is captured.

The schedule is monthly (9th, 16:00 UTC): the Women's Bureau publishes a
release every year or two. Each run, through the one-slot
`dol_childcare_files` pool:

1. `ensure_dol_childcare_schema` applies the DDL.
2. `require_shared_geography` fails fast if the shared county reference is
   not loaded.
3. `ingest_batch_release` (one mapped task per release) captures the
   workbook into `raw_capture` and ledgers the read in
   `control.dol_childcare_file`. A read whose bytes repeat the release's
   last published capture is recorded `unchanged` and replays nothing.
   A never-published release is loaded whole on the first run (LOADER RULE).
4. A changed workbook is replayed: each price is stored in
   `silver_dol_childcare.price_revision` only when its value, status or
   imputation code differs from the newest stored for that county, measure
   and study year; every repeat is counted in `repeated_price_count`
   (CHURN RULE). The read is then published and the glossary told.

## What is stored

- Eight prices per county and study year (`center_infant` ...
  `home_school_age`), the Women's Bureau's own aggregate columns
  (`MCINFANT` ... `MFCCSA`), in dollars per week.
- `imputed` and `imputation_code`: the Women's Bureau fills some prices in
  itself. The five-digit code's positions are, in order: taken from another
  age group, converted to a weekly price, the statewide price given to the
  county, a missing median or 75th percentile filled in, a year between the
  state's surveys. A price's code combines its age bands' codes. Imputed
  prices are served as published and flagged; the warehouse estimates
  nothing.
- A blank price is `missing` (`provider_missing`), never zero. A row that
  cannot be read (a county code that is not a county, a year outside the
  release, a repeated county and year) is set aside in
  `silver_dol_childcare.observation_quarantine` with its reason. A workbook
  missing the registered sheet or any registered column is refused whole:
  the read is `quarantined` and nothing is published.

Gold: `gold_dol_childcare.observation_latest` (newest revision per county,
measure and year), `observation_revision`, `measure_export` and
`metric_publisher`. The API serves `DOL_CHILDCARE:<measure>` on
`/api/v1/observations` with `care_type`, `age_group`, `imputed` and
`imputation_code` dimensions. `/api/v1/observations/derived` serves
each price's hours of work through the reviewed
`minutes_of_work_childcare_<measure>` pairings (`docs/semantics/derived_ratios.json`):
the weekly price over the county's own QCEW average weekly wage over 40
hours, the reader naming the county in `wage_geo_id`.

## Checks after a run

```sql
SELECT release, status, captured_row_count, quarantined_row_count, repeated_price_count, retrieved_at
FROM control.dol_childcare_file ORDER BY retrieved_at DESC LIMIT 5;

SELECT error_code, COUNT(*) FROM silver_dol_childcare.observation_quarantine
GROUP BY error_code ORDER BY 2 DESC;

SELECT year, COUNT(*) FILTER (WHERE value IS NOT NULL) AS priced, COUNT(*) AS rows
FROM gold_dol_childcare.observation_latest WHERE metric_key = 'center_infant'
GROUP BY year ORDER BY year;
```

Dane County, Wisconsin (`state:55|county:025`) reads $293 a week for
center-based infant care in 2022.

## A new release

When the Women's Bureau publishes a new file, add an `NdcpRelease` to
`RELEASES` with its path, years and sheet name, and check its header against
`required_columns`. If it restates every year of `ndcp2022`, replace that
entry rather than adding beside it; the churn rule stores only the prices it
changed.
