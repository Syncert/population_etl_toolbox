# Operating the Monthly Treasury Statement pipeline

The `treasury_mts_ingest` DAG captures Table 9 of the Monthly Treasury
Statement, "Summary of Receipts and Outlays of the U.S. Government by
Source/Function", from Treasury's Fiscal Data API and publishes federal
receipts by source and outlays by budget function for the nation. These are
the federal government's own receipts and outlays: not state or local
spending, and not where the money was spent (that is USAspending, a
separate source).

## What is read

`GET https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/mts/mts_table_9`
with every registered field, `filter=record_date:gte:<start>`,
`sort=record_date,src_line_nbr` and `page[size]=10000`. No credential is
needed. Pages are read until `meta.total-pages`; every page is captured
before anything is parsed.

The read starts three months before the newest statement already published
(Treasury can restate a recent month), or at 2015-03-31 when nothing is
published or the run is triggered with `{"history": true}`.

On 2026-11-12 (read through OFFICE_LAB) the table held 4,587 rows, record
dates 2015-03-31 to 2026-09-30, 33 lines a month.

| Lines | What they are |
| --- | --- |
| 10, 40, 130 | Headings (Receipts, Social insurance, Net outlays); printed `null` |
| 20 to 110 | Receipts by source |
| 120 | Total receipts |
| 140 to 330 | Outlays by budget function (19 lines) |
| 340 | Total outlays |

The registered lines are `LINES` in `registry.py`; the gold
`line_definition` view repeats them and a unit test holds the two equal.

## How a row is read

- Each amount is kept as Treasury wrote it beside the parsed number:
  `current_month_source`, `value_source` (fiscal year to date) and
  `prior_fytd_source`. The text `null` is `missing` with no number, never
  zero.
- A row is quarantined alone (`silver_treasury_mts.quarantine`) when it
  lacks a registered field, its record date, line, month or year cannot be
  read, its month differs from its record date's, an amount is not a
  number, or it repeats a (record date, line) already read. A body that is
  not a Table 9 page is refused whole as row 0.
- Outlays are net of offsetting collections, so a line can be negative
  (line 330, undistributed offsetting receipts, always is).

## Churn

A run writes only what changed: a line whose published text equals the
stored one is counted, not rewritten (`written_count` on the page ledger
counts what changed). A restated line moves to
`silver_treasury_mts.statement_line_revision` first. Treasury's
classification ids are kept but are not compared, so a renumbering alone
rewrites nothing.

## What is served

- `gold_treasury_mts.observation_latest` and `observation_revision`: each
  line's fiscal-year total, read from the September statement's
  fiscal-year-to-date column (never added up from months), period
  October 1 to September 30, geography `us:1`, time grain ANNUAL. The API
  serves them through `/api/v1/observations`; the aligned analysis routes
  decline the source because its years are fiscal years.
- `gold_treasury_mts.outlays_by_function`: each budget function's outlays
  and share of total outlays (line 340), for `fiscal_year`, for
  `fiscal_year_to_date` (the newest statement of a fiscal year with no
  September statement yet), and for each `month` (current-month column).
  The function lines add to line 340 as published; because line 330 is
  negative, the other shares add to more than 100.

Check (read-only):

```sql
SELECT budget_function, outlays_amount, share_of_total_outlays_pct
FROM gold_treasury_mts.outlays_by_function
WHERE period_kind = 'fiscal_year' AND fiscal_year = 2026
ORDER BY outlays_amount DESC;
-- Total outlays fiscal 2026: 7,395,799,983,313.23; net interest 1,051,753,969,311.11.
```

## Schedule and checks

The DAG runs at 16:00 UTC on the 16th of each month through the one-slot
`treasury_mts_pages` pool. A replay that failed is settled at the start of
the next run. Data-quality rules:

- `DQ-TREASURY-001` (enforced): one row per record date and line.
- `DQ-TREASURY-002`: no captured page left unreplayed, and no stored line
  from a page that was not published.
- `DQ-TREASURY-003` (enforced): a missing line has no amount; a valid one
  has its fiscal-year-to-date amount.
- `DQ-TREASURY-004`: for every statement month, lines 140 to 330 add to
  line 340 within one dollar.

## First load

Load the shared geography first (the nation, `us:1`), then trigger the DAG
once with `{"history": true}`. The whole table is one page of about 4,600
rows, under 0.05 GB with indexes; then run the glossary harvest.
