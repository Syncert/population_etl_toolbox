# Operating the IRS SOI county and state income pipeline

The `irs_soi_ingest` DAG captures and publishes the IRS Statistics of Income
(SOI) individual income tax statistics. For every county and state, and the
nation, it loads how many returns were filed, the income reported on them,
the federal income tax owed and the credits claimed. It loads them for all
returns and, from tax year 2010, by size of adjusted gross income (AGI).
Values are served by `/api/v1/observations` in the neutral observation shape.

## What is registered

`src/data_ingestion_toolbox/irs_soi/registry.py` names every file the
pipeline requests from `https://www.irs.gov/pub/irs-soi/`. No credential is
needed.

| Tax years | File | Path | What it holds |
| --- | --- | --- | --- |
| 1989–2009 | County totals | `YYYYcountyincome.zip` | One `.xls` workbook per state: the state and its counties, six items, all returns only |
| 2010 | County totals and by AGI | `2010countydata.zip` | The two CSVs below, zipped |
| 2011–2023 | County totals | `YYincyallnoagi.csv` | Every county and each state's total (county code `000`), all returns |
| 2011–2023 | County by AGI | `YYincyallagi.csv` | The same places, one row per AGI band |
| 2020–2023 | State (Historic Table 2) | `YYin55cmcsv.csv` | Every state and the nation, all returns and by AGI band |

The items (`MEASURES`), each by SOI's column name:

- returns (`N1`) and individuals (`N2`);
- AGI (`A00100`) and total income (`A02650`);
- wages (`N00200`, `A00200`), taxable interest (`A00300`), dividends (`A00600`),
  business income (`A00900`), capital gains (`A01000`), pensions (`A01700`),
  unemployment compensation (`A02300`) and taxable Social Security (`A02500`);
- taxable income (`A04800`);
- income tax after credits (`N06500`, `A06500`) and total tax liability (`A10300`);
- the earned income tax credit (`N59660`, `A59660`) and the additional child
  tax credit (`A11070`);
- itemized state and local income taxes (`A18425`) and real estate taxes (`A18500`).

Money amounts are in thousands of dollars, as SOI publishes them. A metric
code is `IRS_SOI:<item>` for all returns and `IRS_SOI:<item>:<band>` for one
AGI band, such as `IRS_SOI:returns:agi_200k_plus`.

### What each era carries

- **1989–2009 workbooks.** SOI's per-state workbooks carry returns,
  exemptions (loaded as individuals), AGI, wages and salaries, dividends,
  and interest received. They have no income tax, no credits and no AGI
  bands. Interest received counts taxable and tax-exempt interest together,
  so it is loaded as its own item, `interest_received_amount`, not as
  taxable interest. Columns are found by their labels, not their positions.
  Where SOI posted a revised workbook beside the original (2000's
  `Iowa00cir.xls`), the revision is read first and the original's repeated
  rows are set aside as duplicates.
- **County files from 2010.** These have every item, by AGI band. 2010 and 2011 use
  seven bands (under $1, $1–25k, 25–50k, 50–75k, 75–100k, 100–200k, 200k
  and over), and 2012 onwards use eight (the $1–25k band split at $10k).
  The by-AGI file's all-returns rows repeat the totals file, so only the
  totals file serves all returns.
- **State files from 2020.** These have ten bands, including $200–500k, $500k–1m and
  $1m and over, and the nation (`US`). SOI's "Other areas" (`OA`) and
  Puerto Rico (`PR`) rows are kept in silver as unmapped and never served.
  Historic Table 2 is published as CSV only from tax year 2020; earlier
  years are Excel workbooks and are not registered.

Where a county file and the state file both speak for a state, the county
file is served (`observation_latest` prefers `product = 'county'`). The
nation, and the bands only the state file splits out, come from the state file.

### The band scheme is part of the registration

`BAND_SCHEMES` binds each file and year range to its AGI bands. A file whose
`agi_stub` values are not exactly its registered scheme is refused whole
(`unregistered_agi_stub`), so a file with renumbered bands is never served
under the wrong dollar ranges. When SOI publishes a new year, check its
documentation guide (`YYincydocguide.docx`) for the stub table before
adding the year.

## Schedule and scope

The DAG runs at 15:00 UTC on the 7th of each month. Each run captures the
files of the newest two tax years, because SOI posts a corrected file now
and then. It also captures every registered year a file of which has never been
published, so a fresh warehouse loads every year from 1989 on its first
scheduled run. No manual history run is needed. To re-read every
year, trigger with `{"history": true}`.

Files go through the one-slot `irs_soi_files` pool, which is shared with
the migration DAG. Create it if deployment automation has not:

```text
airflow pools set irs_soi_files 1 'IRS SOI migration files (serialized)'
```

The first full load is about 60 files, mostly the 1989–2009 zips of 2 to
10 MB each, and takes well under an hour.

## Deployment prerequisites

Apply `sql/bootstrap/warehouse_manifest.json`. The DAG's first task,
`ensure_irs_soi_schema`, re-applies the package's DDL before any capture.
The shared geography reference must be loaded: values for a place that is not in
`silver_ref.dim_geo_entity` are kept as `unmapped` and not served. The
scheduler image needs `xlrd` for the 1989–2009 workbooks; it is part of
the `airflow` extra.

## What a run does

1. Commits one capture per file and records it in `control.irs_soi_file`.
2. Replays each capture into `silver_irs_soi.observation_revision`, one row
   per place, band and item. Rows whose geography cannot be read, that are
   short, or that repeat a place and band go to `observation_quarantine`. A
   file whose header, member or band scheme is not the registered one is
   refused whole and marked `quarantined` with no values, so the next run asks
   for it again. Each place is recorded in `silver_ref.geography_resolution`,
   and values land in `silver_irs_soi.fact_observation`. The run commits
   only if every captured row is accounted for.
3. Marks the file `published`, which exposes it through
   `gold_irs_soi.observation_revision` and `observation_latest`, runs
   `ANALYZE` on the fact, and signals the glossary harvest.

A cell SOI left blank or marked is loaded as `missing` with no value, and
SOI's text is kept in `value_source`. It is never loaded as zero.

## Derived rates

`docs/semantics/derived_ratios.json` pairs two SOI items from the same file,
tax year and place:

- `income_tax_share_of_agi`: income tax after credits ÷ AGI, in percent.
- `eitc_share_of_returns`: returns claiming the EITC ÷ all returns, in percent.

Both are served by `/api/v1/observations/derived`. For counties they start
in tax year 2010, because the 1989–2009 county files carry no tax or
credit items. For the nation they start in 2020.

## Checks after a run

```sql
SELECT file_kind, tax_year, status, captured_row_count, quarantined_row_count
FROM control.irs_soi_file ORDER BY tax_year DESC, file_kind;

SELECT error_code, COUNT(*) FROM silver_irs_soi.observation_quarantine GROUP BY error_code;

-- States per tax year with a total (expect 51 from the county files).
SELECT tax_year, COUNT(*) FROM gold_irs_soi.observation_latest
WHERE metric_key = 'returns' AND geo_type = 'state' GROUP BY tax_year ORDER BY tax_year;
```

## Quality rules

- `DQ-SOI-001` (uniqueness) is enforced by the fact's key. Named CHECK
  constraints refuse a resolved row without a geography key and a value
  whose status disagrees with it.
- `DQ-SOI-002` is the file-ledger reconciliation and runs in the daily
  sweep: every captured row is values or quarantine, and no file is left
  unreplayed.
- `DQ-SOI-003` warns when, in the newest county totals file of a year, a
  state's counties do not sum to its total within 1 percent, in returns or
  AGI. SOI rounds county counts, so small differences are expected.
- `DQ-SOI-004` warns when the returns across a place's AGI bands do not sum
  to its all-returns count within 1 percent.

## Tests

- `tests/unit/irs_soi/` checks the registry, the client and the parser for every era
  against real Delaware rows (`tests/fixtures/irs_soi/SOURCE_NOTES.md`).
- `tests/integration/database/test_irs_soi_capture_replay.py` takes the
  fixtures to gold against real PostgreSQL, and checks the quality rules
  and a refused file.
- `tests/e2e/test_irs_soi_pipeline.py` reads the values and the derived
  rates through the API.
- `tests/external/test_irs_soi_source_contracts.py` (EXT-032) is the live
  check of one file of each layout.
