# Operating the Census government finance pipeline

The `census_govfin_ingest` DAG captures and publishes the Census Bureau's
state and local government finance statistics (taxes-and-public-money T-5):

| Dataset | Where it comes from | Periods | Places |
| --- | --- | --- | --- |
| Annual Survey of State and Local Government Finances (`asslgf`) | Census Data API `/timeseries/govslocalfin` | fiscal years 2017 to 2024 | nation, states |
| Quarterly Summary of State and Local Tax Revenue (`qtax`) | Census Data API `/timeseries/eits/qtax`, category `QTAXCAT3` | quarters from 1994 | states (state government taxes) |
| Census of Governments Individual Unit Files (`cog_units`) | zips on `www2.census.gov/programs-surveys/gov-finances` | 2002, 2012, 2017, 2022 | county areas, states |

All amounts are thousands of dollars except QTAX, which Census publishes in
millions. Each figure carries its government level: `state_and_local`,
`state` or `local`.

## Measures

Fifty-three measures (fifteen revenue, aid and debt; thirty-eight spending), defined by the Census aggregate-line definitions
(`Finance_Aggregate_Lines_2017.xlsx`):

- Taxes: total, property, general sales, selective sales (T10 to T19),
  individual income, corporate income, and other taxes.
- Revenue: current charges (A01 to A89; liquor stores A90 and utilities A91
  to A94 are not charges), miscellaneous general revenue (U codes), and
  own-source general revenue (taxes, charges and miscellaneous).
- Intergovernmental revenue: from the federal government (B), from the
  state (C), from local governments (D), and the total.
- Debt outstanding at the end of the fiscal year (44T, 49U and 64V).
- Spending (where-public-money-goes, 2026-11-12): one measure per Census
  aggregate line of direct spending, each the item codes the file lists for
  it (`registry.SPENDING_LINES`): direct general spending (line 66) and its
  29 function lines (higher, elementary and secondary and other education;
  libraries; public welfare; hospitals; health; police; fire; correction;
  highways; parks; interest on general debt; and the rest), education's
  total (line 69), utilities (water, electric, gas, transit and their
  total), liquor stores, and insurance trust payments. Payments to other
  governments (M, Q and S items) are not direct spending and are not read,
  so a county area never counts a payment from one of its governments to
  another twice.

`govslocalfin` carries an `AGG_DESC_LABEL` attribute. The spending codes
(`LF0103` direct general spending to `LF0222` insurance trust) were mapped
by that label on 2026-11-12, and the 2017 and 2024 national lines were
checked to add to `LF0103` and `LF0107`. The revenue codes predate the
label check: the registry's
mapping (`LF0008` taxes, `LF0009` property, `LF0011` general sales, `LF0012`
selective sales, `LF0023` individual income, `LF0024` corporate income,
`LF0041` current charges, `LF0059` miscellaneous, `LF0007` own source,
`LF0003` to `LF0006` intergovernmental, `LF0231` debt) was matched against
the published Wisconsin and national tables on 2026-11-05. The live
contract test (EXT-033) checks that each registered code still answers and
that each state's tax parts do not exceed its total.

Derived, in gold, labelled as derived (ADR-0009):

- `gold_census_govfin.tax_mix`: each tax type's share of a place's taxes in
  the same dataset, level and period. No share where the total is missing
  (`suppressed_input`) or zero (`denominator_zero`).
- `gold_census_govfin.spending_mix`: each of the 29 function lines as a
  share of direct general spending (line 66) in the same dataset, level,
  year and place. Their item codes partition line 66, so a place's shares
  add to 100 (Census's published annual lines within its rounding).
  Utilities, liquor stores and insurance trust payments are outside general
  spending and are never in these shares. County areas exist only for
  Census of Governments years.
- `gold_census_govfin.tax_burden`: state and local taxes over BEA personal
  income (CAINC1 line 1) and per person (CAINC1 line 2) for the same place
  and year. No ratio where BEA has not loaded that place and year
  (`denominator_missing`).

## Rules the figures follow

- A fiscal year is dated by the calendar year it ends in; 46 states end
  theirs on June 30. `period_label` says "fiscal year ending in 2022".
- A county-area figure is the sum of the local governments Census places in
  that county. A government spanning counties (a school district, a
  transit authority) is counted whole in the home county Census gives it,
  so pages say the money was "collected in" the county, not that the
  county's residents paid it. `gold_census_govfin.county_area_government`
  lists the governments behind each county area.
- County areas exist only for Census of Governments years. Between them
  Census surveys a sample of governments and publishes no county figures.
- The 2002 public file holds reported governments only; Census dropped the
  imputed records from it. Its roll-ups carry
  `record_coverage = reported_only` and read low beside 2012 onward
  (`reported_and_imputed`).
- 2007 is not registered: Census publishes it only inside the 262 MB
  `_IndFin_1967-2012.zip` historical database, in another layout.

## Schedule and scope

The DAG runs at 07:00 UTC on the 18th of each month. Each dataset captures
its revision window (the two newest years of `asslgf` and `qtax`; none for
the unit files, which Census does not revise) plus every registered period
whose slices have not all been published or answered empty
(`utility.backfill`, ETL-110). On a fresh warehouse the first run loads
every registered year and edition with no manual step. A manual run with
`{"history": true}` re-captures everything:

```text
airflow dags trigger census_govfin_ingest --conf '{"history": true}'
```

When Census publishes a new year, raise `GOVS_LAST_YEAR` in
`src/data_ingestion_toolbox/census_govfin/registry.py`; QTAX years follow
the calendar. A new Census of Governments edition (2027) needs a new
`CensusEdition` entry with its archive path and layouts.

## Deployment prerequisites

Deploy `src/`, `dags/` and `sql/` from one revision and apply
`sql/bootstrap/warehouse_manifest.json`. The DDL lives in the package
(`census_govfin/DDL/` and `gold_census_govfin/DDL/`), and the DAG's first
task, `ensure_census_govfin_schema`, re-applies it. `gold_tax_burden.sql`
reads `gold_bea`, so BEA's gold DDL has to be applied first; the manifest
orders it that way.

Every capture task runs in the `census_api` pool the ACS and SAIPE use.
`require_shared_geography` refuses to start until `silver_ref` has loaded.

`CENSUS_API_KEY` is the key the ACS adapter already reads. It is read when
a request runs and rides only on the outgoing query string: it is not in
captured parameters, request fingerprints, logs or errors. The unit files
need no key.

## What a run does

1. One run per (dataset, period) in `control.ingestion_run`; one committed
   capture per slice, recorded in `control.census_govfin_slice`. API slices
   are `us` and `state`; a unit-file edition has one `bundle` slice (2002
   has `units` and `directory`). An API answer of `204` is status `empty`.
2. `replay_<dataset>` parses each capture. Unit files are read by position
   from the edition's own layout (2002 and 2012 use Census-alphabetical
   state codes, mapped to FIPS); every record lands in
   `silver_census_govfin.unit_amount`, `unit_government` or
   `state_type_total`, or in `quarantine` with a reason. The roll-up then
   writes county areas, each state's local governments and each state
   government into `fact_observation` with `derivation`,
   `government_count` and `record_coverage`. Before the roll-up the
   replay runs `ANALYZE` on `unit_amount`, `unit_government` and
   `measure_item` inside its own transaction, because the run's records
   are not committed yet and older statistics estimate them as one row;
   the roll-up then runs with nested loops off. A 2002 edition (about
   1.4 million records) rolls up in well under a minute; one running for
   hours with no wait event is planning from stale statistics.
   API rows land in
   `api_revision` and then `fact_observation`.
   A slice whose bytes and parser contract repeat its last published
   capture, with no geography added to `silver_ref.dim_geo_entity` since,
   is recorded `unchanged` and replayed into nothing, so a monthly re-read
   of the revision window stores no second copy (`utility/unchanged.py`).
   If any member of a unit-file edition changed, the whole edition is
   replayed, because its county areas sum the unit file over the directory.
3. `publish_<dataset>` marks reconciled slices `published`, which exposes
   them in `gold_census_govfin.observation_revision` (every capture) and
   `observation_latest` (the newest), and signals the glossary harvest.
4. `replay_new_measures` reads the newest published capture of every
   unit-file edition and annual-survey slice from the warehouse (never the
   network) and writes the facts of any registered measure its run does
   not hold yet, one transaction per run. It is how measures added to the
   registry after a capture was published (the spending lines) reach the
   captures already held, since an unchanged re-capture replays nothing.
   Existing facts are never rewritten; a second call writes nothing. When it
   writes, it requeues the glossary harvest for `gold_census_govfin`.

## Checks after a run

```sql
SELECT dataset_id, period, slice_key, status, captured_row_count, quarantined_row_count
FROM control.census_govfin_slice
ORDER BY dataset_id, period DESC, slice_key;

SELECT member, error_code, count(*)
FROM silver_census_govfin.quarantine
GROUP BY member, error_code;

-- Dane County's 2022 taxes, and the governments they sum.
SELECT measure_id, value, government_count, record_coverage
FROM gold_census_govfin.observation_latest
WHERE geo_id = 'state:55|county:025' AND fiscal_year = 2022
  AND measure_id LIKE 'tax%';

-- Dane County's 2022 local spending by Census line.
SELECT measure_id, spending_amount, share_of_general_spending_pct
FROM gold_census_govfin.spending_mix
WHERE geo_id = 'state:55|county:025' AND dataset_id = 'cog_units'
  AND fiscal_year = 2022
ORDER BY spending_amount DESC;

SELECT geo_id, fiscal_year, taxes_pct_of_personal_income, taxes_per_person_dollars
FROM gold_census_govfin.tax_burden
WHERE geo_id = 'state:55'
ORDER BY fiscal_year;
```

Run `ANALYZE silver_census_govfin.unit_amount, silver_census_govfin.fact_observation;`
after the first full load (about 20 million unit records across the four
editions).

## Quality rules

- `DQ-GOVFIN-001` and `DQ-GOVFIN-002` are enforced by the warehouse:
  primary keys, value-status CHECK constraints, and CHECKs that only unit
  files carry a roll-up derivation and that every roll-up states its
  record coverage.
- `DQ-GOVFIN-003` (QUARANTINE) fails a slice left at `captured` and a
  replayed run with readable records but no facts.
- `DQ-GOVFIN-004` (WARN) compares, item by item, each published edition's
  state-government and local-government records with Census's own state
  totals (`statetypepu`, levels 2 and 3) and names every state and item
  more than 1 percent apart. 2002 has no state totals, so it is not
  checked.
