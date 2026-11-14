# Operating the USAspending pipeline

The `usaspending_ingest` DAG captures federal award obligations by place of
performance from USAspending.gov's search API and publishes them for states
and counties, by fiscal year, for every award and for each award type group
and awarding agency. This is federal money awarded to work done in a place.
It is not what the governments of that place spend (that is
`CENSUS_GOVFIN`, a separate source) and the two are never added.

## What is read

Two endpoints, both `POST` with a JSON body and no credential:

- `https://api.usaspending.gov/api/v2/search/spending_by_category/awarding_agency/`,
  filtered to the fiscal year, 100 agencies a page, read until
  `page_metadata.hasNext` is false. It names the awarding agencies with any
  obligation that year (78 in fiscal 2024).
- `https://api.usaspending.gov/api/v2/search/spending_by_geography/` with
  `scope=place_of_performance`, `geo_layer` `state` or `county`, and the
  fiscal year (October 1 to September 30) as `time_period`. Each year is
  cut four ways for each layer:
  - every award (`cut_kind` `all`);
  - each award type group (`award_type`): contracts (A-D), grants (02-05),
    direct payments (06, 10), loans (07, 08) and other assistance (09, 11,
    -1);
  - each awarding agency the year's list names (`agency`, keyed by its
    code, filtered by its name).

Every request is one committed capture before anything parses it, and one
row of `control.usaspending_request` (the year's agency pages are recorded
there too, layer `nation`, cut `agency_list`, key the page number).

On 2026-11-12 (read through OFFICE_LAB) the fiscal 2024 state answer held 57
rows (the states, DC, five territories and one row with an empty code),
summing to $4,862,215,212,591.58; the five groups added to that within about
$97 million.

## Which requests a run makes

- An ordinary run (monthly, the 20th at 09:00 UTC) reads the current and prior
  fiscal years again, every request, because agencies are still reporting
  them (most within 30 days, defense within about 90).
- `{"history": true}` also reads fiscal 2008 onward, oldest year first, but
  only the requests no earlier run captured. A run stops after
  `max_requests` requests (default 1,500; pass `{"max_requests": N}` to
  change it). Trigger it again to continue: it resumes from the requests
  still never captured, not from fiscal 2008.

A retry of the same scheduled or triggered run (Airflow's task retries)
skips the refresh-year requests its earlier attempts captured after the run
started; those captures are replayed first. Only each year's agency list is
read again, because it decides the year's agency reads.

Requests are spaced one second apart through the one-slot `usaspending_api`
pool. A `429` or a server error is retried with backoff (up to four
attempts) and logged with its rate headers only; any other client error
fails the run at once with the path and status.

## How a row is read

- Each amount is kept as USAspending wrote it (`value_source`) beside the
  parsed number. `null` is `missing` with no number, never zero.
- The county layer's code is the five-digit county FIPS code and the state
  layer's the postal abbreviation; `geo_id` follows from the code alone.
  An empty code (awards with no recorded place) or one that names no state
  or county is kept `unsupported` and is never served.
- The agency list can hold a row with an amount and no agency code or name
  (fiscal 2026 page 1: 77 agencies and one such row of 57,284,285.65). The
  page is captured and its agencies are read; that row is quarantined alone
  as `unattributed_agency` with its amount as published, so no agency cut
  is made for it and the agency shares of a place need not add to 100%.
- A row is quarantined alone (`silver_usaspending.quarantine`) when it lacks
  a registered field, its amount is not a number, or it repeats a code
  already read from the answer. An answer for another layer is refused
  whole as row 0 and withdraws nothing.

## Churn

A run writes only what changed: a row whose published text equals the
stored one is counted, not rewritten (`written_count` on the request
ledger). A changed row moves to `silver_usaspending.geography_amount_revision`
first (`replaced`); a place a later answer no longer lists moves there as
`withdrawn` and leaves the current table.

## What is served

- `gold_usaspending.observation_latest` and `observation_revision`:
  `federal_award_obligations` and one measure per award type group, for
  states and counties, period October 1 to September 30, time grain ANNUAL.
  The API serves them through `/api/v1/observations`; the aligned analysis
  routes decline the source because its years are fiscal years and its
  amounts are recorded where the work is performed.
- `gold_usaspending.awards_by_award_type` and `awards_by_agency`: each
  group's and each agency's obligations in a place and its share of the
  place's all-award total, with `unavailable_reason` when that total is not
  published. Every row carries the place-of-performance note: pass-through
  grants sit with the state agency that receives them, and some payments
  (Medicare) where the claims processor is, so a county can show far more
  than its residents receive.

Check (read-only):

```sql
SELECT award_group, obligated_amount, share_of_place_total_pct
FROM gold_usaspending.awards_by_award_type
WHERE geo_id = 'state:55|county:025' AND fiscal_year = 2024
ORDER BY obligated_amount DESC;
-- Dane County grants fiscal 2024: 2,978,864,182.74.
```

## Schedule and checks

A replay that failed is settled at the start of the next run.
Data-quality rules:

- `DQ-USASPENDING-001` (enforced): one row per fiscal year, layer, cut, key
  and shape code.
- `DQ-USASPENDING-002`: no captured request left unreplayed, and no stored
  row from a request that was not published.
- `DQ-USASPENDING-003` (enforced): a missing amount has no number; a valid
  one has one.
- `DQ-USASPENDING-004` (warn): for each published state-layer fiscal year,
  the award type groups add to the all-awards total within 0.1%.

## First load

Load the shared geography first (states and counties), then trigger the DAG
with `{"history": true}`. Fiscal 2008 to the current year is about 3,200
requests (each year: one or two agency pages, and two layers of all awards,
five groups and about 80 agencies), so about three runs of 1,500, roughly an
hour each at one request a second plus answer time. Then run the glossary
harvest.

Size, measured at about 320 bytes per silver row with its indexes: about
2.2 million rows (per fiscal year about 4,000 state rows and 115,000 county
rows, most of them agency cuts), about 0.7 GB, plus about 0.25 GB of
captures. Each monthly run then re-reads the two refresh years (about 340
requests, about 25 MB of captures) and keeps every changed amount as a
revision: up to about 240,000 rows, about 75 MB, a month while those years
are still being reported, so about 1.2 GB a year at most.
