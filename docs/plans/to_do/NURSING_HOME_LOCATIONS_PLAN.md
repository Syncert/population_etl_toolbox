---
id: nursing-home-locations
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/cms_ltc tests/unit/api tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cms_ltc" -q
  - python -m pytest tests/dags -m dag -q
---

# Every certified nursing home is on the map with its beds and residents

## Status

Built (steps 1-3, 2026-11-12); step 4, the load, follows the deploy. Step 2 of
[`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md). As built, DQ rules
are `DQ-CMSLTC-001` (one row per report per file), `-002` (a valid figure
carries a value) and `-003` (Form 671 facilities not placed on a county,
WARN); placement statuses are `resolved`, `unmapped`, `ambiguous` and
`not_listed`.

## Source

**Provider Information is refused.** CMS's Provider Data Catalog
(`data.cms.gov/provider-data/...`, dataset `4pq5-n9py`, the one with
coordinates, certified beds and average residents) answered HTTP 403 to the
toolbox's own client on the internal stack on 2026-11-12, for the metastore,
the CSV and the archive alike. No user agent or header changes are tried
(no bot-check circumvention). Provider Information is not used unless CMS
opens that path to an ordinary client.

**What stands in**, from CMS's main catalog (`data.cms.gov/data.json`), whose
`data-api/v1` and `/sites/default/files/` paths answered HTTP 200 the same
day:

1. **Long-Term Care Facility Characteristics** (CMS-671 survey form),
   quarterly; Q3 2026 has 14,688 facilities (data-api id
   `129a6503-c0f1-4132-b186-4c0232c2d894`, CSV `Form_671_Q3_2026.csv`,
   2.8 MB). Kept: provider number (CCN), facility name, city, state, ZIP,
   certification date, Medicare, Medicaid and other census, **total
   residents**, hospital based, ownership type, specialised bed counts.
2. **Skilled Nursing Facility Cost Report**, yearly by fiscal year (2024:
   14,587 reports, data-api id `a69d3df7-3f66-4a0d-b5b8-0d66049bd565`). Kept:
   CCN, street address, city, state, ZIP, **county name**, CBSA, fiscal year,
   **number of beds**, total bed days available, total patient days.

Joined by CCN as published, never blended: residents come from the CMS-671
quarter, beds and county from the newest cost report, each with its own
period.

What this lacks compared with Provider Information:

| Provider Information has | Stand-in |
| --- | --- |
| Latitude and longitude | None. A facility is placed on its county by the cost report's county name within its state (`geography_status = 'county_by_name'`); a name that matches no county, or a facility with no cost report, stays `unmapped` with its city and ZIP. No address is geocoded by a third party. |
| Certified beds | The cost report's number of beds (beds reported for the cost reporting year), labelled as such. |
| Average residents per day | The CMS-671 total residents, a count on the survey date, labelled as such. |
| Monthly refresh | Quarterly (CMS-671) and yearly (cost reports). |
| Ratings, staffing, penalties | Not needed by this plan. |

The CMS COVID-19 nursing home data page is a script-only page with no listed
dataset in the catalog; [`outbreak-calibration-sets`](OUTBREAK_CALIBRATION_SETS_PLAN.md)
covers it.

## Design

- New adapter `cms_ltc` (CMS main catalog, data-api v1, no key), two
  registered datasets, each capture of one release's CSV distribution
  named in `data.json` (the URL is read from the catalog, never written
  down), captured losslessly.
- Silver: `facility_quarter` (CMS-671 rows per quarter) and
  `facility_cost_report` (cost report rows per fiscal year), values as
  published, blank kept as missing.
- Gold: a facility list per county with residents (and their survey
  quarter), beds (and their fiscal year), placement source; county sums of
  residents and beds with the facility count, labelled as sums of published
  figures.
- API observations and the county facility list; catalog and glossary rows.
- DAG `cms_ltc_ingest`, monthly; loaders take only releases not yet loaded.
- DQ: `DQ-CMSLTC-001` one row per CCN per release; `DQ-CMSLTC-002` facilities
  with residents but no cost report or no county match are counted (WARN);
  `DQ-CMSLTC-003` row count equals the catalog's `found_rows`.

## Steps

1. Done: probes (2026-11-12) found the stand-in paths and their columns.
2. Adapter and tests with Wisconsin fixtures.
3. Gold, API, DQ, DAG, docs.
4. Load every published CMS-671 quarter and cost report year.

## Size

About 14,700 facilities: about 10 MB a quarter and 30 MB a cost-report year with capture; under 1 GB for every published year.

## Acceptance criteria

1. Dane County's nursing homes are listed with their published residents
   and beds, each with its period, placed on the county by the cost report's
   county name, and the gap against Provider Information stated.
2. A refresh loads only a new release.
3. Speed gate under 500 ms.
