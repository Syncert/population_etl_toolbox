---
id: nursing-home-locations
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/cms_provider tests/unit/api tests/unit/shared -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cms_provider" -q
  - python -m pytest tests/dags -m dag -q
---

# Every certified nursing home is on the map with its beds and residents

## Status

To do. Step 2 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).

## Source

CMS Provider Data Catalog, **Provider Information** (`4pq5-n9py`), one row per
Medicare- or Medicaid-certified nursing home, refreshed monthly (release of
2026-10-28; next 2026-12-02). Metadata at
`https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/4pq5-n9py`,
CSV distribution named there (the URL changes each month, so it is read from
the metastore, never written down). `accessLevel: public`; a U.S. government
work. Columns kept (names confirmed by the probe against the CMS data
dictionary): CMS certification number, provider name, address, city, state,
ZIP, county name, latitude, longitude, number of certified beds, average
number of residents per day, ownership type, provider type, whether in a
hospital, and the processing date. Ratings, staffing and penalty columns are
not requested by this plan.

Archived monthly files (`data.cms.gov/provider-data/archived-data/nursing-homes`)
give history; the probe records how far back they go and whether they are
the same file shape.

## Design

- New adapter `cms_provider` (CMS Provider Data Catalog, no key), built so
  Hospital General Information (`xubh-q36u`, plan
  [`hospital-capacity-and-locations`](HOSPITAL_CAPACITY_AND_LOCATIONS_PLAN.md))
  is a second registered dataset on the same client.
- Capture the metastore item and the CSV it names, losslessly.
- Silver `silver_cms_provider.nursing_home`: one row per facility per
  processing month, values as published. The county is the one whose 2025
  county shape contains the published point (point-in-polygon on
  `silver_ref`, restricted to the facility's state first); a facility whose
  point no shape covers keeps `geography_status = 'unmapped'` and its
  published county name. A published county name that disagrees with the
  shape is kept and flagged (DQ-CMS-002, WARN), never corrected.
- Gold: a facility list view (point, beds, residents) per county, and county
  sums of certified beds and average residents with the facility count
  (sums of published figures, labelled as such).
- API: county facilities list and the county sums through the observation
  routes; catalog and glossary rows.
- DAG `cms_provider_ingest`, monthly on the 5th; loaders detect a release
  not yet loaded and do nothing otherwise.
- DQ: `DQ-CMS-001` every facility has a certification number and beds;
  `DQ-CMS-002` published county agrees with the point; `DQ-CMS-003` the row
  count of a release is within the published row count.

## Steps

1. Probe: metastore item, CSV header, Dane rows, archive listing.
2. Adapter and tests with a Dane fixture.
3. Gold, API, DQ, DAG, docs.
4. Load current release, then archives back as far as published (Nick told
   the size first if beyond a few years).

## Size

About 15,000 facilities: about 15 MB a month with capture, 0.2 GB a year.

## Acceptance criteria

1. Dane County's nursing homes are listed with their published beds and
   residents and placed on the county by their own coordinates.
2. Monthly refresh loads only a new release.
3. Speed gate under 500 ms.
