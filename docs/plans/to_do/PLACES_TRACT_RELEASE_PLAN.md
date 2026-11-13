---
id: places-tract-release
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/cdc tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "cdc" -q
  - python -m pytest tests/dags -m dag -q
---

# Every tract shows CDC PLACES chronic-illness estimates

## Status

Built 2026-11-12; the load follows the deploy. Step 7 of
[`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).

As built:

- `places_tract` is a third `CdcAsset`, marked bulk: 50,000-row pages
  (probe 6 showed data.cdc.gov answers a limit above 1,000 to the plain
  client), each recorded in `control.cdc_release_page` for resume, and the
  paged total checked against a captured `count(*)`.
- Replay streams page by page (a whole-release replay would hold about 3
  million rows in memory); older runs without page rows replay as before.
- Tracts resolve on 2020 tracts (`geography_vintage = 2020`); an 11-digit
  code is the only tract shape.
- The tract release publishes crude prevalence only (no age-adjusted) and
  carries no footnotes on the rows read.
- Not built here: tract map tiles and a per-tract completeness DQ rule; the
  count check guards completeness of the read.
- Dane County: 4,920 rows, 123 tracts x 40 measures; 2023 measure year for
  35 measures, 2022 for 5.

## Source

CDC PLACES: Local Data for Better Health, **Census Tract Data, 2025 release**
(`data.cdc.gov` `cwsq-ngmh`), 3,047,284 rows, 40 measures, Public Domain. The
release mixes BRFSS 2023 and 2022 measure years; each row keeps its own year.
PLACES figures are CDC's model-based estimates; they are served as CDC
publishes them, with CDC's confidence limits, and the API names them
estimates.

## Design

- A third `CdcAsset` in `cdc/registry.py` (`places_tract`) on the existing
  Socrata client, paged with a per-page control table, the same columns as
  `places_county` plus the tract FIPS.
- Tracts resolve on 2020 tracts in `silver_ref`.
- Gold tract view and tract map tiles; API observations at tract grain.
- DQ: every measure present for every tract CDC lists; the tract's
  population figure as published.
- Measures that are about income or race are not part of PLACES; nothing to
  exclude.

## Size

About 3 million rows: about 4 GB a release with capture and indexes at the measured 1,300 bytes per row (was 1 GB).

## Acceptance criteria

1. Dane tracts show the 40 PLACES measures with confidence limits as
   published.
2. Speed gate under 500 ms for one county's tracts.
