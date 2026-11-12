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

To do. Step 7 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md).
The county release is loaded; the tract release is not.

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
