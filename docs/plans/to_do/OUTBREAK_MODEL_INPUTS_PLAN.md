---
id: outbreak-model-inputs
depends_on:
  - group-quarters-by-type
  - nursing-home-locations
  - lodes-block-grain-for-dane
  - block-population-and-age-for-dane
  - college-housing-capacity
  - hospital-capacity-and-locations
  - places-tract-release
  - outbreak-calibration-sets
  - air-travel-and-mobility
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/tooling -q
---

# The warehouse holds every free, published input an outbreak model of a county needs

## Status

To do. Requested by Nick on 2026-11-12 (18:19Z, "Work this.") with the
document "Outbreak factors mapped to data" attached. That document maps 22
outbreak factors to data, finds 8 the toolbox cannot yet feed, and orders the
work in nine steps. Each step is its own plan, listed below; this plan closes
when they have all reached `needs_review/` or been set aside by Nick.

The outbreak model itself, and the reference files that ship with it (contact
matrices, severity curves, strain cards, roster mapping, the behaviour
response curve), are **outside this repository**. Nothing here builds them.

## What the document says is already held, checked against `dev` (0b64229e, 2026-11-12)

| Document's claim | Checked | Finding |
| --- | --- | --- |
| ACS B01001, B11001 requested at tract grain | `census_acs/config.py` `tract_tables` | Correct. B25010 and B09020 are county only, also correct. |
| CDC PLACES county loaded, tract release missing | `cdc/registry.py` `ALL_ASSETS` = CDI and PLACES county | Correct. |
| LODES, NCES, IRS migration, SAHIE, USDA ERS "built, in review" | Plans sit in `needs_review/` | The pipelines are merged to `dev` and loaded and served on the internal stack; only Nick's plan acceptance is outstanding. "Merge the in-review LODES pipeline" (step 3) needs no merge. |
| LODES block captured, served only as county sums | `census_lodes/silver_census_lodes/parse.py` | Correct: the raw captures keep every block row; silver sums to counties. |
| No group quarters, block population, hospitals, nursing homes, IPEDS, air travel, mobility or case data | Source packages under `src/data_ingestion_toolbox/` | Correct; none exists. There is also no block geography in `silver_ref`. |

## Sources confirmed (2026-11-12, read from the providers' own pages)

| Source | Confirmed |
| --- | --- |
| 2020 Census redistricting data `dec/pl` | P5 "Group quarters population by major group quarters type", P5_001N to P5_010N (correctional adult, juvenile, nursing, other institutional, college housing, military, other noninstitutional); H1 occupancy; P1 is race only, so its total P1_001N is the one P1 cell taken. |
| 2020 Census DHC `dec/dhc` | Block is a published geography (state > county > tract > block). P12 sex by age for selected age categories; P18 GQ population by sex by age by major GQ type; PCO1 to PCO10 GQ population by sex by age for each GQ type (PCO3 adult correctional, PCO5 nursing, PCO8 college housing). PCT18A to PCT18I are race iterations and are not requested. |
| CMS Provider Information `4pq5-n9py` | One row per nursing home, monthly (released 2026-10-28, next 2026-12-02), CSV distribution, `accessLevel: public`. |
| CMS Hospital General Information `xubh-q36u` | Every Medicare-registered hospital, CSV, public. |
| CDC Case Surveillance with Geography `n8mc-b4w4` | 106,219,500 rows, Public Domain U.S. Government, frozen (no updates after 2024-07). |
| CDC PLACES tract, 2025 release `cwsq-ngmh` | 3,047,284 rows, 40 measures, Public Domain. |
| HHS hospital capacity by facility `anag-cw7u` | Weekly per facility, Public Domain U.S. Government, frozen after 2024-05-03. |
| BTS T-100 Domestic Segment | Monthly 1990 to 2026; bulk-download route to confirm on the internal stack before the plan starts. |
| CDC FluView ILINet | Weekly by national, HHS region and state, with age groups; download route to confirm. |
| Google COVID-19 Community Mobility Reports | Frozen 2022-10-15. Use is under the **Google Terms of Service**, not an open licence. Not loaded until Nick decides (see `air-travel-and-mobility`). |
| New York Times COVID-19 data | Non-commercial licence. **Never loaded.** |

The cloud session cannot reach census.gov, cms.gov or cdc.gov downloads, so
each plan's first step is a live probe on the internal stack.

## Rules every plan here follows

- Free, public, provider-published data only; no hand-sourced file; official
  APIs and download paths only, with the toolbox's ordinary user agent.
- **No race, ethnicity, income or poverty field is requested by these plans.**
  The outbreak model's guardrail: those tables stay in the warehouse for the
  website, and none is added here. Where a source row carries such a field
  (CDC case records, LODES WAC/RAC race columns), it is not requested or not
  kept.
- True to source: figures as published, no extrapolation, no scaling, no
  imputation. A suppressed or noisy figure keeps its published form with a
  note (2020 block counts carry disclosure-avoidance noise; CDC case records
  are suppressed in small counties).
- Block grain is **Dane County (55025) first**. Any wider block load needs
  Nick's go with a measured size.
- No subjective words in user-facing text.
- Read speed gate: every new served view answers a place's read in under
  500 ms (BEGIN / EXPLAIN ANALYZE / ROLLBACK on the internal stack).

## Order and size (estimates, before any probe)

Drive H had 4,043 GB free on 2026-11-12. Estimates count raw capture, silver,
gold and indexes, and are rough (about a factor of two either way).

| # | Plan | Grain | Estimate |
| --- | --- | --- | --- |
| 1 | [`group-quarters-by-type`](GROUP_QUARTERS_BY_TYPE_PLAN.md) | nation, state, county, tract; block for Dane | 0.3 GB |
| 2 | [`nursing-home-locations`](NURSING_HOME_LOCATIONS_PLAN.md) | facility point, monthly | 0.2 GB a year |
| 3 | [`lodes-block-grain-for-dane`](LODES_BLOCK_GRAIN_FOR_DANE_PLAN.md) | block, Dane, 2002 to 2023 | 0.6 GB (no new download) |
| 4 | [`block-population-and-age-for-dane`](BLOCK_POPULATION_AND_AGE_FOR_DANE_PLAN.md) | block, Dane, 2020 | 0.1 GB |
| 5 | [`college-housing-capacity`](COLLEGE_HOUSING_CAPACITY_PLAN.md) | campus point | under 0.1 GB |
| 6 | [`hospital-capacity-and-locations`](HOSPITAL_CAPACITY_AND_LOCATIONS_PLAN.md) | facility, weekly 2020 to 2024 | 3 GB |
| 7 | [`places-tract-release`](PLACES_TRACT_RELEASE_PLAN.md) | tract | 1 GB a release |
| 8 | [`outbreak-calibration-sets`](OUTBREAK_CALIBRATION_SETS_PLAN.md) | county by month by age; facility weekly; state weekly | 3.5 GB (about 20 GB if the full case file were captured) |
| 9 | [`air-travel-and-mobility`](AIR_TRAVEL_AND_MOBILITY_PLAN.md) | airport, monthly | 2 GB |
| | Total | | about 12 GB, 30 GB at most |

The two volumes that would matter: block grain beyond Dane (LODES jobs by
block for every county about 150 GB; 2020 people and age by block for every
county 80 to 100 GB), and capturing the whole 106-million-row case file.

Steps 1 to 4 are what the Dane County slice needs first; 5 to 9 can follow
the first playable.

## Block data at the same scope as the other sources (Nick to decide)

Nick asked on 2026-11-12 (18:25Z) what block data would cost at national
scope, and what it is for, to decide whether it is built here or in the game
project. **Until he decides, `lodes-block-grain-for-dane` and
`block-population-and-age-for-dane` stay at plan stage**; the county, tract
and facility plans go ahead.

The 2020 Census has about 8.1 million blocks. Figures below are estimates
from row counts and the warehouse's measured bytes per fact (about 150 to
200 bytes with indexes); none is measured yet. A Dane load (about 0.7 GB in
all) would measure the real bytes per block, and every figure here scales
from Dane by roughly 1 to 230 for people and 1 to 170 for jobs.

| Item | Rows | Disk | Requests or files | Load time (estimate) |
| --- | --- | --- | --- | --- |
| Block shapes (TIGER `tabblock20`, 56 state files) | 8.1 M shapes | 10 to 15 GB | 56 downloads, about 8 GB zipped | hours |
| 2020 people and age by block (P12 + P1/H1/P5 totals) | about 510 M facts | 80 to 100 GB | about 85,000 Census API calls (one per tract), or 52 DHC summary files | about a day of requests at the Census API pace used for tracts; then silver and gold refresh of a day or more |
| LODES jobs by block, 2002 to 2023, 29 columns | about 4.4 billion facts at full width; about 1 billion with zeros dropped | about 150 GB | none (files already captured) | several days of replay and indexing |
| Block map tiles (Martin) | from the shapes | 5 to 20 GB of tile cache | none | per request |
| **Total** | | **about 250 to 290 GB** | | **about a week of loading** |

That is about 7% of the 4,043 GB free on drive H, and it would make block
the warehouse's largest grain after ACS tracts.

What in this repository would use block data today: **nothing yet.** The
finest grain the website and API serve is the tract (ACS, PLACES county
only), and ACS does not publish below block group. Uses that would have to be
built:

- a block map inside a tract page (people, ages, jobs by block);
- sums over an area a person draws, built from blocks (a neighbourhood, a
  school zone, a game district);
- daytime versus night-time population for small areas (LODES work blocks
  against home blocks).

The game needs block data for one thing: placing people and jobs inside
districts of about 0.5 sq km. That needs only the counties the game ships,
which is the case for building block data in the game project, or here
scoped to a declared county list.

## Acceptance criteria

1. Each listed plan is in `needs_review/` or recorded here as set aside by Nick.
2. The factor-by-factor table in Nick's document can be restated with every
   "Missing" row either served or set aside, each with its source and grain.
