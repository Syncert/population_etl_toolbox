---
id: decennial-2000-and-1990-history
depends_on:
  - acs-place-tract-history-backfill
parallel_safe: false
complexity: high
verify:
  - python -m pytest -m unit -q
  - ruff check .
  - make test-linux TEST_ARGS='-m "integration and database" tests/integration/database'
---

# Census 2000 and 1990 history for places and tracts

## Status

In progress. Approved by Nick on 2026-10-26 as part of the v1.0 build-out
(Wave W): "Is there a way to get data further back than 2010?", then "Yes"
to adding the 2000 and 1990 censuses. NHGIS (1970/1980, harmonized tract
series) is a possible later addition and needs Nick's own free key.

## Why

The ACS reaches back to 2005 only for places over 65,000 people, and to 2009
for everything else. A citizen asking how their city or neighbourhood has
changed wants the 2000 and 1990 censuses: the last two full counts before the
ACS, at every place and tract. Loading Census 2000 also closes the backfill
plan's open item: 401 places in the 2009 ACS ceased before 2010 and stay
`unmapped` because no place reference before 2010 is captured.

## What the sources are (verified 2026-10-26)

- **2000:** the Census Data API serves `2000/dec/sf1` (100-percent counts:
  population, age, sex, race, Hispanic origin, households, housing units,
  tenure, vacancy) and `2000/dec/sf3` (sample: income, poverty, education,
  employment, commute, rent and owner costs). Both answer
  `for=place:*&in=state:NN` and `for=tract:*&in=state:NN county:*` in the
  same JSON shape as the ACS, with a key. Variables are named by table
  (`P001001`), not by ACS table (`B01003_001E`), and there are no margins:
  SF1 is a count, SF3 a sample estimate without published margins.
- **2000 place reference:** the Bureau's Gazetteer `places2k.zip`
  (`www2.census.gov/geo/docs/maps-data/data/gazetteer/places2k.zip`, fixed
  width, 1.07 MB) names every 2000 place with its land and water area.
  Census 2000 tracts are already published (ETL-089).
- **1990:** no longer on the Data API. The Bureau's archive
  (`www2.census.gov/census_1990/`) keeps STF1 and STF3 as the original
  CD-ROM extracts (dBase files per state). Reading them is a separate parser
  and a separate deliverable.

## Decisions

1. **One adapter shape, its own datasets.** 2000 is requested, captured and
   replayed through the Census ACS adapter's request/capture/replay path
   (same API, same payload shape), as datasets `dec2000sf1` and `dec2000sf3`
   with year 2000, so raw capture, revision history, resolution against the
   reference vintage and serving are reused rather than copied. Margins are
   absent, not zero: replay records a decennial value with no margin.
2. **Comparable series are declared, not inferred.** A curated crosswalk
   (`docs/semantics/decennial_acs_crosswalk.json`) names each decennial
   variable that measures the same thing as an ACS variable (total
   population, households, housing units, occupied/vacant, owner/renter,
   race and Hispanic origin, median household income in its own dollars,
   educational attainment 25+, commute mode). Only crosswalked variables
   join an ACS series; the rest stay their own series. ADR-0011's wording
   applies: a census count and a survey estimate are labelled as such.
3. **Geography is the decade's own.** 2000 values resolve against the
   2000 reference vintage (tracts from ETL-089, places from `places2k`);
   nothing is reassigned to 2010 boundaries by name.

## Deliverables

- D1. Census 2000 place reference from `places2k.zip` in `silver_ref`
  (closes the 401 unmapped 2009 places).
- D2. 2000 SF1 and SF3 datasets in the ACS adapter: config, variable
  metadata capture, request, replay without margins, DAG slices for place
  and tract.
- D3. Decennial-to-ACS crosswalk and its serving: crosswalked 2000 values
  appear in the place and tract series the site reads.
- D4. One live load of 2000 (places and tracts) with measured size.
- D5. 1990 STF1/STF3 from the Bureau's archive: capture, parser, 1990
  place and tract reference, crosswalk, live load.

## Progress

- 2026-10-26: plan written; API shapes and file locations verified above.
- 2026-10-26: D1 code: `parse_place_gazetteer_2000_capture`, 2000-2009 places
  map to vintage 2000 and `places2k.zip` (ETL-092). All 25,375 rows of the
  real file parse. Live `silver_ref` publication still to run.
