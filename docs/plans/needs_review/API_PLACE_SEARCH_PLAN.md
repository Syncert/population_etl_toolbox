---
id: api-place-search
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api/test_place_search.py -m integration -q
  - ruff check .
---

# A reader finds their place by name, or by where they are

## Status

Ready for review (v1.0 build-out, item A-1), 2026-10-26, branch
`feat/api-place-search`. Merge after the Wave W branches it was built on
(`feat/acs-tract-tables-expanded` and its ancestors); the code itself touches
only the API.

## Why

Search and "use my location" landed on a county: there was no server-side
place search, so a reader in Austin could not reach Austin's page, and
nothing offered Travis County for "Austin".

## What changed

- `GET /api/v1/catalog/geographies/search`: name matches across states,
  counties, metros, places and ZCTAs from `gold_glossary.dim_geo_latest`,
  ranked exact, prefix, word, sounds-like (`dmetaphone`, already installed
  with `fuzzystrmatch`), then by served ACS population (a metro without its
  own figures by its counties' sum). A city carries the county containing its
  internal point and that county's metro; a city matched by name brings its
  county right after it.
- `GET /api/v1/catalog/geographies/locate`: the geographies whose boundary
  contains a point, smallest first; a metro through its county.
- Both are catalog routes, so the existing catalog cache and rate limit
  apply. Consumer guide section "Finding a place by name, or by where you
  are"; OpenAPI snapshot regenerated.
- No page slug is returned: the web already builds addresses from ids and
  names, and a second copy of that rule in the API would drift.

## Evidence (2026-10-26)

- Against the internal stack's warehouse: "Austin" answers the Austin-Round
  Rock-San Marcos metro (2,426,592 by its counties), Austin city, Texas
  (979,539, in Travis County), then Travis County itself, in 0.5 s;
  "Madisen" finds Madison, Wisconsin by sound; the point 30.2672, -97.7431
  answers tract 11.03, ZCTA 78701, Austin city, Travis County, the Austin
  metro and Texas in 0.05 s.
- `pytest tests/integration/api/test_place_search.py` against the test
  PostGIS: 3 passed (API-171). `pytest tests/unit`: 2621 passed.
