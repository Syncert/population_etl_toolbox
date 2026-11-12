---
id: place-climate-normals
depends_on:
  - weather-and-disaster-history
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# A county page shows its monthly climate normals

## Status

Built (2026-11-12), awaiting review. Nick (2026-11-12): "Incorporate the
climate piece as well, yes. Nothing extreme though." Web only: no new load,
source, API route or warehouse object.

Evidence: `tests/frontend/unit/climate-normals.test.js` and the climate
test in `tests/frontend/browser/places.spec.js` (WEB-175).

## Why

The NOAA 1991-2020 monthly normals (#355) and the nearest-county placement
note (#360) were served by the API and the explorer's CSV export only; the
place page had no climate card, so a reader never saw the placement note.

## What

- `apps/web/lib/climateNormals.ts`: four monthly normals (mean, mean daily
  high, mean daily low, precipitation), laid out by calendar month from the
  served `month` dimension; values shown at NCEI's station precision with a
  fixed number of decimals (0.1 F, 0.01 in), lead line and table alike (the
  county mean is stored to two decimals; the second temperature decimal comes
  from averaging and is rounded, follow-up after the 2026-11-12 deploy showed
  "57.0°F" beside "71.46°F"); station counts from `station_count`; every
  `placement_note` shown as served.
- `apps/web/components/ClimateNormals.tsx`: a "Climate averages" section on
  every place page, after Starting a family, linked from the chapter rail.
  A county reads its own four normals (four reads of twelve rows, behind the
  page's one catalog read). Every other level says NOAA publishes normals for
  stations and this site shows them for counties; a city links the counties
  it lies in. No state, metro or city figure is made from counties.
- Rules kept: no annual figure is made from the months (NCEI's own annual
  normals exist but are not shown here); a month with no row says "Not
  published"; no subjective word.

## Not in scope

Snowfall and degree days, county monthly temperature and precipitation
since 1895 (nClimDiv), storm events and FEMA declarations by type are served
but not on the place page. Any of them is a possible follow-up.
