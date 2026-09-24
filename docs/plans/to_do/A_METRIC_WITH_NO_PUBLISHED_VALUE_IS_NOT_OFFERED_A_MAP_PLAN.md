---
id: no-value-metric-not-mapped
branch: claude/no-value-metric-not-mapped
depends_on:
  - maps-offer-only-what-a-source-publishes
parallel_safe: false
complexity: medium
verify:
  - python -m pytest tests/unit -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A metric with no published value is not offered a map

## Plan status

- **Status:** Unclaimed.
- **Last updated:** 2026-09-24
- **Dependencies:** `maps-offer-only-what-a-source-publishes`, which made
  every publisher derive `valid_geo_grains` from rows that carry a value.
- **Next pickup:** NV-1.

## Why

Since `maps-offer-only-what-a-source-publishes`, a metric's catalog grains
list only the levels where the provider publishes a value. A metric whose
every row is withheld therefore publishes `valid_geo_grains = []`, and the two
readers of that field disagree about what it means:

- the publishers and the catalog mean **"no level has a value"**;
- the explorer's `metricSupportedGeoLevels` returns `[]`, and its callers
  read an empty list as **"unknown"** (`preferredGeoLevelForMetric`: "Unknown
  grains, not none: the caller's fallback still applies"), so the explorer
  would offer its default levels and a map that can only say "value not
  published".

On the development warehouse on 2026-09-24 no metric is in that state
(measured per source against the new publisher definitions: 0 of 4,447 ACS, 0
of 281 CDC, 0 of 101 NASS, ...), so this is latent. It becomes real the first
time a provider withholds a whole measure.

## Decision to take in NV-1

- **(a)** The catalog states it: an empty list means "no level has a value",
  and the explorer offers no map for such a metric (table only), with the
  reason on screen.
- **(b)** The publisher does not publish such a metric at all, and it retires
  through the harvest's grace period like any key a publisher stops emitting.

(a) keeps the metric discoverable and honest about why nothing is drawn. (b)
is simpler for every client, but hides a measure the provider does define.

## Work items

- [ ] **NV-1: decide (a) or (b)** and record it in the API consumer guide's
  catalog section.
- [ ] **NV-2: implement,** with a failing-first test at each layer it
  touches (publisher view, catalog, explorer).
- [ ] **NV-3: extend the map sweep** so a metric with `[]` grains that the
  explorer still offers a map for fails by name.

## Acceptance criteria

1. `valid_geo_grains = []` has one documented meaning, and every reader
   honours it.
2. No map is offered for a metric with no published value anywhere.
