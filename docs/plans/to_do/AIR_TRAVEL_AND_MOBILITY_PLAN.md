---
id: air-travel-and-mobility
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/bts_t100 tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "bts" -q
  - python -m pytest tests/dags -m dag -q
---

# Every airport shows its monthly passengers and routes

## Status

**Air travel: set aside** (Nick, 2026-11-13: "Let's set it aside then. It's
not necessary"). Probes 1 and 13 (2026-11-12/13, read-only, toolbox client
identity) found no plain download of the T-100 segment routes:

- BTS's PREZIP folder carries on-time and DB1B files but no T-100 files.
- TranStats offers T-100 only through its download form
  (`DL_SelectFields.aspx?gnoyr_VQ=FIM`), an ASP.NET postback that would
  replay the form's hidden state; that is not proposed.
- `www.bts.gov` (the Data Bank 28DS page and the download index) answered
  403 and was not retried.
- data.gov lists no record for the dataset (404) and data.bts.gov has none.
- The only plain route is the USDOT_BTS NTAD ArcGIS layer, a 2024
  summary per origin airport (1,279 rows) with no destinations or routes;
  it is not loaded.

Nothing is built for air travel. Behaviour change (Google mobility) stays
blocked as below.

## Air travel

**BTS T-100 Domestic Segment (All Carriers)**, monthly 1990 to 2026: each
non-stop segment with carrier, origin, destination, passengers, seats and
departures performed. A U.S. government work. Madison is MSN. The cloud
session found no bulk-download link on the database page; the probe on the
internal stack confirms an official download route (BTS's prezipped monthly
files or its documented download form) before anything is built. The FAA's
calendar-year enplanements per airport are a second, smaller table.

Design: new adapter `bts_t100`; airports placed on counties by the BTS
airport master coordinates; gold views of monthly passengers per airport and
per route.

Size: about 250,000 segment rows a year, about 9 million rows for 1990 onward: about 12 GB at the measured 1,300 bytes per row (was 2 GB; see the umbrella plan).

## Behaviour change: Google COVID-19 Community Mobility Reports (blocked)

Frozen 2022-10-15, county daily. Google states: "In order to download or use
the data or reports, you must agree to the Google Terms of Service." That is
not an open data licence, and the game is sold, so **nothing is loaded until
Nick decides** whether those terms are acceptable. If he declines, the
behaviour response curve is left to the game's own reference files.

Size if approved: about 20 GB at the measured 1,300 bytes per row (was 3 GB; US county rows of one frozen file).

## Acceptance criteria

1. MSN and every Wisconsin airport show monthly passengers by route as
   published.
2. Mobility: Nick's decision recorded here before any load.
