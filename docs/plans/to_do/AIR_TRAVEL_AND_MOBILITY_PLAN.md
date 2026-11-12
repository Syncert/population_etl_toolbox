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

To do, and only if long-range seeding or behaviour change make it into the
model's design (step 9 of [`outbreak-model-inputs`](OUTBREAK_MODEL_INPUTS_PLAN.md)).

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

Size: about 250,000 segment rows a year, about 2 GB for 1990 onward.

## Behaviour change: Google COVID-19 Community Mobility Reports (blocked)

Frozen 2022-10-15, county daily. Google states: "In order to download or use
the data or reports, you must agree to the Google Terms of Service." That is
not an open data licence, and the game is sold, so **nothing is loaded until
Nick decides** whether those terms are acceptable. If he declines, the
behaviour response curve is left to the game's own reference files.

Size if approved: about 3 GB (US county rows of one frozen file).

## Acceptance criteria

1. MSN and every Wisconsin airport show monthly passengers by route as
   published.
2. Mobility: Nick's decision recorded here before any load.
