---
id: irs-county-migration
depends_on: []
parallel_safe: true
complexity: high
verify:
  - python -m pytest tests/unit/irs_migration -q
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/database/test_irs_migration_capture_replay.py -m "integration and database" -q
  - python -m pytest tests/dags -m dag -q
  - ruff check .
---

# IRS county-to-county migration flows

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

PEP tells a county its net migration. The IRS Statistics of Income migration
data tells it where people came from and where they went: county-to-county
inflows and outflows by number of returns, number of exemptions, and
adjusted gross income, annually, derived from address changes on tax
returns. It turns one number into a story with named origins, which is both
the most engaging section of the People chapter and a natural video. It is
also the first flow-shaped dataset in the warehouse, so the dimensional
model must gain an origin-destination fact shape without bending the
existing one-geography fact pattern.

## What exists

- PEP components of change, including domestic and international migration,
  at county grain.
- The source-adapter starter and checklist; geography resolution by county
  FIPS; the shared time dimension.

## Deliverables

1. **Adapter package** `src/data_ingestion_toolbox/irs_migration/` from the
   starter: a client for the published county inflow and outflow files per
   filing-year pair (exact file names, layouts, and the meaning of the
   aggregate rows such as same-state, other-state, foreign, and
   non-migrant verified against the official SOI documentation); no
   credential expected.
2. **Flow fact shape.** A silver fact keyed to the time dimension and two
   geography keys (origin, destination) with returns, exemptions, and
   adjusted gross income as measures; the SOI suppression rule (small flows
   suppressed and rolled into aggregate rows) preserved as withheld and the
   aggregate rows kept as their own labeled subjects, never redistributed.
   The dimensional contract for a two-geography fact is recorded as a
   decision beside ADR-0001.
3. **Gold.** Deterministic publication of flows per (origin, destination,
   filing-year pair, measure) and the per-county totals the files carry
   (total inflow, total outflow); no net figure is computed here unless the
   files publish one.
4. **Serving.** A flow resource (route shape decided against the consumer
   guide, additive) answering "top origins into this county" and "top
   destinations from this county" for one year with the suppression and
   aggregate rows visible; discovery entry; the consumer guide gains a
   section on reading a flow row (returns are filers, exemptions
   approximate people, income is AGI of movers); OpenAPI snapshot updated.
5. **Quality and operations.** Quality rules (suppression preserved,
   origin and destination both resolve, totals reconcile to the files' own
   totals within the documented tolerance), DAG, operations guide, external
   contract module, bootstrap and reset instructions.
6. **Web, last.** The People chapter gains "Where people came from" and
   "Where people went" lists for the latest year with the withheld note,
   and the Change chapter links them beside PEP net migration with the
   statement that the two sources measure different populations.

## Acceptance criteria

- Configuration imports without I/O; checked-in inflow and outflow fixtures
  for one state replay offline into the flow fact with suppressed flows
  withheld and aggregate rows labeled; a malformed fixture is quarantined.
- The two-geography fact shape is recorded as a decision document and
  covered by a unit test that refuses a flow whose origin or destination
  does not resolve.
- Gold publishes flows and file totals with no computed net figure; the
  glossary contract test passes.
- Idempotent re-run; both checksums retained on a changed file.
- The flow resource answers top origins and destinations for a county
  fixture with withheld counts visible; capabilities advertise the source;
  consumer guide and OpenAPI snapshot updated.
- Quality rules, DAG parse, and external contract registration are in place.
- The county page shows both lists with the withheld note and the
  different-populations statement beside PEP; a browser scenario asserts
  the statement.
- Unit, database integration, DAG, and Ruff checks pass; evidence recorded
  here.

## Open items to resolve during implementation

- Year-pair labeling (filing years versus tax years) and how it maps to the
  shared time dimension; label exactly as SOI does.
- Whether the flow resource belongs under observations or a new namespace;
  it is provider-published data, not derived, so it must not be labeled
  derived.

## Checkpoint

Next pickup: draft the two-geography fact decision document, then copy the
starter and write the failing replay test for one state's inflow fixture.
