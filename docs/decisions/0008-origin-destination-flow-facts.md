# ADR-0008: Origin-destination flow facts

- **Status:** Accepted
- **Date:** 2026-10-06
- **Accepted:** 2026-10-06
- **Decision owners:** Data engineering and data-product maintainers
- **Related work:** [IRS county-to-county migration plan](../plans/needs_review/IRS_COUNTY_TO_COUNTY_MIGRATION_PLAN.md); [ADR-0001](0001-data-layer-boundaries.md)

## Context

Every fact in the warehouse so far describes one geography: a value for a
county, a state or the nation in a period. The IRS Statistics of Income
migration data describes movement between two: returns, individuals and
adjusted gross income moving from an origin county to a destination
county between two filing years. SOI also publishes, beside those flows,
rows that are not between two counties at all: a county's totals, its
non-migrants, and the "Other flows" categories into which every flow of
fewer than 20 returns is aggregated, some of them deleted (`-1`) to
protect taxpayers.

Bending the one-geography fact to hold a flow would put the second county
in a dimension column the dispatcher cannot filter, join or resolve, and
would let a flow pass with only one end resolved.

## Decision

1. **A flow is its own fact shape**, `silver_irs_migration.fact_flow`,
   with two geography keys, `origin_geo_id`/`origin_geo_sk` and
   `destination_geo_id`/`destination_geo_sk`, beside the `subject_geo_id`
   the provider's file describes (the destination of an inflow file, the
   origin of an outflow file). The period is the provider's pair of filing
   years, labelled as the provider labels it (`2022-2023`).
2. **Both ends resolve or the flow is refused.** A county-to-county row is
   admitted only when the subject, origin and destination all resolve to
   `silver_ref.dim_geo_entity`; otherwise it is quarantined with the side
   that failed (`subject_unresolved`, `origin_unresolved`,
   `destination_unresolved`). A named CHECK
   (`irs_flow_endpoints_resolved`) refuses a county flow with a missing
   key, and the replay reconciles flows plus refusals to parsed rows.
3. **Provider categories stay categories.** A row whose counterpart is one
   of the provider's own categories carries no origin or destination
   county (`irs_flow_category_names_no_county`) and keeps the category and
   label. Nothing is redistributed from a category into counties, and a
   deleted category is `withheld` with no value, never zero.
4. **One-geography figures the provider publishes stay one-geography.** A
   file's totals for its subject county are projected as ordinary
   observations (`gold_irs_migration.total_observation_*`) and served by
   `/api/v1/observations` and the glossary. The flows are served by their
   own resource, `/api/v1/migration-flows`, because the neutral
   observation shape has one geography.
5. **No derived figure.** No net migration is computed from inflows and
   outflows; a net figure is published only if a provider publishes one.

## Consequences

- A later flow source (commuting, trade) reuses this shape: two resolved
  geography keys, the provider's categories as labelled rows, and its own
  resource.
- A consumer can ask "top origins into this county" from one subject's
  rows without a join across files.
- The fact is larger than a one-geography fact would be, because the
  categories and totals are stored as rows rather than columns.
