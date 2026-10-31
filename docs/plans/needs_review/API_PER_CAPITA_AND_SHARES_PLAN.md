---
id: api-per-capita-and-shares
branch: feat/api-per-capita-and-shares
depends_on:
  - api-plain-labels
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api tests/unit/semantics tests/unit/shared -q
  - ruff format --check . ; ruff check .
---

# Per-person and share-of-total values, as a declared derived product

## Status

Needs review (v1.0 build-out, item A-3). Built 2026-10-31 on branch
`feat/api-per-capita-and-shares` (#175), with the provider-rate fix (#179)
and the state filter for PEP and SAIPE/SAHIE (below); every warehouse check
passed on OFFICE_LAB. What remains is Nick's review of the drafted pairings. Governed by
[ADR-0009](../../decisions/0009-per-capita-and-share-views.md), accepted by
Nick on 2026-10-26, so no new ADR is needed. Unblocks A-4 (cross-level
comparison) and D-6 (the per-capita toggle).

## Why

Place pages show counts. A reader comparing a city of 30,000 with a county of
a million needs "share of households" or "per 100,000 residents", and no
provider publishes most of those. Dividing two numbers is easy to get wrong:
different universes, different periods, different programs, and a survey
ratio with no margin of error presented as exact. ADR-0009 sets the rules;
this plan serves them.

## Deliverables

1. `docs/semantics/derived_ratios.json` with schema
   `derived_ratio.schema.json`: one reviewed pairing per derived view
   (`share`, `per_capita` with its scale, or `provider_rate`), its numerator
   and denominator metric codes, the universe, whether the numerator is a
   subset of the denominator (which picks the Bureau's MOE formula), owner,
   reviewer, status and citations. ACS pairings are declared once and expand
   to the 5-year and 1-year datasets.
2. `data_ingestion_toolbox.semantics.derived_ratios`: the loader. It refuses
   a registry that pairs across programs or datasets, a share whose
   numerator is not a subset, a recomputed view for a numerator whose
   provider publishes the rate (ADR-0009 rule 5), or an approved entry
   without a reviewer.
3. The computation, pure and unit-tested: the value scaled by the pairing,
   and for ACS inputs the margin of error by the Bureau's proportion formula
   (falling back to the ratio formula when the radicand is negative) or its
   ratio formula. A missing, suppressed or zero input gives no value and a
   reason, never zero.
4. `GET /api/v1/observations/derived?ratio_id=...`: the numerator's served
   rows (same filters, scope, release pin and reductions as
   `/observations`) each beside the denominator row with the same
   geography, period and release. Every row says `derived`, the pairing's
   identity and version, both inputs with their release and period, the
   universe, and the MOE method. A provider rate is served as the
   provider's own rows, `derived: false`.
5. `GET /api/v1/catalog/derived-ratios`: the pairings, filterable by
   numerator metric code, each saying whether it is served.
6. Only `approved` pairings are served (ADR-0009 rule 1). The local stack can
   set `DERIVED_RATIOS_SERVE_DRAFTS=1` to serve drafts, each row marked
   `pairing_status: draft`, so Nick can review them on the running site;
   listed empty in `infra/docker/stack.env.example`.
7. `API_CONSUMER_GUIDE.md` section, OpenAPI snapshot regenerated, behavioural
   catalog rows.

## Decisions taken here (within ADR-0009)

- **Controlled margins.** The served ACS rows do not keep the Bureau's MOE
  annotation (`*****`, sent as `-555555555`): silver keeps it in
  `value_source`, gold nulls it with every other sentinel. A null input MOE
  therefore cannot be told apart from an MOE the Bureau could not compute.
  The registry names, per denominator, the geography levels at which the
  Bureau controls that estimate (total population and housing units at
  nation, state, county and metro); there a null MOE is treated as zero, as
  the Bureau instructs. Anywhere else a null input MOE gives a null derived
  MOE with `margin_of_error_method: unavailable`. Carrying the annotation
  into gold is a warehouse follow-up, not done here, because it needs every
  ACS year re-published while the tract reload is running.
- **Rounding.** Values and MOEs are rounded to two decimals of the scaled
  unit (percent, per 1,000, per 100,000).

## Acceptance criteria

- A share is computed only from the same program, dataset, release, period
  and geography; anything else is unavailable with `different_period`,
  `denominator_missing`, `denominator_zero` or `suppressed_input`.
- MOEs follow the Bureau's formulas (*Understanding and Using ACS Data*,
  2020, chapter 8), checked in unit tests against hand-computed values.
- A draft pairing is refused with its reason unless the opt-in is set.
- A provider rate is served as the provider's, never recomputed beside it.
- Unit, Ruff and OpenAPI snapshot checks green; consumer guide documents the
  routes.

## Evidence

2026-10-31, cloud session, branch `feat/api-per-capita-and-shares`:

| Check | Result |
| --- | --- |
| `python -m pytest tests/unit -q` | 2,746 passed (new: `tests/unit/api/test_derived_ratios.py` 22, `tests/unit/semantics/test_derived_ratio_registry.py` 16) |
| `python -m ruff format --check .` ; `python -m ruff check .` (ruff 0.15.16, the pinned version) | clean |
| OpenAPI snapshot | regenerated: two new operations, `/catalog/derived-ratios` and `/observations/derived` |
| Catalog register | ETL-104, API-173, API-174 added; API-171/172 belong to the place search PR (#172) |
| DAG tier | not run here (no Airflow in the cloud container); this change touches no DAG |
| Database integration tier, warehouse checks | not run here; handed to OFFICE_LAB (see below) |

The registry holds 35 entries, all `draft`, expanding to 64 pairings: 29 ACS
shares for both datasets and 6 provider rates (FBI violent and property
crime, PEP births and deaths, SAIPE poverty, SAHIE uninsured). Until Nick
approves them, the route refuses each with its status unless the stack sets
`DERIVED_RATIOS_SERVE_DRAFTS=1`.

## Residual verification (OFFICE_LAB)

- Against the warehouse: `/observations/derived?ratio_id=poverty_share:acs5&geo_level=county&state_fips=48`
  answers every Texas county with a value and an MOE, and Travis County's
  value agrees with data.census.gov table S1701's percent below poverty
  within rounding.
- Every pairing's numerator and denominator metric codes exist in
  `gold_glossary.dim_metric`.

## OFFICE_LAB results (2026-10-31, dev at be571b3)

- `poverty_share:acs5`, Texas counties: all 254 counties have a value and a
  margin (`proportion`). Travis County is 10.93 ± 0.57 (142,566 / 1,304,589),
  and data.census.gov S1701_C03_001, 2024 ACS 5-year, gives 10.9 ± 0.6. Pass.
- `violent_crime_rate`, states: every row was unavailable (`suppressed_input`)
  because usability was a literal list of status words (`valid`), and FBI UCR
  says `reported`. Fixed in #179: an input is usable when it carries a
  number, which is the neutral contract's own rule.

Re-check on the API rebuilt from dev at f12d013, drafts served:

| Pairing | Query | Result |
| --- | --- | --- |
| `violent_crime_rate` | states, `period_start=2022-01-01` | 52/52 with a value, `derived: false`, no reason. Alaska 59.43, Alabama 32.21 |
| `property_crime_rate` | same | 52/52. Alaska 131.88, Alabama 130.98 |
| `birth_rate` | states, `newest_per_geography` | 52/52, period 2025-07-01. Alabama 11.139, Alaska 12.033 |
| `poverty_rate_saipe` | counties, `newest_per_geography` | 3,144 counties; all 254 in Texas have a value. Travis 2024 is 10.5 |

The SAIPE query with `state_fips=48` was refused: SAIPE/SAHIE and PEP rows
carry no `state_fips` column, so neither source declared the filter. Both
now declare it over the canonical `geo_id` (`state:NN|...`): a state and
every county in it match. The per-capita toggle and the compare pages ask
for one state's counties, so this was worth fixing here.

## Follow-up

- Keep the ACS MOE annotation in gold so controlled margins are known per
  row rather than declared per level.
- Nick reviews the drafted pairings; each becomes `approved` with his name.
