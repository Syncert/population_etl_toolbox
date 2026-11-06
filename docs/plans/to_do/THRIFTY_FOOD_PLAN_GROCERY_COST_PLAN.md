---
id: thrifty-food-plan-grocery-cost
depends_on:
  - cost-of-living-section
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/usda_fns tests/unit/api -q
  - ruff format --check . ; ruff check .
  - RUN_INTEGRATION_TESTS=1 python -m pytest -m "integration and database" tests/integration/database -k "usda_fns" -q
  - python -m pytest tests/dags -m dag -q
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Every profile shows what a week of groceries costs a family, in dollars and hours of work

## Status

To do. Slice 3 of
[`EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md`](EVERYDAY_PRICES_PEOPLE_RECOGNISE_PLAN.md),
approved by Nick 2026-11-06 18:15Z.

## Why

"A week of groceries" is the most relatable food figure, and it must be a
published one: USDA's Food Plans (Thrifty, Low-Cost, Moderate-Cost,
Liberal) state the monthly cost of a healthy diet for reference families.
This project never sums BLS items into a basket of its own.

## Source

- USDA Food and Nutrition Service "Cost of Food" monthly reports, PDF,
  `fns.usda.gov` (served from `fns-prod.azureedge.us`). Nation (the
  contiguous states), with separate Alaska and Hawaii tables for the
  Thrifty plan. Reports back to the 1990s.
- **OFFICE_LAB check first:** download one report per five years and
  record whether the table layout is stable, which years carry which plans,
  and the Alaska/Hawaii coverage, before a parser is written.

## Data work

- New adapter `usda_fns` from the source-adapter starter. Registry of
  monthly reports, built from the publisher's index page; a scheduled run
  loads every month never loaded (LOADER RULE).
- Capture the PDF bytes; an unchanged file replays nothing (CHURN RULE).
  Parse the reference-family rows (family of four with two children 6–8 and
  9–11, and the individual age-sex rows) into silver by (plan, household
  type, month, area). A layout the parser does not recognise is
  quarantined, never guessed.
- Gold: weekly and monthly cost per plan. Minutes of work becomes hours of
  work for a week, using the `time_cost` entry from slice 1.

## Display

One line in the "Cost of living" section: "A thrifty week of groceries for
a family of four: $X, or N hours of work here". The area is the nation
(Alaska and Hawaii their own), named beside the number. A note links the
Food Plan definition in the glossary.

## Acceptance criteria

- Fixture PDFs from at least three layout eras parse to the published
  family-of-four figures exactly; an unknown layout is quarantined.
- A rerun over unchanged files writes zero rows.
- The line renders with its area and source; unit, DAG, build and browser
  tiers pass, with evidence recorded here.
