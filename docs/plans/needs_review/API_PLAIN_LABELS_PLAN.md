---
id: api-plain-labels
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api tests/unit/shared -q
  - ruff check .
---

# Every metric is named in plain words, served by the API

## Status

Ready for review (v1.0 build-out, item A-2), 2026-10-26, branch
`feat/api-plain-labels`. Completes RU-2's per-metric definitions half of
`TIME_WINDOWS_AND_ROLLUPS_PLAN.md`.

## Why

Readers met raw provider labels: the ACS variable path ("Estimate!!Gini
Index"), PEP column codes ("Rnetmig"). The place pages had plain wording,
but only in client code (`apps/web/lib/placeChapters.ts`), so every other
screen fell back to the provider label, and the rule "warehouse, then API,
then web" was inverted for the words a reader sees first.

## Deliverables

1. `docs/semantics/metric_definitions.json`: 106 entries defining 144 metric
   codes -- every measure a place page shows (ACS for both datasets, PEP,
   LAUS, QCEW and its sectors, BEA and its earnings sectors, SAIPE, SAHIE,
   CDC PLACES, FBI UCR, building permits). Labels, universes and notes are
   the place pages' own wording moved behind the API; each one-sentence
   definition follows the provider's documentation and is cited. All are
   `draft`: Nick reviews them in the dev -> main release, as he did RU-2's
   time-aggregation methods.
2. `data_ingestion_toolbox.semantics.definitions`: the registry's reader and
   `plain_label`, which never guesses: a metric with no entry gets its
   provider's label, with only the ACS `Estimate!!` path set as words.
3. API: `metric_label` and `metric_label_status` on catalog entries,
   observation rows (dashboard and neutral) and distinctive ranks; catalog
   metrics also carry `definition`. `metric_display_name` is unchanged.
   OpenAPI snapshot regenerated; consumer guide section "Plain labels and
   definitions".

## Acceptance criteria

- Every metric code `placeChapters.ts` asks for has an entry (the NASS crop
  code excepted: its label is the page's own "Corn for grain, acres
  harvested" and the code is a content hash) -- API-170.
- A defined metric is served its label with its status; an undefined ACS
  metric is served the Bureau's label without `Estimate!!` and status
  `provider`; the registry refuses a broken entry -- API-170.

## Evidence

- `pytest tests/unit` (2026-10-26, Windows): 2604 passed.
- Every defined code checked against the internal stack's catalog: 141 of
  144 exist; `CENSUS_BPS:units:{1_unit,5_plus_units}:annual` (annual permits
  are not loaded locally) and `BLS_QCEW:employment:92:5` (public
  administration has no private ownership) do not, and are kept because the
  page asks for them.
- `ruff check .`, `ruff format --check .`: clean.

## Follow-up

D-2 makes every web screen read `metric_label` instead of its own wording.
