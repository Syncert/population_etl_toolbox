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

Ready for review (v1.0 build-out, item A-2), first built 2026-10-26 and
finished 2026-10-31 on branch `feat/api-plain-labels` after merging `dev`. Completes RU-2's per-metric definitions half of
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
4. (2026-10-31) Every route that names a metric by code now labels it:
   `metric_label_a`/`_b` with statuses on `/comparison`,
   `/comparison/preflight` and `/comparison/correlation`; `metric_label` on
   each `/comparison/matrix` measure, on `/distribution/bins` and on a
   distinctive measure that was not ranked. The labels come from glossary
   rows those routes already resolve, so no query was added.
5. (2026-10-31) The place page's groceries-and-gas card: four new entries
   (21 codes) for EIA regular gasoline, BLS regional gasoline, the BLS
   food-at-home CPI for the nation, each region and each division, and BEA
   all-items price parities. The coverage test now expands the card's
   region and division templates, so a new card line without an entry fails.
6. (2026-10-31) `METRIC_LABELS_REVIEWED_ONLY=1` serves only `approved`
   labels: a draft gives way to the provider's label while its definition is
   still served, marked draft. Off by default so the drafts can be reviewed
   on the running site. Listed in both env examples and passed by both
   Compose files.

Registry now: 110 entries, 165 metric codes, all `draft`.

## For Nick's review: before and after

What a reader saw, and what the API now serves as `metric_label`:

| Metric code | Before (provider label) | After (draft plain label) |
|---|---|---|
| `CENSUS_ACS:acs5:B19083_001` | Estimate!!Gini Index | Gini index of income inequality |
| `CENSUS_ACS:acs5:B19013_001` | Estimate!!Median household income in the past 12 months (in 2023 inflation-adjusted dollars) | Median household income |
| `CENSUS_PEP:RNETMIG` | Rnetmig | Net migration rate |
| `EIA:EPMR` | EIA's series name (no entry until now) | Regular gasoline, average retail price |
| `BLS:CUUR0000SAF11` | BLS's series name (no entry until now) | Food at home prices (Consumer Price Index) |
| `BEA:SARPP:1` | BEA's line name (no entry until now) | Regional price parity, all items |
| an undefined ACS code, `B01001_003` | Estimate!!Total:!!Male:!!Under 5 years | Total: Male: Under 5 years (status `provider`) |

To review: read `docs/semantics/metric_definitions.json`; for each entry you
accept, set `status` to `approved` and `last_reviewed_date`. Any you reject,
correct the wording or delete the entry (its metric falls back to the
provider label). Before a public deployment, decide whether to set
`METRIC_LABELS_REVIEWED_ONLY=1` there.

## Acceptance criteria

- Every metric code `placeChapters.ts` asks for has an entry (the NASS crop
  code excepted: its label is the page's own "Corn for grain, acres
  harvested" and the code is a content hash) -- API-170.
- Every code the groceries-and-gas card can ask for has an entry -- API-170.
- Every response naming a metric by code carries its label, per side for
  two-sided responses; the reviewed-only switch serves the provider label
  for a draft -- API-170.
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

- 2026-10-31 (Linux cloud session, after merging `dev`):
  `python -m pytest tests/unit -q`: 2708 passed. `ruff check .` and
  `ruff format --check .`: clean. OpenAPI snapshot regenerated
  (`python -m tests.support.regenerate_openapi_contract`: 57 operations,
  92 schemas; the diff is the 20 added label fields).

## Residual verification

Not required by a criterion; filed as
[`docs/plans/human_testing/PLAIN_LABEL_CODES_EXIST_IN_THE_LOCAL_GLOSSARY.md`](../human_testing/PLAIN_LABEL_CODES_EXIST_IN_THE_LOCAL_GLOSSARY.md):
that the 21 codes added on 2026-10-31 exist in the internal warehouse's
glossary, and the live routes return the labels. Handed to the OFFICE_LAB
session.

## Follow-up

D-2 makes every web screen read `metric_label` instead of its own wording.
