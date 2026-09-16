---
id: no-client-authored-provider-facts
branch: claude/no-client-authored-provider-facts
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
  - python -m pytest tests/unit/api/test_viz_coverage.py -q
---

# A provider fact is not authored in the client

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started. **Decision taken 2026-09-16 by the
  repository owner: the ACS coverage sentences are deleted, not moved
  upstream.** No API change is needed by this plan.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

`AGENTS.md` forbids duplicating warehouse or API rules in client code, and
`docs/reference/WEB_FIRST_WAVE_HANDOFF.md` forbids "a client-authored
definition that could be mistaken for a provider fact". Four remain:

- **A Census publication rule.** `apps/web/components/SourceExplorerPage.tsx:2379`
  renders "ACS 1-year county coverage is partial: Census publishes counties
  with populations of 65,000 or more..."; `:563` labels an uncoloured county
  "Not published in ACS1"; `apps/web/lib/explorerViewModel.ts:101` carries
  `DATASET_FACET_LABELS` ("ACS 5-year — complete county coverage", "ACS
  1-year — partial county coverage") and `preferredDatasetFacet` hard-prefers
  `acs5`. The 65,000 threshold is a Census rule that can change and the API
  publishes no field for it: `SourceCapability.datasets` is `string[]` and
  `MetricCatalog` carries no dataset note.
- **A display name for one variable.** `apps/web/lib/format.js:2-3` returns
  "Total population" for any metric code ending `B01003_001`, overriding the
  catalog's `metric_display_name`; `tests/frontend/unit/format-and-persistence.test.js`
  enshrines it.
- **A Census link for every source.** `apps/web/components/SourceNote.js:26`
  renders a fixed ACS estimate-guidance URL for every source, BLS, FRED,
  CDC, NASS and FBI included, while the API publishes `reference_url` per
  source (`apps/api/schemas/catalog.py`) and no file under `apps/web` reads
  it.
- **A featured metric and two facts on the home page.** `apps/web/app/page.js`
  hard-codes `CENSUS_ACS:acs5:B01003_001` as the featured metric and prints
  "County / national map coverage" and "Live / API-backed observations" as
  facts; the completed WEB-080 work removed the source enumeration and left
  these, and neither `home-status` nor `home-source-list` is referenced by a
  test.

## Deliverables

### 1. The source note reads the source's own reference

`SourceNote` takes the source's `reference_url` and label from
`/catalog/sources` (already fetched by the explorer and the home page) and
renders no link when the source publishes none.

### 2. The display name is the catalog's

Delete the `B01003_001` override; `displayMetricName` returns the catalog's
display name and falls back to the code. Update the enshrining test to
assert the fall-through.

### 3. The home page derives its feature

The featured metric is the first metric the catalog answers (the fallback
`page.js` already has), and the two strip cells are derived from
`/catalog/capabilities` (a source that publishes a drawable grain; a source
that is `serving`) or removed.

### 4. The ACS coverage sentences go away

Delete the two coverage sentences, the "Not published in ACS1" label and
`DATASET_FACET_LABELS`. The uncoloured-geography reason (WEB-079) already
tells a reader why a county has no colour, and the catalog's dataset list
names the datasets a source publishes. The dataset selector shows the
dataset code the catalog answers, with the catalog's own label if it has
one, and nothing else. `preferredDatasetFacet` may keep preferring `acs5`
as a serving default, expressed as a preference in a comment, not as a
statement about coverage.

If a later plan wants a per-dataset note, it adds one to
`SourceCapability` on the API first, harvested from the source's own
metadata, and the client renders exactly what is published; that is not
this plan.

## Acceptance criteria

- [ ] `grep -rn "65,000\|B01003\|census.gov" apps/web/{app,components,lib}`
      returns nothing, except a test that asserts the absence.
- [ ] A `SourceNote` unit test renders the source's `reference_url` for a
      non-Census source and no link when absent.
- [ ] A home-page browser test asserts the featured link targets the
      catalog's first metric and that the strip states nothing the
      capabilities payload does not.
- [ ] `tests/frontend/browser/accessibility-operations.spec.js` and
      `tests/frontend/unit/explorer-sources.test.js` no longer assert the
      hard-coded sentence.
- [ ] `DATASET_FACET_LABELS` is gone and the dataset selector renders only
      what the catalog answers, asserted in `explorer-sources.test.js`.
- [ ] `TESTING_CONTRACT.md` gains `WEB-` rows.

## Definition of done

Every sentence the web app states about a provider comes from the API, and a
grep for the four literals above finds only the tests that forbid them.

## What this plan deliberately does not do

- It does not change which dataset the explorer prefers by default; a
  preference is serving policy and may stay, but it is expressed as a
  preference, not as a fact about coverage.
