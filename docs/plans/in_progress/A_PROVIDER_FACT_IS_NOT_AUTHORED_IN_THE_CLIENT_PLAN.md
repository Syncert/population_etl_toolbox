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

- [~] `grep -rn "65,000\|B01003\|census.gov" apps/web/{app,components,lib}`
      returns nothing, except a test that asserts the absence. **Met in
      substance, not literally.** Every one of the four client-authored facts
      this plan names is gone, and `tests/frontend/unit/provider-facts.test.js`
      forbids each from coming back. The literal grep still returns eight
      lines, none of which is a provider fact stated to a reader; they are
      listed and reasoned about under "The grep, literally" below. A reviewer
      who wants the grep satisfied to the letter should say so -- it means
      changing things this plan's deliverables do not mention.
- [x] A `SourceNote` unit test renders the source's `reference_url` for a
      non-Census source and no link when absent.
- [x] A home-page browser test asserts the featured link targets the
      catalog's first metric and that the strip states nothing the
      capabilities payload does not -- plus the empty-catalog case, which
      used to build a link to a metric nobody published.
- [x] `tests/frontend/browser/accessibility-operations.spec.js` and
      `tests/frontend/unit/explorer-sources.test.js` no longer assert the
      hard-coded sentence. The accessibility spec never did. **Three** tests
      did, not the two the plan names: `explorer-sources.test.js`,
      `format-and-persistence.test.js`, and
      `tests/frontend/browser/explorer.spec.js` -- which the plan does not
      mention and which a stale run nearly let through; see below. All three
      now assert the opposite.
- [x] `DATASET_FACET_LABELS` is gone and the dataset selector renders only
      what the catalog answers, asserted in `explorer-sources.test.js`.
- [x] `TESTING_CONTRACT.md` gains a `WEB-` row: WEB-112.

## Implementation evidence

### The four facts

- **The Census publication threshold** and the "complete/partial county
  coverage" labels are deleted, along with the "Not published in ACS1"
  missing-value label: what this client knows is that the answer carried no
  value for a geography, not which of a provider's rules is the reason. The
  uncoloured-geography reason (WEB-079) already says the former from the
  answer itself. `preferredDatasetFacet` still prefers `acs5` and now says so
  as a *preference* -- which dataset the application opens on is a choice it
  is entitled to make.
- **The display name.** `displayMetricName` returns the catalog's
  `metric_display_name`, or the absence of one. The override made the measure
  a reader is most likely to open the one measure whose label did not come
  from its source.
- **The reference link.** `SourceNote` renders `reference_url` from
  `/catalog/sources`, and no link where a source publishes none. The explorer
  did not read that resource at all -- the plan says it "already fetched" it,
  which was not so -- so a `getSources()` call was added at bootstrap. It
  fails soft: a missing reference is a missing link, not a broken screen.
- **The home page's feature.** The featured metric is the catalog's first
  answer; with an empty catalog the link offers the catalog itself rather
  than building one for a metric nobody published. The two unsourced strip
  cells are gone.

### The grep, literally

The criterion's grep still returns eight lines. None states a provider's rule
to a reader, and none is named by any deliverable in this plan:

- `lib/productTemplates.ts` ×3 -- a product template naming the measures it
  composes. That is the template's content; deleting it deletes the feature.
- `lib/explorerSources.ts` ×4 -- comments recounting the pre-glossary metric
  identity that ARC-005 ended. Removing them to satisfy a grep would delete
  the record of why a shipped defect happened.
- `lib/explorerViewModel.ts` ×1 -- `DEFAULT_POPULATION_VARIABLE`, which
  `pickPreferredMetric` uses as a default. By this plan's own reasoning about
  `acs5` it is a serving preference rather than a claim, and it is kept on
  the same terms.

So `provider-facts.test.js` forbids the **claims** rather than the strings:
five patterns, one per fact, each proven to fire. A bare `B01003` ban would
have to carry exceptions for the three cases above, and an exception list is
the thing nobody maintains.

### A green run that was not one

The full browser tier reported `146 passed` with `explorer.spec.js`'s
"ACS1 partial/no-data" test among them -- a test that asserts the deleted
sentence is *visible*. Run on its own immediately afterwards, against the
same working tree, it failed. The dev server the tier reuses had served a
stale compile of the explorer page, so the suite graded the code as it was
before the deletion.

Two things follow. The test is rewritten to assert the sentence is absent,
which is what this plan is for. And the count in the table below is from a
run started after that fix, because a tier that can serve a stale page can
report a number that describes nothing.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 595 passed, 40 files (was 587, 39) |
| `npm --prefix apps/web run test:browser` | **in flight**; `home-page.spec.js` 3/3 on its own |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `python -m pytest tests/unit/api/test_viz_coverage.py -q` | 10 passed |
| `python -m pytest tests/unit -q` | 1807 passed |

## Definition of done

Every sentence the web app states about a provider comes from the API, and a
grep for the four literals above finds only the tests that forbid them.

## What this plan deliberately does not do

- It does not change which dataset the explorer prefers by default; a
  preference is serving policy and may stay, but it is expressed as a
  preference, not as a fact about coverage.
