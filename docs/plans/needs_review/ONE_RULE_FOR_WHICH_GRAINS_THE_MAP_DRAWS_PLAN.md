---
id: one-rule-for-which-grains-the-map-draws
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# One rule for which grains the map draws

## Plan status

- **Status:** Implemented; awaiting review. Claimed and completed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/tileGrains.ts` (new),
  `apps/web/lib/viewModes.ts`, `apps/web/lib/explorerViewModel.ts`,
  `apps/web/lib/tiles.js`

## Context

Two functions decide what the map can draw, and they contradict each other.
Both are asserted, so both look deliberate.

`spatialGrains` decides whether the map is offered at all, and its test says
the considered position plainly:

```js
test("no published field identifies a national geometry", () => {
  // A national series is not a map that failed to colour — the boundary
  // has no polygon for it at all.
  expect(spatialGrains(TILE_FIELDS)).not.toContain("NATIONAL");
});
```

`tileFilterForGeoLevel` says the opposite, in a branch of its own:

```ts
  // … The national view keeps states and counties as its backdrop.
  if (geoLevel === "NATIONAL") {
    return ["in", ["get", "geo_level"], ["literal", ["STATE", "COUNTY"]]];
  }
```

and that branch is pinned too, in `explorer-contracts.test.js`.

**The branch is unreachable, unconditionally.** `spatialGrains` never returns
`NATIONAL`, so `describeViewModes` always reports the map unsupported at that
grain; `useMapLibre` removes the map when unsupported ("the map is removed
rather than hidden"), and both call sites early-return on `!map || !mapReady`.
So no national backdrop is ever drawn, and the comment describes a
presentation the application does not have.

There is a second disagreement underneath. `spatialGrains`' docstring
justifies reading the fips fields by a filter rule that
`tileFilterForGeoLevel` explicitly replaced:

> a feature with `county_fips` is a county and one without it is a state,
> **which is the filter the map already applies**

against

> The layer carries every geography with a shape — some 32k places among
> them, which have no `county_fips` either — so a level is matched on the
> published `geo_level` **rather than inferred from which fips columns a
> feature happens to carry**.

The second is current. The first is the rule it replaced, still standing as
the reason the grain list is derived that way.

Nothing a user sees is wrong. What is wrong is that a reader of either
function is told something the other contradicts, and a future reader adding
a national presentation would find half of one already asserted.

## Acceptance criteria

1. One declaration of the grains the boundary can draw, read by both
   `spatialGrains` and `tileFilterForGeoLevel`, so the two cannot disagree
   and adding a grain later is one edit.
2. `tileFilterForGeoLevel` answers a filter that matches nothing for a grain
   the boundary cannot draw — not the county filter by fall-through, and not
   a states-and-counties backdrop that never renders. An empty filter is the
   honest answer for a map that is not drawn, and if one ever were, an empty
   map beats the wrong grain's polygons.
3. `tileFilterForSelection` stays correct on top of that: narrowing nothing
   to a state is still nothing.
4. `spatialGrains`' docstring says what it actually relies on — the layer's
   published *schema*, which is a capability check and not a statement about
   which grains have rows.
5. The pinned expectation for `NATIONAL` changes, deliberately, with the
   reason recorded in the test.
6. No user-visible behaviour changes, and the browser tier is the evidence.
7. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-062).

## Non-goals

- Adding a national or place presentation. Whether the boundary should
  publish place geometry is a warehouse and tile-configuration question;
  this plan makes the client's answer to "can I draw this grain" single and
  honest.

## What changed

A third site turned up during implementation, and it was the one with a
cost. `loadPreviewTileFeatures` decided the grain with
`isCountyObservation` — the rule `tileFilterForGeoLevel`'s comment says it
replaced — so `geoLevel === "STATE" && !isCounty` kept every place polygon
the tile carries. The collection handed to the choropleth source therefore
held some 32k places for the layer filter to hide again. Nothing a user saw
was wrong, because the layer filter is right; the work was wasted twice over
and the retired rule was still load-bearing.

- `apps/web/lib/tileGrains.ts` (new) declares `DRAWABLE_TILE_GRAINS` — each
  grain with the layer field that attributes it — and `isDrawableTileGrain`.
  It is a leaf module so neither `viewModes` nor `explorerViewModel` has to
  import the other.
- `spatialGrains` filters the declaration against the layer's published
  fields, and its docstring now says what it relies on: a capability check
  against the layer's *schema*, saying nothing about which grains carry rows.
- `tileFilterForGeoLevel` normalises the grain, answers `false` for one
  outside the declaration, and otherwise matches the published `geo_level`.
  `NATIONAL`'s backdrop branch is gone; `PLACE` and `AGENCY` no longer fall
  through to the county filter.
- `tileFilterForSelection` returns `false` when the level filter is `false`,
  rather than `["all", false, …]`. Its `levelFilter === true` branch went
  with the fall-through that could have produced it.
- `isCountyObservation` was deleted: the decoder was its only caller, and the
  rule it encoded is the one being retired.
- `featuresAtGrain` was extracted from `loadPreviewTileFeatures` so the grain
  selection is testable without a real protobuf, and the loader returns early
  for an undrawable grain — no map is drawn there, so no tile is fetched.
  The empty grain means "every feature the layer carries", which is what
  checking a decoded tile needs; it is spelled as the absence of a grain
  instead of borrowing `NATIONAL`.

## Validation

- `npm --prefix apps/web run test:unit` — 22 files, **331 passed** (325
  before: +6 nodes).
  - `explorer-contracts.test.js`: the `NATIONAL` expectation is replaced by
    "a grain the boundary cannot draw filters to nothing, not to counties",
    which pins `false` for `NATIONAL`, `PLACE`, `AGENCY` and the empty grain,
    and records in the test why the old expectation changed. The selection
    node gains `tileFilterForSelection("PLACE", "06") === false`.
  - `view-modes.test.js`: "the grain offered and the grain drawn come from
    one declaration" walks every `GEO_LEVELS` entry and requires the offer
    and the filter to agree, then withdraws each attribution field in turn
    and requires the grain to go with it.
  - `tile-discovery.test.js`: four nodes over `featuresAtGrain` and the
    loader's early return, against a synthetic layer whose features publish
    the `geo_level` `martin.yml` declares — including the place with no
    `county_fips`, which is the feature the old rule misread as a state.
- `npm --prefix apps/web run test:browser` — **80 passed**. This is
  acceptance criterion 6: the tier exercises the map through the same
  fixtures and nothing a user sees moved.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `ruff format --check .` / `ruff check .` — clean (442 files).
- `pytest tests/unit/shared/test_repository_hygiene.py
  tests/unit/shared/test_catalog_evidence.py` — 16 passed, and
  `python -m tests.support.catalog_evidence` renders WEB-062 `FULL`.

### Not run

- `tests/frontend/smoke/live-stack.smoke.test.js` needs a deployed stack and
  is not part of the local tiers. Its `loadPreviewTileFeatures(..., "NATIONAL")`
  call — which relied on the decoder treating that grain as "no filter" — is
  updated to `""` with the reason recorded, and a node was added asserting
  that states plus counties cannot outnumber the features the tile carries,
  which is what a decoder answering one grain with another's polygons would
  violate.

## Remaining work

- None. Review is the remaining step.
