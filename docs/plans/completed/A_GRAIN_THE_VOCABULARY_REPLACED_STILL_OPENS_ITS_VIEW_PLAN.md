---
id: a-grain-the-vocabulary-replaced-still-opens-its-view
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
  - python -m pytest tests/unit/shared -q
---

# A grain the vocabulary replaced still opens the view it names

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/urlState.ts`,
  `apps/web/lib/explorerViewModel.ts`,
  `tests/unit/shared/test_grain_vocabulary_agreement.py`

## Context

ADR-0002 promises that a saved configuration or a shared link holding
`NATION` keeps answering: the catalog published that word for CDC, Census PEP
and USDA NASS before the grains were unified, so stored documents and links
carry it. The API keeps that promise in one function --
`registry.normalize_geo_level` trims, upper-cases and de-aliases — and
API-122 and API-123 are both built on it: a request or a stored document
carrying `NATION` is accepted and answers as `NATIONAL`.

The web application declares the same vocabulary again, because a browser
cannot import a Python module. It honoured case and **not** the aliases:

```text
parseExplorerState("?geo_level=NATION")        -> {}
parseExplorerState("?geo_level=county")        -> { geoLevel: "COUNTY" }
parseComparisonState("?geo_level=NATION")      -> {}
serializeExplorerState({ geoLevel: "NATION" }) -> ""
reopenHref({ filters: { geo_level: "NATION" }, … })
  -> "/explore?metric=X%3A1&scope=latest"      # the grain is gone
metricSupportedGeoLevels({ valid_geo_grains: ["NATION"] })
  -> ["NATION"]                                # a sixth grain nothing knows
```

Four consequences, all of them silent:

1. A saved configuration recording `geo_level: "NATION"` — which the API
   accepts and replays — reopens with no grain in the link, so the explorer
   opens on its **default** grain. For a national-only measure that is a
   request for county rows the measure does not publish, which is WEB-038's
   defect in its own words: "a shared link opened on a grain the measure does
   not publish".
2. A shared link carrying the alias loses it the same way.
3. The comparison workspace loses it too, through its own parser.
4. A metric whose catalog entry carries the alias reports `["NATION"]` as its
   supported grains, so `preferredGeoLevelForMetric` finds neither `COUNTY`,
   `STATE` nor `NATIONAL` in the list and falls through to the caller's
   fallback — the picker offers a grain, and the default chooses one, from a
   word neither of them recognises.

And nothing compared the two declarations of this one vocabulary. Both halves
of that duplication have already gone wrong on their own: WEB-038 dropped
`PLACE` and `AGENCY` from the web list, and this found the alias map missing
entirely.

## What was changed

- `urlState.ts` declares `GEO_GRAIN_ALIASES` beside `GEO_LEVELS` and exports
  `normalizeGeoLevel`, the three steps the API function takes, with the same
  rule stated: normalising is not validating, so a word that is not a grain
  comes back unchanged and each reader still drops it.
- Every grain entering the application goes through it: both URL parsers,
  both serializers (so a state built from an alias produces a link carrying
  the vocabulary word, and a link shared onward reads the same everywhere),
  `metricSupportedGeoLevels`, and `tileFilterForGeoLevel` — which removes the
  third local `toUpperCase()` of the same idea.
- `explorerViewModel.normalizeGeoLevel` is now a re-export rather than a
  second implementation; its existing callers are unchanged.
- `tests/unit/shared/test_grain_vocabulary_agreement.py` compares the two
  declarations for a living: the grains in order (the web list is what the
  picker offers), the alias maps for equality, the invariants an alias map
  must satisfy — every alias names a word the vocabulary carries, and no alias
  is itself a vocabulary word — and the observation scopes, which are the
  other closed vocabulary both ends declare, read from the served document
  rather than the router (ENV-017).

## Validation

Frontend, in `tests/frontend/unit/url-state.test.js` and
`geography-picker.test.js`, every case derived from `GEO_GRAIN_ALIASES` and
`GEO_LEVELS` rather than listed:

- every alias resolves to its word in any case, and with surrounding space
- a word that is not a grain comes back unchanged and both parsers drop it
- an aliased link opens on the grain it names, in the explorer and the
  comparison workspace
- a state carrying an alias serializes as the vocabulary word, and equals the
  default when it is the default
- every vocabulary word survives the round trip
- `valid_geo_grains` carrying an alias resolves, so the picker offers the
  grain and `preferredGeoLevelForMetric` chooses it

With the de-aliasing removed from `normalizeGeoLevel` and the tests in place:

```text
→ expected 'NATION' to be 'NATIONAL' // Object.is equality
→ expected {} to deeply equal { geoLevel: 'NATIONAL' }
→ expected '' to be 'geo_level=NATIONAL' // Object.is equality
→ expected [ 'NATION' ] to deeply equal [ 'NATIONAL' ]
Tests  4 failed | 27 passed (31)
```

And the agreement guard, with each declaration drifted in turn:

```text
E  AssertionError: apps/web/lib/urlState.ts declares ('NATIONAL', 'STATE',
   'COUNTY', 'AGENCY') and apps/api/registry.py declares ('NATIONAL',
   'STATE', 'COUNTY', 'PLACE', 'AGENCY'); a grain in one and not the other is
   a grain the API serves and the application cannot select, or the reverse

E  AssertionError: apps/web/lib/urlState.ts declares {'NATION': 'NATIONAL'}
   and apps/api/registry.py declares {'NATION': 'NATIONAL', 'US': 'NATIONAL'}
E  AssertionError: apps/web/lib/urlState.ts declares ('latest',) and the
   served contract declares ('latest', 'as_released'); a scope in one and not
   the other is a read the application offers and the API refuses, or the
   reverse
```

## Deliberately not done

- **The aliases are not offered in the picker.** They are words the
  vocabulary replaced, not grains to choose; `GEO_GRAIN_ORDER` stays
  `GEO_LEVELS`, which the agreement guard now holds to the API's order.
- **No new alias.** The map is the API's map, and adding to it is a decision
  about what the catalog once published, not about this application.
