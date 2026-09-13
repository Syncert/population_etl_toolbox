---
id: explorer-grain-vocabulary
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The explorer offers every grain a measure declares

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the explorer's geography-grain handling.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/SourceExplorerPage.tsx`,
  `apps/web/lib/explorerViewModel.ts`, `apps/web/lib/urlState.ts`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`API_CONSUMER_GUIDE.md` states the vocabulary and the promise it carries:

> `geo_level` on a served row is always one of `NATIONAL`, `STATE`,
> `COUNTY`, `PLACE` (Census PEP), or `AGENCY` (FBI UCR), and a metric's
> `valid_geo_grains` in the catalog uses the same five words — so a grain
> read from the catalog can be sent straight back as the `geo_level` filter
> and will answer.

The explorer knows three of those words. `GEO_LEVEL_ORDER` is
`["NATIONAL", "STATE", "COUNTY"]`, and `urlState.GEO_LEVELS` the same, so a
measure published at `PLACE` or `AGENCY` falls through every decision:

1. `metricSupportedGeoLevels` reads `["AGENCY"]` from the catalog row.
2. `preferredGeoLevelForMetric` checks for COUNTY, STATE, then NATIONAL,
   finds none, and returns its `fallbackGeoLevel` — `COUNTY`, a grain the
   measure does not publish.
3. The correction effect sees the current level is not supported and sets it
   to that same unsupported preference.
4. `offeredGeoLevels` intersects the declared grains with the three known
   words and is **empty**, so the view-level control renders no options at
   all.
5. The observation request goes out as `geo_level=COUNTY`, the API answers
   honestly with nothing, and the panel reports "0 COUNTY records published
   for this selection".

The screen therefore says the measure publishes nothing, when what happened
is that the explorer asked for a grain the measure never claimed and never
offered the one it did. That is exactly the confusion the grain-narrowing
comment in this component says it set out to end — "reads as the app losing
the click rather than as the measure not being published there" — reproduced
one layer down, for the two words the client does not know.

FBI UCR is a completed source reachable through `/observations`, so it
appears as an explorer tab today, and every one of its measures declares
`AGENCY`. Census PEP's subcounty products declare `PLACE`.

A link is affected too: `parseExplorerState` drops `geo_level=PLACE` as
invalid, so a shared explorer link naming a place-grain view does not
reproduce it.

## Objective

The explorer's grain vocabulary is the API's, so a grain a measure declares
can be offered, selected, requested, and shared.

## Acceptance criteria

1. The explorer and the URL contract both carry the five published words,
   with a label for each.
2. `preferredGeoLevelForMetric` never returns a grain the measure does not
   declare: where the measure declares any, the preference falls through the
   spatial three and then to the measure's own first declared grain.
3. A measure declaring only `PLACE` or only `AGENCY` offers that grain,
   requests it, and receives its rows.
4. `geo_level=PLACE` and `geo_level=AGENCY` survive a round trip through
   `parseExplorerState`/`serializeExplorerState`.
5. The map continues to decline a grain the tile boundary has no geometry
   for, with the published reason it already gives — this plan does not make
   places or agencies mappable.
6. A measure that declares no grains keeps the current behavior: the full
   vocabulary, because unknown grains are not the same as none.
7. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Drawing place or agency geometry, or discovering a tile layer for either.
- Adding a geography picker for grains the map cannot draw; selecting one
  geography for the trend is a separate question this plan does not answer.
- Changing `/catalog` or the comparison workspace, whose grain controls are
  bounded by the analysis routes rather than by this vocabulary.

## Evidence

### The gap, established first

Two unit tests failed before any change: `preferredGeoLevelForMetric` returned
`COUNTY` for a measure declaring only `AGENCY` or only `PLACE`, and
`parseExplorerState` dropped `geo_level=PLACE` and `geo_level=AGENCY` as
invalid. A browser test then drove the whole path with a new FBI UCR
capability — a neutral source with no route segment, which is how the API
actually serves it — and an `AGENCY`-grain measure.

That browser test surfaced a second instance of the same defect, one the unit
tests could not see: even after the vocabulary was widened, a request went
out at `COUNTY` before the selection settled. The observation effect is
declared ahead of the effect that corrects the grain, so the first render
spent a request on a grain the measure does not publish and flashed
"0 COUNTY records published for this selection". The effect now returns early
while the selected grain is one the measure does not declare, and the
assertion is that the request set for an FBI UCR measure is exactly
`{AGENCY}` — not merely that it contains it.

### Scope, as delivered

The plan's acceptance criteria are about the client's vocabulary, and three
files carried the three-word version of it: `GEO_LEVEL_ORDER` and
`GEO_LEVEL_LABEL` in the explorer, `GEO_LEVELS` in the link contract, and the
preference ladder in `explorerViewModel`. All three changed together; the map
is untouched and still declines a grain the tile boundary has no geometry
for, with the reason it already gives — the new browser test asserts the map
tab is absent for an agency measure.

The explorer spec's capability fixture declares six sources now rather than
five, so two existing `data-source-count` assertions moved with it.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 230 passed |
| `npm --prefix apps/web run test:browser` | 59 passed (Chromium), up from 58 |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded |
| `npm --prefix apps/web run check:bundle` | every route within its declared budget |
| `npm --prefix apps/web run check:csp` | passed |
| `pytest tests/unit -q` | 1380 passed |
| `python -m tests.support.catalog_evidence` | 303-row register renders; WEB-038 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. It is where an agency-grain request would be proved
against a real warehouse; the browser tier proves the client asks for the
grain the capability and catalog declare, which is the part this plan owns.
The API side of the promise is already held by API-073, which asserts every
dispatch entry's `geo_level` filter compares the same expression it projects.

## Remaining work

None.
