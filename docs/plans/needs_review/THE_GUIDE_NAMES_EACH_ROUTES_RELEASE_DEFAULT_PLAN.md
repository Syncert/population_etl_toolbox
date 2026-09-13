---
id: the-guide-names-each-routes-release-default
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest tests/unit/api/test_consumer_guide.py
---

# The guide names each route's release default, which are not the same

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `docs/reference/API_CONSUMER_GUIDE.md`

## Context

Three resources in this API answer the same question — which release's values
do you want — and they answer it three different ways. Read off the live API:

```
GET /observations?metric_code=…                 -> "scope": "latest"
GET /cdc/observations?limit=2                   -> "release_selection": "latest_release"
GET /usda-nass/observations?limit=2             -> "release_scope": "as_released",  total 28
```

- Three envelope field names: `scope`, `release_selection`, `release_scope`.
- Three vocabularies: `latest`/`as_released`, `latest_release`/`single_release`,
  `latest`/`as_released`.
- **Opposite defaults.** `/observations` and `/cdc/observations` answer the
  newest release. `/usda-nass/observations` answers *every* published
  release: 28 rows spanning two releases of the same figures, where the same
  bare request to its sibling answers one.
- `/cdc/observations` has no history parameter at all — the newest release,
  or one pinned with `release=`. CDC revisions are only reachable through
  `/observations?scope=as_released`.

Every one of those facts is pinned behaviourally, at three tiers: the unit
tier asserts both CDC vocabulary values, the integration tier both NASS ones,
and the end-to-end tier the CDC pair again. So this is not a defect in the
code. It is a defect in what a consumer is told: the guide's entire word on
these two routes is that they "remain for source-specific exploration", and
it lists them.

The consequence is the repository's own recorded failure mode. WEB-047 wrote
it down for a saved map: a request that does not reduce "replays as the whole
latest publication, which for a source publishing a series per geography
colours whichever row arrived last". A consumer charting
`/usda-nass/observations` without `latest=true` is handed every revision of
each figure and has not been told to expect them.

## Acceptance criteria

1. The guide names, per route: how you ask for a release, what you get if you
   do not ask, the envelope field that says which you got, and that field's
   vocabulary.
2. It says plainly that the defaults differ between the source-scoped routes,
   because that is the part a reader cannot guess.
3. It says `/cdc/observations` answers no as-released history and names the
   resource that does.
4. The claims are derived and guarded, not prose alone: every route the table
   names is served, the parameter it names is declared on that route (and
   where the table says none, none is declared), its default matches the
   served contract, and the envelope field it names exists on that route's
   response schema in the reviewed snapshot.
5. No behaviour changes. Renaming a served field or flipping a default is a
   breaking contract change and is not this plan's to take.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-110).

## Non-goals

- Aligning the three field names or the two defaults. Both are breaking
  changes with real consumers on the other side; recording what they are is
  what makes the choice reviewable later.

## Validation

**Found by probing the USDA NASS surface** over a real warehouse — the
hand-written source router with the least attention — and reading its
envelope beside CDC's from an earlier probe. `total: 28` over two releases
with `release_scope: "as_released"`, against `release_selection:
"latest_release"` for the same bare request to its sibling.

**Nothing in the code changed, and that was checked before assuming it.**
Every fact the new table states is already pinned behaviourally:
`tests/unit/api/test_cdc_observations.py` asserts both CDC vocabulary values,
`tests/unit/api/test_usda_nass_api.py` and the NASS integration contract
assert both NASS ones, and `tests/e2e/test_cdc_pipeline.py` asserts the CDC
pair end to end. So this was never a defect in the API; it was a defect in
what a consumer is told.

**The guard is derived, and proved non-vacuous.** It parses the table out of
the guide and checks each row against the served document and the reviewed
snapshot: the route is served, each parameter named in backticks is declared
on it, a row claiming no history parameter is checked against the route
declaring none, `latest`'s served default is `false`, and the envelope field
exists on that route's response schema. Mislabelling one row proved it
bites:

```
E   AssertionError: /api/v1/cdc/observations answers
E   CdcObservationListResponse, which publishes no `release_scope`
```

A floor (`>= 3` rows) stops a parsing change from making the whole guard
pass by reading nothing.

**What is deliberately not done.** Aligning the three field names, the three
vocabularies or the two defaults would each be a breaking contract change
with consumers on the other side. Recording exactly what they are is what
makes that choice reviewable later, and the non-goals say so.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | **1489 passed** (was 1488) |
| Unit, the guide | `pytest tests/unit/api/test_consumer_guide.py` | 8 passed (was 7) |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**Register.** 367 rows.

## Remaining work

- None.
