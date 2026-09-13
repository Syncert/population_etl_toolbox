---
id: unregistered-source-explanation
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# A metric from an unregistered source is explained, not a 500

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the analysis routes' failure paths.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/distribution_service.py`,
  `apps/api/services/neutral_observations_service.py`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

The glossary can publish a metric whose source has no reviewed
`OBSERVATION_DISPATCH` entry. The repository says so out loud:
`catalog_service.get_metric_capability` documents "a source accepted after
this registry was last reviewed" and answers its published semantics with no
routes and `served_by_neutral_routes: false`. Warehouse work lands before API
registry work by design (`AGENTS.md`'s dependency order), so this state is
expected, not hypothetical.

Four of the five surfaces that can be handed such a metric answer it
honestly. Measured against a glossary row for a fictional `NEWSRC` source:

| Route | Answer |
| --- | --- |
| `GET /api/v1/catalog/metrics/{code}` | `200` — semantics, no routes |
| `GET /api/v1/observations` | `422` — "observations for source 'NEWSRC' are not served by this API version; see /catalog/capabilities …" |
| `GET /api/v1/comparison/preflight` | `200` — `comparable: false`, naming the missing dispatch entry |
| `GET /api/v1/comparison` | `422` — the failed rule |
| `GET /api/v1/distribution/bins` | **`500 Internal Server Error`** |

`list_distribution_bins` calls `observation_dispatch(source_code)`, which
raises `UnknownObservationDispatch` — a `KeyError` — and the router catches
`UnknownAnalysisMetric`, `NeutralQueryError`, and `SQLAlchemyError`, none of
which it is. The intent was clearly to explain: the very next lines check
`dispatch.analysis_ready` and decline a stratified source with its declared
restriction and a `422`. The unregistered case was simply missed.

The consumer guide sets the contract this breaks: "An incompatible pair is a
`200` explanation, not an error; only an unknown metric code is a `404`."
A known metric producing a `500` is neither.

The duplication is the root cause. `neutral_observations_service._dispatch_for`
already turns this exact lookup into the exact explanation, and
`distribution_service` — which imports three other helpers from that module —
went to the registry directly instead.

## Objective

`/distribution/bins` explains an unregistered source the same way
`/observations` does, by construction rather than by a second copy.

## Acceptance criteria

1. A metric whose source has no reviewed dispatch entry answers
   `422 {"detail": …}` from `/distribution/bins`, naming the source and
   pointing at `/catalog/capabilities`.
2. That detail is produced by the same helper `/observations` uses, so the
   two routes cannot drift apart.
3. The helper is a public name rather than an underscore-prefixed import
   across module boundaries.
4. Every other `/distribution/bins` behavior is unchanged: `404` for an
   unknown metric code, `422` with the declared restriction for a stratified
   source, and the bins themselves.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Serving bins for a source that has no dispatch entry.
- Promoting `_filter_conditions` and `_metric_conditions` to public names;
  those are a separate cleanup and nothing is broken by their spelling.
- Changing the comparison routes, which already reach the same conclusion
  through the declared compatibility rule before any dispatch lookup runs.

## Evidence

### The gap, measured

The five-route table in "Context" is not reasoning about code paths — it is
the output of a probe that overrode the database session with a glossary row
for a fictional `NEWSRC` source and called all five routes through the
served HTTP surface. `/distribution/bins` answered `500 Internal Server
Error`; the other four answered as the table records. After the change the
same probe answers `422` there, with the detail `/observations` gives.

`tests/unit/api/test_distribution.py` states it as two tests — the status and
detail, and that the two routes' details are byte-identical — which failed
2/2 first.

### What changed

- `neutral_observations_service._dispatch_for` becomes the public
  `dispatch_for_metric`, since it is now the one place any route turns "this
  metric's source has no reviewed entry" into an explanation. Its two callers
  inside the module moved to the new name; no compatibility alias was left,
  because nothing outside the module used the old one.
- `distribution_service` calls it instead of `observation_dispatch`, and no
  longer imports the registry at all. The second test asserts the two routes
  produce the *same* detail rather than two similar strings, so a future edit
  to one wording cannot leave the other behind.
- The test also asserts no query runs for such a metric: the refusal happens
  before the relation guard, as it did for a stratified source.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_distribution.py -q` | 11 passed |
| `pytest -m "unit and api" tests/unit/api -q` | 313 passed |
| `pytest tests/unit -q` | 1378 passed |
| `python -m tests.support.catalog_evidence` | 299-row register renders; API-078 is `FULL` |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 440 files already formatted |

### Not run

No integration or e2e tier is implicated: the behavior is a refusal that
happens before any statement reaches the database, and the unit tier
exercises it through the real HTTP surface.

## Remaining work

None.
