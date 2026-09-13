---
id: newest-release-per-period
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# The API answers a geography's settled history

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of what `apps/web` computes that the API declares.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/routers/observations.py`,
  `apps/api/services/neutral_observations_service.py`
- **Depends on:** nothing. It is the mirror image of API-066
  (`newest_per_geography`) and follows its shape.
- **Next pickup:** none. The client migration is filed separately.

## Context

`AGENTS.md` sets the dependency order and the rule that follows from it:
build the API on stable warehouse contracts, build the web on stable API
contracts, "do not duplicate warehouse or API rules in client code", and
"when a downstream requirement exposes a missing upstream foundation, fix or
plan the upstream contract first".

`apps/web` duplicates one. A source whose latest relation keeps a single row
per geography — Census ACS holds only the newest vintage — has no history
under `scope=latest`: a geography's trend comes back as one point. The
explorer therefore re-reads the metric under `scope=as_released` and reduces
it in the browser, in `collapseToNewestRelease`: group the rows by period,
keep the one from the newest release, sort by period.

Deciding which release is newer is the part that does not belong there. The
client guesses:

```ts
// Release identities compare as numbers where both sides are numeric
// (a vintage year, a watermark) and as text otherwise (an as-of date).
```

The API does not guess. Every dispatch entry declares
`release_order_expression` — `as_of_date` for BLS and FRED, `vintage_year`
for Census ACS, `pep_vintage` for PEP, `release_watermark::BIGINT` for CDC,
`release_key` for FBI UCR, `release_watermark` for NASS — and
`/observations/releases` already orders by it. Two places now decide which
release is newer, and a numeric-looking text key is enough to make them
disagree: `2023.10` and `2023.9` order one way as numbers and the other as
text, and the API's rule is the one the warehouse published.

The reduction itself is also the mirror image of one the API already serves.
`newest_per_geography=true` (API-066) reduces a multi-period *latest*
publication to one row per geography, ranking inside the source's own
relation because "the source is the only place that knows which row that is".
The same sentence applies to reducing a multi-release *as-released* read to
one row per period.

## Objective

`/observations` can answer a geography's settled history, ranked by the
source's own declared release order.

## Acceptance criteria

1. `newest_release_per_period=true` reduces an as-released read to one row
   per geography and period: the row from the newest release, by the source's
   declared `release_order_expression`.
2. It is valid only with `scope=as_released`, and refused with a stated
   reason otherwise — the mirror of API-066's refusal, because "every
   published release, reduced to the newest" is a question only an
   as-released read can ask.
3. It combines with `release=`? No: pinning a release and asking for the
   newest release of each period are contradictory, and the contradiction is
   refused with its reason rather than silently resolved.
4. Ranking happens inside the source's own relation, before projection and
   before paging, so `total` counts reduced rows and a page of them is a page
   of the answer.
5. Every declared filter still applies inside the ranked subquery.
6. The parameter is declared on the route, so `/catalog/capabilities`
   advertises it; the reviewed OpenAPI snapshot is regenerated deliberately;
   the consumer guide documents it beside `newest_per_geography`; and the
   behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing `scope=latest`, `scope=as_released`, or `newest_per_geography`.
- Migrating `apps/web` onto it. The client change is downstream of this
  contract and is a separate plan; this one is the upstream foundation the
  repository's dependency order asks for first.
- Serving the reduction on the legacy or source-scoped routes.

## Evidence

### The gap, established first

Six tests failed before the parameter existed: the ranking and its partition,
that declared filters stay inside the ranked subquery, the three refusals,
and that the route declares the parameter at all.

### What changed

- `_newest_release_per_period_source` mirrors `_newest_per_geography_source`:
  it ranks inside the source's own as-released relation, partitioned by
  geography and period, ordered by the dispatch entry's declared
  `release_order_expression` descending. That expression is documented never
  to carry `DESC` itself, which is what makes reversing it here safe.
- Three refusals, each with its reason: with `scope=latest` there are no
  releases to reduce; with a pinned `release` the two requests contradict;
  and the two reductions cannot be combined. The third is unreachable while
  each refuses the other's scope, and is stated anyway so a future scope
  admitting both does not inherit whichever branch was written first.
- Filtering and ranking both happen inside the subquery, so `total` counts
  reduced rows and a page of them is a page of the answer — the property
  API-066 established and this reuses.

### Additive, and pinned

The reviewed snapshot's diff is one query parameter on one operation and
nothing else: additive under ADR-0002, the same shape as API-066.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_neutral_observations.py -q` | 46 passed |
| `pytest -m "unit and api" tests/unit/api -q` | 323 passed |
| `pytest tests/unit -q` | 1388 passed |
| `npm --prefix apps/web run test:unit` | 250 passed |
| `npm --prefix apps/web run test:browser` | 62 passed (Chromium) |
| `python -m tests.support.catalog_evidence` | 312-row register renders; API-081 is `FULL` |
| `ruff check .` / `ruff format --check .` | clean |

The frontend suites are recorded because the fixtures WEB-043 converted read
their parameter list from the snapshot, so this change reached them
automatically — which is the point of that conversion, and evidence that it
works.

### Not run

`make test-integration` and `make test-e2e` need PostgreSQL and Docker, which
this environment has no daemon for. They are where the ranking would be
proved over real releases. The SQL it generates is the same construct
API-066's ranking uses, over the same declared expressions, and every
expression it names is one the dispatch entry already uses elsewhere in this
module.

## Remaining work

None here. The client migration — `apps/web` still reduces across releases
itself in `collapseToNewestRelease` — is filed as its own plan, because the
repository's dependency order puts the API contract first and the consumer
after it.
