---
id: reduction-tie-determinism
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - observation-paging-determinism
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared/test_repository_hygiene.py -q
---

# A reduction that ties picks the same row every time

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row API-083.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/neutral_observations_service.py`,
  `apps/api/services/comparison_service.py`
- **Depends on:** API-074 (`observation-paging-determinism`), which put the
  declared total order on every serving contract. This reads the same
  declarations.
- **Next pickup:** none.

## Context

Three services reduce a source's rows to one per group, and all three rank
with `ROW_NUMBER()` over an ordering that does not decide every tie:

```sql
-- neutral_observations_service._newest_per_geography_source
ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY <period_start> DESC)

-- neutral_observations_service._newest_release_per_period_source
ROW_NUMBER() OVER (PARTITION BY geo_id, <period_start>
                   ORDER BY <release_order_expression> DESC)

-- comparison_service.ranked_latest_cte  (comparison and distribution)
ROW_NUMBER() OVER (PARTITION BY geo_id ORDER BY <period_start> DESC)
```

`ROW_NUMBER` assigns 1 to *some* row of each tie group, and SQL does not say
which. Ties are not hypothetical here — they are what the serving contracts
already declare. API-074 wrote the reason down on the registry entry itself:

> PEP's latest publication is a series, not a value: every estimated year of
> the current vintage, so one geography carries several rows and `geo_id`
> alone leaves ties a page boundary can fall inside.

Its answer was `latest_order` / `released_order`, each the relation's own
unique-index key with the pinned columns removed — a *total* order, so no two
rows of one response tie on the full list. The paging routes use it. The
reductions do not: `PARTITION BY geo_id ORDER BY estimate_date DESC` leaves
Census PEP's `dataset_code` (and ACS's `variable_code`, and CDC's
`observation_sk`) undecided inside the newest period.

So `/observations?newest_per_geography=true` can answer one value for a
county on one request and a different value for the same county on the next,
with no publication in between — the plan changed, or the relation was
re-clustered by a refresh. Nothing reports it, because each answer is a real
published row; they are just not the same real published row.

Three claims in this codebase depend on the choice being the same one:

- `_newest_per_geography_source`: *"The distribution and comparison services
  already rank the same way, so a page taken this way and a set of bins
  describe the same rows."* Two reductions that each pick arbitrarily do not
  rank the same way; they rank by the same expression and then diverge.
- WEB-047 saves a map view as `newest_per_geography=true` so that reopening
  it reproduces the map. A reduction that can answer differently makes the
  saved configuration reproduce the *request*, not the view.
- ADR-0003's saved analysis is "intent replayed against live publications".
  A replay that is not stable against an unchanged warehouse is not a replay.

`_newest_release_per_period_source` has the same gap by a different route:
two rows of one period inside one release — an ACS vintage captured twice, a
CDC period at one watermark carrying two `observation_sk` — tie on
`release_order_expression`, and API-081 promised the settled history is the
one the source's own declared order names.

## Acceptance criteria

1. Each of the three reductions ranks on a total order: the expression it
   ranks by, then the dispatch entry's own declared order for the relation it
   reads (`latest_order` for the latest relation, `released_order` for the
   released one).
2. The declared order is read from the registry, never restated: a source
   added with a total order gets a deterministic reduction with no further
   edit, and a source whose declaration changes changes with it.
3. A dispatch entry that declares no order still serves — the reduction is no
   worse than today — and the gap is visible rather than silent.
4. The reduction the neutral resource applies and the reduction the
   comparison and distribution services apply are the same order, so the
   claim in the docstrings is true and testable.
5. Each behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing which row wins a tie in any *published* sense. Nothing here
  invents a ranking: it appends the order the registry already declares, so
  the winner becomes fixed, not different-on-purpose.
- The distribution route's two-statement read of one CTE, which is its own
  defect and its own plan.
- Paging order on the serving routes, which API-074 and API-080 already made
  total.

## What was built

`registry.ranking_tie_break(order)` renders a declared order as the tail of a
ranking `ORDER BY`. All three reductions append it — the two in
`neutral_observations_service` over `latest_order` and `released_order`, and
`comparison_service.ranked_latest_cte` (which the distribution route shares)
over `latest_order`. The order reaches SQL from the registry entry, so the
three reductions cannot drift from each other or from the paging order the
serving routes already use, and a source added to the registry gets a
deterministic reduction with no edit here.

The tail is appended verbatim rather than pruned against what the ranking
expression already decided. A column that is constant inside a tie group
contributes nothing to the result, and pruning would mean this module
parsing the SQL expressions it hands to the database — a second, weaker copy
of a rule the registry already states exactly once. So Census PEP's settled
history ranks `pep_vintage DESC, estimate_date, geo_id, pep_vintage,
dataset_code`: redundant in the middle, total at the end, and derived.

An entry that declares no order still serves, ranking exactly as it does
today. The gap is not silent: a registry test names any source whose
`latest_order` or `released_order` is empty, at the point the source is
added. All seven declare both today.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| API unit | `python -m pytest tests/unit/api -q` | 336 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1400 passed |
| Catalog guards | `python -m pytest tests/unit/shared -q` | 197 passed |
| Register | `python -m tests.support.catalog_evidence` | 316 rows; API-083 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api tests/support` | clean |
| Format | `ruff format --check` on the five changed files | already formatted |

The three tests were confirmed failing-first. Before the change, the two
neutral reductions ranked `ORDER BY estimate_date::TEXT DESC` and
`ORDER BY pep_vintage DESC` with nothing after them, and the comparison
assertion reported `FRED ranks on an order that can tie` against the rendered
CTE.

The OpenAPI snapshot is unchanged: no route, parameter, or response shape
moved — only the order inside a reduction.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. That is where the change would be observed
end to end, against a real relation with a real tie; the unit tier asserts
the rendered SQL, which is what this repo's other ordering rows (API-074,
API-080) assert too.

## A stale total, found and fixed here

`AUDITED_COUNTS` and the totals in `TESTING_CONTRACT.md` disagreed by one
when this plan was claimed: WEB-047 raised its family count but left the
register's declared total at 314, because that commit ran
`test_repository_hygiene.py` and not `test_catalog_evidence.py`. Both totals
now read 316 and the whole `tests/unit/shared` tier passes. The lesson is
narrow and worth writing down: the two guards check different things, and
registering a catalog row means running both.

## Acceptance criteria, as delivered

1. **Met.** All three reductions rank on the expression plus the entry's
   declared order for the relation they read.
2. **Met.** The order is rendered from `dispatch.latest_order` /
   `dispatch.released_order`; nothing restates it.
3. **Met.** An empty declaration renders an empty tail, and
   `test_every_dispatch_entry_declares_the_order_its_reductions_need` names
   any source that has one.
4. **Met.** `test_both_sides_break_ties_on_the_declared_order` asserts the
   comparison CTE carries, for each side, the same ordering the neutral
   resource's reduction renders for that source.
5. **Met.** `API-083` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 83.

## Remaining work

- None.
