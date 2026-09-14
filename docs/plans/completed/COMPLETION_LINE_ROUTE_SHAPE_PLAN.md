---
id: completion-line-route-shape
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - unhandled-failure-answer
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# The completion line names the route, not the caller's identifiers

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row API-089.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/telemetry.py`

## Context

`telemetry.py` says the completion line carries "method, **route path**,
status, duration, and the cache disposition", and that parameter values are
user input and never belong in logs by default. It logs
`scope["path"]` — the request path, with every path parameter's value in it.
Probed against the running application:

```
api_request method=GET path=/api/v1/evidence-packets/12345 status=200 …
api_request method=GET path=/api/v1/catalog/metrics/CENSUS_ACS:acs5:B01003_001 status=200 …
```

Two consequences.

**The line cannot be aggregated.** This is the operational signal the module
exists to produce — latency, error rate, cache behaviour — and those are
per-route facts. With one distinct `path` per packet id and per metric code,
there is no route to group by: `/evidence-packets/{packet_id}` has as many
`path` values as the account has packets, and the p95 of a route is not
computable from the line that was written to provide it.

**A private identifier reaches the log.** `packet_id` and
`configuration_id` name an account's own saved work. The web client goes out
of its way to keep exactly these out of the URL — WEB-031 asserts that no
packet id reaches the address bar — and the API writes them to disk on every
request. The module's own rule already covers this; it was applied to the
query string and not to the path.

The router has already resolved the template by the time this line is
written: `scope["path_params"]` carries each parameter's name and the value
it matched.

## Acceptance criteria

1. A request that matched a route logs the route's shape, with each path
   parameter's value replaced by its name.
2. A request that matched no route logs its path as it arrived — an
   unmatched path names no resource of this API, so there is no template to
   report, and the path is what makes a 404 actionable. Every route this API
   serves that takes an identifier *does* match, so no identifier of a served
   resource is logged.
3. A route with no path parameters logs exactly what it logs today.
4. The query string is still absent, as it already was.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Adding a second field for the raw path. One line, route-shaped facts only,
  is the module's stated design.
- Changing the field's name. `path=` stays, so a log query written against it
  keeps working; its value becomes the route's shape, which is what the
  module already documented it as.

## What was built

`route_shape(scope)` replaces each path parameter's value with its name and
is used by both lines this module writes — the completion line and the
failure line API-088 added.

It reads `scope["path_params"]`, which the router has already resolved, and
replaces by whole segment. Two consequences are worth stating, both checked:
a value that is a substring of a longer segment is left alone, and a literal
segment that happens to equal a parameter's value is replaced too. The second
is conservative in the right direction — it can only remove an identifier and
lower cardinality, never add either — and the alternative, reconstructing
which segment the router actually matched, would mean re-deriving the
router's own match here.

The field keeps its name. `path=` is what a log query is written against, and
the module already documented its value as the route path; this makes that
true.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| API unit | `python -m pytest tests/unit/api -q` | 357 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | passed |
| Register | `python -m tests.support.catalog_evidence` | 326 rows; API-089 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

The five tests were confirmed failing-first (`route_shape` did not exist),
and the result was checked through `create_app()` rather than only the unit
harness:

```
api_request method=GET path=/api/v1/catalog/metrics/{metric_code} status=404 …
api_request method=GET path=/api/v1/catalog/metrics                status=200 …
api_request method=GET path=/api/v1/no-such-route/9                status=404 …
```

The first was `path=/api/v1/catalog/metrics/CENSUS_ACS:acs5:B01003_001`
before. The second is unchanged, and its query string (`?q=secret`) is
absent, as it already was. The third matched no route and is logged as it
arrived.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. Nothing about a route, a schema, or a response
changed, so the reviewed OpenAPI snapshot is untouched.

## Acceptance criteria, as delivered

1. **Met.** `test_the_completion_line_names_the_route_not_the_identifier`.
2. **Met.** `test_an_unmatched_path_is_logged_as_it_arrived`.
3. **Met.** `test_a_route_without_parameters_logs_what_it_always_did`, and
   the pre-existing API-057 line assertion passes unmodified.
4. **Met.** `test_the_line_still_carries_the_route_and_no_query_values`.
5. **Met.** `API-089` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 89.

## Remaining work

- None.
