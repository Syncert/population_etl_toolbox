---
id: catalog-search-literal-text
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# Catalog search matches the text you typed

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of the catalog discovery queries.
- **Last updated:** 2026-09-13
- **Owner surface:** `src/data_ingestion_toolbox/sql/catalog_queries.py`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`GET /api/v1/catalog/metrics?q=` and `/catalog/geographies?q=` build their
filter by wrapping the caller's text in wildcards and handing it to `LIKE`:

```python
clauses.append("(UPPER(metric_code) LIKE UPPER(:q) OR ...)")
params["q"] = f"%{q}%"
```

The wrapping makes it a substring search, which is the intent. Nothing
escapes the two characters `LIKE` reserves, so whatever the caller typed is
also a pattern:

- `_` matches any single character. Every metric code in this warehouse
  carries at least one — `CENSUS_ACS:acs5:B01003_001`, `BLS:LAU:UNEMP_RATE`,
  `CENSUS_PEP:pep_2020s:POPESTIMATE` — so the most ordinary search there is
  runs as a pattern.
- `%` matches any run of characters, so `q=50%` finds every metric with a
  `50` anywhere after it, and a bare `q=%` or `q=_` returns the whole
  catalog under a filter the caller believes narrowed it.

`apps/web`'s catalog page sends its search box straight through
(`catalog.ts::catalogRequestParams`), so this is what a person typing in the
UI gets. The route's own name is `q`, the guide calls it "Metric search",
and nothing in the contract offers pattern syntax — so the result set is
simply not the one the caller asked for.

This is not an injection: the value is bound, never interpolated. It is a
wrong answer.

## Objective

`q` matches the literal text it was given, and stays a substring search.

## Acceptance criteria

1. `%`, `_`, and the escape character itself are literal in `q` for both
   `/catalog/metrics` and `/catalog/geographies`.
2. The search remains case-insensitive and remains "contains", unchanged for
   every `q` that holds none of those characters.
3. The escape character is declared in the SQL rather than left to the
   server's default.
4. The value is still bound, never interpolated.
5. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Offering pattern or regular-expression search as a documented feature.
- Full-text search, ranking, or trigram indexes.
- Changing which columns `q` searches.

## Evidence

### The gap, measured

The claim was not left as reasoning about `LIKE`. Five rows were loaded into
an in-memory SQL engine and searched both ways — ANSI `LIKE` escaping is the
same construct PostgreSQL implements — using the real `like_contains` from
this change:

| `q` | Before | After |
| --- | --- | --- |
| `CENSUS_ACS` | `CENSUSXACS:acs5:B01003X001`, `CENSUS_ACS:acs5:B01003_001` | `CENSUS_ACS:acs5:B01003_001` |
| `B01003_001` | `CENSUSXACS:acs5:B01003X001`, `CENSUS_ACS:acs5:B01003_001` | `CENSUS_ACS:acs5:B01003_001` |
| `50%` | `CPI 50% share`, `CPI 5099 share` | `CPI 50% share` |
| `_` | all five rows | the two rows that contain an underscore |
| `UNEMP_RATE` | `BLS:LAU:UNEMP_RATE` | `BLS:LAU:UNEMP_RATE` |

Searching the source code every ACS metric carries returned rows from a
different source whose code merely had the same shape. The last row is the
point of the change as much as the first: a `q` holding no metacharacter
answers exactly as it did.

`tests/unit/api/test_sql_query_builders.py::test_catalog_search_matches_literal_text`
states the same six cases against both builders and failed 6/6 first.

### What changed

- `like_contains(value)` escapes the escape character, then `%`, then `_`,
  and wraps the result in the wildcards that make it a substring search.
  Order matters: escaping the backslash last would double the ones this
  function just added.
- Every `LIKE` branch in both builders declares `ESCAPE '\'` rather than
  relying on the server default.
- The value is still bound. This was never an injection — the test asserts
  the bound operand does not appear in the rendered statement — it was a
  wrong answer.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_sql_query_builders.py -q` | 16 passed |
| `pytest tests/unit -q` | 1376 passed |
| `python -m tests.support.catalog_evidence` | 298-row register renders; API-077 is `FULL` |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 440 files already formatted |

### Not run

`make test-integration` needs PostgreSQL, and this environment has no Docker
daemon. The conclusion it would carry — that PostgreSQL reads `ESCAPE '\'`
as one backslash — rests on `standard_conforming_strings`, on by default
since PostgreSQL 9.1 and never disabled in this repository's bootstrap or
connection configuration.

## Remaining work

None.
