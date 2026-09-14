---
id: source-route-grain-vocabulary
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# The source-scoped routes speak the warehouse's grain vocabulary

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row API-092.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/registry.py`,
  `apps/api/services/observations_service.py`

## Context

Migration 018 unified the geography-grain vocabulary and wrote down both the
defect and where the mapping must be called:

> Three publishers derived their grains as `UPPER(geo_type)`, whose national
> value is `nation` — so they published NATION while BLS, ACS, FRED, and FBI
> published NATIONAL. […] every national NASS statistic was unanswerable by
> construction, and nothing reported it because STATE and COUNTY happened to
> coincide.
>
> The mapping lives here once. Publisher views call it to say what they
> publish; the API's dispatch entries call it to say what they serve. A
> mapping written in five places is how this defect happened.

Two callers are named. There is a third serving surface: the source-scoped
routes, which read the `ServingContract` relations rather than the dispatch
ones. They were not updated.

`gold_pep.rpt_pep_observations` projects `revision.geo_type AS geo_level`, so
`gold_pep.mv_pep_latest` carries the source's own words — `nation`, `state`,
`county`, `place` — under a column named `geo_level`.
`list_latest_observations_for_source` filters `UPPER(geo_level) = UPPER(:geo_level)`
and projects the column as it stands. Run against a live PostgreSQL over that
shape:

| `geo_level=` | rows today | rows with the mapping |
|---|---|---|
| `NATIONAL` | **0** | 1 |
| `NATION` | 2 | 0 |
| `COUNTY` | 3 | 3 |
| `PLACE` | 4 | 4 |

`NATIONAL` is the word the catalog publishes in `valid_geo_grains` and the
word `/observations` accepts. On `/pep/observations/latest` it matches
nothing and answers an empty page that looks exactly like a geography with no
published values — the same sentence migration 018 wrote about NASS, about a
different route.

The projection has the mirror problem: every PEP row from these routes
reports `geo_level: "county"` where the catalog and the neutral resource
report `COUNTY`, so a grain read off a row is not the grain the rest of the
API speaks.

BLS, ACS, and FRED are unaffected — their gold views derive an upper-case
vocabulary word — which is why this has been invisible.

## Acceptance criteria

1. A serving contract declares how its relations spell the grain, the way a
   dispatch entry already does, and the source-scoped routes project and
   filter through that declaration.
2. `geo_level=NATIONAL` answers Census PEP's national rows on the
   source-scoped route, as it does on the neutral one.
3. A row served by these routes reports the vocabulary word, so a grain read
   off a row can be sent back as a filter to any route that accepts one.
4. The legacy words keep answering: `NATION` and `US` are normalized on the
   way in, as ADR-0002 requires of a shared link or a saved configuration
   holding one.
5. A source whose dispatch entry maps the grain and whose serving contract
   does not is a test failure, so the two registries cannot disagree again.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing `gold_pep.rpt_pep_observations` to project a mapped grain. That is
  a warehouse change with its own migration and replay; the API can serve the
  vocabulary from the relation as it stands, which is what the dispatch
  entries already do.
- Touching the neutral resource, which has been correct since ARC-005.

## What was built

`ServingContract` gained `geo_level_expression`, defaulting to the bare
column, with Census PEP declaring `gold_glossary.geo_grain(geo_level)` — the
same mapping migration 018 defines and the dispatch entries already call,
written here once for the column *these* relations carry.

`_source_select_sql` projects through it, so a served row reports the
vocabulary word. The latest route filters through it and binds
`normalize_geo_level(geo_level)`, so `NATION` and `US` resolve to `NATIONAL`
on the way in — a shared link or a saved configuration holding the catalog's
earlier word keeps answering, which the versioning decision record requires.

BLS, ACS and FRED keep the bare column: their gold views already derive an
upper-case vocabulary word, and wrapping them would add a function call per
row for nothing.

`test_the_two_registries_agree_on_which_sources_map_the_grain` is what keeps
this closed. The dispatch entry and the serving contract read different
relations for the same source, so they cannot share an expression — but they
must agree on *whether* that source's relations store a source-shaped grain,
and the test asserts exactly that pairing.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Source-route unit | `python -m pytest tests/unit/api/test_source_observations.py -q` | 18 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1437 passed |
| Contract snapshot | `python -m tests.support.regenerate_openapi_contract` | no diff |
| Register | `python -m tests.support.catalog_evidence` | 330 rows; API-092 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

The four tests were confirmed failing-first: the rendered SQL read
`where metric_code = :metric_code and upper(geo_level) = upper(:geo_level)`
against `gold_pep.mv_pep_latest`, which is the defect as SQL.

### Against a real database

The mismatch was measured, not argued, on a live PostgreSQL 16 carrying
`gold_glossary.geo_grain` from migration 018 and a relation shaped like
`mv_pep_latest` (`geo_type` projected as `geo_level`, one row per grain):

| `geo_level=` | rows today | rows with the mapping |
|---|---|---|
| `NATIONAL` | **0** | 1 |
| `NATION` | 2 | 0 |
| `COUNTY` | 3 | 3 |
| `PLACE` | 4 | 4 |

The first row is the defect: the catalog's own word reached none of its own
rows. The second is why the caller's word is normalized rather than matched
raw. The last two are why it stayed hidden — the same coincidence migration
018 recorded for NASS.

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. That tier is where this would be exercised
against the real `mv_pep_latest`; the ad-hoc cluster above covers the
mapping's semantics against the real function.

## Acceptance criteria, as delivered

1. **Met.** `geo_level_expression` on `ServingContract`, read by both the
   projection and the filter.
2. **Met.** `test_a_source_route_filters_and_projects_the_vocabulary_word`,
   and measured on a live database above.
3. **Met.** The projection is `{expression} AS geo_level`.
4. **Met.** `test_a_legacy_grain_word_still_answers`.
5. **Met.** `test_the_two_registries_agree_on_which_sources_map_the_grain`.
6. **Met.** `API-092` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 92.

## Remaining work

- None. `gold_pep.rpt_pep_observations` still projects the source's own word
  under the name `geo_level`; serving the vocabulary from it is what this
  does, and changing the view itself is a warehouse plan with its own
  migration and replay.
