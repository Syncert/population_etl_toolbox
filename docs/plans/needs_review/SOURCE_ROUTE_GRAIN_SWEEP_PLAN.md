---
id: source-route-grain-sweep
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - source-route-grain-vocabulary
  - containerless-integration-tier
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit/shared -q
---

# The published-grain sweep reaches every route that accepts a grain

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row DB-030.
- **Last updated:** 2026-09-13
- **Owner surface:** `tests/integration/api/test_catalog_serving_agreement.py`,
  `apps/api/registry.py`, `apps/api/services/observations_service.py`

## Context

DB-028 is the repository's strongest guard on the grain vocabulary. Its own
docstring lists four sources that broke the contract four ways and says "no
tier saw any of them", and it sweeps every current catalog code against every
grain the catalog publishes for it.

It sweeps one route: `/api/v1/observations`.

API-092 was a grain defect on a different route — `/{source}/observations/latest`
filtered `UPPER(geo_level)` against a relation storing the source's own word,
so `geo_level=NATIONAL` reached none of Census PEP's national rows. The guard
that exists for exactly this class did not see it, for two reasons, and both
need closing:

1. It never asks the source-scoped routes anything.
2. The fixture warehouse publishes no Census PEP metric, so even the neutral
   sweep is vacuous for the one source whose relations store a source-shaped
   grain. ACS and FRED are seeded; PEP is the case the mapping exists for.

Until now this was also unrunnable here: the tier needed services this
environment was documented as unable to provide. ENV-013 established that it
only needs a PostgreSQL with PostGIS, so this can be written and actually run.

## Acceptance criteria

1. The sweep asks every route that accepts `geo_level` for a source, not only
   the neutral one: where a serving contract exists for a source, its
   source-scoped latest route is swept with the same catalog grains.
2. A row from any of those routes carries the grain it was asked for, in the
   vocabulary, exactly as DB-028 requires of the neutral route.
3. One Census PEP metric is published through the real views the way the ACS
   and FRED fixtures publish theirs, so the source whose relations store
   `nation` is actually exercised and the assertion is not vacuous.
4. The PEP fixture cleans up exactly what it created, including the
   registration side effects the harvest causes, as the existing fixtures do.
5. Reverting API-092 makes this fail, and says which route and which grain.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Sweeping routes that do not accept `geo_level` — `/cdc/observations` takes
  the source's own `geo_type` under its own name, and NASS takes
  `agg_level_desc`. Those are different parameters, not this vocabulary.
- Seeding the remaining sources. PEP is the one the grain mapping exists for;
  the others are covered by the neutral sweep already.

## What the sweep found

Writing the guard found a defect larger than the one that prompted it, which
is the argument for the guard.

One Census PEP measure has three identities, read straight out of the real
warehouse objects:

| Where | The code |
|---|---|
| Catalog — what a client can discover | `CENSUS_PEP:AGREEMENT_781D9E50` |
| `gold_pep.mv_pep_latest` — what the source-scoped routes read | `CENSUS_PEP:pep_agreement_test:AGREEMENT_781D9E50` |
| `gold_pep.population_estimate_latest` — what `/observations` reads | `AGREEMENT_781D9E50` |

`rpt_pep_observations` composes `'CENSUS_PEP:' || dataset_code || ':' ||
metric_code`; `gold_pep.metric_publisher` publishes the bare measure code, so
the glossary composes `CENSUS_PEP:<measure>`. The source-scoped routes matched
`metric_code = :metric_code` literally against the first of those with the
second. They never met, and could not: the disagreement is structural, not a
data accident.

So **every Census PEP metric answered no rows on
`/api/v1/pep/observations/{latest,timeseries}`**, for every request, while the
same metric answered normally on `/observations` — which resolves through the
lineage key, the arrangement ARC-005 set up. ARC-005's static guard checks
that a serving relation's composed *prefix* matches its publisher's
`source_code`; `CENSUS_PEP:` does, and the extra segment slips past it. DB-025
sweeps the neutral route only. Nothing looked at this pairing.

## What was built

`ServingContract` gained a metric-identity declaration, mirroring the one
`ObservationDispatch` already has: `metric_match_condition`, defaulting to
equality, and `binds_lineage_key`. Census PEP declares
`SPLIT_PART(metric_code, ':', 3) = :metric_key`, and the service resolves that
key from the glossary's `physical_lineage` — never by cutting a segment out of
the request, which is the string-surgery defect ARC-005 exists to prevent. An
unknown code binds an identity nothing stores, so the route answers an empty
page exactly as it did.

The sweep itself asks every serving contract's `/{segment}/observations/latest`
with the same catalog grains DB-028 sends the neutral route, and requires
every returned row to carry the grain it was asked for. And the PEP fixture
publishes one national metric end to end — silver rows through the real views
and the real glossary harvest — so the source the grain mapping and the
identity mapping both exist for is actually exercised. Without it the sweep
would have passed while proving nothing: no fixture published a PEP metric at
all, which is the second reason API-092 went unseen.

## Validation

Run 2026-09-13 on this branch, against a live PostgreSQL 16 with PostGIS via
the path ENV-013 documents.

| Tier | Command | Result |
|---|---|---|
| This file | `pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and database and not slow"` | 6 passed |
| Integration (api + database + redis) | `-m "integration and (database or redis) and not slow"` | 126 passed, 1 skipped |
| Whole unit tier | `python -m pytest tests/unit -q` | 1439 passed |
| Contract snapshot | `python -m tests.support.regenerate_openapi_contract` | no diff |
| Register | `python -m tests.support.catalog_evidence` | 332 rows; DB-030 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |

Failing-first was established against the real database twice: first as the
raw defect (the sweep reported `CENSUS_PEP publishes grain 'NATIONAL' for
'CENSUS_PEP:AGREEMENT_…', which /api/v1/pep/observations/latest answers with
no rows`), and again after the fix by removing only PEP's
`metric_match_condition`, which reproduces the same message.

## Tests that moved with the change

Four unit tests pinned the source-scoped routes' exact statements or bound
parameters. A composed-identity contract now resolves its lineage key first,
which is a real new statement, so:

- the relation-targeting tests exclude the glossary lookup and say that it is
  the only other statement these routes may issue;
- the parameter assertion reads `binds_lineage_key` off the contract rather
  than hard-coding which sources bind a key, so it stays source-agnostic;
- the two fake sessions gained the glossary answer and a `first()`, so the
  PEP path is exercised rather than skipped.

## Acceptance criteria, as delivered

1. **Met.** Every serving contract's latest route is swept.
2. **Met.** Same row-carries-its-grain assertion DB-028 makes.
3. **Met.** `published_pep_metric`, seeded through the real views and harvest.
4. **Met.** The fixture deletes its rows in foreign-key order — the revision
   rows before the release they reference — and removes the registration rows
   the harvest created, and only those.
5. **Met.** Removing PEP's declaration reproduces the failure, naming the
   route and the grain.
6. **Met.** `DB-030` in `docs/reference/TESTING_CONTRACT.md`, owned by
   `postgres-integration`, with `AUDITED_COUNTS["DB"]` raised to 30.

## Remaining work

- None. Whether `gold_pep.rpt_pep_observations` should compose a
  three-segment code at all is a warehouse question with its own migration and
  replay; the API now serves the catalog's code against the relation as it
  stands, which is what the dispatch entries already do.
