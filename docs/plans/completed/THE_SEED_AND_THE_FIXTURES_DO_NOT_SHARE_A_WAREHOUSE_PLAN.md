---
id: seed-and-fixtures-do-not-share-a-warehouse
branch: fix/seed-and-fixtures-do-not-share-a-warehouse
depends_on:
  - deployment-observability
parallel_safe: true
complexity: medium
verify:
  - docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
  - docker compose -f infra/docker/docker-compose.test.yml up -d postgres redis
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit -q
---

# The live-stack seed and the integration fixtures do not share a warehouse

## Plan status

- **Status:** Accepted. All four deliverables are implemented and verified
  against running stacks, and the three findings originally scoped out are
  fixed too. Delivered as `ed33276` on
  `claude/branch-merge-audit-119eof`, on top of `e500d40`; accepted by the
  repository owner on 2026-09-14.
- **Last updated:** 2026-09-14
- **Current milestone:** complete.

Measured after the change, each database recreated from scratch:

| Tier | Before | After |
| --- | --- | --- |
| `tests/integration/api`, compose database (`docker-compose.test.yml`) | 1 failed, 10 errors | **78 passed** |
| `tests/integration/api`, bare Postgres (the shape CI uses) | 76 passed | **77 passed**, then 78 with the new contract |
| `tests/run.ps1 web-smoke` | 17 passed | **17 passed** |
| `tests/unit` | 1729 passed, 3 failed | **1734 passed, 0 failed** |
| `ruff format --check .` / `ruff check .` | — | clean, 469 files |

Both integration paths pass, which is the acceptance criterion below: the fix
is not "the compose database works again" but "neither database is the only one
that works".

## Why

`infra/docker/docker-compose.test.yml:57` mounts `tests/sql/frontend_smoke_seed.sql`
into the initdb directory of the database the **pytest integration tier** uses.
That was harmless while the seed published one Census ACS measure for one
county. The deployment-observability work grew it to one measure per registered
source, so the seed now writes rows into seven source schemas — and the
integration fixtures, which assume they are the only writer of their own
lineage, collide with it.

The collision is invisible to CI. The `api-integration` workflow
(`.github/workflows/api-integration.yml:53`) uses a bare Postgres service
container and lets the fixtures apply the reviewed warehouse manifest
themselves; the smoke seed never reaches it. So the tier is green in CI and red
on the path `docs/user-guides/RUNNING_TESTS.md:184` and `tests/run.ps1` tell a
developer to use. A guard that passes in CI and fails locally trains people to
ignore the local run, which is the one that catches things first.

Measured on 2026-09-14, same host, database recreated from scratch for each run:

| Tree | Tier | Result |
| --- | --- | --- |
| `origin/main` (762519a) | `tests/integration/api/test_catalog_serving_agreement.py` | **20 passed** |
| branch `claude/branch-merge-audit-119eof` (d86f9b0) | same | **1 failed, 9 errors**, 10 passed |
| same branch, CI's bare-Postgres path | `tests/integration/api -m "integration and database and not slow"` | **76 passed** |

The bisect is the seed, not the test file: checking out `origin/main`'s
`frontend_smoke_seed.sql` alone and recreating the database turns the branch's
own test file green.

## What is actually broken

### 1. The PEP fixture's release insert is silently swallowed

`silver_pep.pep_release` carries a **global** `UNIQUE (product_code)`
(`sql/migrations/009_census_pep_registry.sql:46`) — not unique per dataset and
vintage, but unique across the whole relation.

The seed now writes `('pep_smoke', 2098, 'alldata', ...)`. The fixture
`published_pep_metrics` writes `('pep_agreement_test', 2095, 'alldata', ...)`
with a bare `ON CONFLICT DO NOTHING`
(`tests/integration/api/test_catalog_serving_agreement.py:1356-1367`). A
conflict target is absent, so the clause absorbs *any* unique violation,
including the one on `product_code` that has nothing to do with the row's
identity. Nothing is inserted and nothing is raised.

The next statement writes `silver_pep.release_load`, whose foreign key targets
`pep_release(dataset_code, vintage_year, product_code)`, and fails:

```text
psycopg2.errors.ForeignKeyViolation: insert or update on table "release_load"
violates foreign key constraint "release_load_dataset_code_release_vintage_product_code_fkey"
DETAIL:  Key (dataset_code, release_vintage, product_code)=(pep_agreement_test, 2095, alldata)
         is not present in table "pep_release".
```

Every test depending on the PEP fixture errors at setup. That is nine of them
on the branch as committed, ten on the working tree as of this writing, because
a newly added test picked up the same fixture.

The `UNIQUE (product_code)` constraint is not itself a production defect — real
releases carry vintage-specific product codes (`CO-EST2024-ALLDATA`,
`NST-EST2025-ALLDATA`; all thirteen rows in the dev warehouse are distinct), so
ingestion never meets it. It is a latent constraint that only fixtures using a
generic `'alldata'` can trip, which is exactly why it went unnoticed.

### 2. A seeded CDC measure stops answering while the fixtures are live

`test_every_published_grain_of_a_current_code_answers_in_the_vocabulary` fails:

```text
CDC publishes grain 'COUNTY' for 'CDC:cdi:SMOKE_CDI_01:crude',
which /api/v1/observations answers with no rows
```

The metric is the seed's own. Queried standalone against the same database it
answers one row at `COUNTY`; queried while the ACS, FRED and CDC fixtures are
active it answers none. The seeded row is intact and county-grain in
`gold_cdc.health_observation`, so the loss is in geography resolution rather
than in the fact: the fixtures seed their own geographies through
`tests/support/capture_seed.py`, and the projection that feeds
`gold_glossary.dim_geo_latest` is shared. Root-causing to the exact predicate is
part of this plan's work, not a precondition for it.

This is the same class as finding 1 — seed content and fixture content in one
warehouse — and it should be fixed by the same decision.

## Deliverables

### 1. The two tiers no longer share seeded content — DONE

The seed mount **moved** from `infra/docker/docker-compose.test.yml` to
`infra/docker/docker-compose.smoke.yml`, rather than being deleted. Deleting it
would have returned the smoke tier to grading an empty warehouse: the overlay
reshapes only `api` and `proxy`, so `postgres` and every initdb mount come from
the base file.

Compose merges `volumes` as a sequence append, which makes the move exact —
verified with `docker compose config`:

- base file alone: 0 smoke-seed mounts, 44 initdb mounts
- base + smoke overlay: smoke seed present, 45 initdb mounts, all 44 base
  mounts retained and ordered ahead of it

`tests/sql/martin_seed.sql` stays in the base file and must: it creates the
`martin_test` role and the single `gold_glossary.dim_geo_latest` row the base
stack's **own healthcheck** selects, so the deployment and Martin tiers cannot
start without it. Confirmed no test outside the smoke tier references the
seeded identifiers.

Observed after the change: the integration database holds no
`pep_release` row with `product_code = 'alldata'`; the smoke database holds
`pep_smoke/2098/alldata` and one catalog measure for each of the seven sources.

### 2. The swallowed insert can no longer be swallowed — DONE

Both halves, because they fail differently and the mount decision should not be
the only thing standing between this fixture and a silent skip.

- The clause now names its target:
  `ON CONFLICT (dataset_code, vintage_year, product_code) DO NOTHING`. A
  collision on any *other* constraint now raises where it happens instead of
  surfacing as a foreign-key violation one statement later. This also matches
  the ACS fixture in the same file, which already named
  `ON CONFLICT (dataset_code, vintage_year, table_id)`.
- The fixture owns its product code: all four `'alldata'` literals in
  `published_pep_metrics` became `'agreement_test_alldata'`, so it cannot
  collide with a seed or another fixture regardless of where either is mounted.

The literals stay inline rather than becoming a bound parameter: they sit inside
positional `VALUES` lists whose tuples a neighbouring session was editing at the
time, and swapping the text is the change that cannot misalign a parameter.

### 3. The divergence that hid this is now guarded, not just described — DONE

Neither option in the original plan was taken as written. Rewiring CI to compose
the stack is a large change to a required job, and a prose note is exactly the
kind of thing this repository has learned not to rely on. So the rule became a
test — `DB-045`,
`tests/unit/shared/test_warehouse_manifest.py::test_no_tier_seeds_the_database_another_tier_fills_itself`:

- the base file may mount exactly one `tests/` seed, `martin_seed.sql`
- the smoke overlay must mount `frontend_smoke_seed.sql`

Both directions, because separating the tiers by *deleting* the seed satisfies
half the rule and quietly returns the smoke tier to grading an empty warehouse.

It is a unit test on purpose. This defect survived because CI never saw it, so
the guard has to run where no database is needed — and the sibling
`DB-002` guard cannot catch it, because it filters `tests/` mounts out before
comparing against the manifest.

`docs/user-guides/RUNNING_TESTS.md` additionally records that the compose
database is not the database CI grades against, so a local red that CI does not
show reads as a real difference rather than a flake.

Catalog bookkeeping: `DB-045` added to `TESTING_CONTRACT.md`, `AUDITED_COUNTS`
raised to 45, the summary table's range and the register total moved to 465.

### 4. `tests/run.ps1 web-smoke` sets the bounds the workflow sets — DONE

The runner now exports `SMOKE_REQUIRE_ALL_SOURCES=1` and
`SMOKE_REQUIRE_FRESH_SOURCES=1` beside `SMOKE_REQUIRED`, and removes all four in
its `finally` block. The block claimed to compose the tier "exactly as CI
composes it" and did not, so reproducing a CI failure locally meant knowing to
set two variables by hand. The verification run below used the runner alone,
with nothing exported by the caller.

## Also fixed here — DONE

Originally scoped out of this plan, then folded in.

### 5. The served publication time is the ISO-8601 the guide documents

`content_health.py` cast the column with `::TEXT`, so the value reaching a
client was Postgres's own rendering — `2026-09-10 20:27:49.130325+00`, a space
where the `T` belongs. `API_CONSUMER_GUIDE.md` documented
`"2026-09-01T04:11:22+00:00"` throughout.

The cast is gone; `_text_or_none` calls `datetime.isoformat` and passes text
through untouched, so the unit tier's hand-built rows still read as written.
The field stays `str | None`, so the OpenAPI contract digest is unchanged.

That shape was the worst kind of mismatch to carry: `Date.parse` accepts it in
V8 and a strict ISO-8601 parser rejects it, so it worked in whichever browser
someone tried it in and failed in the consumer that read the guide.

Pinned in both tiers, because they prove different halves and neither is
sufficient alone:

- `tests/unit/api/test_content_health.py` — a datetime becomes the documented
  string and round-trips through `fromisoformat`.
- `tests/integration/api/test_content_health_contract.py` — a real
  `TIMESTAMPTZ` written to `publisher_harvest_state` is read back through the
  whole stack. Only a warehouse can prove the value arriving *is* a datetime;
  under the old cast it was already a string, and a test building its own
  datetime would have passed throughout.

The guide gained an explicit sentence, so the format is a promise rather than
an example inferred from one sample. Verified against the live warehouse: all
seven sources now answer offset-aware ISO-8601.

### 6. The stale comment

`tests/frontend/smoke/content-health.smoke.test.js` still said "Against the
Compose stack, which seeds one ACS measure, six sources are silent", which the
same file's header contradicted after the seed was expanded. Rewritten to say
why the bound is opt-in *now*, keeping the old reason as history rather than as
a claim.

### 7. The Windows-only unit failures

Not this branch's doing, fixed because they were the last thing between this
host and a green unit tier. Three tests built a path with
`str(path.relative_to(ROOT))`, which yields backslashes on Windows and misses
allowlists spelled with forward slashes — so they passed in CI and failed
locally, which is the same shape of divergence deliverable 3 exists to close.
All three now use `.as_posix()`, the idiom `tests/support/catalog_evidence.py`
already used.

`tests/unit` is now **1734 passed, 0 failed** on this host.

## What was verified working

Recorded so the remediation is not mistaken for a verdict on the feature. The
deployment-observability work itself checks out against the live warehouse
(internal stack, `docker-compose.yml` with `stack.env`, 7 sources loaded):

- `GET /api/v1/health/content` matches
  `SELECT source_code, freshness_state, count(*) FROM gold_glossary.dim_metric_catalog GROUP BY 1,2`
  row for row — BLS 63 current / 13,261 retired / 13,324 total, CENSUS_ACS
  4,447, CDC 281, USDA_NASS 101, FRED 24, CENSUS_PEP 17, FBI_UCR 4.
- The resource answers no `cache-control` while `/api/v1/catalog/metrics`
  answers `public, max-age=300`; the served OpenAPI declares `200/422/429/503`
  for it; `apps/api/ratelimit.py`'s `EXEMPT_PATHS`, built from the two other
  health routers, correctly does not exempt it.
- `frontend-smoke` as CI runs it, with both new bounds set: **17/17 passed**,
  including all seven of `content-health.smoke.test.js`. The unhandled-error
  trailers seen when pointing the tier at the `next start` origin disappear
  behind the composed nginx proxy, as `tests/frontend/smoke/unhandledErrors.js`
  documents.
- Full unit tier: **1729 passed** (plus the three Windows-only failures above).
- All seven registered sources answer real observations through the web proxy
  against the live warehouse.

## Verification

Reproduce the failure, then prove it gone. The database must be recreated
between runs — the seed is applied at initdb, so an existing volume hides the
change:

```bash
docker compose -f infra/docker/docker-compose.test.yml down --volumes --remove-orphans
docker compose -f infra/docker/docker-compose.test.yml up -d postgres redis
# expect no rows once deliverable 1 lands:
docker exec population-testing-postgres-1 psql -U population_test -d population_etl_test \
  -tAc "SELECT dataset_code, vintage_year, product_code FROM silver_pep.pep_release WHERE product_code = 'alldata';"

RUN_INTEGRATION_TESTS=1 \
TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=55432 \
TEST_POSTGRES_USER=population_test TEST_POSTGRES_PASSWORD=population_test \
TEST_POSTGRES_DATABASE=population_etl_test \
TEST_REDIS_URL=redis://127.0.0.1:56379/15 \
python -m pytest tests/integration/api -m "integration and database and not slow" -q
```

Acceptance: `tests/integration/api` passes against the compose database, and
still passes against a bare Postgres with no initdb mounts (the shape CI uses),
so neither path is the only one that works.

The smoke tier must stay green through the change — it is the tier the seed
exists for:

```powershell
$env:SMOKE_REQUIRE_ALL_SOURCES = "1"; $env:SMOKE_REQUIRE_FRESH_SOURCES = "1"
.\tests\run.ps1 web-smoke
```
