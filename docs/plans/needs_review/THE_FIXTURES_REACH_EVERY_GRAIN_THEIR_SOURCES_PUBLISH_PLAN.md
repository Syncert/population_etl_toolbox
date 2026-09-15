---
id: the-fixtures-reach-every-grain-their-sources-publish
branch: claude/web-viz-metrics-checks-qttkzz
depends_on:
  - deployment-observability
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/integration/api -m "integration and not e2e and not redis and not martin" -q
  - python -m pytest tests/integration/database -m "integration and not e2e and not redis and not martin" -q
  - python -m pytest tests/unit -q
  - ruff format --check . ; ruff check .
---

# The fixture corpus reaches every grain its sources publish

## Plan status

- **Status:** Ready for review. The guard is implemented, four fixtures are
  widened to their sources' full range, two teardown defects found on the way
  are fixed, and the behavioral catalog is updated. Delivered as `e500d40` on
  `claude/web-viz-metrics-checks-qttkzz`.
- **Last updated:** 2026-09-14
- **Current milestone:** delivered.

## Why

The question this plan answers was asked directly: *couldn't a sample of the
raw tables be pulled for every data source and seeded into the integration
warehouse, so the ETL can be tested without a live service?*

The first half of that is already the architecture. `tests/fixtures/<source>/`
holds provider-shaped captures with documented provenance, the support modules
replay them through the real production functions, `tests/e2e` covers all seven
sources, and the unit tier denies sockets outright (`tests/conftest.py:65-113`).
Nothing about testing the ETL needs the network. What still needs a live
service is `tests/external/` — whose whole purpose is detecting provider drift,
which a frozen sample can never do — and `live-deployment-smoke.yml`, which
grades a real deployment.

The second half — *a mix of place, county, state and national rows* — named a
real gap, and not the one about volume. `valid_geo_grains` is **derived**:
every publisher aggregates it out of the rows that exist
(`sql/migrations/018_geo_grain_vocabulary.sql:71` and the six views calling
`gold_glossary.geo_grain`). That is the right design, and it makes every grain
sweep self-limiting. A fixture seeding one county publishes a catalog whose
only grain is COUNTY, so DB-028's "every published grain answers" passes over a
single word and DB-030's route sweep asks every route about that one word. Both
report exactly the green they would report for a corpus covering all five.
Nothing in the stack could tell the two apart.

That is not hypothetical here. It is the shape of the two defects closed
immediately before this work: `4ea898f`, where the smoke seed published one
measure of one source for one county while its summary line named seven, and
`d86f9b0`, where the catalog sweeps `continue`d past four of seven sources and
reported green.

Volume was not the gap. `tests/performance/test_volume_database.py` already
covers scale with synthetic rows, which is the right place for it — scale
testing does not need source fidelity. A thousand sampled rows per source would
have re-proved the same code path several hundred times, cost seven thousand
unreviewed fixture rows, and lost the property the existing fixtures are built
on: every row is explained. There is also no raw row table to sample from.
Migration `007_remove_legacy_parsed_raw.sql` dropped the parsed-raw relations;
raw is capture-first, so the sampling unit is a whole provider response.

## The audit

Measured 2026-09-14 against a bare pinned PostGIS container (CI's path, not the
compose stack — see *Environment* below), with each source's published
`valid_geo_grains` compared to what its own reviewed declaration says the
pipeline can publish:

| Source | Can publish | Fixtures published | Gap |
| --- | --- | --- | --- |
| BLS | NATIONAL, STATE, COUNTY | NATIONAL | STATE, COUNTY |
| CDC | NATIONAL, STATE, COUNTY | STATE | NATIONAL, COUNTY |
| CENSUS_ACS | NATIONAL, STATE, COUNTY | STATE | NATIONAL, COUNTY |
| CENSUS_PEP | NATIONAL, STATE, COUNTY, PLACE | NATIONAL | STATE, COUNTY, PLACE |
| FBI_UCR | NATIONAL, STATE, AGENCY | all three | — |
| FRED | NATIONAL | NATIONAL | — |
| USDA_NASS | NATIONAL, STATE, COUNTY | all three | — |

Eleven of twenty-two source/grain pairs were unreachable, and the split is not
random. **The covered sources are the ones that do not hand-write their rows:**
FBI UCR and USDA NASS replay reviewed captures through their real release
pipelines, and FRED is national by construction. Every hand-seeded fixture was
narrow.

Where each "can publish" comes from, since the repository states it per source
rather than in one place:

- **BLS** — `bls/geography.py` parses a LAUS area code to `state` or `county`
  and to nothing else ("LAUS has no national series"); the national CPS/CES
  series carry `us:1`, which `gold_bls.fact_bls_observation` reads as NATIONAL.
- **CDC** — `cdc/registry.py:157,174` declares `geography_levels` per asset:
  `("us", "state")` for CDI, `("us", "county")` for PLACES county.
- **CENSUS_ACS** — `census_acs/config.py:139` declares
  `geo_levels = ["us", "state", "county"]`.
- **CENSUS_PEP** — `census_pep/silver_pep/transform.py:95-99` maps summary
  levels 010, 040, 050 and 162 to nation, state, county and place, and every
  other level to `unsupported`, which reaches no served row.
- **FBI_UCR** — `fbi_ucr/registry.py:138` closes `subject_type` to `national`,
  `state` and `agency`.
- **FRED** — `gold_fred.sql:36-37` writes `'us:1'` and `'NATIONAL'` as
  literals.
- **USDA_NASS** — `sql/migrations/012_usda_nass_crop_pipeline.sql:111` closes
  `geo_type` to `nation`, `state`, `county` and `unsupported`.

## Deliverables

### 1. DB-044 — the grain-coverage guard

`test_every_source_fixture_corpus_reaches_every_grain_its_pipeline_publishes`
in `tests/integration/api/test_catalog_serving_agreement.py`, beside the grain
sweeps it protects. It checks four things, and every failure names the source
and the word:

- The declaration's keys equal `OBSERVATION_DISPATCH`'s, so a source added to
  the dispatch without declaring its grains fails rather than joining the set
  nobody measures — DB-043's defect, one level up.
- Every declared word is in `apps.api.registry.GEO_GRAINS`.
- Every vocabulary word has a declaring source. A grain the API accepts that no
  source owns is a filter that can only ever answer empty.
- For each source, every declared grain is both published in the catalog **and**
  answerable with at least one row through `/api/v1/observations`.

`ADVERTISED_GEO_GRAINS` is the reviewed declaration, each entry carrying the
citation above in its comment. `_published_grains` deliberately does not sample:
unlike `_current_catalog_grains` it unions over the source's whole current
catalog, because a grain seeded by one fixture among many would fall outside the
first `SWEEP_SAMPLE` codes and the coverage question would answer itself
wrongly.

### 2. Census ACS — a narrow measure and a wide one

`_publish_acs_variable` now takes the table, vintage, period, time row and
grain rows as parameters, and two fixtures call it:

- `published_acs_metric` — unchanged behavior: one state row.
- `published_acs_grain_metric` — `us`, `state` and `county`, on table `B99996`,
  vintage 2092, time row 20920101.

Both run in the same test, which is why every identifying value is a parameter:
sharing a table, a vintage or a time row would make the first teardown the
second's foreign-key violation.

### 3. BLS — its LAUS grains beside the national series

`published_bls_metric` writes one row per grain from `BLS_GRAIN_ROWS` into both
`gold_bls.rpt_bls_observations` and `gold_bls.mv_bls_latest`: `us:1`/NATIONAL,
`state:93`/STATE and `state:93|county:001`/COUNTY. The rows carry the
vocabulary word because that is what the gold view puts in those relations in
production; the publisher still derives the catalog's grains from them.

### 4. CDC — split by asset, because the assets differ

`_publish_cdc_measure` takes the asset, watermark and grain rows:

- `published_cdc_metric` — CDI at `us` and `state`, matching `CDI_ASSET`.
- `published_cdc_county_metric` — PLACES county at `us` and `county`, matching
  `PLACES_COUNTY_ASSET`. Nothing exercised that asset before.

The two releases carry distinct watermarks (`3975004800`, `3975004801`) so each
stays the newest of its own asset, which is what the latest surface orders on.

### 5. Census PEP — all four grains, and two teardown defects

`published_pep_metrics` seeds one revision and one fact per measure per grain
across summary levels 010, 040, 050 and 162, and the dataset row now declares
the levels it carries. Two defects surfaced while doing it:

- The fixture seeded the national geography as `us:1` — what `canonical_geo_id`
  composes and therefore what `seed_geography` writes — and then labelled its
  fact row `nation:us`, a spelling nothing else in the warehouse uses. Its
  teardown deleted `nation:us`, a geo_id the fixture never wrote, so **the
  national geography leaked on every run**.
  `test_a_source_scoped_row_names_the_catalogs_code` filtered its timeseries
  request on the same invented string, so the pair agreed with each other and
  with nothing. Both now read `PEP_NATION_GEO_ID`.
- The `silver_pep.dim_measure` delete sat outside the per-measure loop, so a
  parameterised run publishing two measures left the first one's dimension row
  behind.

### 6. Shared geography helpers

`preexisting_geographies` and `delete_shared_geographies` in
`tests/support/capture_seed.py`. Four fixtures now want `us:1`; the first to
run creates it and the rest find it, and whichever tears down first must not
delete what it did not create. The pattern is `tests/support/fbi_release.py`'s,
which had solved this privately — the delete is skipped for a pre-existing
geography and savepoint-guarded so one still referenced elsewhere stays in place
rather than aborting the caller's cleanup transaction.

### 7. Catalog bookkeeping

- `docs/reference/TESTING_CONTRACT.md` — the DB-044 row, the range
  `DB-001–DB-044`, the total `464 of 464`, and the "464-row register" sentence.
- `tests/support/catalog_evidence.py` — `AUDITED_COUNTS["DB"]` 43 → 44.

## Decisions worth reviewing

**The declaration is a reviewed constant, not derived.** Reading each source's
grains back from the warehouse would return the set the fixtures just produced,
so it would agree with any corpus and prove nothing about the one it was given.
Deriving it instead from each source's config would put a second spelling of
that logic beside the first, which is the failure migration 018 exists to stop:
"a mapping written in five places is how this defect happened." Widening an
entry is therefore a review, not a refresh. The reviewed-constant precedent is
`apps/api/registry.py`'s own.

**Census ACS keeps a narrow measure on purpose.** DB-028 proves
`valid_geo_grains` is derived rather than declared, and that proof needs a
measure narrower than its source: it asserts the ACS fixture's code publishes
STATE and *not* NATIONAL or COUNTY. Widening the existing fixture in place would
have deleted that proof — with ACS covering exactly its config's three levels, a
regression to declaring grains from the dataset code would pass. Hence two
fixtures rather than one wider one.

**CDC is split by asset rather than given one measure with three grains.** The
registry declares different geographies per asset, and a fixture publishing a
county row under CDI would be publishing a row the source never does.

**PEP's `geo_id` correction changes a passing test's request.** The old
filter value was wrong and the old fixture agreed with it. The test still
asserts the same behavior, now against the identity the warehouse actually uses.

## Validation

All runs on a bare pinned PostGIS container, 2026-09-14:

| Command | Result |
| --- | --- |
| `pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and not e2e"` | 21 passed |
| `pytest tests/integration/api -m "integration and not e2e and not redis and not martin"` | 77 passed, 5 deselected |
| `pytest tests/integration/database -m "integration and not e2e and not redis and not martin"` | 171 passed |
| `pytest tests/unit` | 1729 passed, 3 failed |
| `pytest tests/unit/shared/test_catalog_evidence.py tests/unit/shared/test_repository_hygiene.py` | 18 passed |
| `ruff check tests/` / `ruff format` | clean |

The guard was written before the widening and its failure output is the audit
table above; it now passes.

Two entries need their asterisks stated rather than hidden:

- **The three unit failures are pre-existing and host-specific.** They are
  Windows path-separator assertions in
  `tests/unit/deployment/test_warehouse_target_contracts.py` and
  `tests/unit/quality/test_rule_automation.py` comparing `src\...` to `src/...`.
  They are unrelated to this change and fail on an unmodified tree.
- **The database tier was run with
  `--ignore=tests/integration/database/test_usda_nass_dag_tasks.py`**, per the
  known `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` collection defect on this host.

### Environment

The database-backed tiers here were run against a **bare** pinned PostGIS
container, matching `.github/workflows/api-integration.yml:53`, rather than
against `infra/docker/docker-compose.test.yml`. At the time of measurement the
compose stack mounted `tests/sql/frontend_smoke_seed.sql` into the initdb of the
database the integration tier uses, and the tier collided with those rows; the
bare container was the only path on which this change could be measured at all.

**That divergence has since been fixed** by `ed33276`, which moved the seed to
`docker-compose.smoke.yml` and added DB-045 to guard the separation in both
directions. It was found and closed independently of this work — see
[`../completed/THE_SEED_AND_THE_FIXTURES_DO_NOT_SHARE_A_WAREHOUSE_PLAN.md`](../completed/THE_SEED_AND_THE_FIXTURES_DO_NOT_SHARE_A_WAREHOUSE_PLAN.md).
A reviewer verifying this plan on either path should now see the results in the
table above. The note is kept because it explains why the recorded runs took the
shape they did, not because anything here still depends on it.

## Known limitation

The guard unions grains over each source's **whole current catalog**, not over
the codes its fixtures published. On the disposable CI warehouse those are the
same set, which is where the guard runs and where it does its work. On a
populated development warehouse they are not: real content could satisfy the
coverage check while the fixtures stayed narrow.

DB-043 has the same property and mitigates it by additionally asserting each
fixture's own code answers. The equivalent here — asserting the grains of the
fixture-published codes specifically, rather than of the source's catalog —
would close it, and the seven fixture codes are already in the test's signature.
It is left out of this change rather than added unreviewed, and is the first
thing to consider if this guard is ever run against a warehouse carrying real
content.

## Out of scope

- Sampling ~1000 raw rows per source. The reasoning against it is in *Why*
  above; the decision is recorded here so it is not re-litigated from scratch.
- Anything in `tests/external/`. A frozen sample cannot detect provider drift,
  which is that tier's entire purpose.
- The compose-versus-CI warehouse divergence, filed separately.
