---
id: catalog-grain-vocabulary
branch: fix/observations-unanswered-metrics
depends_on: []
parallel_safe: false
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
  - ./tests/run.ps1 web-unit
---

# A published geography grain is answerable, in one vocabulary

## Plan status

- **Status:** Ready for review. Claimed and delivered 2026-09-12. Opened
  from the live-stack smoke tier (WEB-027), which found 13 catalog metrics
  answering no row through the explorer's own access shape; chasing that
  down found a repo-wide contract defect rather than a stack-state one.
- **Last updated:** 2026-09-12
- **Owner surface:** `sql/migrations/018_geo_grain_vocabulary.sql`,
  `src/data_ingestion_toolbox/census_pep/gold_pep/DDL/gold_pep.sql`,
  `apps/api/registry.py`, `apps/api/services/neutral_observations_service.py`,
  `tests/integration/api/test_catalog_serving_agreement.py`,
  `tests/frontend/smoke/live-stack.smoke.test.js`
- **Depends on:** nothing open.

## Implementation checkpoint

**Last updated:** 2026-09-12

**Next pickup:** none — human review. One named follow-on below.

- [x] GV-001 establish the defect with warehouse and API evidence (below)
- [x] GV-002 one vocabulary function; CDC, NASS, PEP publishers use it; ACS derives its grains
- [x] GV-003 the neutral route projects and filters the vocabulary for
      every source; `NATION` accepted as an alias
- [x] GV-004 DB-028 guard: every published grain of a current code answers
- [x] GV-005 smoke tier asks the catalog as the explorer does
- [x] GV-006 docs, catalog rows, dev-stack re-harvest and verification

## Delivery record (2026-09-12)

- `sql/migrations/018_geo_grain_vocabulary.sql`: `gold_glossary.geo_grain(text)`
  and the CDC and NASS publishers through it. Registered in the manifest,
  the test compose, and the migrations README.
- `census_acs/gold_census/DDL/publisher.sql`: grains derived from
  `gold_census.mv_acs_latest`, empty for a variable nothing serves. On the
  development warehouse the declared model was wrong in both directions:
  derived grains are COUNTY 2,789 (declared 1,927 — 862 `acs1` variables
  serve county rows the configuration denied), STATE 2,789 (declared 4,447),
  NATIONAL 4,447.
- `census_pep/gold_pep/DDL/gold_pep.sql`: `measure_export` grains from the
  served revision relation, mapped, aggregated once per measure (the first
  cut joined the served relation row-for-row beside the silver fact — a
  cross product that ran ten minutes before it was killed; recorded so the
  next person aggregates first). `UNSUPPORTED` no longer published.
- `apps/api/registry.py`: the vocabulary and alias table; CDC and PEP through
  `gold_glossary.geo_grain(geo_type)`; FBI through `UPPER(subject_type)`
  with the `geo_level` filter declared; NASS `UPPER(agg_level_desc)`.
  `neutral_observations_service._filter_conditions` normalizes the incoming
  word, which also covers the comparison and distribution services.
- DB-028 and API-073; the smoke tier asks for `active_only=true` and joins
  on a county-grain metric.
- Development stack: 018, the ACS publisher, and the PEP DDL applied;
  `glossary_reconciliation` forced for `gold_census`, `gold_cdc`,
  `gold_nass`, `gold_pep` (run `gv_reconcile_20260912T222926Z`, success in
  45 s); API restarted. Verified live: the NASS statistic that published
  only `NATION` answers `geo_level=NATIONAL` with its one national row; CDC
  and PEP answer `NATIONAL` and the `nation` alias with rows carrying
  `NATIONAL`; PEP answers `PLACE` (19,483 rows); FBI answers `STATE`,
  `AGENCY`, `NATIONAL` with rows carrying those words.

### Validation (2026-09-12)

| Check | Command | Result |
| --- | --- | --- |
| Python unit | `pytest tests/unit` | 1340 passed (register guard at 289 rows, all FULL; API-073 in `test_serving_registry.py` and `test_neutral_observations.py`) |
| Python lint | `ruff check .` | clean |
| Real-schema agreement | `pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and not e2e"` against a test warehouse recreated so it bootstraps with 018 | 5 passed (DB-025, DB-026, DB-028; the ACS fixture publishes exactly `STATE`) |
| Live-stack smoke | `SMOKE_BASE_URL=http://localhost:3001 npm --prefix apps/web run test:smoke` | **6 of 6 tests passed**, including the two WEB-027 tests that opened this plan; the process still exits non-zero on three unhandled errors — see the follow-on |

Not run, recorded as not run: the rest of the integration tier, e2e, DAG,
Martin, and deployment tiers. The DAG tier is unaffected (no DAG change);
the publisher views are exercised by the real harvest in DB-025/DB-028.

### Follow-on, named rather than absorbed

The smoke tier's process exits non-zero on three unhandled
`AssertionError: assert(!this.paused)` from Node's bundled undici
(`Parser.finish` on `Socket.onHttpSocketEnd`), raised while
`loadPreviewTileFeatures`/`discoverTileMetadata` run. They were present —
four of them — on the first run of this tier today, before any change here.
The likely cause is a discovery probe that checks a response's status and
content type and abandons the body, so the socket ends with the parser
paused; consuming or cancelling the body in `tiles.js` would settle it. It
is a client hygiene defect in the tier's own harness, not a warehouse or
API contract, and it belongs to the frontend plan that owns WEB-027.

## The defect (GV-001, evidence 2026-09-12)

`API_CONSUMER_GUIDE.md` promises: *"`geo_level` on a served row is always
`NATIONAL`, `STATE`, or `COUNTY`, and the `geo_level` filter matches that
vocabulary."* The catalog's `valid_geo_grains` is the discovery surface for
that filter. Neither half of the promise holds for five of seven sources,
in four different ways, and nothing in the stack reported any of them.

Against the development warehouse and the live API:

| Source | Catalog grains | Row `geo_level` served | `geo_level=NATIONAL` | `geo_level=NATION` |
| --- | --- | --- | --- | --- |
| BLS, FRED | `NATIONAL`, … | `NATIONAL` | answers | 0 rows |
| CENSUS_ACS | declared, not derived | `NATIONAL` | answers | 0 rows |
| CDC | `NATION`, … | `nation` | **0 rows** | answers |
| CENSUS_PEP | `NATION`, `UNSUPPORTED`, … | `nation` | **0 rows** | answers |
| USDA_NASS | `NATION`, … | `NATIONAL` | answers | **0 rows** |
| FBI_UCR | `AGENCY`, `NATIONAL`, `STATE` | `fbi_agency:WI0050700` | filter not declared | — |

Four mechanisms:

1. **CDC and PEP** derive grains as `UPPER(geo_type)`, whose national value
   is `nation`, and project `geo_type` verbatim on rows. They agree with
   themselves and disagree with the contract: `NATIONAL` — the documented
   word — answers nothing, and a served row says `nation`.
2. **USDA NASS** derives grains the same way but filters and projects on
   `agg_level_desc`, whose national value is `NATIONAL`. The catalog hands a
   consumer `NATION`; the route cannot match it. Every national NASS
   statistic is unanswerable by construction; `STATE` and `COUNTY` coincide
   by luck. This is what the smoke tier tripped on — nine
   `corn_census_county` statistics, one of which publishes only `NATION`
   and so answers nothing at all while marked `current`.
3. **FBI UCR** derives grains from `subject_type` (`agency`, `national`,
   `state`) but projects `source_geo_level` — a source identifier such as
   `fbi_agency:WI0050700` — as the row's `geo_level`, and declares no
   `geo_level` filter at all.

4. **Census ACS declares its grains from the dataset code** — `acs1` gets
   `NATIONAL, STATE`, everything else all three — instead of deriving them
   from served rows as BLS does. On the development warehouse that
   advertises `STATE` for all 4,447 current metrics, **1,658 of which serve
   no state row**, and `COUNTY` for 1,927, **829 of which serve none**:
   2,487 metric/grain pairs a consumer can ask for and get an empty page it
   cannot tell from a geography that publishes nothing. FRED declares
   `NATIONAL` the same way; every FRED series is national, so it happens to
   be true.

Two further facts from the same probe:

- PEP publishes `UNSUPPORTED` as a grain. Its `measure_export` reads grains
  from `silver_pep.fact_population_estimate`, which holds 1.6M rows whose
  geography could not be resolved; the served gold relations exclude them.
  A geography-resolution status is not a grain, and nothing can be asked
  for it.
- PEP serves `place` and FBI serves `agency`. The guide's three-word
  vocabulary denies two grains the warehouse really publishes. The
  vocabulary is five words: `NATIONAL`, `STATE`, `COUNTY`, `PLACE`,
  `AGENCY`.

The four BLS metrics in the smoke failure were a different, smaller thing:
they are `retired` (the LAUS measure-identity migration retired 13,261
per-series codes), and retired means no rows. The explorer requests its
catalog with `active_only=true`; the smoke tier, which claims to ask as the
explorer does, did not. That is a test defect, fixed under GV-005.

## Decision

**One mapping, in the warehouse, used by everything.** The vocabulary
mapping (`nation`→`NATIONAL`, otherwise upper-case) would otherwise be
written in three publisher views and two dispatch entries — the same drift
that produced this defect. `gold_glossary.geo_grain(text)` is that mapping;
the CDC and NASS publishers and the API's CDC and PEP dispatch entries call
it. PEP's publisher is owned by `gold_pep.sql`, which its DAG re-applies
ahead of the glossary phase, so it carries the one inline copy with a
comment naming the function; DB-028 enforces the outcome regardless of where
the mapping lives.

**A side effect worth naming:** `compatibility.py`'s geography-grain rule compares two metrics' `valid_geo_grains`. A CDC measure (`NATION`) and a BLS one (`NATIONAL`) at the same grain were judged not comparable on a spelling; they agree now.

**FBI projects and filters `UPPER(subject_type)`** — the column its own
publisher derives grains from — and gains the `geo_level` filter. Additive.

**`NATION` stays accepted as an alias** on the neutral route, normalized to
`NATIONAL` before binding. The catalog has published that word to consumers,
and a saved configuration or a shared link holding it must keep answering
(ADR-0002: a `v1` change that would break a client belongs in `v2`).

## Phases

### GV-002 — the function and the publishers
`018_geo_grain_vocabulary.sql`: `gold_glossary.geo_grain(text)`; replace
`gold_cdc.metric_publisher` and `gold_nass.metric_publisher` from 014 with
grains through it. `gold_pep.sql`: `measure_export` reads grains from the
served `population_estimate_revision` relation (no `unsupported`) and maps
them; `metric_publisher` unchanged otherwise. Manifest, compose, README.
Also: the ACS publisher derives `valid_geo_grains` from
`gold_census.mv_acs_latest` (the served latest relation; ~7 s over 4.6M
rows on the development warehouse, paid once per harvest) with an empty
array for a variable nothing serves, so the catalog stops advertising grains
it cannot answer.

### GV-003 — the neutral route
Registry: CDC and PEP `geo_level_expression` and filter through
`gold_glossary.geo_grain(geo_type)`; FBI through `UPPER(subject_type)` with
the `geo_level` filter declared; NASS `UPPER(agg_level_desc)`. The
vocabulary and its alias table live in the registry; the service normalizes
the incoming filter. Unit tests updated deliberately, alias covered.

### GV-004 — the guard
DB-028 in `test_catalog_serving_agreement.py`: for every registered source,
each published grain of each sampled current code answers at least one row
with `geo_level=<grain>`, and every returned row's `geo_level` equals the
grain and is in the vocabulary. Seeded so it cannot pass vacuously.

### GV-005 — the smoke tier
`active_only: "true"` on both catalog reads; the join test picks a metric
whose published grains include `COUNTY`, as the explorer's default selection
does (WEB-029).

### GV-006 — close
Guide vocabulary section (five words, alias, FBI filter); BETA_RESET (apply
018, then forced `glossary_reconciliation` for `gold_cdc`, `gold_nass`,
`gold_pep`, restart the API); catalog rows and register; apply on the
development stack and re-run the smoke tier to green.

## Definition of done

- Every registered source's published grains are answerable through
  `/api/v1/observations` and every served row's `geo_level` is one of the
  five vocabulary words — proven by DB-028 against the real schema.
- The consumer guide states the vocabulary the API actually serves.
- The development stack's catalog is re-harvested and the smoke tier passes.
- Catalog, register, CI map, bootstrap docs synchronized.
