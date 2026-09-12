---
id: acs-catalog-metric-code-mismatch
branch: fix/acs-catalog-metric-code
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 unit
  - ./tests/run.ps1 api
  - ./tests/run.ps1 integration
---

# Every ACS metric the catalog advertises is unresolvable through the API

## Plan status

- **Status:** Approved, unclaimed
- **Last updated:** 2026-09-12
- **Owner surface:** `src/data_ingestion_toolbox/census_acs/gold_census/DDL/`,
  `src/data_ingestion_toolbox/glossary/harvest.py`, `apps/api/registry.py`,
  `tests/integration/database/`
- **Depends on:** nothing open. Found on 2026-09-12 while validating the ACS
  geography re-serve in `needs_review/FORCED_FULL_RESERVE_SCALE_PLAN.md`; the
  defect is older than that work and independent of it.

## Implementation checkpoint

**Last updated:** 2026-09-12

**Current milestone:** none claimed

**Next pickup:** claim the plan, then start at ACM-001 — the decision about
which spelling is canonical governs every other phase, and it is the one thing
this plan deliberately does not decide for the implementer.

### Completed in the current slice

- [ ] ACM-001 decide which spelling is canonical and record why
- [ ] ACM-002 make the two sides agree
- [ ] ACM-003 a guard that fails when a catalog code cannot be served
- [ ] ACM-004 re-serve or re-harvest as the decision requires

## Objective

Make a metric code taken from the catalog answerable by the API. Today an ACS
code read from `/api/v1/catalog/metrics` returns zero rows from
`/api/v1/census/observations/latest`, because the catalog and the serving layer
spell the same metric two different ways. The catalog is the published
discovery surface — a consumer that follows it correctly gets nothing back, and
nothing in the stack reports that.

## Evidence gathered 2026-09-12

Against the development warehouse, immediately after a clean forced full
re-serve of `CENSUS_ACS` (20/20 chunks `COMPLETE`, 0 failed).

The catalog publishes the code with the source code as its first segment:

```
SELECT metric_code FROM gold_glossary.dim_metric_catalog
WHERE source_code = 'CENSUS_ACS' LIMIT 1;
-> CENSUS_ACS:acs1:B01001_001
```

The serving layer publishes the same metric with `ACS` as its first segment:

```
SELECT metric_code FROM gold_census.mv_acs_latest
WHERE geo_level = 'NATIONAL' LIMIT 1;
-> ACS:acs1:B01001_001
```

So the catalog's own code resolves to nothing, and the serving layer's code —
which no published surface advertises — is the one that works:

```
GET /api/v1/census/observations/latest?metric_code=CENSUS_ACS:acs1:B01001_001&geo_level=NATIONAL
-> {"total": 0, "items": []}

GET /api/v1/census/observations/latest?metric_code=ACS:acs1:B01001_001&geo_level=NATIONAL
-> {"total": 1, ... "value": "340110990.0"}
```

### The scale, and the two sources that are healthy

Counting catalog rows whose `metric_code` exists in the source's `mv_*_latest`:

| Source | Catalog rows | Resolvable | Note |
| --- | --- | --- | --- |
| `CENSUS_ACS` | 4,447 | **0** | every advertised code is dead |
| `BLS` | 13,324 | 63 | correct — 63 current, 13,261 deliberately retired |
| `FRED` | 24 | 24 | correct |

BLS and FRED are the control: their publisher emits the same `source_code` that
their refresh procedure builds the code from, so the two sides agree by
construction. Only ACS disagrees, which is why this reads as a defect in ACS
rather than a design question about the catalog.

`CENSUS_PEP` is the most important control, because it is the other
`CENSUS_*` source and the one that could have shared the pattern. It does not:
`gold_pep.mv_pep_latest` serves `CENSUS_PEP:`-prefixed codes, matching its
catalog rows. So ACS is a single instance, not a family convention, and this
plan closes the whole pattern rather than one of two cases.

### The two lines that disagree

- [`publisher.sql:3`](../../../src/data_ingestion_toolbox/census_acs/gold_census/DDL/publisher.sql#L3)
  emits `'CENSUS_ACS'::TEXT AS source_code`, and the harvest composes
  `metric_code` as `source_code || ':' || source_object_key`.
- [`gold_acs.sql:293`](../../../src/data_ingestion_toolbox/census_acs/gold_census/DDL/gold_acs.sql#L293)
  builds the served code as `'ACS:' || ao.dataset_code || ':' || v.variable_code`.

Blamed to `298b73d` (2026-08-19, the capture-first cutover), so this has been
true of every ACS row the warehouse has ever served. It was invisible because
no test and no page crosses the two surfaces: the web explorer reads metrics
and observations from the same endpoint family and never round-trips a catalog
code, and the API's own tests build their fixtures from one side or the other.

## Decisions

1. **This is a real defect, not a naming preference.** The catalog is
   documented as the discovery surface for metric codes. A code it publishes
   that returns nothing is a broken contract regardless of which spelling wins.
2. **Do not fix it by teaching the API to translate.** A prefix-rewriting shim
   in `apps/api` would make the symptom disappear while leaving two identities
   for one metric in the warehouse, which is exactly the condition
   `needs_review/GLOSSARY_HARVEST_IDENTITY_CHANGE_PLAN.md` exists to prevent.
   One of the two producers changes.
3. **The choice of canonical spelling is the implementer's to make and record.**
   ACM-001 requires it in writing before any code moves, because the two
   options have very different costs — see the phase.

## Non-goals

- No change to BLS, FRED, or PEP metric identity. They agree already.
- No new API surface, and no alias parameter.
- No change to the catalog's retirement semantics. Codes retired under the old
  spelling stay retired; this plan does not delete catalog history.

## Implementation phases

### ACM-001 — Decide which spelling is canonical

Deliverables:

- A recorded decision, in this plan, choosing one of:
  - **Serving follows the catalog** (`CENSUS_ACS:`). Consistent with every
    other source, and the catalog needs no change. Costs a `metric_code`
    rewrite of `gold_census.rpt_acs_observations` — 68.3 million rows, a
    forced full re-serve, measured at roughly 4h36m on a tuned box (see
    `BETA_RESET_REINGESTION.md` section 7). Every stored ACS code changes, so
    any consumer holding a saved `ACS:`-prefixed code breaks.
  - **Catalog follows serving** (`ACS:`). Cheap — a publisher change and one
    harvest. But it makes ACS the only source whose catalog code does not begin
    with its `source_code`, which the harvest composes generically, so it needs
    either a per-source override or a documented exception, and it puts ACS at
    odds with `CENSUS_PEP`, which already serves `CENSUS_PEP:`.

Acceptance:

- The decision and its reasoning are written into this plan before ACM-002
  opens, including which consumers hold stored codes and how they are migrated.

### ACM-002 — Make the two sides agree

Deliverables:

- The single producer chosen in ACM-001 changed, with no translation layer
  anywhere between them.
- If serving moves: the `metric_code` expression in `gold_acs.sql` and every
  place that reproduces it, including the affected-keys temp tables that key on
  `metric_code`.
- If the catalog moves: the publisher view, plus whatever the harvest needs so
  the exception is explicit rather than emergent.

Acceptance:

- Every `CENSUS_ACS` catalog row's `metric_code` exists in
  `gold_census.mv_acs_latest`, except rows legitimately `retired`.

### ACM-003 — A guard that fails when a catalog code cannot be served

Deliverables:

- A test that, for every source with a serving contract in
  `apps/api/registry.py`, takes a `current` catalog code and requires the API
  to answer it with at least one row. Source-agnostic, so a fourth source
  cannot reintroduce the defect.
- A `TESTING_CONTRACT.md` row for catalog/serving code agreement, mapped in
  `CI_EVIDENCE_MAP.md`.

Acceptance:

- Reverting ACM-002 makes the guard fail, naming `CENSUS_ACS` and the code that
  did not resolve.

### ACM-004 — Re-serve or re-harvest as the decision requires

Deliverables:

- If serving moved: a forced full re-serve of `CENSUS_ACS` per
  `BETA_RESET_REINGESTION.md` section 7, with `acs_ingest` paused for its
  duration, then a forced `glossary_reconciliation` so retirement of the old
  codes proceeds.
- If the catalog moved: a forced `glossary_reconciliation` scoped to
  `gold_census`, and confirmation that the old `CENSUS_ACS:`-prefixed codes
  retire rather than lingering as `current`.
- Either way: `docs/reference/BETA_RESET_REINGESTION.md` gains this as a worked
  example of an identity change, since it is precisely the class of change
  section 7 describes.

Acceptance:

- ACM-003's guard passes against the development warehouse, and the catalog
  reports no `current` ACS code that the API cannot answer.

## Test plan

| Layer | Tier | What it proves |
| --- | --- | --- |
| Code composition | `unit` | The publisher's `source_code` and the refresh procedure's prefix are derived from one definition, not typed twice |
| Catalog/serving agreement | `integration` | Every `current` catalog code exists in its source's serving relation |
| API round-trip | `api` | A code read from `/catalog/metrics` answers from `/observations/latest` for every registered source |
| Retirement | `integration` | Codes under the abandoned spelling reach `retired` rather than staying `current` |

## Risks and mitigations

- **A 68-million-row rewrite is the expensive option.** ACM-001 exists so that
  cost is accepted deliberately rather than discovered in ACM-002. The forced
  re-serve path is now measured and resumable, which is what makes the
  expensive option viable at all.
- **Stored consumer codes break either way.** Saved analysis configurations
  (`gold.analysis_configuration`) may hold ACS metric codes. ACM-001 must
  enumerate them and say how they migrate; a silent break there is worse than
  the defect.
- **A sibling source could share the defect.** Checked and ruled out on
  2026-09-12: `CENSUS_PEP` serves `CENSUS_PEP:` and agrees with its catalog.
  ACM-003's guard is what keeps a future source from reintroducing it.
- **The guard could be written to pass vacuously** if it skips sources with no
  catalog rows. It must fail, not skip, when a source in the registry has a
  serving contract and no resolvable catalog code.

## Open questions for the reviewer

1. ACM-001's core question: is consistency with the other sources
   (`CENSUS_ACS:`) worth a 68-million-row re-serve and a breaking change to
   every stored ACS code, or is the cheap publisher-side fix (`ACS:`) plus a
   documented exception the better trade? The plan takes no default.

## Implementation evidence

_Empty until claimed._
