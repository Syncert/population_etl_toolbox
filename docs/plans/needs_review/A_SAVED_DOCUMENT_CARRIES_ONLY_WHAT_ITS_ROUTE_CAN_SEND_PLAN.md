---
id: saved-document-carries-only-what-its-route-can-send
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and (redis or database) and not slow" -q
---

# A saved document carries only what its own route can send

## Plan status

- **Status:** Implemented; awaiting review. Claimed and completed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/registry.py`,
  `apps/api/services/saved_analysis_service.py`

## Context

`AnalysisDocument` is one model for three kinds, and its docstring makes a
promise about all of them:

> Validated at write time against the same contracts the live routes
> enforce, so a stored configuration can never encode a request the API
> would refuse.

`validate_document` keeps that promise for `kind: "observations"` — it
refuses every scope/release/reduction contradiction the live route refuses,
with a comment saying why ("Storage is not a back door for a request the API
would refuse"). For the other two kinds it checks the metric, the filters and
the compatibility verdict, and nothing about the rest of the document.

But the three routes do not accept the same parameters:

| Kind | Route | Query parameters beyond the filters |
| --- | --- | --- |
| `observations` | `/observations` | `metric_code`, `scope`, `release`, `newest_per_geography`, `newest_release_per_period` |
| `distribution` | `/distribution/bins` | `metric_code`, `bin_count` |
| `comparison` | `/comparison` | `metric_code_a`, `metric_code_b` |

Read off the validator, with the metric resolution stubbed:

```
ACCEPTED  distribution + as_released + release + newest_per_geography
ACCEPTED  comparison + as_released + release + reductions + bin_count
ACCEPTED  comparison carrying a stray metric_code
ACCEPTED  observations carrying bin_count and a pair
refused   observations, the contradiction the route refuses: release can
          only be combined with scope=as_released
```

So a reader can save "the distribution of this measure **as it was released
in 2022**", be told the configuration is valid, and reopen it to the latest
publication — with nothing anywhere saying the pin was dropped, because
`/distribution/bins` has no `release` to send it to. That is not a request
the API would refuse; it is worse, an intent the API accepts and then cannot
honour. The same document reports `valid: true` on every later read.

The asymmetry is the tell: `model_config = ConfigDict(extra="forbid")`, so
the API refuses a field it has never heard of, and accepts one it knows its
route cannot use.

The shipped web client is not the source of such a document —
`observationsDocument` sets `release: null` outside `as_released` and
`comparisonDocument` carries neither scope nor release — so this closes a
hole rather than fixing a break. Evidence packets reuse `validate_document`
for every analytical block, so the fix reaches both surfaces.

## Acceptance criteria

1. A document is refused when it carries a value its own kind's route cannot
   send, naming the field and why, at write time — for saved configurations
   and for an evidence packet's analytical blocks alike.
2. A field left at its default is never a refusal. A document stored before
   this existed, or one that spells `scope: "latest"` explicitly, keeps
   validating exactly as it did; only a value that changes the request is
   refused.
3. The per-kind field sets live in the reviewed registry, in one place, and
   a test derives the truth from the served contract: each kind's route must
   declare every field the registry credits it with, and must not declare a
   document field the registry withholds. Adding a parameter to one of the
   three routes without extending the registry fails.
4. An already-stored document carrying such a field reports `valid: false`
   with the reason on read, rather than being rewritten — the documented
   behaviour for a configuration that no longer matches live capabilities.
5. The consumer guide says a document carries only its kind's fields.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-112).

## Non-goals

- Splitting `AnalysisDocument` into three models. It is one stored shape, and
  a reader's document survives a kind change today; the fix is to validate
  what the shape means rather than to fork it.
- Teaching `/distribution/bins` or `/comparison` a `release` parameter. What
  either route would mean by an as-released distribution or comparison is a
  design question, not a validation gap.

## What changed

- `apps/api/registry.py` gains `CONFIGURATION_ROUTES` (the route each kind
  replays through) and `CONFIGURATION_DOCUMENT_FIELDS` (the document fields
  each kind's route accepts, beyond `filters` and `visualization`). They sit
  together so the document's meaning and the route's parameters are checked
  against one another rather than each maintained alone.
- `_require_fields_the_route_can_send` refuses any field set to a
  non-default value outside its kind's set, naming the fields, the route
  that could not have taken them, and what the kind does carry. Called from
  `validate_document`, so it reaches saved configurations and evidence-packet
  analytical blocks alike.
- The default comparison is deliberate rather than `model_fields_set`: a
  document round-trips through JSONB, where every field is present, so
  "carries a value" has to mean "differs from the default" for a document
  written before the check to keep validating.
- The guide's saved-analysis section states the per-kind fields, why the
  other two routes cannot take a scope or a release, and that a default is
  not a refusal.

## Validation

- `pytest tests/unit` — **1502 passed** (1490 before: +12 nodes).
  `tests/unit/api/test_saved_analysis.py` alone: 67 passed.
- **The tests fail without the fix.** Commenting out the one call in
  `validate_document` leaves `6 failed, 61 passed` — the five refusal cases
  and the read-reporting case — so the guard is load-bearing rather than
  describing what already happened.
- `pytest tests/integration -m "integration and (redis or database) and not
  slow"` — 145 passed, 2 skipped, 14 deselected.
- e2e on a freshly created database
  (`E2E_REQUIRE_ALL_PRODUCTS=1 RUN_E2E_TESTS=1 RUN_INTEGRATION_TESTS=1`) —
  **9 passed**. The shipped client writes no cross-kind field, which this
  confirms rather than assumes.
- `ruff format --check .` / `ruff check .` — clean (442 files).
- `python -m tests.support.catalog_evidence` renders API-112 `FULL`; the
  register is 371 rows.

### Ground truth

Read off `validate_document` with the metric resolution stubbed, before the
change:

```
ACCEPTED  distribution + as_released + release + newest_per_geography
ACCEPTED  comparison + as_released + release + reductions + bin_count
ACCEPTED  comparison carrying a stray metric_code
ACCEPTED  observations carrying bin_count and a pair
refused   observations, the contradiction the route refuses
```

and after it, each of the first four refused by name, for example:

```
a configuration of kind 'distribution' cannot carry newest_per_geography,
newest_release_per_period, release, scope: /api/v1/distribution/bins has no
such parameter, so the value could not be replayed. This kind carries:
bin_count, metric_code
```

## Remaining work

- None. Review is the remaining step.
