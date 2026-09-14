---
id: storage-is-not-a-back-door-for-a-refused-value
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-grain-that-is-not-one-is-refused]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
---

# A saved document cannot encode a filter value the route refuses

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Investigated, authored and implemented 2026-09-13. **Defect opened by `a-grain-that-is-not-one-is-refused` and closed here.**)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/registry.py`,
  `apps/api/services/saved_analysis_service.py`,
  `apps/api/dependencies.py`, `docs/reference/API_CONSUMER_GUIDE.md`

## Context

Found by probing the private surface of a running API: a probe warehouse with
`app_api` applied, two issued account tokens, and the documented flows
exercised. Authentication, cross-account isolation (`404`, not `403`),
`private, no-store`, and the version conflict all behaved. Then:

```text
POST /api/v1/analysis-configurations
  {"document": {"kind": "observations", "metric_code": "…",
                "filters": {"geo_level": "NOPE"}}}
  -> 201
```

`AnalysisDocument`'s own docstring says the opposite:

> Validated at write time against the same contracts the live routes enforce,
> so a stored configuration can never encode a request the API would refuse.

and `validate_document` says why it matters, in a comment:

> Storage is not a back door for a request the API would refuse, and a stored
> contradiction would replay as a 422 the reader never saw when they saved it.

`state_fips: "ZZ"` and `county_fips: "ZZZ"` stored the same way.

## Findings

- The gap is precisely one level deep, and the layers above and below it are
  both already closed. API-117 made storage refuse a filter **name** the route
  would refuse. API-091 and API-105 made it refuse a value of the wrong
  **shape or length**. Nothing checked a value that is the right type, inside
  its bound, and outside the closed set the route accepts.
- Before `a-grain-that-is-not-one-is-refused`, such a document replayed as
  `200` with an empty page -- wrong, but not a broken link. That plan made the
  live routes refuse it, which turned a latent inconsistency into the exact
  failure the comment above forbids: **stored clean, replays as a 422**.
- `ConfigurationValidation` already exists for "whether a stored document
  still matches live capabilities", so the documents stored before the rule
  have a home: they must keep opening, keep returning what was stored, and
  report why they will not replay.
- Evidence packets carry a `geo_level` too, and it is not this defect: the
  packet records the grain a query *was* answered at and the service compares
  it (`normalize_geo_level(envelope.geo_level) != normalize_geo_level(
  queried_grain)`) rather than replaying it as a request.

## Acceptance criteria

1. Saved-analysis validation refuses a filter value outside a closed set,
   from the same rule the request layer applies -- not a second copy.
2. A document stored before the rule still reads, unmodified, with
   `validation.valid = false` and the reason.
3. Nothing the route accepts is refused at write. The empty value a document
   records for a filter its source does not declare keeps storing.
4. A guard asserts the two layers agree, rather than asserting one
   implementation.
5. `API_CONSUMER_GUIDE.md` says a closed-set filter is held to it at write
   time, and how a document stored before a rule tightened reads.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (`API-123`).

## Non-goals

- `geo_id`. Its shape is source-dependent, as the previous plan's non-goals
  record; nothing here changes that.

## Validation

- **Criterion 1, and a correction to what the previous plan shipped.** The
  closed-value rule was in `dependencies.py`, which is the request layer --
  so storage could not read it without importing the HTTP layer. It moved to
  `registry.closed_value_refusal`, beside the vocabulary it already holds,
  and both layers call it: the dependency is now only the HTTP translation of
  the answer, and `validate_document` raises `ConfigurationInvalid` with the
  same text. The FIPS shapes moved with it, so there is one statement of the
  rule and one statement of its reason.
- **Criterion 2, verified against the running API.** The four documents this
  investigation stored before the rule existed (`geo_level: "NOPE"`,
  `geo_level: "COUNTRY"`, `state_fips: "ZZ"`, `county_fips: "ZZZ"`) all still
  answer `200`, return exactly what was stored, and carry
  `validation.valid = false` with the refusal as the reason. A write of the
  same document now answers `422 filter 'geo_level': geo_level must be one
  of: NATIONAL, STATE, COUNTY, PLACE, AGENCY`.
- **Criterion 3, and the case that proves it is a real constraint.** The
  existing `test_a_value_the_route_accepts_is_still_stored` carries
  `{"state_fips": 6}` with the comment "Read off the live route against a
  real warehouse: it passes validation ... so refusing them at write would be
  stricter than the API". That case now fails, because the previous plan made
  the route refuse a one-digit state code: the reference layer stores `06`
  and `6` can never match it. So it moved to the refused set with the reason
  written down, rather than being weakened on either side -- storage is
  exactly as strict as the route, which is the property both plans are about.
  The other five cases in that list are untouched, `{"geo_id": ""}` included.
- **Criterion 4.** `test_storage_and_the_route_refuse_the_same_value`
  parametrizes the closed parameters, asserts `closed_value_refusal` refuses
  the value at all, and then asserts the document validator raises with *that
  same text* -- so it fails if either layer stops refusing, or if they drift
  into two reasons. Break-test: removing the storage-side loop fails it with
  `DID NOT RAISE ConfigurationInvalid` for every parameter.
  - The refused-value list gained the four cases that are the right shape and
    length and outside the closed set, and that test's `Covers:` label now
    names API-123 beside API-105, because it checks more than API-105 claimed.
- **Criterion 5.** The guide's `filters` bullet says a closed-set filter is
  held to it at write time and that `state_fips: 6` is refused here exactly
  as `?state_fips=6` is refused there, with the reason ("a document that
  stored clean and replayed as a 422 is a broken link its owner never saw
  coming"); the `validation` bullet says how a document stored before a rule
  tightened reads. The sentence that said `state_fips: 6` was accepted is
  gone, because it is not.
- **Criterion 6.** `API-123` is in `TESTING_CONTRACT.md`, the family range
  reads `API-001–API-123`, `AUDITED_COUNTS["API"]` is 123, and the totals are
  410.
- **What else the private-surface probe found, which was nothing.** Two
  accounts, one document: the owner reads `200`, the other account reads
  `404 configuration not found` -- not a `403`, so existence is not disclosed.
  A missing or unknown bearer token is `401 a valid bearer token is
  required`. Every private response carries `cache-control: private,
  no-store` and no `x-cache`. A duplicate name is `409`. A document naming an
  undeclared filter is `422` naming the supported filters (API-117 holding).
- **Tiers.** `pytest tests/unit` 1606 passed. `pytest tests/integration -m
  "integration and (redis or database) and not slow"` 169 passed, 2 skipped,
  14 deselected. `ruff check .` and `ruff format --check .` clean.

## Remaining work

- None.
