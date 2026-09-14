---
id: a-stored-filter-is-one-value
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest tests/unit/api/test_saved_analysis.py tests/unit/api/test_observations.py
  - pytest tests/integration/api -m "integration and database"
---

# A stored filter value is one value the route could receive

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/schemas/observations.py`

## Context

API-091 closed half of a gap and named the other half in the process:

> the names were checked while the values were not -- a 5,000-character
> `geo_id` stored clean and failed only when its owner reopened it

The bound it added checks how *long* a value is. It does not check what the
value *is*, and `AnalysisDocument.filters` is `dict[str, Any]`, so a stored
filter can be an array, an object, a null, or a fractional number. Probed
against `validate_document` as it stands:

```
ACCEPTED  {"geo_id": ["01", "02"]}
ACCEPTED  {"geo_id": {"nested": "object"}}
ACCEPTED  {"year_from": 2020.7}
ACCEPTED  {"state_fips": 6}
refused   {"state_fips": null} -> filter 'state_fips' must be at most 2 characters
refused   {"year_from": true}  -> filter 'year_from' must be at least 1700
```

And the live route's ground truth, read off `/api/v1/observations` against a
real warehouse:

```
year_from=2020.7  -> 422 int_parsing
year_from=2020.0  -> passes validation
year_from=true    -> 422 int_parsing
year_from=        -> 422 int_parsing
state_fips=6      -> passes validation
```

Three distinct faults fall out of that pair:

1. **`year_from: 2020.7` is stored and reported valid, and the route answers
   it 422.** `rejection` coerces with `int()`, which truncates, so the bound
   was checked against 2020 — a year the caller never wrote. This is exactly
   the failure API-091 exists to prevent, one type down.
2. **A null is refused or accepted depending on the width of the bound.**
   `len(str(None))` is 4, so `state_fips` (max 2) and `county_fips` (max 3)
   refuse it while `geo_id` (max 200) stores it. The same value, the same
   question, two answers, decided by an accident.
3. **An array or an object is stored and reported valid.** The route would
   not 422 the serialized form — it would answer a request for the literal
   text `['01', '02']`, finding nothing — and that is worse than a refusal:
   `filters` maps a parameter name to *the* value, these parameters are
   single-valued, and a document saying two cannot be replayed as what it
   says.

The reasons the two refusals give are also wrong. A null `state_fips` is not
"at most 2 characters" away from valid, and `year_from: true` is not a year
below 1700 — it is `int(True) == 1`. An owner reopening a configuration is
told to shorten a value that has no length and to raise a year they never
wrote.

## Acceptance criteria

1. `FilterBound.rejection` states what a value must *be* before measuring it,
   and refuses nothing the live route accepts. Ground truth above:
   `state_fips: 6` and `year_from: "2020.0"` stay accepted.

   **Still true of this plan's rule; no longer true of the route.** API-123
   later gave `state_fips` and `county_fips` a closed *shape* — two and three
   digits — on the live route and in the validator alike, so `state_fips: 6`
   is refused now. `FilterBound.rejection` still accepts it; what refuses it
   is the closed-shape check beside it. The criterion held when it was
   written and the behaviour it names has been superseded rather than broken:
   `docs/reference/API_CONSUMER_GUIDE.md:711-713` states the refusal, and
   `tests/unit/api/test_saved_analysis.py:1171-1173` moved that case from the
   accepted parametrisation to the refused one.
2. An integer filter accepts a whole number, including an integral float or
   its text (`2020`, `"2020"`, `2020.0`, `"2020.0"`), and refuses a
   fractional one, a boolean, a null, and text that is not a number — each
   with a reason naming what is wrong.
3. A string filter accepts any single scalar, and refuses a null, an array,
   and an object.
4. Every filter any source declares has a bound, asserted rather than true by
   coincidence: a filter added to a dispatch with no entry in
   `OBSERVATION_FILTER_BOUNDS` reopens API-091 for that filter silently,
   because the validator skips a name it has no bound for.
5. The live route's own parameter validation is unchanged — this is the
   stored-document path catching up to it, not a new bound.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-105).

## Non-goals

- Typing `AnalysisDocument.filters` per source. The filter set is registry-
  derived and varies by source; a per-source schema would be a second
  declaration of the registry, which is the thing API-076 and API-091 both
  moved away from.
- Refusing an empty string. `?geo_id=` is a request the route accepts, so
  storing `""` must stay accepted.

## Validation

**Failing first**, nine nodes, in both directions:

```
FAILED …must_be_one_value_the_route_could_receive[{"year_from": 2020.7}]
FAILED …must_be_one_value_the_route_could_receive[{"year_to": "2020.7"}]
FAILED …must_be_one_value_the_route_could_receive[{"year_from": None}]
FAILED …must_be_one_value_the_route_could_receive[{"geo_id": None}]
FAILED …must_be_one_value_the_route_could_receive[{"state_fips": None}]
FAILED …must_be_one_value_the_route_could_receive[{"geo_id": ["01","02"]}]
FAILED …must_be_one_value_the_route_could_receive[{"geo_id": {"nested":…}}]
FAILED …must_be_one_value_the_route_could_receive[{"county_fips": []}]
FAILED …a_value_the_route_accepts_is_still_stored[{"year_from": "2020.0"}]
```

The last one is the direction worth naming: `?year_from=2020.0` passes the
live route's validation, and the store refused it, because `int("2020.0")`
raises. So the coercion was both too loose (`2020.7` truncated to a year the
caller never wrote, then measured against the bound) and too tight (an
integral float in text refused for a request that works). Both are the same
mistake: `int()` is not what the route does.

**Ground truth, not assumption.** The accepted set is read off
`/api/v1/observations` against the real warehouse — `year_from=2020.7` → 422
`int_parsing`, `year_from=2020.0` → passes, `year_from=true` → 422,
`year_from=` → 422, `state_fips=6` → passes — and
`test_a_value_the_route_accepts_is_still_stored` pins each of those as still
stored. Without that half, "state the shape" is an invitation to refuse
things the API serves, which is the mistake the saved-analysis grain
investigation earlier on this branch declined to make.

**The null was the clearest symptom.** `len(str(None))` is 4, so `state_fips`
(bound 2) refused it and `geo_id` (bound 200) stored it — the same question
answered two ways by the width of a bound that has nothing to do with it. And
the reasons were wrong where the verdict was right: a null `state_fips` was
"at most 2 characters" away from valid, and `year_from: true` was a year
below 1700, because `int(True)` is 1.

**The coincidence is now asserted.** `test_every_filter_a_source_declares_has_a_bound`
holds `OBSERVATION_FILTER_BOUNDS` equal to the union of every dispatch's
declared filters. (As written it also seeded the two analysis-universal
names; API-117 removed that seeding, so the test reads the dispatch
declarations alone — `tests/unit/api/test_saved_analysis.py:1248-1254`.) It
passes today — the two sets already agree — but `_require_declared_filters` `continue`s past a
name it has no bound for, so a filter added to a dispatch without an entry
would have reopened API-091 for that filter with nothing failing.

**Documented.** The consumer guide's saved-analysis section now states that
`filters` maps a name to one value and which shapes are refused. As written
it also said `state_fips: 6` and `state_fips: "06"` are both accepted because
`?state_fips=6` is; API-123 closed that shape on both sides afterwards, and
the guide now says the opposite at `API_CONSUMER_GUIDE.md:711-713` — refused
here exactly as it is refused there. The rule this plan delivered is
unchanged: storage refuses what the live route refuses, whatever that is.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | **1488 passed** (was 1470) |
| Unit, this file | `pytest tests/unit/api/test_saved_analysis.py` | 55 passed (was 46) |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 140 passed, 2 skipped |
| Lint / format | `ruff format --check .`, `ruff check .` | clean, 442 files |

Evidence packets inherit this for free: `validate_packet` calls
`validate_document`, so an analytical block's own query is held to the same
shape.

**Register.** 356 rows.

## Remaining work

- None.
