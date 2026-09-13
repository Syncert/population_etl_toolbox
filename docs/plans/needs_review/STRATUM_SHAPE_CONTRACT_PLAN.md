---
id: stratum-shape-contract
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - cdc-sweep-fixture
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/integration/api -m "integration and database and not slow" -q
  - python -m pytest tests/unit -q
---

# The warehouse refuses a stratum shape the API cannot serve

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `sql/migrations/`, `silver_cdc.dim_stratum`,
  `silver_cdc.observation_revision`

## Context

`/api/v1/cdc/observations` publishes a stratum with every value: the
response model declares `strata: list[Any]`, and the CDC parser produces one,
a tuple of `(category, category_label, value, value_label)` tuples that
psycopg2 stores as a JSON array of arrays.

Nothing says so anywhere the database can check. `silver_cdc.dim_stratum.strata`
and `silver_cdc.observation_revision.strata` are `jsonb NOT NULL` with no
shape constraint, and `jsonb` accepts an object, a string, a number, or
`null` just as happily as an array. The API has no guard either: the service
hands each row straight to `CdcObservation.model_validate`.

So a stratum stored as a JSON object is accepted by every write path and then
crashes the read path:

```
GET /api/v1/cdc/observations?limit=5  ->  500
pydantic_core.ValidationError: 1 validation error for CdcObservation
strata
  Input should be a valid list [type=list_type, input_value={'overall': 'overall'}]
```

That is an unhandled exception, not a refusal: the caller gets
`{"detail": "The API failed to complete this request."}` with no way to tell
which row is unserveable, and the row stays in the warehouse answering 500
for every page that includes it.

This was found by writing one — the DB-032 fixture seeded
`{"overall": "overall"}`, a shape no parser produces, and every layer between
the insert and the response model accepted it. The fixture is wrong and is
fixed here; the reason it got that far is the defect.

The neighbouring jsonb column shows what the guard should look like. The
serving registry reads `physical_lineage` through
`_lineage_of`, which checks `isinstance(lineage, Mapping)` before trusting it,
and `_require_lineage_agreement` then fails loudly when the publication and
the registry disagree. `strata` has neither.

## Acceptance criteria

1. A stratum whose `strata` is not a JSON array is refused by the database,
   on both relations that store one, at the moment it is written.
2. The refusal names the constraint rather than surfacing later as a 500.
3. The migration is rerun-safe, is in the bootstrap manifest, and is
   described in `sql/migrations/README.md` (DB-029).
4. The DB-032 fixture seeds the shape the parser produces, and the CDC
   source-explorer route serves the row it seeds — so the fixture's realism
   is load-bearing rather than asserted.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Coercing a bad shape in the API. An empty list where a stratum belongs is
  exactly the "missing presented as a value" this repository refuses; the
  write boundary is where the shape is decided.
- Constraining `stratum_id` to be the digest of `strata`. It is one in
  production (`_stratum_id` hashes the canonical JSON), and proving it in SQL
  means reimplementing `json.dumps(separators=(",", ":"))` in the database.
  That is its own work with its own failure modes.
- The other jsonb columns. `physical_lineage` is already read defensively and
  checked against the registry; `source_record` and `registered_years` are
  not projected into a typed API field.

## Validation

Run against a local PostgreSQL 16.13 + PostGIS 3.4 (`population_etl_test`).

**The defect, observed.** Before the constraint, with a stratum stored as
`{"overall": "overall"}`:

```
GET /api/v1/cdc/observations?limit=5            -> 500
{"detail": "The API failed to complete this request."}

apps/api/services/cdc_service.py:66
    items=[CdcObservation.model_validate(dict(row)) for row in rows]
pydantic_core.ValidationError: 1 validation error for CdcObservation
strata
  Input should be a valid list [type=list_type, input_value={'overall': 'overall'}]
```

Nothing between the insert and the response model saw it: the write
succeeded, the silver row published, the harvest wrote a catalog row, and
`/api/v1/observations` served the same observation without complaint —
`strata` is a `dimensions` entry there, not a typed field.

**Failing first.** `test_a_stratum_that_is_not_a_json_array_cannot_be_written`
against the pre-migration schema: `Failed: DID NOT RAISE
<class 'psycopg2.errors.CheckViolation'>` on the first of five shapes
(`{"overall": "overall"}`, `"overall"`, `12`, `null`, `true`). After the
migration, all five are refused on both relations, and
`[["OVERALL","Overall","OVR","Overall"]]` is accepted — so the guard refuses a
wrong shape, not the column's vocabulary.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1442 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 130 passed, 2 skipped |
| Lint | `ruff format --check .`, `ruff check .` | clean |

**Register.** 336 rows; `test_repository_hygiene`, `test_catalog_evidence`,
`test_warehouse_manifest` pass — the last one needed the compose mount as well
as the manifest entry and the README paragraph, which is DB-002 and DB-029
doing their jobs.

**Bootstrap.** The integration harness reapplies the manifest at session
start, which is how this was confirmed rather than assumed: dropping the
constraint by hand and re-running put it straight back, and the fixture's old
object-shaped stratum was then refused at the insert.

### The fixture this came from

DB-032's CDC fixture seeded `{"overall": "overall"}` — a shape no parser
produces. It is corrected here to the parser's own
`[[category, category_label, value, value_label]]`, and DB-033's API test
fetches the seeded row back through `/api/v1/cdc/observations`, so the
fixture's realism is now load-bearing instead of asserted. A fixture is only
evidence while the rows it seeds are rows the warehouse could hold.

## Remaining work

- None.
