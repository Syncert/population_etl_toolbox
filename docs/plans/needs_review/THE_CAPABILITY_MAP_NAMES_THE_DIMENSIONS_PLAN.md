---
id: the-capability-map-names-the-dimensions
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest tests/unit/api
  - pytest tests/integration/api -m "integration and database"
---

# The capability map names the fields a neutral row's `dimensions` carries

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/schemas/catalog.py`,
  `apps/api/services/catalog_service.py`

## Context

The guide's "Reading a row honestly" section opens:

> Each row carries typed core fields plus **everything the source publishes**

and its `dimensions` bullet gestures at four examples:

> `dimensions` carries the source's own published fields under their own
> names (CDC strata and footnotes, FBI subject/offense/program, NASS
> commodity/domain/practice, Census dataset and vintage).

`NeutralObservation`'s own docstring says the same thing: "everything a
source publishes beyond them rides in `dimensions` under the source's own
published field names."

It is a **reviewed subset**, and the gap between the claim and the set is
large. `gold_nass.latest_release_observation` has 53 columns; the USDA NASS
dispatch declares 14 dimensions. Among the rest are surrogate keys and slice
bookkeeping that obviously should not ride there — `observation_sk`,
`geo_sk`, `slice_key`, `slice_mode` — and that is the point: the set *is* a
review, and it is right to be one. What is wrong is calling it everything.

The practical problem is not the overstatement, though. It is that the set is
**registry-derived and unpublished**. `/catalog/capabilities` already answers
`observation_filters` per source, from `dispatch.supported_filters()`, and
says exactly why:

> `observation_filters` is the contract for per-source filtering … Read
> capabilities once at startup rather than guessing.

There is no equivalent for `dimensions`. A consumer coding against
`dimensions` has the guide's four parentheses and nothing else — so they read
a row, see what is in it, and hard-code that. A field the registry adds later
reaches the rows and no client knows to look for it, and a field it drops
breaks a client with nothing having said it was a contract.

## Acceptance criteria

1. `/catalog/capabilities` publishes, per source, the field names a neutral
   row's `dimensions` carries, derived from the same registry entry the rows
   are built from — never a second list.
2. A served row's `dimensions` keys agree with what the capability map
   declares for its source, asserted against a real warehouse.
3. The guide stops claiming the row carries everything the source publishes,
   says the declared set is the contract, and points a consumer who needs the
   relation's full shape at the source-scoped routes, which is what they are
   for. `NeutralObservation`'s docstring says the same.
4. The reviewed OpenAPI snapshot is regenerated deliberately, as a contract
   change, and the diff is read.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-109).

## Non-goals

- Widening `dimensions`. Which fields ride there is a review, and this plan
  publishes that review rather than second-guessing it.
- Publishing the relation's column list. The source-scoped routes serve the
  relation's own shape already; the neutral envelope is deliberately a
  projection.

## Validation

**What each source declares**, printed from the registry after wiring it:

```
BLS           2  seasonal_adjustment_status, series_id
CDC          14  adjustment_status, asset_id, dataset_title, estimate_method, …
CENSUS_ACS    2  dataset_code, variable_code
CENSUS_PEP    4  dataset_code, product_code, summary_level, value_source
FBI_UCR      12  counted_entity_basis, geography_basis, max_data_month, …
FRED          2  seasonal_adjustment_status, series_id
USDA_NASS    14  class_desc, commodity_desc, county_fips, domain_desc, …
```

**The agreement is asserted against real rows, not a fixture's shape.** The
node reads `/catalog/capabilities` and then three published metrics — ACS,
Census PEP and CDC — and requires each row's `dimensions` key set to *equal*
the declared set for its source. Floors (`CDC >= 10`, `USDA_NASS >= 10`)
stop an empty or collapsed declaration from passing by agreeing with an
empty row.

It passes on the first run, and that is the point: the served set already was
the registry's: what was missing is that nothing published it, so a consumer
had to infer the shape from a row they happened to read. The node is the
guard that keeps the published contract and the served row from drifting now
that there is a contract to drift from.

**The contract change was read, not just regenerated.** The snapshot diff is
one line:

```
+        "observation_dimensions": "array<string>",
```

Purely additive, on `SourceCapability`. The cache key rotates with the
contract fingerprint, which is what that fingerprint is for.

**Also declared in the web client's type.** `SourceCapability` has an index
signature, so the field would have typechecked unread — which is exactly how
WEB-057's published bin bounds stayed invisible. Declared rather than left to
the index signature.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1488 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | **145 passed**, 2 skipped (was 144) |
| Frontend units | `npm --prefix apps/web run test:unit` | 320 passed |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

**Register.** 365 rows.

## Remaining work

- None. One note: the explorer builds its dimension columns from the rows it
  loaded rather than from this declaration. Reading it instead would let a
  declared dimension a page happens not to publish still appear as a column
  — a smaller version of what WEB-051 fixed for coverage — but that is a
  frontend change with its own review, not part of publishing the contract.
