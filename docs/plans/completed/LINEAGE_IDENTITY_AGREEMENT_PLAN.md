---
id: lineage-identity-agreement
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - source-route-code-sweep
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/shared -q
---

# The publisher's lineage and the registry's identity strategy agree

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row ARC-007.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/unit/shared/test_catalog_serving_identity.py`

## Context

A metric's serving rows are found through `physical_lineage`, which each
source's `metric_publisher` view builds and the glossary carries into the
catalog. The neutral resource reads it two ways, both declared in the
registry:

- `identity_columns` — CDC, FBI UCR, USDA NASS — where each declared column
  name must also be a key in the published lineage, because the service binds
  `lineage.get(field)` to `field = :…` on the serving relation.
- `lineage_key_column` — Census ACS, Census PEP — where the lineage must
  publish a `key`.

And every read first requires the lineage's declared `schema`/`relation` to
equal the registry's, "so a publication/registry disagreement fails loudly
instead of reading the wrong rows".

Fails loudly means a sanitized **503 on every request for that source**. The
API is right to refuse rather than read whichever rows happen to match; what
is missing is anything that notices before a deployment does.

All seven sources agree today — checked by hand against the view definitions
in a bootstrapped warehouse while investigating DB-030. Nothing asserts it.
ARC-005 attributes composed `metric_code` prefixes to their publishers and
forbids a rewriting dispatch prefix, which is the identity's *front* half; the
lineage is the half that finds the rows, and it is unguarded. DB-030 is the
recent demonstration of what an unguarded pairing costs: an entire route
family unanswerable for one source, past two guards that each covered a
neighbouring concern.

## Acceptance criteria

1. For every publishing schema with a reviewed dispatch entry, the `schema`
   and `relation` its `metric_publisher` declares in `physical_lineage` equal
   the ones that dispatch entry declares.
2. A dispatch entry identifying rows by `identity_columns` requires its
   publisher to declare a lineage key of each of those names.
3. A dispatch entry identifying rows by a lineage key requires its publisher
   to declare `key`.
4. The check is static and source-agnostic: it reads the repository's own SQL
   and the registry, so a source added later is checked without an edit, and
   it needs no database.
5. A publisher whose schema has no dispatch entry is not failed for it — the
   registry is the list of sources the API serves, not of schemas that exist.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Checking that the named relation *exists* or has those columns. That needs
  a warehouse, and the integration tier already reaches it.
- Changing any publisher or dispatch entry: they agree. This is the guard
  that keeps them agreeing.

## What was built

Three static checks in `test_catalog_serving_identity.py`, beside ARC-005's,
reusing its `_publisher_bodies()` so the SQL is read the way that contract
already reads it.

`_declared_lineage` pulls the `jsonb_build_object(...) AS physical_lineage`
out of a publisher body and returns its `schema`/`relation` literals and the
set of key names it declares. `_dispatch_by_schema` pairs each publishing
schema with its reviewed dispatch entry, where one exists.

Then: the declared `schema`/`relation` must equal the registry's; the lineage
must carry every key the entry binds (each `identity_columns` name, or `key`
for a lineage-key source); and a schema with no dispatch entry is out of scope
rather than a failure — the registry lists what the API serves, not what
exists, and `get_metric_capability` already says exactly that about such a
source.

The third test also stops the first two from being vacuous: it asserts that
every dispatch entry whose source publishes through a `metric_publisher` view
is actually judged, and that at least four are.

All seven sources agree today. This changes no publisher and no dispatch
entry; it is the guard that keeps them agreeing.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| This file | `pytest tests/unit/shared/test_catalog_serving_identity.py -q` | 8 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1442 passed |
| Register | `python -m tests.support.catalog_evidence` | 334 rows; ARC-007 is `FULL` |
| Lint | `ruff check .` | clean |
| Format | `ruff format --check .` | 440 files already formatted |

A guard that passes on the first run has to be shown to fail on a real
disagreement, or it proves nothing. Drifting the CDC dispatch entry two ways
at once — `lineage_relation` to `health_observation_DRIFTED` and an extra
`drifted_column` in `identity_columns` — fails both checks, each naming the
file, the schema, what the publisher publishes and what the registry declares:

```
018_geo_grain_vocabulary.sql: gold_cdc.metric_publisher publishes lineage
gold_cdc.health_observation but the dispatch entry for CDC declares
gold_cdc.health_observation_DRIFTED

018_geo_grain_vocabulary.sql: gold_cdc.metric_publisher publishes lineage keys
['asset_id', 'measure_id', 'value_type_id'], which do not include
['drifted_column'] that the dispatch entry for CDC binds
```

The extractor was also checked directly against all seven publishers, so the
comparison is between real values rather than two empty sets:

| Schema | Lineage | Keys |
|---|---|---|
| `gold_bls` | `gold_bls.fact_bls_observation` | `key` |
| `gold_cdc` | `gold_cdc.health_observation` | `asset_id`, `measure_id`, `value_type_id` |
| `gold_census` | `gold_census.fact_acs_observation` | `key` |
| `gold_fbi` | `gold_fbi.crime_observation` | `measure_id`, `product_id` |
| `gold_fred` | `gold_fred.fact_fred_observation` | `key` |
| `gold_nass` | `gold_nass.crop_observation` | `product_id`, `statistic_sk`, `statisticcat_desc`, `unit_desc` |
| `gold_pep` | `gold_pep.population_estimate_revision` | `key` |

Not run, and why: nothing here needs a database — that is the point of the
row. The integration tier already reaches whether the named relation exists
and carries those columns.

## Acceptance criteria, as delivered

1. **Met.** `test_every_publisher_declares_the_lineage_its_dispatch_entry_reads`.
2. **Met.** `test_every_publisher_declares_the_identity_its_dispatch_entry_binds`.
3. **Met.** Same test, `{"key"}` branch.
4. **Met.** Static, and derived from `_publisher_source_codes()` and the
   registry rather than any list written here.
5. **Met.** `test_a_schema_without_a_dispatch_entry_is_not_judged`, which also
   pins that the other two are not vacuous.
6. **Met.** `ARC-007` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `etl-unit` CI job like every other ARC row, with `AUDITED_COUNTS["ARC"]`
   raised to 7.

## Remaining work

- None.
