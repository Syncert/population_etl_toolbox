---
id: a-served-row-names-the-catalogs-code
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest tests/integration/api/test_catalog_serving_agreement.py -m "integration and database"
  - pytest tests/unit/api
---

# A served row names the catalog's metric code

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/observations_service.py`

## Context

Read off the live API against a real warehouse, with one Census PEP metric
published through the real gold refresh and the real harvest:

```
catalog code : CENSUS_PEP:AGREEMENT_14A5A8EE
row code     : CENSUS_PEP:pep_agreement_test:AGREEMENT_14A5A8EE   (same row)

GET /catalog/metrics/CENSUS_PEP:pep_agreement_test:AGREEMENT_14A5A8EE  -> 404
GET /catalog/metrics?q=CENSUS_PEP:pep_agreement_test:AGREEMENT_14A5A8EE
      -> {"total": 0, "items": []}
GET /observations?metric_code=CENSUS_PEP:pep_agreement_test:AGREEMENT_14A5A8EE
      -> 404 {"detail": "metric_code not found"}
```

`/pep/observations/latest` answers a row whose own `metric_code` no other
resource in the API recognises. A consumer that reads rows and carries each
row's identity forward — the obvious thing to do — holds a code that 404s on
the catalog, 404s on `/observations`, and that API-105's stored-document
validation refuses as "not a published metric".

**This is one source, and the guide already states the rule it breaks.** The
source-scoped column list selects the relation's `metric_code` verbatim:

```python
        metric_code,
        metric_display_name,
```

For BLS, Census ACS and FRED that column *is* the catalog's code — proved by
the same probe: an ACS row reports its catalog code exactly. Census PEP's
serving relation composes its own identity from the dataset, which is why the
route's match condition has to accept both spellings at all
(`SPLIT_PART(metric_code, ':', 3) = :metric_key`). So the composed form is an
implementation detail of one relation, and the row publishes it as identity.

The guide's own identity section settles what should happen. It is explicit
that a provider's own key travels beside the catalog's code rather than as
it:

> The BLS series id is still on every row, under `dimensions.series_id`, so
> lineage back to the provider's series is never lost.

And the source-scoped shape already publishes the composed code's only extra
component under its own name: every PEP row carries `dataset_code` and
`dataset`.

**One claim in this section was wrong, and the end-to-end tier proved it.**
It read "the composed form adds nothing the row does not already say". As an
*address* it adds a great deal: the catalog publishes `CENSUS_PEP:POPESTIMATE`
across two datasets (`pep_nst_alldata` and `pep_subcounty`), and the composed
spelling selects one of them. See Validation.

## Acceptance criteria

1. A source-scoped row's `metric_code` is the code the catalog publishes, for
   every serving contract — including the one whose relation composes its
   own.
2. The relabel happens where the request named a catalog metric. A request
   that names the relation's composed spelling answers **exactly what it
   answered before**, labels included: that spelling is a narrower address,
   and relabelling it would have meant resolving it, which widens the match.
   (This criterion replaced a wrong one. See Validation.)
3. Nothing about which requests are accepted changes. All three source-scoped
   routes answer an empty page for an unknown code today and `/observations`
   404s; that split is out of scope and is left exactly as it is.
4. No row ever reports a null identity. Where no catalog row can be resolved
   at all, the relation's own column stands, as today.
5. The contracts whose relation already stores the catalog code are
   byte-identical.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-108).

## Non-goals

- Making the composed code a 404. That would be a new acceptance rule for
  the whole source-scoped family, which answers an empty page for anything
  unknown; changing it is a separate decision.
- Changing the serving relation. The composed identity is how `gold_pep`
  keys its rows and the match condition already reconciles it.

## Validation

**Found by reading answers off a live API** over a real warehouse with a
Census PEP metric published through the real gold refresh and the real
harvest. `/pep/observations/latest` answered a row whose `metric_code` is 404
on `/catalog/metrics/{metric_code}`, 0 results on `/catalog/metrics?q=`, and
404 on `/observations` — while an ACS row, probed the same way, reports its
catalog code exactly. That contrast is what made it one contract's defect
rather than a design.

**Failing first**, two integration nodes against the real warehouse.

**Then the end-to-end tier refused the fix, and it was right.**

The first implementation went one step further than the defect required: when
the request did not resolve to a catalog metric, it resolved the request's own
third segment as a lineage key, so that a caller holding the composed spelling
would also be answered with the catalog's code. That reads well and is wrong.
`_metric_identity`'s `metric_key` is what the *match condition* binds, so
resolving the composed spelling bound the key too — and the key matches
`SPLIT_PART(metric_code, ':', 3)`, which every dataset publishing that
measure satisfies:

```
assert replayed.json() == latest_payload
E   {'total': 12} != {'total': 6}
E   'dataset': 'pep_subcounty'  !=  'dataset': 'pep_nst_alldata'
```

`/pep/observations/latest?metric_code=CENSUS_PEP:pep_nst_alldata:POPESTIMATE`
answered six rows and began answering twelve. The plan's own acceptance
criterion 3 said nothing about which requests are accepted may change, and
this changed what one *returns*.

So the premise was corrected rather than the test: the composed spelling is a
**narrower address** — one dataset's rows of a measure the catalog publishes
across several — which is exactly why the e2e fixture uses two of them
(`POPULATION_METRIC` and `PLACE_METRIC` differ only in their dataset
segment). The relabel now happens only where the request named a catalog
metric; `COALESCE` leaves the relation's own column standing otherwise, which
is byte-identical to the old behaviour. The over-reaching node was replaced
by one that pins the narrowness: asked by the composed spelling, the answer
still holds one dataset and no more rows than the catalog code's own answer.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1488 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 144 passed, 2 skipped (was 142) |
| Integration, this file | `pytest …/test_catalog_serving_agreement.py` | 15 passed (was 13) |
| End-to-end | `pytest tests/e2e -m e2e`, fresh `e2e_fresh_test` | 9 passed |
| Lint / format | `ruff format --check .`, `ruff check .` | clean |

One pinned unit param dict gained `catalog_metric_code`; the fake glossary
answers the requested code, so both bound values are the same string there.

**Register.** 364 rows.

## Remaining work

- None.
