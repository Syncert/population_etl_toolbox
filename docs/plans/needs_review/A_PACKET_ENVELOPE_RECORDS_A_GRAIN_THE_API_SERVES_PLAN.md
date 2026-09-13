---
id: a-packet-envelope-records-a-grain-the-api-serves
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-grain-that-is-not-one-is-refused, storage-is-not-a-back-door-for-a-refused-value]
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
---

# A packet's envelope cannot record a grain the API does not serve

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **The third and last surface in the closed-grain family:
  API-122 closed the request, API-123 the stored query, this the stored
  envelope.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/evidence_packet_service.py`

## Context

Found by probing the evidence-packet write path with the module's own storage
double. `ReproducibilityEnvelope`'s docstring names exactly which of its
fields are request parameters rather than composer observations:

> Only the fields that duplicate the block's query are cross-checked against
> it: `metric_codes`, `scope`, `release`, the two reductions …, and the
> geography (`geo_id`, `geo_level`) when the query filters to one.

`geo_level` is therefore held here as a request parameter, and the API
refuses a `geo_level` outside `registry.GEO_GRAINS` on every route
(API-122) and inside a stored analysis document (API-123). The envelope was
the one place left, because the only rule it had was a *comparison*:

```python
if envelope.geo_level and queried_grain and normalize(…) != normalize(…):
    return "records geography grain X but its query asks for Y"
```

Both sides must name one for that to fire. A block whose query names no
grain — which is the ordinary shape for a national series, and for any block
whose geography comes from `geo_id` alone — had nothing to differ from:

```text
POST /api/v1/evidence-packets
  blocks[0].envelope.geo_level = "COUNTRY"
  blocks[0].document.filters   = {}
  -> 201, validation {"valid": true, "blocks": [{"valid": true}]}
```

`EvidenceEnvelope` renders that word to a reader as the block's **Geography**,
`packetExport` writes it into the CSV the packet is handed over as, and
nothing on either surface could tell the reader that `COUNTRY` is not a grain
this API serves. A packet is the one artifact in this application whose whole
purpose is being read by somebody who was not there when it was composed,
which is what makes the identity fields worth more here than anywhere else —
the module's own opening rule is that a block must never display one
identity over another's numbers.

## What is wrong

The envelope's grain was validated only against the block's query, so a query
that named no grain left it unvalidated — a closed vocabulary with a
comparison in place of a rule.

## What was changed

`_contradiction` refuses an envelope `geo_level` that is not a grain, through
the same `registry.grain_refusal` the routes and saved-analysis storage use,
before the comparison runs — so the block is told what is wrong with the word
rather than which other word it differs from. Because `_contradiction` is
also the read path's first check (the shape API-120 established), a packet
stored before the rule reads back unmodified with `validation.valid = false`
and the block named, rather than being repaired or hidden.

`geo_id` is deliberately left alone, for the reason `registry` already
records: its shape is source-dependent (`us:1`, `state:NN|place:NNNNN`,
`agency:<ORI>` whose tail is a provider string), so a shape rule here would
be a second declaration of something the reference layer owns.

## Validation

- `test_contradictions_are_refused_at_write_naming_the_block`
  [`envelope-records-a-grain-that-is-not-a-grain`]
- `test_every_grain_the_api_serves_is_a_grain_a_packet_may_record` — read
  from `GEO_GRAINS` and `GEO_GRAIN_ALIASES`, so a grain added to the
  vocabulary is composable the day it is published and the words ADR-0002
  keeps answering stay composable
- `test_a_grain_that_is_not_one_is_reported_on_a_packet_stored_before_the_rule`

Proved by reverting the service change with the tests in place:

```text
E  AssertionError: {"packet_id":1,…,"envelope":{…,"geo_level":"COUNTRY",…},
   "document":{…,"filters":{}},…,"validation":{"valid":true,"blocks":
   [{"block_id":"unemployment","valid":true,"reason":null,"missing":[]}]}}
E  assert 201 == 422
E  assert True is False
```

## Deliberately not done

- **No web change.** The composer builds an envelope from the explorer's own
  state, so its grain always comes from the application's picker; the
  reachable paths are a direct API write and a row stored before the rule.
  Both are answered by the server's `validation`, which the packet surfaces
  already carry into the status pill and the CSV export.
