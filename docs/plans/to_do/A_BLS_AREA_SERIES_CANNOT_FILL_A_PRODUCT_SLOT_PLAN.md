---
id: bls-area-series-product-identity
branch: claude/bls-area-series-product-identity
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - python -m pytest tests/unit/api -q
  - ruff format --check . ; ruff check .
---

# A BLS area series cannot fill a product slot

## Plan status

- **Status:** Unclaimed. Authored 2026-09-18 from a finding made while
  implementing `second-wave-product-templates`, which required candidate codes
  to be verified against a live catalog rather than guessed. The first wave's
  were not, and one of them cannot resolve anywhere.
- **Last updated:** 2026-09-18
- **Current milestone:** not started.

## Why

`apps/web/lib/productTemplates.ts` gives the community-conditions product an
`unemployment-rate` slot whose only candidate is `BLS:LAU:UNEMP_RATE`.

**No warehouse can publish that code.** The glossary harvest builds a metric
code as `source_code || ':' || source_object_key`
(`glossary/harvest.py`), and `gold_bls.metric_publisher` sets
`source_object_key` to `series.series_id`
(`bls/gold_bls/DDL/publisher.sql:79`). A BLS metric code is therefore always
`BLS:<series_id>` — `BLS:LAUCN120010000000003` — and never
`BLS:<program>:<measure>`. The slot has reported a gap since it was written
and will report one forever, so "Unemployment rate" is permanently absent from
the product that names it.

**It is not fixable by choosing a better code.** Measured on the deployed
warehouse: 12,900 distinct `BLS:LAUCN%` metric codes, each serving exactly one
geography. A LAU series *is* an area. A template slot names one code and is
read for whatever place the reader chose, so any single BLS LAU candidate
answers for one county and reports a gap everywhere else — which is a worse
failure than the current one, because it looks like it works.

**The browser tier cannot see it**, and that is why it survived.
`tests/frontend/browser/profiles.spec.js` mocks a catalog containing
`BLS:LAU:UNEMP_RATE`, so the slot resolves in the test and gaps in production.
The mock was written to match the template rather than the warehouse.

This is an upstream contract question, not a client edit, which is why it is a
plan rather than a fix: `AGENTS.md` requires the upstream contract be resolved
first, and `WEB_FIRST_WAVE_HANDOFF.md` forbids the client from authoring a
measure the API does not serve.

## The decision to take first

A product slot needs an identity that is stable across places. BLS LAU is not
one. Three ways out, and the plan should not start until one is chosen:

1. **The API resolves an area series for a place.** A caller asks for "BLS
   local unemployment rate" for a geography and the API returns the series
   that covers it. This is the option that makes the product work as written,
   and it is an API contract change with its own semantics to define — what
   happens for a place BLS does not cover, and which of the several LAU
   measures (rate, level, labor force) the identity means.
2. **The warehouse publishes a geography-independent measure identity**
   alongside the per-area series, so `BLS:LAU:UNEMP_RATE` becomes real and
   resolves per geography the way an ACS variable does. This is the largest
   change and the one that makes BLS behave like the other sources.
3. **The product stops claiming it.** The slot is removed or replaced with
   `CENSUS_ACS:acs5:B23025_005` (unemployed, civilian labor force), which
   publishes one code for every geography. This is honest and cheap, and it
   changes what the product says: an ACS count is not a BLS rate, and the
   community-conditions summary would have to stop implying otherwise.

Option 3 is a client edit and needs no upstream work; 1 and 2 are API and
warehouse work respectively. The second-wave products already took option 3
for their own labour slots and say so in the slot note, so there is precedent
for it being acceptable — but only as a deliberate choice, not as a silent
substitution.

## Deliverables

### 1. The decision, recorded

Whichever option is taken, written down where the next reader meets it: an
ADR if the API or warehouse contract changes, or the template's own note if
the product stops claiming a BLS rate.

### 2. The slot stops lying

`community-conditions` no longer carries a candidate that cannot resolve.

### 3. A candidate that cannot resolve fails a test

The unit tier gains a check that every candidate in every template matches a
shape some publisher can emit. It cannot assert presence — a template is not
rewritten per deployment, and a code for a source a deployment lacks must
still be allowed to report its gap — but it can assert that a code is
*publishable*: that its source prefix is a registered source and that its key
shape matches what that source's publisher builds.

### 4. The browser mock stops inventing identities

`profiles.spec.js` mocks the catalog. Any metric code it serves must be one a
publisher could emit, so a template and its test cannot agree with each other
and disagree with every warehouse.

## Acceptance criteria

- [ ] No template carries a candidate whose source prefix and key shape no
      publisher can emit, asserted in the unit tier and proven failing-first
      by restoring `BLS:LAU:UNEMP_RATE`.
- [ ] The browser tier's mocked catalog contains no identity a publisher
      could not emit.
- [ ] The community-conditions product either serves a real BLS identity or
      no longer claims to, with the decision recorded.
- [ ] If option 1 or 2 is taken, the API contract change is documented in
      `API_CONSUMER_GUIDE.md` and `TESTING_CONTRACT.md` gains its row.

## What this plan deliberately does not do

- It does not decide between the three options. That is a product and
  contract decision, and making it inside an implementation plan is how the
  original defect happened.
- It does not touch the second-wave products. They take their labour
  measures from ACS tables that publish one code per geography, and
  `test_no_second_wave_slot_names_a_bls_area_series` holds them there.
