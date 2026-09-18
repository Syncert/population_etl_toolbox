---
id: second-wave-product-templates
branch: claude/second-wave-product-templates
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run test:browser
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
---

# The second wave of packaged products

## Plan status

- **Status:** Ready for review. Five templates authored, every candidate
  resolved against a deployed warehouse's publisher views before it was
  written down, and the resolution recorded per slot. The exercise also found
  a first-wave candidate that cannot resolve on any warehouse; it is filed as
  `bls-area-series-product-identity` rather than fixed here, because the fix
  is an upstream contract decision.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

## Why

Seven source pipelines publish, the API serves all seven — four through the
per-source serving contracts and the rest through the registry-dispatched
neutral observation resource (`apps/api/registry.py`) — and the web app reaches
every one of them generically. `docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md`
describes twenty packaged products over that data.

`apps/web/lib/productTemplates.ts` carries three: community conditions,
population growth, and workforce. With the evidence-packet builder and the
data-quality explorer, that completes the first wave the use-case document
names. The second wave — community disease and illness burden, public-safety
trend normalized by population, rural and agricultural economy profile,
housing affordability and household pressure, and aging population and
health-service planning — has nothing.

This is the cheapest value in the repository right now and the plan says so
plainly: a template is "navigation and presentation over stable API catalog
identities", it "computes nothing, combines nothing, and defines no measure of
its own", and adding one is an edit to a configuration array rather than a new
component. The warehouse work is done. The API work is done. What is missing is
the curation.

## Scope

**In scope**

1. **Five `ProductTemplate` entries** in `lib/productTemplates.ts`, one per
   second-wave use case, each with its sections, slots, candidate metric codes
   in preference order, and — required by the existing shape — a `limits`
   string stating what the product does not claim.
2. **The guardrail from the use-case table travels into the product.** Each
   row of that table names the essential guardrail; it is the source for the
   template's `limits` and for the per-slot notes. The health slots carry
   denominator, age-adjustment, suppression, and provisional status; the
   safety slots carry reporting participation and the rule that a missing
   agency report is not zero crime; the agricultural slots carry commodity
   units, survey years, and suppression.
3. **Candidate codes verified against the live catalog**, not guessed. A slot
   whose candidates the catalog does not publish must be authored knowing it
   will report a stated gap, and the plan's evidence record should say which
   slots those are.
4. **The `/profiles` surface lists them** with no per-product component.
5. **Tests** in the tier `tests/frontend/unit` already uses for the existing
   templates, plus a browser check that a second-wave profile renders, states
   its gaps, and reaches the explorer from a filled slot.

**Out of scope**

- Any new API route, and any warehouse change. If a template wants a measure
  the API cannot serve, that is an upstream finding to record, not something
  to work around in the client (`AGENTS.md`: fix the upstream contract first).
- Third-wave use cases (peer benchmarking, business location, shock and
  recovery, crime and economic context, agricultural price context).
- Any change to how a template resolves, displays, or reports a slot. The
  resolution rules are the existing ones.

## The normalization question, decided before implementation

"Public-safety trend normalized by population" is the one second-wave product
that reads like an instruction to divide one source's values by another's. The
client must not do it. `WEB_FIRST_WAVE_HANDOFF.md` forbids a client-authored
composite outright, requires that anything the API names in `derivations` be
labelled derived, and the API owns semantics. The use-case table's own
guardrail points the same way: counts, population-based rates, and reporting
participation are shown *beside* one another.

So the template presents the reported count, the population base, and the
participation coverage as separate published slots, and any rate it shows is
one a source publishes or the API derives and labels. If a genuine
population-normalized rate is wanted and nothing publishes one, that is an API
plan, filed separately. Implementing it in `productTemplates.ts` is the defect
this section exists to prevent.

## Acceptance criteria

- [x] Five templates exist, each resolving against the live catalog, each
      stating its limits, and each carrying its use-case guardrail in terms a
      non-specialist reads.
- [x] Every slot names explicit candidate codes; no slot falls back to a
      "similar" measure, and an unfillable slot reports what it looked for --
      asserted in the unit tier and seen in the browser for a gapped CDC slot.
- [x] No client-side arithmetic across sources is introduced. No score, index,
      grade, or ranking. No causal phrasing anywhere in the copy. Asserted over
      every string in every second-wave template.
- [x] Suppressed, not-reported, and missing stay distinct from each other and
      from zero, in every new slot.
- [x] `/profiles` lists all eight products with no per-product component and
      no new fetch path -- asserted in the browser against the rendered
      selector rather than against the array.
- [x] The plan's evidence record names which slots resolve today and which
      report a gap, with the query that established it. See "What resolves,
      and what does not".

## Implementation evidence

### The catalog the plan asked for was empty, and the publishers were not

Criterion 3 says the candidates are verified against the live catalog. On the
deployed warehouse `gold_glossary.dim_metric_catalog` holds **zero rows**, as
do `publisher_registry` and `publisher_harvest_state`: the glossary harvest has
never run there. So the catalog could not be read.

The publishers it is built from could. `gold_census.metric_publisher` carries
4,447 identities, `gold_bls.metric_publisher` 13,317 and
`gold_fred.metric_publisher` 24 — **17,788 published identities** — and the
harvest composes a metric code from exactly those rows as
`source_code || ':' || source_object_key` (`glossary/harvest.py:300`). Reading
the publishers is therefore reading what the catalog would contain, and it
needs no write to a production warehouse to find out.

That is how every candidate below was checked. Two facts about the deployment
follow from the same queries and are worth recording separately, because
neither is this plan's to fix:

- **The deployed warehouse serves observations from an empty catalog.** 68M ACS
  observations and 17,788 published identities, and `/catalog/metrics` would
  answer with nothing until a harvest runs.
- **It carries three of the seven registered sources.** Its schemas are
  `gold_census`, `gold_bls` and `gold_fred`; there is no `gold_pep`,
  `gold_cdc`, `gold_fbi` or `gold_nass`. Slots naming those sources report a
  gap there, which is the designed behaviour and not a defect in the template.

### What resolves, and what does not

Every candidate in the five new templates, resolved against those 17,788
published identities. **32 slots resolve, 8 report a gap**, and every gap is a
source this deployment does not carry rather than a code that is wrong:

| Product | Resolve | Gap | The gaps |
|---|---|---|---|
| `housing-affordability` | 12 | 0 | — |
| `aging-population` | 8 | 2 | `CENSUS_PEP`, `CDC` |
| `disease-illness-burden` | 5 | 2 | `CDC`, `CENSUS_PEP` |
| `public-safety-trend` | 1 | 3 | `FBI_UCR` ×2, `CENSUS_PEP` |
| `rural-agricultural-economy` | 6 | 1 | `USDA_NASS` |

`housing-affordability` resolves completely because it is built from ACS
tables and FRED series this deployment publishes; `public-safety-trend` is
almost entirely gapped because FBI UCR is not deployed here at all. Both are
correct outcomes: a template is not rewritten per deployment, and a slot that
cannot be filled states what it looked for.

Each ACS candidate was confirmed present in both `acs5` and `acs1` before
being written in that preference order, so the fallback is real rather than
decorative.

### The normalization question, held to

`public-safety-trend` is the product that reads like an instruction to divide
one source by another. It does not. The reported count, the reporting
participation, and the population base are three separate sections, so no slot
holds both sides of a ratio, and `limits` says why: a reported count and a
population estimate come from different programs with different coverage, and
their quotient would read as a crime rate nobody published. A test asserts the
three sections exist and that the count section holds only `FBI_UCR:`
candidates.

### A first-wave candidate that cannot resolve anywhere

Checking the first wave against the same publishers, to be sure the method was
sound before trusting it on the new ones, found that **`BLS:LAU:UNEMP_RATE`
resolves nowhere** — and not because BLS is undeployed. BLS *is* deployed, with
13,317 published identities. The code shape is impossible:
`gold_bls.metric_publisher` sets `source_object_key` to `series.series_id`, so
a BLS metric code is always `BLS:<series_id>`.

It cannot be repaired by choosing a better code either. There are 12,900
distinct `BLS:LAUCN%` codes on that warehouse and each serves **exactly one
geography**: a LAU series *is* an area, so any single candidate answers for one
county and gaps everywhere else.

It survived because `tests/frontend/browser/profiles.spec.js` mocks a catalog
containing `BLS:LAU:UNEMP_RATE`. The mock was written to match the template
rather than the warehouse, so the slot resolves in the test and gaps in
production — the exact failure criterion 3 exists to prevent, one wave earlier.

Filed as `bls-area-series-product-identity` in `to_do/` rather than fixed here.
The choice between making the API resolve an area series for a place, making
the warehouse publish a geography-independent identity, and having the product
stop claiming a BLS rate is a contract decision, and `AGENTS.md` requires the
upstream contract be resolved first. The second-wave products take their labour
measures from ACS tables that publish one code per geography, and a test holds
them there so this cannot be repeated by accident.

### Tests

The unit tier gains eight assertions over the second wave: the products exist
with stated limits, every candidate is explicit and shaped like something a
publisher can emit, no score or index or computed rate appears in any
non-denial sentence, no note claims causation, the safety product keeps count
and base and participation apart, the suppression-bearing products distinguish
withheld from zero, no slot names a BLS area series, and every FRED slot says
it is national.

Two of them were confirmed to fail against a mutated template — a BLS area
series substituted into a second-wave slot, and the word "national" removed
from a FRED slot. The first attempt at that proof did nothing, because the
mutation was written against a single-line candidate list that Prettier had
already split across two lines; the guard was untested until the mutation was
repeated correctly.

The browser tier gains two: a second-wave product renders, shows its filled
slot's resolved identity, states the candidate its gapped CDC slot looked for,
and keeps its path into the explorer; and `/profiles` offers all eight
products from the rendered selector, which is the claim that the second wave
needed no component.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 647 passed, 43 files (was 639) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run check:bundle` / `check:csp` | pass |
| `npx playwright test profiles.spec.js` | 6 passed (was 4) |
| `npm --prefix apps/web run test:browser` (whole tier) | 157 passed (was 155) |
| `RUN_INTEGRATION_TESTS=1 python -m pytest -o addopts='' tests/integration/api -m "integration and not external" -q` | 86 passed, 0 skipped |

The API tier is run with `TEST_REDIS_URL` set, so the four cache and
middleware-order tests that skip without a Redis actually ran; without one this
command reports 82 passed and 4 skipped and says nothing about the cache path.

`@axe-core/playwright` was declared in `devDependencies` and absent from this
machine's `node_modules`, so the browser tier aborted on an import before
running anything. `npm ci` fixed it. That is a property of this checkout, not
of the repository.

## Validation

```bash
npm --prefix apps/web run test:unit
npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
npm --prefix apps/web run test:browser
python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
```

Catalog resolution is evidence, not decoration: record the actual catalog
response that justified each candidate list.
