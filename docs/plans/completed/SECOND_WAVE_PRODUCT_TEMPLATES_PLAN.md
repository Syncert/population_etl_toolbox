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

### The catalog the candidates were checked against

**Read this before the rest: the first version of this evidence was measured
against the wrong warehouse, and its conclusions were wrong.**

The internal stack -- `infra/docker/docker-compose.yml`, started by
`scripts/deploy_stack.py --mode internal` -- is the development target. Its
`gold_glossary.dim_metric_catalog` carries **18,198 identities across all seven
registered sources**:

| Source | Metrics | | Source | Metrics |
|---|---|---|---|---|
| BLS | 13,324 | | CDC | 281 |
| CENSUS_ACS | 4,447 | | USDA_NASS | 101 |
| FRED | 24 | | CENSUS_PEP | 17 |
| FBI_UCR | 4 | | | |

Resolved against it, **every slot in all eight products fills: 56 of 56, no
gaps.** The second wave is 41 of 41.

### What the first measurement got wrong, and what it cost

The first pass was run against a warehouse at `192.168.50.16`, which is a
separate and much older deployment. It carries three of the seven sources and
had never had its glossary harvested. Measured there, eight second-wave slots
gapped, and this section originally recorded them as "a source that deployment
does not carry rather than a code that is wrong".

**That conclusion was false, and it hid a real defect.** The same eight slots
gapped on the internal stack *with all seven sources present*, because the
candidate codes themselves were wrong -- inherited from the first wave and
copied into the second without being resolved against a catalog that could
have refuted them:

| Source | The templates named | The publishers emit |
|---|---|---|
| CENSUS_PEP | `CENSUS_PEP:pep_cty_alldata:POPESTIMATE` | `CENSUS_PEP:POPESTIMATE` |
| CDC | `CDC:cdi:ALC1_1:crude` | `CDC:cdi:ALC06:AGEADJPREV` |
| FBI_UCR | `FBI_UCR:summarized_violent_crime:actual` | `FBI_UCR:summarized_violent_crime:V:offense:absolute_total` |
| USDA_NASS | `USDA_NASS:corn_survey_annual:41` | `USDA_NASS:corn_survey_annual:<sha256>` |

All four are corrected, in the first wave as well as the second, because a
candidate that resolves nowhere is a slot that reports a gap forever. The first
wave went from 10 resolving with 5 gaps to 15 resolving with none.

**A retraction.** This plan originally filed
`bls-area-series-product-identity`, asserting that `BLS:LAU:UNEMP_RATE` could
resolve on no warehouse because a BLS metric code is always `BLS:<series_id>`.
That is wrong. `gold_bls.measure_export` publishes seven geography-independent
measure keys, and `BLS:LAU:UNEMP_RATE` is one of them, valid at COUNTY and
STATE. The remote warehouse was missing `gold_bls.dim_bls_measure` and
`gold_bls.measure_export` entirely -- both are in its 68-relation shortfall --
so it published only raw series and looked like it could publish nothing else.
The ticket is withdrawn and the test it produced now forbids the *series*
shape rather than the `BLS:` prefix.

The lesson is the one the plan already stated and this evidence briefly stopped
obeying: resolve a candidate against a catalog that is in a position to refute
it. A warehouse missing the source cannot refute anything.

### Where the fabricated identities came from

`tests/frontend/browser/profiles.spec.js` mocks a catalog. It contained the
same wrong codes the templates did, so every slot resolved in the browser tier
and gapped against a warehouse -- the mock was written to agree with the
template rather than with a publisher. It is corrected here, and
`tests/frontend/unit/comparison.test.js` still carries the old PEP identity as
an arbitrary string in filename assertions; harmless to that test's subject and
noted rather than churned.

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
