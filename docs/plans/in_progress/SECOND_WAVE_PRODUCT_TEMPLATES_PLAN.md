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

- **Status:** Claimed 2026-09-18 by a machine session with the deployed
  analytics warehouse reachable, which is what criterion 3 needs: the candidate
  codes are to be verified against the live catalog rather than guessed.
- **Last updated:** 2026-09-18
- **Current milestone:** read the use-case table and the existing templates,
  then query `gold_glossary.dim_metric_catalog` on the deployed warehouse to
  establish which candidates exist before authoring any.
- **Next pickup:** the catalog query is the first step and its output is
  evidence the plan requires; do not author a candidate list from the use-case
  document alone.

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

- [ ] Five templates exist, each resolving against the live catalog, each
      stating its limits, and each carrying its use-case guardrail in terms a
      non-specialist reads.
- [ ] Every slot names explicit candidate codes; no slot falls back to a
      "similar" measure, and an unfillable slot reports what it looked for.
- [ ] No client-side arithmetic across sources is introduced. No score, index,
      grade, or ranking. No causal phrasing anywhere in the copy.
- [ ] Suppressed, not-reported, and missing stay distinct from each other and
      from zero, in every new slot.
- [ ] `/profiles` lists all eight products with no per-product component and
      no new fetch path.
- [ ] The plan's evidence record names which slots resolve today and which
      report a gap, with the catalog query that established it.

## Validation

```bash
npm --prefix apps/web run test:unit
npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
npm --prefix apps/web run test:browser
python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
```

Catalog resolution is evidence, not decoration: record the actual catalog
response that justified each candidate list.
