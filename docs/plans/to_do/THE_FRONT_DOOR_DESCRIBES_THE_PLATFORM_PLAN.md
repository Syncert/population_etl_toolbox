---
id: front-door-describes-the-platform
branch: claude/front-door-describes-the-platform
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/tooling -q
  - ruff format --check . ; ruff check .
---

# The front door describes the platform

## Plan status

- **Status:** Unclaimed. Authored 2026-09-15 from the repository assessment;
  no implementation has started.
- **Last updated:** 2026-09-15
- **Current milestone:** not started.

## Why

`AGENTS.md` opens by saying this repository is the foundation for a public-data
analytics website and social hub. `README.md` opens by saying it is "A
production-grade ETL system for ingesting, transforming, and serving economic
and demographic data" over "Census ACS, Census PEP, BLS, and FRED". Both were
true once; only one still is.

What the README, as of `765f1c0`, tells a new reader:

- **Four sources.** Seven pipelines ship — CDC, FBI UCR and USDA NASS have
  DAGs, silver and gold packages, API surfaces, operator guides under
  `docs/user-guides/`, and end-to-end coverage.
- **"Current State (August 2026)"**, listing under *Roadmap* a "Public API for
  warehouse access" that shipped, and under *In Progress* analytical-layer work
  that closed.
- **A dead link.** Line 9 points at
  `docs/product/ECONOMIC_DATA_STUDIO_MANIFESTO.md` for "longer-term product
  design". No such file exists anywhere in the repository; `docs/product/`
  contains only `TOP_20_DATA_PRODUCT_USE_CASES.md`.
- **The web application as a scaffold.** "Next.js Web App (Local Iteration)"
  and "Run the new web app scaffold" describe roughly 22,000 lines of
  TypeScript across ten surfaces, with its own CSP, bundle budgets, browser
  tier, and live-stack smoke tier.

The cost is not cosmetic. The README is what orients a contributor, an agent
after a context reset, and the repository's owner six months from now, and it
currently orients all three toward a toolbox. A reader who believes it will not
look for the product they are supposed to be building.

## Scope

**In scope**

1. **Rewrite the README's framing and state sections** — opening paragraph,
   Project Vision, Current State, and Roadmap — so they describe the platform
   the repository is, across all seven sources, with the web application as a
   delivered layer rather than a scaffold. The technical reference sections
   below them are accurate and stay.
2. **Resolve the missing product document.** Either write
   `docs/product/ECONOMIC_DATA_STUDIO_MANIFESTO.md` as the product's stated
   intent — the audience, what the platform refuses to do (composite scores,
   causal claims, filled-in suppressed values), and how the twenty use cases
   sequence — or repoint the README at
   `TOP_20_DATA_PRODUCT_USE_CASES.md` and delete the reference. Do not leave a
   link to a document that does not exist.
3. **A dead-link check that runs.** Every relative Markdown link under `docs/`
   and in the root Markdown files resolves to a file that exists, enforced by a
   test in `tests/unit/tooling/` so the next stale link fails a suite rather
   than waiting to be noticed. `README.md` also currently renders link *text*
   pointing at `docs/plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md` while its
   href points into `completed/`; the check should be written knowing the
   difference, and the text fixed.
4. **A dated state line** rather than an undated claim, so the next reader can
   tell how stale what they are reading is.

**Out of scope**

- Rewriting the reference contracts under `docs/reference/`. They are current.
- Any code change outside the new link test.
- Renaming the package, the repository, or the product.

## Acceptance criteria

- [ ] The README's first paragraph describes an analytics web platform over
      seven public-data sources, with the warehouse as its foundation.
- [ ] Current State and Roadmap reflect what shipped, and nothing listed as
      roadmap is already delivered.
- [ ] Every relative link in every root and `docs/` Markdown file resolves,
      proven by a test that fails when one does not.
- [ ] `docs/product/` either contains the referenced product document or is no
      longer referenced as containing it.
- [ ] `AGENTS.md`'s architecture order and the README's description of the same
      order do not contradict each other.

## Validation

```bash
python -m pytest tests/unit/tooling -q
ruff format --check . ; ruff check .
```
