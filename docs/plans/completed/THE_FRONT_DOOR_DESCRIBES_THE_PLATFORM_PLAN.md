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

- **Status:** Accepted 2026-09-16 (Ready for review. Authored 2026-09-15,
  claimed and delivered 2026-09-16; merged to `main` in `985b093`.) Every
  acceptance criterion has inspectable evidence, and the two defects the plan
  names are now caught by a test rather than by a reader.
- **Last updated:** 2026-09-16
- **Current milestone:** complete.
- **Dependencies:** none declared; none required.
- **Next pickup:** none.

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

- [x] The README's first paragraph describes an analytics web platform over
      seven public-data sources, with the warehouse as its foundation.
- [x] Current State and Roadmap reflect what shipped, and nothing listed as
      roadmap is already delivered.
- [x] Every relative link in every root and `docs/` Markdown file resolves,
      proven by a test that fails when one does not.
- [x] `docs/product/` either contains the referenced product document or is no
      longer referenced as containing it.
- [x] `AGENTS.md`'s architecture order and the README's description of the same
      order do not contradict each other.

## What was delivered

- **`README.md` framing and state rewritten.** The opening describes an
  analytics platform over seven sources with the warehouse as its foundation;
  Project Vision lists all seven; `Current State` is dated `2026-09-16` and
  carries a *Delivered* list and a *Not yet delivered* list; the architecture
  order is the `AGENTS.md` block verbatim rather than a restatement.
- **`tests/unit/tooling/test_documentation_links.py`** — three tests under a
  new catalog row **ENV-021**, registered in `TESTING_CONTRACT.md` and marked
  audited in `tests/support/catalog_evidence.py`.
- **Both link defects fixed**: the dead product link, and the label that
  dropped `completed/`.

## Decisions taken during implementation

**Repointed at `TOP_20_DATA_PRODUCT_USE_CASES.md` rather than writing the
manifesto.** The plan permitted either. `TOP_20` already carries what the
manifesto was described as carrying — the audience per use case, the
guardrails ("Cross-source association must never be presented as causation",
"do not collapse unlike measures into an unexplained score"), a delivery
sequence, and a product-wide definition of done. A second document would have
restated reviewed intent, and the parts it would *not* have restated are
product direction the repository's owner has not stated and an implementer
should not invent. The README now links the document that exists and quotes
two of its guardrails in the opening, so a reader meets the refusals early.

**The label check compares resolved files, not strings.** This repository
writes an href relative to the containing file and a label from wherever reads
most clearly — sometimes from the repository root
(`docs/plans/README.md` linked as `../plans/README.md`), sometimes relative to
the file (`completed/GATE.md`). Both are honest, and a string comparison
failed on eleven of them. The check accepts a label that names the linked file
under *either* reading, which left exactly the one genuine defect: `README.md`
advertised `docs/plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md` while opening
the copy in `completed/`. In a repository where the containing folder is a
plan's authoritative workflow state, that label claims finished work is still
in flight.

**A corpus assertion guards the guard.** `test_documentation_files_are_found`
asserts the glob matches more than a hundred files and includes known ones. A
link checker whose glob silently stops matching reports success forever, which
is the failure a link checker can least afford.

**One stale claim outside the named sections.** Line 379 read "Run the new web
app scaffold". The plan scoped the reference sections as accurate and
out of scope, and the *instructions* there are accurate — but "scaffold" is
the same misdescription the plan was written to remove, applied to ~21,000
lines of TypeScript. Changed that one phrase to "Run the web application
locally"; no instruction was touched.

## Evidence

| Check | Result |
|---|---|
| Broken relative links before the change | 1 (`docs/product/ECONOMIC_DATA_STUDIO_MANIFESTO.md`, never existed) |
| Mislabelled links before the change | 1 (`README.md`, label dropped `completed/`) |
| Both reproduced as failing tests before the fix | yes — 2 failed, 1 passed |
| `python -m pytest tests/unit/tooling -q` (declared) | **83 passed** |
| `ruff format --check .` / `ruff check .` (declared) | 474 formatted; all checks passed |
| `python -m pytest tests/unit -q` | **1756 passed** |
| `python -m tools.plan_dispatcher inventory` | graph resolves |
| `python -m tests.support.catalog_evidence` | ENV-021 renders `FULL` |
| Architecture-order block matches `AGENTS.md` | verified by string containment |
| Every *Not yet delivered* item maps to a real plan in `to_do/` | 4 of 4 |

The link defects were reproduced as failures before being fixed, so the tests
are known to fail on the condition they exist to catch rather than merely
passing on a clean tree.

## Not done, deliberately

- **`docs/reference/` untouched.** The plan scopes those contracts as current.
- **No code change outside the new test**, and no package, repository, or
  product rename.
- **The technical sections below `Current State` were not rewritten.** They
  are accurate; only the one "scaffold" phrase was corrected.

## Validation

```bash
python -m pytest tests/unit/tooling -q
ruff format --check . ; ruff check .
```
