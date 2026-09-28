---
id: curated-use-case-pages
branch: codex/analytics-backlog-2026-09-28
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Dedicated pages for catalog-backed use cases

## Plan status

- **Status:** Ready for review on `codex/analytics-backlog-2026-09-28`.
- **Last updated:** 2026-09-28

## Why

`docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md` names twenty opportunities.
Eight have reviewed, catalog-backed templates in `apps/web/lib/productTemplates.ts`
but are only modes of `/profiles`, so a user cannot navigate to a page named for
one use case without knowing its query parameter. Their content, source
attribution, limits, gap behavior, export, save, and explorer paths already
exist. Give each a stable entry point while reusing those proven behaviors.

The other twelve need candidate metric and semantic review. A page that merely
relabels generic exploration as a completed use case would make an unsupported
product claim; this plan does not create those pages or invent candidate codes.

## Work items

- [x] Add a discoverable `/use-cases` page listing the eight live use cases.
- [x] Add `/use-cases/<template-id>` routes with their own title, summary,
  analytical limits, and fixed template while keeping geography shareable.
- [x] Link the discovery page from the front door and the existing profile
  surface. Preserve `/profiles` and its query links.
- [x] Test direct navigation, fixed-template URL behavior, unknown slug refusal,
  source attribution, and a partial-catalog gap against the existing fixtures.

## Acceptance criteria

1. Every reviewed template has one stable use-case URL with distinct metadata
   and an answer page driven by the published catalog, not a new hard-coded
   metric or client-side derivation.
2. A user can find and share each page; a place link reopens on the same use
   case, including when a conflicting `template` query key is present.
3. The existing `/profiles` flow and its saved/exported answers keep working.
4. Unknown use-case IDs return a 404. Missing published measures remain stated
   gaps with their limits visible.

## Evidence and scope boundary

- The eight IDs are `community-conditions`, `population-growth`, `workforce`,
  `housing-affordability`, `aging-population`, `disease-illness-burden`,
  `public-safety-trend`, and `rural-agricultural-economy`.
- The other twelve top-20 opportunities remain a separate curation and
  upstream-contract backlog, pending a populated catalog and reviewed semantic
  definitions. They are not claimed complete by this plan.
- `lib/useCasePages.ts` derives the eight URLs from the existing reviewed
  templates; `/use-cases` lists them and `/use-cases/[id]` serves each through
  `ProfileProduct` with a fixed template. A conflicting query cannot substitute
  another template. Both routes render per request so the CSP nonce is present.
  The old `/profiles` selector and profile links remain available.
- The published route list feeds both robots and sitemap, and the new routes
  have measured bundle budgets. `TESTING_CONTRACT.md` WEB-123 and
  `CI_EVIDENCE_MAP.md` name the behavior and owning frontend job.
- Test-first evidence: the route unit test initially failed because the route
  model did not exist; the browser test saw zero links before the page existed;
  the sitemap test failed before the routes were added. Focused tests passed
  after implementation.
- `npm --prefix apps/web run test:unit`: 709 passed. `npm --prefix apps/web
  run lint` and `npm --prefix apps/web run typecheck` passed.
  `npm --prefix apps/web run test:browser`: 173 passed before the route-mode
  correction; `npm --prefix apps/web run test:browser -- profiles.spec.js
  route-titles.spec.js`: 21 passed afterward. The production build passed,
  including both dynamic use-case routes. `check:csp` passed with zero
  prerendered documents, and `check:bundle` passed with both new route budgets.
