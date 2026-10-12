---
id: explainer-pages
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# Explainer pages

## Status

Ready for review, 2026-10-06, on branch `feat/explainer-pages`.

### Implementation evidence

- **Content:** twelve Markdown files in `apps/web/content/explainers/`, one
  per caveat the plan lists, each with frontmatter (question title, summary,
  `caveat_keys`, `metric_codes`, `sources`, `example_metric`, `reviewed`,
  `reviewer`, optional `definitions` and `video_url`) and the sections Short
  answer, What it is not, Worked example, Where it is used. The reviewer
  field reads "Agent draft; editorial review pending": the text was written
  by an agent from the use-case guardrails and the providers' published
  methods, and needs a person's editorial review before it is presented as
  reviewed. `docs/semantics/` holds no definitions yet, so none is cited; the
  test checks any that are cited exist.
- **Format and rendering:** `lib/explainerContent.ts` parses and validates
  a deliberately small format (paragraphs and lists, no HTML, no inline
  markup), so rendering needs no Markdown library and nothing in a file
  reaches the page as markup. Files are read at request time by
  `lib/explainerFiles.ts`; `outputFileTracingIncludes` in `next.config.mjs`
  copies them into the standalone build. Every page is server-rendered per
  request, so the CSP nonce contract is untouched (`check:csp` passes).
- **Routes:** `/explain` and `/explain/<slug>`, titles and descriptions
  from the files, sitemap entries for all thirteen addresses, an unknown slug
  a 404, budgets declared for both routes.
- **Worked example:** `components/ExplainerExample.tsx` reads the example
  measure's newest value for the nation and, when `lib/lastPlace.ts` holds a
  place the reader looked at in this tab (session storage, written by the
  profile and use-case pages), for that place too. With none, it shows the
  national value and says so. Nothing is written to the URL. The three-level
  card from `place-pages` is not on this branch; once both merge, place pages
  should also remember the last place.
- **Linking contract:** template slots declare an optional `caveat` key
  (seventeen do); `components/ProfileProduct.tsx` links each to its
  explainer through `lib/explainerIndex.ts`, and renders nothing for a key no
  explainer answers.
- **Video hook:** `video_url`, rendered only when present.
- **Contract:** WEB-126, the evidence-map row, catalog total 548. The
  `/profiles` bundle budget moved from 450 to 456 kB for the caveat links
  (measured 450.4 kB).

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: 50 files, 742 tests passed
  (`explainers.test.js`, 23).
- `lint`, `typecheck`, `build`, `check:csp` (0 prerendered documents),
  `check:bundle`: passed.
- `npx playwright test` (production server): 206 passed, including
  `explainers.spec.js`.
- `python -m pytest tests/unit/shared tests/unit/tooling -q`: passed.

### Open items, decided

- Markdown is read at request time, not compiled, and carries no HTML.
- Authorship review is a frontmatter field; every file currently records
  that editorial review is pending.

## Why

Every caveat the pipelines preserve (margins of error, vintages, suppression,
reporting participation, modeled prevalence, national versus local indexes)
needs one plain-language home, written once and linked from every chart that
carries it. Today those caveats live in source notes and the semantic
definitions, which readers do not open. Explainers are also the evergreen
video scripts in the production kit: the short answer is the voice-over, the
diagram is the key frame, the worked example is the payoff.

## What exists

- `docs/semantics/` holds reviewed definitions with intended use,
  limitations, and citations per `metric_code`, validated by
  `definition.schema.json`; the glossary harvest never reads it, so content
  there cannot block data access.
- `components/SourceNote.js` and the use-case intro render source-native
  limitations per page.

## Deliverables

1. **Content home.** Explainers live as Markdown with frontmatter under
   `apps/web/content/explainers/<slug>.md`: title as a question, the
   `metric_code`s and sources it applies to, reviewed date, and the sections
   Short answer, What it is not, Worked example, Where it is used. They are
   reader-facing prose, distinct from the reviewed definitions in
   `docs/semantics/`, and must cite those definitions where one exists.
2. **Route.** `/explain` lists them; `/explain/<slug>` renders one with the
   same metadata, sitemap, title, and bundle-budget treatment as other
   routes. The worked example reads the reader's last-viewed place from page
   state (never from the URL) and shows the county, state, and nation value
   for the latest period through the existing three-level card when
   `place-pages` has shipped, and the national value alone otherwise.
3. **The first twelve**, each written from the guardrails in
   `docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md`:
   what the unemployment rate counts; why a survey estimate has a margin of
   error; PEP estimates versus ACS surveys; what a 5-year estimate is; why
   missing crime reports are not zero crime; why the CPI is not your local
   cost of living; what a suppressed cell means; why modeled prevalence is
   not a case count; what a revision is and why numbers change; what a
   vintage is; jobs versus employed people (establishment versus household
   surveys); what a percentile rank among peers does and does not mean.
4. **Linking contract.** A unit test asserts every explainer's
   `metric_code`s exist in the capabilities fixture and every chart surface
   that declares a caveat key resolves to an explainer or renders no link; a
   dead explainer link is a test failure.
5. **Video hook.** Each explainer carries an optional external link field
   for its published video; absent, nothing renders.

## Acceptance criteria

- `/explain` and `/explain/<slug>` render for all twelve with one `main`
  landmark, one level-1 heading, metadata, and sitemap entries; an unknown
  slug is a 404.
- Each explainer file validates against a frontmatter schema in a unit
  test, including at least one applicable `metric_code` present in the
  capabilities fixture and a reviewed date.
- The worked example shows the reader's last-viewed place from page state
  and never writes it to the URL; with no last-viewed place it shows the
  national value and says so.
- Chart surfaces that declare a caveat key link to the matching explainer;
  the unit test proves no declared key is unresolved.
- Browser scenarios cover one explainer at desktop and 390px with WCAG AA
  checks; `check:csp` and `check:bundle` pass with a declared budget for
  the new routes.

## Open items to resolve during implementation

- Whether explainers render Markdown at request time or are compiled at
  build; either must keep the CSP nonce contract and `style-src-elem
  'self'`.
- Authorship review: explainers are editorial content, so the frontmatter
  carries a reviewer and date like the semantic definitions do.

## Checkpoint

Implementation complete; awaiting human review, including editorial review
of the twelve texts.
