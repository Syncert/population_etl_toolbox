---
id: csp-style-nonce
branch: feat/web-csp-style-nonce
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - ./tests/run.ps1 web-browser
  - ./tests/run.ps1 web-build
---

# Styles under the Content-Security-Policy nonce

## Plan status

- **Status:** Ready for review. Claimed and delivered 2026-09-12. The element
  door is closed in the build that ships; the attribute door is open on a
  recorded list of owners, each of which was measured rather than assumed.
- **Last updated:** 2026-09-12
- **Owner surface:** `apps/web/middleware.ts`,
  `apps/web/scripts/check-csp-nonce.mjs`,
  `tests/frontend/browser/csp-nonce.spec.js`, `apps/web/README.md`,
  `docs/reference/WEB_FIRST_WAVE_HANDOFF.md`
- **Depends on:** nothing open; built on the script nonce in `middleware.ts`
  (WEB-032) and its production-build guard `scripts/check-csp-nonce.mjs`.

## Context

WEB-032 closed the wide door for scripts: `script-src 'self' 'nonce-…'
'strict-dynamic'`, with a per-request nonce Next stamps on every script it
emits. Styles still carried `'unsafe-inline'`, which admits any injected
`<style>` element or `style=""` attribute. Inline styles are a smaller attack
surface than inline scripts — CSS injection exfiltrates through selectors and
background requests rather than executing — but it was the one remaining
`unsafe-*` token in the policy, and it was left in untested rather than
switched off untested.

## What was measured

The plan listed three unknowns. All three were resolved by measurement on
2026-09-12, against a real production server (`next start`, port 3105) and the
dev server, with a Playwright probe counting `<style>` elements and `[style]`
attributes after load — including `/explore` with the map rendered and hovered.

| Question | Answer |
| --- | --- |
| Does Next emit inline `<style>` elements? | **Production: none.** Zero on `/`, `/explore`, `/builder`, `/catalog` — in the served HTML and in the DOM after load. Three `<link rel="stylesheet">` instead: the application has one global stylesheet and the build extracts it. **Development: exactly one**, Next's `@font-face` block for the dev-overlay font. |
| Does MapLibre force inline styles? | Attributes only, never elements. With the map rendered and hovered, the owners were `canvas.maplibregl-canvas` and its container/controls — and still zero `<style>` elements. |
| The application's own `style={{…}}` props? | Three, all values rather than rules: the legend swatch's colour (`ChoroplethLegend.tsx`), a coverage bar segment's width (`DataQualityExplorer.tsx`), the map tooltip's pointer position (`SourceExplorerPage.tsx`). A CSS custom property would still be an attribute, so it changes nothing. |

One source the plan did not anticipate: **Next writes a style attribute on its
own `next-route-announcer`**, in production as well as development. So even an
application with no inline styles of its own would need `style-src-attr`.

## Delivery

```
style-src 'self' 'unsafe-inline'                     # CSP2 fallback, unchanged
style-src-elem 'self'                                # + 'unsafe-inline' in dev only
style-src-attr 'unsafe-inline'
```

- **The element door is closed where it counts.** `style-src-elem 'self'` in
  the build that ships. The development exception exists for exactly one
  Next-internal element and is folded away by the build.
- **`style-src` is left as it was**, so a browser without the CSP3 split reads
  the old policy and behaves exactly as before. Nothing regresses for it while
  the element door closes for everyone else.
- **The attribute door stays open, narrowly and on the record.**
  `csp-nonce.spec.js` lists every element allowed to carry a style attribute,
  with the reason beside it, and fails on any other — so a new inline style is
  a decision someone makes, not one that arrives silently.
- **The production policy is graded on the artefact.**
  `check-csp-nonce.mjs` reads `.next/server/middleware.js` and fails if the
  shipped `style-src-elem` admits an inline element, or if `style-src-attr` is
  missing. The browser suite runs against `next dev`, where the exception is
  live, so it cannot see the shipped policy — the same reason that script
  exists at all.
- **Documentation.** `apps/web/README.md` and
  `docs/reference/WEB_FIRST_WAVE_HANDOFF.md` describe the style policy as
  precisely as they describe the script policy, including the rule for a
  future plan: add an attribute on a recorded owner, or a rule to the
  stylesheet, never a `<style>` element.

The browser assertion is written as "every `<style>` element is Next's own"
rather than "there are none", so the same test states the same rule in both
modes and holds vacuously in production, where the list is empty.

## Acceptance

- [x] `style-src` no longer blanket-admits inline styles: the policy is split
      so `style-src-elem` forbids inline elements in production, and the plan
      records the specific sources that force `style-src-attr 'unsafe-inline'`
      — MapLibre's own canvas and controls, Next's route announcer, and three
      data-driven styles that cannot become classes.
- [x] The browser suite passes with no CSP violation on any core route: 55 of
      55, including the explorer with MapLibre and its blob workers, the
      composer, the reader, and the new style test over `/`, `/explore`, and
      `/builder`.
- [x] `apps/web/README.md` and the handoff describe the style policy as
      precisely as they now describe the script policy.

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Browser tier | `npx playwright test` | 55 passed |
| Build tier | `./tests/run.ps1 web-build` | lint clean, build clean, every route within budget |
| Typecheck | `npm run typecheck` | clean |
| Production CSP guard | `npm run check:csp` | passed: 0 prerendered routes, middleware at `/`, shipped `style-src-elem` admits no inline element |
| Gap | `style-src-elem 'self' 'unsafe-inline'` forced into the production policy, rebuilt | `check:csp` fails: `the shipped style-src-elem admits inline style elements: 'self' 'unsafe-inline'` |
| Unit tier | `python -m pytest tests/unit` | 1340 passed (catalog bookkeeping for WEB-035) |
| Runtime, production server | Playwright probe against `next start` | `/explore` with the map rendered and hovered: 1 canvas, **0 style elements**, 8 style attributes, **0 violations** |

## A note on the catalog number

This row is **WEB-035**, and WEB-034 is skipped deliberately:
`WEB_ANALYTICS_FIRST_WAVE_PLAN.md` already uses that identifier as the label
of its own delivery record for the live-stack smoke close-out. One identifier
naming two different things is worse than a gap, and the reason is recorded in
`TESTING_CONTRACT.md` beside the sequence rather than left to be rediscovered.

Like the other two plans delivered today, this one moves the catalog total
from `main`'s 291 to 292 in `TESTING_CONTRACT.md` and
`tests/unit/shared/test_catalog_evidence.py`. Whichever of the three merges
second and third must resolve those numbers upward rather than restate them.

## Non-goals

Changing the script policy; adding third-party origins. Removing
`style-src-attr 'unsafe-inline'`, which the measurements above show is not
available without giving up MapLibre, Next's own announcer, and every
data-driven style in the application.
