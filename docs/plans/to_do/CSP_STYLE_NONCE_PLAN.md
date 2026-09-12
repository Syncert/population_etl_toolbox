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

- **Status:** To do. Filed 2026-09-12 when the script nonce landed (WEB-032)
  and `style-src 'unsafe-inline'` was deliberately left in place as a
  separate change.
- **Last updated:** 2026-09-12
- **Owner surface:** `apps/web/middleware.ts`, `apps/web/app/layout.js`,
  `apps/web/app/styles/`, `tests/frontend/browser/csp-nonce.spec.js`
- **Depends on:** nothing open; builds on the script nonce in
  `middleware.ts` (WEB-032) and its production-build guard
  `scripts/check-csp-nonce.mjs`.

## Context

WEB-032 closed the wide door for scripts: `script-src 'self' 'nonce-…'
'strict-dynamic'`, with a per-request nonce Next stamps on every script it
emits. Styles still carry `'unsafe-inline'`, which admits any injected
`<style>` element or `style=""` attribute. Inline styles are a smaller
attack surface than inline scripts — CSS injection exfiltrates through
selectors and background requests rather than executing — but it is the
one remaining `unsafe-*` token in the policy, and it was left in
untested rather than switched off untested.

It was left because the answer is not obvious:

- Next emits inline `<style>` elements for its own layout and for CSS
  modules in development; the production build extracts them to files.
- MapLibre sets `style` attributes on the elements it creates (the canvas,
  controls, popups). A style nonce does not cover attributes; only
  `'unsafe-hashes'` with per-declaration hashes, or moving those styles to
  classes, does.
- `StatusPill`, the legend swatches, and the tooltip position use
  `style={{…}}` props, which React renders as `style=""` attributes.

## Objective

Remove `'unsafe-inline'` from `style-src`, or record exactly which inline
style sources cannot be removed and why, with the policy admitting only
those.

## Scope

- Inventory every inline style source at runtime: Next's own elements,
  MapLibre's, and the application's `style` props (the legend swatch colour
  is the interesting one — it is data-driven and cannot become a class).
- For each: nonce it, hash it under `'unsafe-hashes'`, or move it to a
  class with a CSS custom property. The swatch colour is a custom property
  set on the element (`style="--swatch: #…"`) — still an attribute — so the
  answer may be `'unsafe-hashes'` for a known, fixed set of declarations,
  or accepting attribute styles while forbidding `<style>` elements
  (`style-src-elem` vs `style-src-attr`), which CSP3 allows and which closes
  the element door without touching MapLibre.
- Extend `csp-nonce.spec.js`: zero `securitypolicyviolation` events on
  every core route under the tightened policy, including the explorer's map
  interactions (hover tooltip, legend, extrusion mode) and the comparison
  map.
- Extend `check-csp-nonce.mjs` if the production build must satisfy
  anything new.

## Acceptance

- `style-src` carries no `'unsafe-inline'`, or the plan records the
  specific sources that force `style-src-attr 'unsafe-inline'` and the
  policy is split so `style-src-elem` forbids inline elements.
- The browser suite passes with no CSP violation on any core route.
- `apps/web/README.md` and the handoff describe the style policy as
  precisely as they now describe the script policy.

## Non-goals

Changing the script policy; adding third-party origins.
