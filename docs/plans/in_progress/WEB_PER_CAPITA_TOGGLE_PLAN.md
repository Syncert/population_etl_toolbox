---
id: web-per-capita-toggle
depends_on:
  - api-per-capita-and-shares
  - web-plain-labels-everywhere
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Counts can be read per 1,000 residents or as a share, with the denominator named

## Status

In progress (v1.0 build-out, item D-6). Branch `feat/web-per-capita-toggle`.

## Goal

A reader comparing a county of 30,000 with one of a million can switch a
place page chapter, and the one-measure county map, between the count as
published, a per-capita view and a share, wherever A-3
(`GET /api/v1/catalog/derived-ratios`, `GET /api/v1/observations/derived`,
ADR-0009) serves a pairing for that count. The web computes nothing: every
derived value, its unit and its margin of error is the API's.

## Acceptance criteria

1. The page reads the served pairings once (`served_only=true`). A chapter
   offers "Count", a per-capita view and "Share" only for kinds at least one
   of its measures has a served pairing for; a chapter with none shows no
   toggle. A measure with no pairing of the chosen kind keeps its count and
   says so.
2. Every derived value names its denominator beside it (the pairing's
   `denominator_label`), its unit, its period, and its margin of error with
   the method in words. A provider's own rate (FBI, PEP, SAIPE, SAHIE) is
   labelled as the provider's, with its denominator named, whether or not a
   toggle is shown.
3. A value the API cannot compute reads as the API's reason
   (`different_period`, `denominator_missing`, `denominator_zero`,
   `suppressed_input`), never zero.
4. A pairing whose status is not `approved` is marked "Draft view awaiting
   review" beside the value, the way D-2 marks draft labels. Drafts are only
   served when the API sets `DERIVED_RATIOS_SERVE_DRAFTS=1`.
5. In a derived view a chapter's depth rows show the place beside its
   county (for a city), state and nation, so a county reads against its
   state on one denominator.
6. The one-measure map (`/map/<code>`) offers the same views for its measure;
   the map, legend, ranked tables and coverage block read the derived rows,
   name the denominator, and keep `?view=` in the address.
7. The required example: reported violent crime for a county against its
   state. The FBI publishes no county rate and the county roll-up is counts
   only (API guide, "Derived county crime roll-up"), and ADR-0009 rule 2
   forbids dividing FBI counts by a Census population. So the county row
   says the FBI publishes no county figure, and the state row shows the FBI's
   own rate per 100,000 with its denominator ("population covered by
   reporting agencies") and the draft mark. A county per-1,000 crime figure
   needs an ADR-0009 amendment and Nick's decision.

## Catalog

WEB-145 (frontend unit, frontend browser).

## Evidence (cloud session, Linux, 2026-10-31)

- `npm run test:unit`: 881 passed (6 new in `derived-views.test.js`).
- `npm run lint`, `npm run typecheck`, `next build`: pass.
- `npx playwright test`: 256 passed on this branch, 5 new in
  `per-capita-toggle.spec.js`; 257 passed on a trial merge with D-2 (#177),
  whose scan for variable codes runs after every browser test.
- Screenshots (browser fixtures): `docs/incidents/screenshots-2026-10-31-per-capita/`.
- Not verified here: the running site. A-3 serves pairings only with
  `DERIVED_RATIOS_SERVE_DRAFTS=1`; until A-3's follow-up accepts FBI's
  `reported` status, the live FBI pairing answers `suppressed_input`
  (the place cards show the FBI rate itself, so they are unaffected).
