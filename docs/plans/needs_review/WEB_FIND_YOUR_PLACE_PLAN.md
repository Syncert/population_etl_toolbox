---
id: web-find-your-place
depends_on:
  - api-place-search
parallel_safe: false
complexity: low
verify:
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# A reader reaches their city by name or by location

## Status

Ready for review (v1.0 build-out, item D-1), 2026-10-26, branch
`feat/web-find-your-place`. Merge after `feat/api-place-search`.

## What changed

- The home search (`PlaceSearch`) asks the API's place search a moment after
  typing, so cities and towns are found by name with counties and states;
  the catalog's nation, states and counties still answer instantly, and alone
  when the API cannot. A city opens by its FIPS address, which the place
  route settles on its name.
- "Use my location" asks the API which places contain the point and opens
  the city, else the county; the nearest-county-center approximation remains
  only as the fallback.
- **Defect fixed:** the web's `Permissions-Policy` was
  `geolocation=()`, which blocks the browser's location API on the site's own
  pages, so "Use my location" reported the location unavailable in every
  browser whatever the reader answered. It is now `geolocation=(self)`:
  allowed for the site, never for embedded frames.
- Metro results are not listed yet: there is no metro page to open. They
  join with the metro pages (D-5); the API already returns them (A-1), and on
  the internal stack "Austin" answers the Austin metro, Austin city and
  Travis County.

## Evidence (OFFICE_LAB, Windows, 2026-10-26)

- `npx playwright test`: 253 passed, including WEB-145's two new tests (a
  city found by name, and a location opening the city that contains it).
- `npm run lint`, `npm run typecheck`, `next build`: pass.
