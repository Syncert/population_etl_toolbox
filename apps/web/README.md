# Web App (Next.js)

App Router-based public frontend for the API and Martin tiles services. Both Compose modes build this app and expose it at `http://localhost:3001`.

## Routes

- `/`: product context, live catalog signals, and primary workflows
- `/catalog`: search, deterministic paging over the API's published total, provenance, and direct metric-to-explorer links
- `/explore`: the capability-driven explorer — every completed source, latest and as-released scopes, and only the presentation modes the selection can answer
- `/compare`: the preflight-first comparison workspace
- `/profiles`: the first-wave products (community conditions, population growth, workforce) over published catalog identities
- `/quality`: the source coverage and data-quality explorer
- `/saved`: account-stored analysis configurations
- `/builder`: the evidence packet composer, with a reproducibility envelope on every analytical block
- `/articles`: narrative page using the same live observation contract
- `/bls`, `/census`, `/fred`: retired demonstration dashboards; these redirect into `/explore` for the same source

## Supported browsers

The application targets current evergreen browsers: the latest two stable
releases of Chrome, Edge, Firefox, and Safari, on desktop and mobile. The
browser suite runs on Chromium, which is the gate; the other engines are
supported by using only broadly available platform features and are not
separately gated in CI.

Vector-tile rendering requires WebGL. Where WebGL is unavailable the map
mode is simply not offered — every value it would show remains available in
the observation table and the CSV export, which is why no workflow depends
on the map.

## API compatibility policy

The application consumes `/api/v1` only, as documented in
[`docs/reference/API_CONSUMER_GUIDE.md`](../../docs/reference/API_CONSUMER_GUIDE.md).

- Which sources exist, which routes answer for them, and which filters each
  accepts are read from `/api/v1/catalog/capabilities` at runtime. There is
  no client-side source list, so a source added to the API appears here
  without a frontend change.
- A filter a source does not declare is never sent. The API rejects an
  undeclared filter with a 422 precisely so it cannot be silently ignored,
  and dropping one client-side would silently widen the answer instead.
- A route the capability entry does not declare is not requested, and its
  absence is reported as "not declared for this source" rather than as a
  failed request.
- Breaking changes are expected to arrive as a new version prefix. Within
  `/api/v1`, a field this client does not recognise is ignored, and a field
  it expects but does not receive renders as "not published" rather than as
  a placeholder or a zero.

## Performance budgets

Route bundle budgets are measured on a controlled production build and
enforced in CI:

```bash
npm run build
npm run check:bundle          # fails if any route exceeds its budget
npm run check:bundle:update   # rewrite the baseline, deliberately
```

`scripts/bundle-budgets.json` holds one explicit budget per route. A route
with no declared budget fails the check rather than passing silently, so a
new route cannot grow unnoticed.

## Security headers

`next.config.mjs` serves `Content-Security-Policy`, `X-Content-Type-Options`,
`X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, and
`Cross-Origin-Opener-Policy` on every route. The CSP is same-origin only:
the API and tile server are reached through this server's own rewrites, so
nothing needs a third-party script or connect origin. `worker-src blob:` and
`img-src blob:` exist for MapLibre. Script `'unsafe-inline'` is Next's own
inline bootstrap; removing it requires a per-request nonce and is a
deliberate follow-on.

## Accessibility commitments

These are gated by `tests/frontend/browser/accessibility-operations.spec.js`:

- One `main` landmark and one level-1 heading per route, with a named
  primary navigation landmark.
- Every form control on the core workflows has an accessible name.
- Maps and charts always have a table or textual alternative to the same
  values, and colour is never the sole carrier of state — the legend states
  its bins and counts.
- Selection is fully operable by keyboard and announced through a live
  region.
- Analytical context — source, period, coverage notes — stays visible at a
  390px viewport, and no page scrolls horizontally.

## Privacy boundary

Saved analyses are user-owned and served `private, no-store`. The bearer
token is sent only as an `Authorization` header and is never placed in a
URL, a link, or the address bar; neither is any configuration's name, id,
version, or owner. Public exploration state does travel in the URL by
design, so a shared explorer or comparison link reproduces an analysis —
those links carry a query, never a value or an identity.

## Local Development

```bash
cd apps/web
copy .env.local.example .env.local
npm install
npm run dev
```

Open `http://localhost:3100`.

## TypeScript

The app is incrementally typed. Contract-boundary modules — the versioned
API client (`lib/api/`), URL state (`lib/urlState.ts`), and catalog view
models (`lib/catalog.ts`) — are TypeScript under `strict` plus
`noUncheckedIndexedAccess`; route and component files remain `.js` and are
converted as they are materially changed. `allowJs` keeps both in one
graph, so no rewrite is required.

```bash
npm run typecheck   # tsc --noEmit
```

`npm run build` also checks types, and CI runs `typecheck` as its own step.

## Proxy Rewrites

The Next.js app uses same-origin rewrites so browser calls stay under the app host:

- `/api/*` -> `${API_ORIGIN}/api/*` (default `http://localhost:8000`)
- `/tiles/*` -> `${TILES_ORIGIN}/*` (default `http://localhost:3000`)

`NEXT_PUBLIC_API_ORIGIN` and `NEXT_PUBLIC_TILES_ORIGIN` remain supported for compatibility. Compose builds the app with Docker DNS origins (`api:8000` and `martin:3000`), while browser requests remain on the web origin.

## Explorer interactions

- ACS5 is the nationwide county default; ACS1 remains explicitly selectable with partial-coverage messaging.
- The metric catalog is loaded through pagination, exposing every active public ACS1/ACS5 metric instead of the first catalog page.
- State and county selectors filter the map and drive the selected-county detail panel.
- Hover a county for value, period, source, and margin-of-error context.
- Click a county to pin its details and highlight its boundary.
- The selected-county panel fetches `/api/v1/observations/timeseries` and renders the available trend.
- Historical requests are backed by durable ACS, BLS, and FRED fact views rather than the rolling latest/dashboard serving tables.
- Choropleth colors and legend counts come from `/api/v1/distribution/bins`, with a labeled local fallback if the endpoint is unavailable.
- Explorer tabs expose the underlying table, source metadata, exact API query, and interpretation notes.
- CSV export preserves the canonical metric, geography, source, period, unit, and margin-of-error fields.
- Saved views flow into the Builder without duplicating chart configuration logic.

## Operational diagnostics

- **Health and reachability.** The explorer's `API` and `Tiles` status pills
  report the live result of `/api/v1/health` and tile discovery, including
  the chosen layer and join key. A red API pill with a status code is the
  first thing to read when a page looks empty.
- **What was actually requested.** Every analytical surface exposes the exact
  request that produced what is on screen: the explorer's *API query* tab,
  the comparison workspace's *API Query* panel, and each evidence block's
  recorded request. Paste it at the API to reproduce the answer outside the
  browser.
- **Distinguishing a client problem from a service problem.** A `503` from
  the API renders as a named failure with its status; an empty but healthy
  answer renders as "0 records published for this selection". These are
  different states on purpose — an empty page is never ambiguous between
  them.
- **Correlation.** The API's error bodies are `{"detail": "..."}` and never
  name warehouse objects; a 503 detail is deliberately sanitized, so
  deployment-state questions are answered from the API's own logs rather
  than from a browser response.

## Server error recovery

If the dev server shows chunk/module errors (for example `./819.js`), clear the Next.js build cache and restart with:

```bash
npm run dev:reset
```
