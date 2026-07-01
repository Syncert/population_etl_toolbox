# Web App (Next.js)

App Router-based public frontend for the API and Martin tiles services. Both Compose modes build this app and expose it at `http://localhost:3001`.

## Local Development

```bash
cd apps/web
copy .env.local.example .env.local
npm install
npm run dev
```

Open `http://localhost:3100`.

## Proxy Rewrites

The Next.js app uses same-origin rewrites so browser calls stay under the app host:

- `/api/*` -> `${API_ORIGIN}/api/*` (default `http://localhost:8000`)
- `/tiles/*` -> `${TILES_ORIGIN}/*` (default `http://localhost:3000`)

`NEXT_PUBLIC_API_ORIGIN` and `NEXT_PUBLIC_TILES_ORIGIN` remain supported for compatibility. Compose builds the app with Docker DNS origins (`api:8000` and `martin:3000`), while browser requests remain on the web origin.

## MVP Interactions

- ACS5 is the nationwide county default; ACS1 remains explicitly selectable with partial-coverage messaging.
- The metric catalog is loaded through pagination, exposing every active public ACS1/ACS5 metric instead of the first catalog page.
- State and county selectors filter the map and drive the selected-county detail panel.
- Hover a county for value, period, source, and margin-of-error context.
- Click a county to pin its details and highlight its boundary.
- The selected-county panel fetches `/api/observations/timeseries` and renders the available trend.
- Choropleth colors and legend counts come from `/api/distribution/bins`, with a labeled local fallback if the endpoint is unavailable.

## Server Error Recovery

If the dev server shows chunk/module errors (for example `./819.js`), clear the Next.js build cache and restart with:

```bash
npm run dev:reset
```
