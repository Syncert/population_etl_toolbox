# Web App (Next.js)

App Router-based frontend for local iteration against the API and Martin tiles services.

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

- `/api/*` -> `${NEXT_PUBLIC_API_ORIGIN}/api/*` (default `http://localhost:8000`)
- `/tiles/*` -> `${NEXT_PUBLIC_TILES_ORIGIN}/*` (default `http://localhost:3000`)

## Server Error Recovery

If the dev server shows chunk/module errors (for example `./819.js`), clear the Next.js build cache and restart with:

```bash
npm run dev:reset
```
