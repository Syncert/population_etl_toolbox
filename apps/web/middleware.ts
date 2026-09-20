// A per-request nonce for the Content-Security-Policy.
//
// The policy used to carry `script-src 'unsafe-inline'` because Next's own
// bootstrap and flight payload are inline scripts, and a policy that forbids
// inline scripts forbids the framework. `'unsafe-inline'` is a wide door,
// though: it admits every inline script, including one an attacker managed to
// inject, which is the attack CSP exists to stop.
//
// A nonce closes that door without closing it on the framework. Each response
// carries a fresh random value in its policy; Next reads that value from the
// request's own policy header and stamps it onto every script tag it emits;
// the browser runs only scripts carrying it. `'strict-dynamic'` then lets a
// nonced script load the chunks it imports, which is how Next's chunk loader
// works, without listing every chunk. An injected inline script has no way
// to know the nonce and does not run.
//
// The cost, stated rather than hidden: reading a per-request value makes
// every route dynamically rendered. The routes were static before this. The
// pages are small and served through the same-origin proxy, and the bundle
// budgets are measured on the build either way; the trade was recorded as a
// deliberate follow-on in WEB-009 and is taken here on purpose.
//
// The nonce is generated from the platform's CSPRNG, base64-encoded so it is
// a valid CSP source expression, and never logged.

import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

/**
 * Where a violation report goes.
 *
 * Outside `/api` and `/tiles`, which `next.config.mjs` rewrites away to the
 * API and Martin, so this reaches the web process itself. Same-origin, so
 * `connect-src 'self'` admits it and the policy is not widened to gain a log
 * line.
 */
const REPORT_ENDPOINT = "/client-report";

const isDevelopment = process.env.NODE_ENV === "development";

function freshNonce(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return btoa(String.fromCharCode(...bytes));
}

/**
 * The policy for one response. Same-origin only: the API and the tile
 * server are reached through this server's own /api and /tiles rewrites, so
 * `connect-src 'self'` covers both. MapLibre builds its workers from blob URLs
 * and decodes tiles into blob-backed images, which is why worker-src and
 * img-src admit `blob:`. `'unsafe-eval'` is development-only: Next's dev
 * runtime evaluates source maps and hot updates; the production bundle never
 * needs it.
 *
 * Styles are split rather than blanket-trusted (WEB-035). CSP3 separates the
 * two things `style-src` used to conflate:
 *
 * - `style-src-elem` governs `<style>` elements and stylesheet links. This
 *   application has one global stylesheet, which the production build
 *   extracts to a file, so production admits `'self'` and nothing else and an
 *   injected `<style>` block -- the shape that exfiltrates through selectors
 *   and background requests -- does not apply.
 * - `style-src-attr` governs `style=""` attributes, and those cannot be
 *   removed. MapLibre positions its canvas, controls, and popups by writing
 *   style attributes on elements it creates, and three of this application's
 *   own styles are values rather than rules: the choropleth legend swatch's
 *   colour, a coverage bar segment's width, and the map tooltip's pointer
 *   position. A nonce cannot cover an attribute, and `'unsafe-hashes'` needs
 *   one hash per exact declaration, which a data-driven value cannot have.
 *   So this one stays open, deliberately and narrowly.
 *
 * `style-src` is kept as it was for browsers that do not implement the CSP3
 * split: they read it and behave exactly as they did before, so nothing
 * regresses for them while the element door closes for everyone else.
 *
 * Development keeps `'unsafe-inline'` on elements because Next's dev server
 * injects its stylesheets and hot updates as `<style>` elements at runtime.
 * That is the same shape as the `'unsafe-eval'` exception above: what ships
 * is the production policy, and `check-csp-nonce.mjs` grades it.
 */
export function contentSecurityPolicy(nonce: string): string {
  return [
    "default-src 'self'",
    `script-src 'self' 'nonce-${nonce}' 'strict-dynamic'${isDevelopment ? " 'unsafe-eval'" : ""}`,
    // The CSP2 fallback, unchanged, for browsers without the CSP3 split.
    "style-src 'self' 'unsafe-inline'",
    `style-src-elem 'self'${isDevelopment ? " 'unsafe-inline'" : ""}`,
    "style-src-attr 'unsafe-inline'",
    "img-src 'self' data: blob:",
    "font-src 'self' data:",
    "worker-src 'self' blob:",
    "connect-src 'self'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'self'",
    // Where a violation goes. Both spellings, because two generations of the
    // standard are in the field: `report-uri` is deprecated and is what most
    // shipping browsers still implement, `report-to` names an endpoint group
    // declared by the `Reporting-Endpoints` header below. Neither widens the
    // policy -- the sink is same-origin, which `connect-src 'self'` already
    // admits -- and a browser that implements neither is unaffected.
    `report-uri ${REPORT_ENDPOINT}`,
    "report-to csp-endpoint",
  ].join("; ");
}

export function middleware(request: NextRequest) {
  const nonce = freshNonce();
  const policy = contentSecurityPolicy(nonce);

  // Next reads the nonce from the request's policy header and applies it to
  // the script tags it renders; the response must carry the same policy so
  // the browser enforces exactly what those tags were stamped with.
  const requestHeaders = new Headers(request.headers);
  requestHeaders.set("x-nonce", nonce);
  requestHeaders.set("content-security-policy", policy);

  const response = NextResponse.next({ request: { headers: requestHeaders } });
  response.headers.set("content-security-policy", policy);
  // The endpoint group `report-to` names. Same origin, so this adds no
  // destination the policy did not already allow.
  response.headers.set("reporting-endpoints", `csp-endpoint="${REPORT_ENDPOINT}"`);
  return response;
}

export const config = {
  // Pages only. The API and tile rewrites answer with their own headers and
  // never render a script; Next's static assets are immutable files.
  matcher: [
    {
      source: "/((?!api|tiles|_next/static|_next/image|favicon.ico).*)",
      missing: [
        { type: "header", key: "next-router-prefetch" },
        { type: "header", key: "purpose", value: "prefetch" },
      ],
    },
  ],
};
