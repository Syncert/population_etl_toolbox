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
 * needs it. Styles keep `'unsafe-inline'`: Next emits inline style elements
 * for its own layout, and a style nonce is a separate change.
 */
export function contentSecurityPolicy(nonce: string): string {
  return [
    "default-src 'self'",
    `script-src 'self' 'nonce-${nonce}' 'strict-dynamic'${isDevelopment ? " 'unsafe-eval'" : ""}`,
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: blob:",
    "font-src 'self' data:",
    "worker-src 'self' blob:",
    "connect-src 'self'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'self'",
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
