// The Content-Security-Policy nonce, checked on the production build.
//
// The nonce is only real if two things hold in the build that ships: the
// middleware that mints it is registered for every page, and no page is
// prerendered. A prerendered page is HTML written at build time, and a
// per-request nonce cannot be written into it -- so its script tags carry
// none, the policy demands one, and the browser blocks every script on the
// page. That failure is invisible under `next dev`, which renders per
// request regardless, which is why the browser suite alone cannot catch it
// and this check reads the build artefacts instead.
//
// It also grades the style policy (WEB-035). `style-src-elem` admits only
// `'self'` in production, and development's `'unsafe-inline'` exception for
// Next's dev-overlay font is compiled away -- so reading the built middleware
// is how that is proved rather than asserted. The browser suite runs against
// `next dev`, where the exception is live, so it cannot see this.
//
// Usage: node scripts/check-csp-nonce.mjs   (after `npm run build`)

import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, dirname, relative } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const appRoot = join(here, "..");
const distDir = join(appRoot, ".next");

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function htmlFilesUnder(directory) {
  if (!existsSync(directory)) return [];
  const found = [];
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) {
      found.push(...htmlFilesUnder(path));
    } else if (entry.endsWith(".html")) {
      found.push(relative(appRoot, path));
    }
  }
  return found;
}

function main() {
  const failures = [];

  let prerender;
  let middleware;
  try {
    prerender = readJson(join(distDir, "prerender-manifest.json"));
    middleware = readJson(join(distDir, "server", "middleware-manifest.json"));
  } catch {
    console.error("No production build found. Run `npm run build` before checking the CSP nonce.");
    process.exit(2);
  }

  // 1. No *document* is prerendered. A prerendered page is HTML written at
  //    build time, so its script tags carry no nonce against a policy that
  //    demands one.
  //
  //    A prerendered route that serves something other than HTML has no
  //    script tag to stamp: `robots.txt` is `text/plain` and `sitemap.xml` is
  //    `application/xml`, and both are supposed to be static files. Forcing
  //    them to render per request to satisfy a check about script nonces
  //    would be answering the letter of the rule against its reason. The
  //    content type is read from the route's own recorded headers, and a
  //    route recording none is treated as HTML, so the exemption cannot widen
  //    by omission -- and the static-HTML assertion below is an independent
  //    second line on exactly the same failure.
  const prerendered = Object.entries(prerender.routes || {})
    .filter(([, info]) => {
      const contentType = (info.initialHeaders || {})["content-type"] || "text/html";
      return contentType.includes("text/html");
    })
    .map(([route]) => route);
  if (prerendered.length > 0) {
    failures.push(
      `${prerendered.length} route(s) are prerendered and would ship without a nonce: ${prerendered.join(", ")}. ` +
        "Every route must render per request (see app/layout.js).",
    );
  }
  const staticHtml = htmlFilesUnder(join(distDir, "server", "app"));
  if (staticHtml.length > 0) {
    failures.push(`static HTML was emitted for app routes: ${staticHtml.join(", ")}`);
  }

  // 2. The middleware that mints the nonce is registered for the pages, and
  //    only the pages: the API and tile rewrites answer with their own
  //    headers and Next's static assets are immutable files.
  const entry = (middleware.middleware || {})["/"];
  if (!entry) {
    failures.push("no middleware is registered at '/'; the policy is not being served");
  } else {
    const sources = (entry.matchers || []).map((matcher) => matcher.originalSource || "");
    const excludesProxies = sources.some(
      (source) => source.includes("?!api") && source.includes("tiles") && source.includes("_next/static"),
    );
    if (!excludesProxies) {
      failures.push(
        `the middleware matcher must exclude the api and tiles rewrites and _next/static; found: ${sources.join(" | ")}`,
      );
    }
  }

  // 3. The style policy that ships forbids inline style elements. The build
  //    folds `process.env.NODE_ENV` away, so the built middleware carries the
  //    production policy as a literal and the development exception is simply
  //    absent from it.
  const middlewarePath = join(distDir, "server", "middleware.js");
  if (!existsSync(middlewarePath)) {
    failures.push("no built middleware at .next/server/middleware.js to read the policy from");
  } else {
    const built = readFileSync(middlewarePath, "utf8");
    const elem = built.match(/style-src-elem ([^;]*)/);
    if (!elem) {
      failures.push("the built middleware declares no style-src-elem; inline style elements would fall back to style-src, which admits 'unsafe-inline'");
    } else if (elem[1].includes("unsafe-inline")) {
      failures.push(`the shipped style-src-elem admits inline style elements: ${elem[1].trim()}`);
    }
    if (!/style-src-attr 'unsafe-inline'/.test(built)) {
      failures.push("the built middleware declares no style-src-attr 'unsafe-inline'; MapLibre's own canvas and control styles, and the data-driven style props, are attributes and would be blocked");
    }
  }

  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(`CSP nonce check failed: ${failure}`);
    }
    process.exit(1);
  }
  console.log(
    `CSP check passed: 0 prerendered documents, middleware registered at '/' with ${
      (entry.matchers || []).length
    } matcher(s), and a shipped style-src-elem that admits no inline style element.`,
  );
}

main();
