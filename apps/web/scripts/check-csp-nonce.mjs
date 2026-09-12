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

  // 1. Nothing is prerendered. A prerendered route would ship script tags
  //    with no nonce against a policy that demands one.
  const prerendered = Object.keys(prerender.routes || {});
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

  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(`CSP nonce check failed: ${failure}`);
    }
    process.exit(1);
  }
  console.log(
    `CSP nonce check passed: 0 prerendered routes, middleware registered at '/' with ${
      (entry.matchers || []).length
    } matcher(s).`,
  );
}

main();
