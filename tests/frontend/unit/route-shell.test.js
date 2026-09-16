import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";

import { describe, expect, test } from "vitest";

// Covers: WEB-107 — the route segment's own failure pages exist, and the
// question of whether any route needs a `loading.js` is answered from the
// routes themselves rather than from memory.

/** The repository root, found by walking up for `apps/web`. */
function webRoot() {
  let directory = process.cwd();
  for (;;) {
    const candidate = join(directory, "apps", "web", "package.json");
    if (existsSync(candidate)) return join(directory, "apps", "web");
    const parent = dirname(directory);
    if (parent === directory) throw new Error(`apps/web not found from ${process.cwd()}`);
    directory = parent;
  }
}

const APP = join(webRoot(), "app");

describe("the route segment's shell", () => {
  test("a failed route and an unknown address have a page of this site's own", () => {
    // Without these, Next serves its own built-in page for either case: no
    // header, no navigation, no heading structure, nothing to click. The
    // whole segment fell out of the application on any throw.
    for (const file of ["error.js", "not-found.js", "global-error.js"]) {
      const source = readFileSync(join(APP, file), "utf8");
      // The landmarks the accessibility audit asserts on every core route. A
      // failure page is a route a reader can be on.
      expect(source, `${file} has no main landmark`).toMatch(/<main/);
      expect(source, `${file} has no top-level heading`).toMatch(/<h1>/);
    }

    // An error boundary is a client component; Next refuses it otherwise.
    for (const file of ["error.js", "global-error.js"]) {
      expect(readFileSync(join(APP, file), "utf8").startsWith('"use client"')).toBe(true);
    }

    // `global-error.js` replaces the document, because the layout that would
    // have contained it is the thing that failed.
    const globalError = readFileSync(join(APP, "global-error.js"), "utf8");
    expect(globalError).toMatch(/<html/);
    expect(globalError).toMatch(/<body>/);
  });

  test("no route awaits data on the server, so none needs a loading state", () => {
    // `loading.js` shows while a *server* component awaits. Every route here
    // is a client component that fetches after mount -- where the page's own
    // status surface reports the wait -- or a redirect that renders nothing.
    // Neither can show a loading state, so none is added.
    //
    // The rule is "nothing awaits on the server", not "nothing is server
    // rendered": a server wrapper that only renders a client component (which
    // is what exporting per-route `metadata` would turn these into) still
    // awaits nothing and still needs no loading state. What would need one is
    // a page that awaits, and that is what this fails on.
    const awaiting = [];
    let pages = 0;
    for (const entry of readdirSync(APP)) {
      const page = join(APP, entry, "page.js");
      if (!statSync(join(APP, entry)).isDirectory() || !existsSync(page)) continue;
      pages += 1;
      const source = readFileSync(page, "utf8");
      if (source.startsWith('"use client"')) continue;
      if (/\bawait\b/.test(source) || /export default async/.test(source)) {
        awaiting.push(entry);
      }
    }

    expect(pages).toBeGreaterThan(0);
    expect(
      awaiting,
      "these routes await on the server, so each needs a loading.js",
    ).toEqual([]);
    expect(existsSync(join(APP, "loading.js"))).toBe(false);
  });
});
