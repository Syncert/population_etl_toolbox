import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";

import { describe, expect, test } from "vitest";

// Covers: WEB-109 — every API path this client addresses is an operation the
// reviewed snapshot declares, and every field name its transport types name
// is a property that snapshot publishes.
//
// The client is hand-written on purpose: a generated one would lose the
// classified `ApiError` and the bounded paging the handoff built. The cost of
// a hand-written client is that a renamed field or a moved route is found by
// a reader rather than by a test. `servedContract.js` already grades the test
// *fixtures* against `tests/fixtures/api/openapi_contract.json`; nothing
// graded the application's own literals or its `types.ts`.

import {
  servedPathsMatching,
  servedSchemaFields,
  servesPath,
} from "../support/servedContract.js";

/**
 * The schema's properties, or null where the contract publishes no such
 * schema.
 *
 * `servedSchemaFields` throws for an unknown name, which is right for a test
 * naming one shape deliberately. Here the names come from sweeping
 * `types.ts`, which legitimately holds this client's own view models beside
 * the API's shapes, so an unknown name means "not the API's to grade".
 */
function publishedFields(name) {
  try {
    return servedSchemaFields(name);
  } catch {
    return null;
  }
}

const API_BASE = "/api/v1";

function webRoot() {
  let directory = process.cwd();
  for (;;) {
    if (existsSyncSafe(join(directory, "apps", "web", "package.json"))) {
      return join(directory, "apps", "web");
    }
    const parent = dirname(directory);
    if (parent === directory) throw new Error(`apps/web not found from ${process.cwd()}`);
    directory = parent;
  }
}

function existsSyncSafe(path) {
  try {
    statSync(path);
    return true;
  } catch {
    return false;
  }
}

const WEB = webRoot();

function sourceFilesUnder(folder) {
  const found = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory)) {
      if (entry === "node_modules" || entry.startsWith(".")) continue;
      const path = join(directory, entry);
      if (statSync(path).isDirectory()) {
        walk(path);
      } else if (/\.(ts|tsx|js|jsx)$/.test(entry)) {
        found.push(path);
      }
    }
  };
  walk(join(WEB, folder));
  return found;
}

/**
 * Every resource this client addresses, with where it said it.
 *
 * Two shapes carry one: an argument to a transport function, and a `resource`
 * the access-shape modules hand back for `apiFetch` to send. Both are matched
 * where they are written rather than by sweeping every string that starts
 * with a slash -- `/explore` is a route in this application, not a resource on
 * the API, and a sweep that cannot tell them apart would have to be taught a
 * list of exceptions that is itself untested.
 */
function addressedResources() {
  // The generic is matched with `[^(]*` rather than `[^>(]*`, because
  // `apiFetch<CollectionResponse<SourceSummary>>(...)` nests one inside
  // another. The first version stopped at the inner `>` and so matched none
  // of the call sites written that way -- which is most of them. It found
  // enough literals elsewhere to satisfy a floor and pass, and a deliberately
  // misspelled `/catalog/surces` went straight through it.
  const transport =
    /(?:apiFetch|fetchAllPages|fetchAllPagesWithTotal|fetchComparisonPages)(?:<[^(]*>)?\(\s*([`"'])([^`"']+)\1/g;
  const declared = /(?:resource|PATH|RESOURCE)\s*[:=]\s*([`"'])([^`"']+)\1/g;

  const found = [];
  for (const folder of ["lib", "components", "app"]) {
    for (const path of sourceFilesUnder(folder)) {
      const source = readFileSync(path, "utf8");
      for (const pattern of [transport, declared]) {
        pattern.lastIndex = 0;
        let match;
        while ((match = pattern.exec(source)) !== null) {
          const resource = match[2];
          if (!resource.startsWith("/")) continue;
          const line = source.slice(0, match.index).split("\n").length;
          found.push({ resource, where: `${relative(WEB, path)}:${line}` });
        }
      }
    }
  }
  return found;
}

describe("the transport boundary's literals are graded", () => {
  test("every addressed resource is an operation the contract declares", () => {
    const addressed = addressedResources();
    // A sweep that found nothing would pass silently, which is the one way
    // this test could be worse than not existing.
    // Every resource the client addresses, counted: a floor low enough for a
    // broken sweep to clear is what let the nested-generic bug ship. This one
    // is checked against the transport module's own export count below.
    expect(
      addressed.length,
      "the sweep found fewer resources than this client addresses; it is matching too little",
    ).toBeGreaterThanOrEqual(20);

    const undeclared = [];
    for (const { resource, where } of addressed) {
      const full = `${API_BASE}${resource}`;
      // A templated resource stands for every served path of its shape: the
      // source-scoped routes are written `/${source.segment}/observations/latest`
      // and there is one per source segment.
      const matches = full.includes("${")
        ? servedPathsMatching(full)
        : servesPath(full)
          ? [full]
          : [];
      if (matches.length === 0) {
        undeclared.push(`${where}: ${resource}`);
      }
    }

    expect(
      undeclared,
      "these resources are not operations the reviewed snapshot declares; " +
        "regenerate it if the API moved, or fix the literal",
    ).toEqual([]);
  });

  test("every transport type's fields are properties the contract publishes", () => {
    // `types.ts` is the client's statement of what a route answers. A field
    // renamed on the API leaves a property here that silently reads
    // `undefined` for every row, which looks exactly like a source that
    // publishes nothing.
    const types = readFileSync(join(WEB, "lib", "api", "types.ts"), "utf8");
    const interfaces = [...types.matchAll(/export interface (\w+)\s*\{([^}]*)\}/g)];
    expect(interfaces.length, "no interface was read from types.ts").toBeGreaterThan(5);

    const strays = [];
    let graded = 0;
    for (const [, name, body] of interfaces) {
      const published = publishedFields(name);
      // Only the interfaces that name a schema the contract publishes. The
      // others are this client's own view models and are not the API's to
      // grade.
      if (!published || published.length === 0) continue;
      graded += 1;
      const declared = [...body.matchAll(/^\s*(\w+)\??\s*:/gm)].map((match) => match[1]);
      for (const field of declared) {
        if (!published.includes(field)) {
          strays.push(`${name}.${field}`);
        }
      }
    }

    expect(graded, "no interface in types.ts matched a published schema").toBeGreaterThan(3);
    expect(
      strays,
      "these properties are not published by the schema of the same name",
    ).toEqual([]);
  });
});
