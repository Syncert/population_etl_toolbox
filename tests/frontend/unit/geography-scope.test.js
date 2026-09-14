import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, test } from "vitest";

// Covers: DB-038 — every geography picker asks the catalog for active
// geographies. A geography a new boundary vintage stops listing is now
// published as `geography_state: "retired"` instead of being deleted, because
// the served relations still hold its observations. That is right for a
// client resolving a served row and wrong for a picker, whose question is
// "which geography do I want to look at now" — so the six pickers pass
// `active_only`. They each used to spell their own params object, which is
// how a choice like this drifts; this reads the components rather than
// trusting them.

import { ACTIVE_GEOGRAPHIES_ONLY } from "../../../apps/web/lib/observationAccess";

const COMPONENTS = join(process.cwd(), "..", "..", "apps", "web", "components");

function componentSources() {
  return readdirSync(COMPONENTS)
    .filter((name) => name.endsWith(".tsx"))
    .map((name) => ({ name, source: readFileSync(join(COMPONENTS, name), "utf8") }));
}

describe("the geography scope is declared once and used everywhere", () => {
  test("the declaration is the catalog's own parameter", () => {
    expect(ACTIVE_GEOGRAPHIES_ONLY).toEqual({ active_only: "true" });
  });

  test("every component that reads the geography catalog passes it", () => {
    const readers = componentSources().filter(({ source }) =>
      source.includes("/catalog/geographies"),
    );
    expect(readers.length).toBeGreaterThan(0);

    const offenders = readers
      .filter(({ source }) => !source.includes("ACTIVE_GEOGRAPHIES_ONLY"))
      .map(({ name }) => name);
    expect(offenders).toEqual([]);
  });

  test("no geography read spells the parameter itself", () => {
    // `active_only` is also a metric-catalog filter, and components spell it
    // there on purpose, so this looks only at the geography calls: the
    // parameter object each `/catalog/geographies` fetch passes.
    const offenders = [];
    for (const { name, source } of componentSources()) {
      for (const call of source.split("/catalog/geographies").slice(1)) {
        const parameters = call.slice(0, call.indexOf("}"));
        if (/active_only/.test(parameters)) {
          offenders.push(`${name}: ${parameters.trim()}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });
});
