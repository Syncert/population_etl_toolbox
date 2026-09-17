import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";

import { describe, expect, test } from "vitest";

// Covers: WEB-112 — a provider's fact is published by the API or it is not
// stated at all.
//
// `AGENTS.md` forbids duplicating warehouse or API rules in client code, and
// the frontend handoff forbids "a client-authored definition that could be
// mistaken for a provider fact". Four had accumulated: a Census publication
// threshold rendered as a sentence, a display name for one Census variable
// that overrode the catalog's, a Census guidance link rendered under every
// source including BLS and FBI, and a hard-coded featured metric on the home
// page.

function webRoot() {
  let directory = process.cwd();
  for (;;) {
    if (existsSync(join(directory, "apps", "web", "package.json"))) {
      return join(directory, "apps", "web");
    }
    const parent = dirname(directory);
    if (parent === directory) throw new Error(`apps/web not found from ${process.cwd()}`);
    directory = parent;
  }
}

const WEB = webRoot();

function sourceFiles() {
  const found = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory)) {
      if (entry === "node_modules" || entry.startsWith(".")) continue;
      const path = join(directory, entry);
      if (statSync(path).isDirectory()) walk(path);
      else if (/\.(ts|tsx|js|jsx)$/.test(entry)) found.push(path);
    }
  };
  for (const folder of ["app", "components", "lib"]) walk(join(WEB, folder));
  return found;
}

/**
 * Each forbidden claim, and what makes it one.
 *
 * Patterns rather than a bare grep for `B01003`, because that code also
 * appears in places this rule does not reach: a product template naming the
 * measures it composes, and a comment recounting a past identity migration.
 * Neither states a provider's rule to a reader; what is forbidden is the
 * *claim*, so each pattern matches the claim.
 */
const FORBIDDEN = [
  {
    what: "a Census population threshold",
    // The rule can change, the API publishes no field for it, and this
    // client cannot know it is still true.
    pattern: /65,000|65000\s*or\s*more/i,
  },
  {
    what: "a client-authored display name for one Census variable",
    // `metric_display_name` is the catalog's answer; an override here is a
    // name republishing the catalog cannot change.
    pattern: /endsWith\(\s*["'`]B01003_001["'`]\s*\)/,
  },
  {
    what: "a hard-coded reference link to one provider",
    // `/catalog/sources` publishes `reference_url` per source.
    pattern: /href=\{?["'`]https:\/\/www\.census\.gov/,
  },
  {
    what: "a hard-coded featured metric",
    // The home page's feature is whichever metric the catalog answers first.
    pattern: /metric_code\s*===\s*["'`]CENSUS_ACS:acs5:B01003_001["'`]/,
  },
  {
    what: "a client-authored coverage label for a dataset",
    pattern: /(complete|partial)\s+county\s+coverage/i,
  },
];

describe("no provider fact is authored in the client", () => {
  for (const { what, pattern } of FORBIDDEN) {
    test(`the client states no ${what}`, () => {
      const offences = [];
      for (const path of sourceFiles()) {
        readFileSync(path, "utf8")
          .split("\n")
          .forEach((line, index) => {
            if (pattern.test(line)) {
              offences.push(`${relative(WEB, path)}:${index + 1}: ${line.trim()}`);
            }
          });
      }
      expect(
        offences,
        "this is the API's to publish; read it from the catalog or do not say it",
      ).toEqual([]);
    });
  }

  test("the sweep reads the application, so an empty result means something", () => {
    // A sweep that found no files would pass every assertion above.
    const files = sourceFiles();
    expect(files.length).toBeGreaterThan(20);
    expect(files.some((path) => path.endsWith("SourceNote.js"))).toBe(true);
    expect(files.some((path) => path.endsWith("page.js"))).toBe(true);
  });
});
