import { existsSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { EXPLAINER_SECTIONS, explainerProblems, parseExplainer } from "../../../apps/web/lib/explainerContent";
import { EXPLAINER_INDEX, explainerForCaveat } from "../../../apps/web/lib/explainerIndex";
import { lastPlaceRows } from "../../../apps/web/lib/explainerExample";
import { LAST_PLACE_KEY, readLastPlace, rememberLastPlace } from "../../../apps/web/lib/lastPlace";
import { PRODUCT_TEMPLATES } from "../../../apps/web/lib/productTemplates";
import { PUBLIC_ROUTES } from "../../../apps/web/lib/siteMap";

// Covers: WEB-126 — the explainer format, the index the client links from,
// the caveat-key linking contract, and the worked example's place rule.

const here = path.dirname(fileURLToPath(import.meta.url));
const repository = path.resolve(here, "../../..");
const directory = path.join(repository, "apps/web/content/explainers");
const files = readdirSync(directory).filter((name) => name.endsWith(".md")).sort();
const read = (name) => readFileSync(path.join(directory, name), "utf8");

// The catalog identities the reviewed product templates name: each was
// resolved against a deployed warehouse's publisher views before it was
// written down (lib/productTemplates.ts), so an explainer may cite them.
const known = new Set(PRODUCT_TEMPLATES.flatMap((template) => template.sections.flatMap((section) => section.measures.flatMap((measure) => measure.candidates))));

describe("explainer files", () => {
  it("are the first twelve", () => {
    expect(files).toHaveLength(12);
  });

  for (const name of files) {
    it(`${name} validates against the explainer format`, () => {
      const source = read(name);
      const explainer = parseExplainer(name.slice(0, -3), source);
      expect(explainerProblems(explainer, source, known)).toEqual([]);
      for (const definition of explainer.definitions) {
        expect(existsSync(path.join(repository, definition)), definition).toBe(true);
      }
    });
  }

  it("are listed once in the client index with their own titles and caveats", () => {
    const parsed = files.map((name) => parseExplainer(name.slice(0, -3), read(name)));
    expect(EXPLAINER_INDEX.map((entry) => ({ ...entry, caveatKeys: [...entry.caveatKeys] }))).toEqual(
      parsed.map((explainer) => ({ slug: explainer.slug, title: explainer.title, caveatKeys: explainer.caveatKeys })),
    );
    const keys = parsed.flatMap((explainer) => explainer.caveatKeys);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("are in the sitemap with their index", () => {
    expect(PUBLIC_ROUTES).toContain("/explain");
    for (const entry of EXPLAINER_INDEX) expect(PUBLIC_ROUTES).toContain(`/explain/${entry.slug}`);
  });
});

describe("the format refuses what it cannot render honestly", () => {
  const valid = read("unemployment-rate.md");
  const problems = (source) => explainerProblems(parseExplainer("x", source), source, known);

  it("refuses HTML, unknown fields, and an unknown metric", () => {
    expect(problems(valid.replace("## Short answer\n", "## Short answer\n<script>x</script>\n"))).toContain("explainers carry no HTML");
    expect(problems(valid.replace("reviewer:", "author: someone\nreviewer:"))).toContain('unknown frontmatter field "author"');
    expect(problems(valid.replace("metric_codes: [BLS:LAU:UNEMP_RATE]", "metric_codes: [BLS:LAU:UNEMP_RATE, NOPE:1]"))).toContain("metric_code NOPE:1 is not a known catalog identity");
    expect(problems(valid.replace("reviewed: 2026-10-06", "reviewed: soon"))).toContain("reviewed is not a YYYY-MM-DD date");
  });

  it("requires the four sections in order", () => {
    expect(() => parseExplainer("x", valid.replace("## Worked example", "## Example"))).toThrow('unknown section "Example"');
    const missing = valid.slice(0, valid.indexOf("## Where it is used"));
    expect(problems(missing)).toContain(`sections must be exactly, in order: ${EXPLAINER_SECTIONS.join(", ")}`);
  });

  it("parses paragraphs and lists", () => {
    const explainer = parseExplainer("unemployment-rate", valid);
    expect(explainer.sections[1].blocks[0]).toMatchObject({ kind: "list" });
    expect(explainer.sections[1].blocks[0].items).toHaveLength(3);
    expect(explainer.sections[0].blocks).toHaveLength(2);
    expect(explainer.videoUrl).toBe("");
  });
});

describe("chart surfaces link the caveats they declare", () => {
  it("resolves every caveat key a product template declares", () => {
    const declared = PRODUCT_TEMPLATES.flatMap((template) => template.sections.flatMap((section) => section.measures.map((measure) => measure.caveat).filter(Boolean)));
    expect(declared.length).toBeGreaterThan(0);
    for (const key of declared) expect(explainerForCaveat(key), key).not.toBeNull();
  });

  it("renders no link for a caveat no explainer answers", () => {
    expect(explainerForCaveat("no-such-caveat")).toBeNull();
    expect(explainerForCaveat(undefined)).toBeNull();
  });
});

describe("the worked example's places", () => {
  const nation = { geo_id: "us:1", geo_level: "NATIONAL" };

  it("shows the nation alone, and says so, with no last place", () => {
    const plan = lastPlaceRows(null, nation, ["COUNTY", "NATIONAL"]);
    expect(plan.targets.map((target) => target.key)).toEqual(["nation"]);
    expect(plan.note).toMatch(/national value/);
  });

  it("adds the last place where the measure publishes its grain", () => {
    const place = { geoId: "state:55|county:025", name: "Dane County, Wisconsin", level: "COUNTY" };
    expect(lastPlaceRows(place, nation, ["COUNTY", "NATIONAL"]).targets).toMatchObject([
      { key: "place", geoId: "state:55|county:025" }, { key: "nation", geoId: "us:1" },
    ]);
    expect(lastPlaceRows(place, nation, ["STATE", "NATIONAL"]).targets[0]).toMatchObject({ geoId: "", message: "Not published at county grain" });
    expect(lastPlaceRows(place, nation, ["COUNTY", "STATE"]).targets[1]).toMatchObject({ geoId: "", message: "Not published at national grain" });
  });

  it("keeps the place in session storage and reads blocked storage as none", () => {
    const store = new Map();
    const storage = { getItem: (key) => store.get(key) ?? null, setItem: (key, value) => store.set(key, value) };
    rememberLastPlace(storage, { geoId: "state:55", name: "Wisconsin", level: "STATE" });
    expect(JSON.parse(store.get(LAST_PLACE_KEY))).toEqual({ geoId: "state:55", name: "Wisconsin", level: "STATE" });
    expect(readLastPlace(storage)).toEqual({ geoId: "state:55", name: "Wisconsin", level: "STATE" });
    expect(readLastPlace({ getItem: () => { throw new Error("blocked"); } })).toBeNull();
    expect(readLastPlace({ getItem: () => "{\"geoId\":42}" })).toBeNull();
  });
});
