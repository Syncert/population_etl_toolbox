import { describe, expect, test } from "vitest";

// Covers: WEB-021 — the first-wave products are configuration over stable
// catalog identities. Every slot resolves to an identity the catalog
// actually published and displays it, a slot nothing satisfies is reported
// rather than dropped or filled with a similar measure, and partial source
// coverage leaves a stated gap instead of collapsing the product.

import {
  DEFAULT_TEMPLATE_ID,
  PRODUCT_TEMPLATES,
  findTemplate,
  resolveTemplate,
  templateCoverage,
  templateMetricCodes,
} from "../../../apps/web/lib/productTemplates";
import {
  parseProfileState,
  profileHref,
  serializeProfileState,
} from "../../../apps/web/lib/urlState";

const catalogIndex = (codes) =>
  new Map(
    codes.map((code) => [
      code,
      {
        metric_code: code,
        metric_display_name: `Published name for ${code}`,
        source_code: code.split(":")[0],
        units: "people",
        freshness_state: "fresh",
      },
    ]),
  );

describe("the first-wave products are configuration", () => {
  test("all three products exist and declare their own limits", () => {
    expect(PRODUCT_TEMPLATES.map((template) => template.id)).toEqual([
      "community-conditions",
      "population-growth",
      "workforce",
    ]);
    expect(DEFAULT_TEMPLATE_ID).toBe("community-conditions");
    for (const template of PRODUCT_TEMPLATES) {
      // Each product states what it does not claim, so a reader is never
      // left to infer that adjacent measures explain each other.
      expect(template.limits.length).toBeGreaterThan(20);
      expect(template.sections.length).toBeGreaterThan(0);
      for (const section of template.sections) {
        for (const measure of section.measures) {
          // A slot with no candidate identity could only be filled by
          // guessing, which is exactly what these templates must not do.
          expect(measure.candidates.length).toBeGreaterThan(0);
        }
      }
    }
    expect(findTemplate("workforce")?.title).toContain("Workforce");
    expect(findTemplate("not-a-template")).toBeNull();
    expect(findTemplate(null)).toBeNull();
  });

  test("the community profile spans the cross-source conditions it names", () => {
    const community = findTemplate("community-conditions");
    const sources = new Set(
      templateMetricCodes(community).map((code) => code.split(":")[0]),
    );
    // Health, safety, and rural context come from their own sources rather
    // than being inferred from the Census measures beside them.
    expect(sources).toContain("CDC");
    expect(sources).toContain("FBI_UCR");
    expect(sources).toContain("USDA_NASS");
    expect(sources).toContain("CENSUS_PEP");
    expect(sources).toContain("BLS");
    expect(templateMetricCodes(null)).toEqual([]);
  });
});

describe("slots resolve only to identities the catalog published", () => {
  test("the first published candidate fills the slot and is displayed", () => {
    const template = findTemplate("community-conditions");
    // Only the second candidate for total population is published here.
    const resolved = resolveTemplate(
      template,
      catalogIndex(["ACS:acs1:B01003_001", "BLS:LAU:UNEMP_RATE"]),
    );
    const population = resolved[0].measures[0];
    expect(population.available).toBe(true);
    expect(population.metricCode).toBe("ACS:acs1:B01003_001");
    // The resolved identity and the publisher's own name travel with it, so
    // the reader sees which measure answered rather than the slot's label.
    expect(population.metric.metric_display_name).toContain("ACS:acs1:B01003_001");
    expect(population.metric.source_code).toBe("ACS");
  });

  test("a slot nothing satisfies reports what it looked for", () => {
    const template = findTemplate("community-conditions");
    const resolved = resolveTemplate(template, catalogIndex(["ACS:acs5:B01003_001"]));
    const health = resolved.find((entry) => entry.section.id === "health").measures[0];
    expect(health.available).toBe(false);
    expect(health.metric).toBeNull();
    expect(health.metricCode).toBe("");
    // The reason names the identities, so an operator can see whether the
    // measure is missing or the template is pointing at the wrong code.
    expect(health.reason).toContain("CDC:cdi:ALC1_1:crude");
    expect(health.reason).toContain("no published measure");
  });

  test("an unfilled slot is never filled by a similar measure", () => {
    const template = findTemplate("community-conditions");
    // The catalog publishes an ACS measure but none of the safety
    // candidates. The safety slot must stay empty rather than borrowing it.
    const resolved = resolveTemplate(template, catalogIndex(["ACS:acs5:B01003_001"]));
    const safety = resolved.find((entry) => entry.section.id === "safety").measures[0];
    expect(safety.available).toBe(false);
    expect(safety.metricCode).not.toBe("ACS:acs5:B01003_001");
  });

  test("partial coverage is counted, not hidden", () => {
    const template = findTemplate("community-conditions");
    const full = resolveTemplate(template, catalogIndex(templateMetricCodes(template)));
    const fullCoverage = templateCoverage(full);
    expect(fullCoverage.unavailable).toBe(0);
    expect(fullCoverage.available).toBe(fullCoverage.requested);

    const partial = resolveTemplate(template, catalogIndex(["ACS:acs5:B01003_001"]));
    const partialCoverage = templateCoverage(partial);
    expect(partialCoverage.available).toBe(1);
    expect(partialCoverage.unavailable).toBe(partialCoverage.requested - 1);
    // Every section survives an empty catalog: a profile that dropped its
    // gaps would read as though the place had no such conditions.
    const empty = resolveTemplate(template, new Map());
    expect(empty).toHaveLength(template.sections.length);
    expect(templateCoverage(empty).available).toBe(0);
    expect(templateCoverage(null)).toEqual({ requested: 0, available: 0, unavailable: 0 });
  });

  test("an absent template resolves to nothing rather than a default product", () => {
    expect(resolveTemplate(null, catalogIndex([]))).toEqual([]);
    expect(resolveTemplate(findTemplate("workforce"), null)[0].measures[0].available).toBe(
      false,
    );
  });
});

describe("profile URL state", () => {
  test("names the template and the place, and carries no values", () => {
    const state = { template: "workforce", geoId: "state:55|county:025" };
    expect(parseProfileState(`?${serializeProfileState(state)}`)).toEqual(state);
    expect(profileHref(state, { template: DEFAULT_TEMPLATE_ID })).toContain(
      "/profiles?template=workforce",
    );
    // Reopening re-asks the catalog and the observations, so a shared link
    // can never present a place as it looked when the link was made.
    expect(serializeProfileState(state)).not.toContain("value");
  });

  test("drops invalid template ids and omits the default", () => {
    expect(parseProfileState("?template=Not%2FValid&place=x")).toEqual({ geoId: "x" });
    expect(parseProfileState("")).toEqual({});
    expect(
      serializeProfileState({ template: DEFAULT_TEMPLATE_ID }, { template: DEFAULT_TEMPLATE_ID }),
    ).toBe("");
    expect(profileHref({}, {})).toBe("/profiles");
  });
});
