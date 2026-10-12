import { describe, expect, it } from "vitest";

import { FRAME_FORMATS, SITE_NAME, footerLines, frameRecord, renderFrameSvg, scriptNotes, studioRecord, wrapText } from "../../../apps/web/lib/studioFrame";

// Covers: WEB-132 — every frame carries its provenance footer in every
// format, the record keeps the exact requests and releases, and script notes
// say whether a definition was reviewed.

const spec = (format = "16:9", overrides = {}) => ({
  format, theme: "light", title: "Median household income: Dane County, Wisconsin",
  metricCode: "CENSUS_ACS:acs5:B19013_001", measureName: "Median household income", unit: "dollars",
  period: "2020-01-01 – 2024-12-31", source: "CENSUS_ACS", caveat: "ACS estimates carry a margin of error",
  rows: [
    { level: "COUNTY", name: "Dane County", value: 92000, valueText: "92,000 dollars", uncertainty: "margin of error ±1,200" },
    { level: "STATE", name: "Wisconsin", value: 74000, valueText: "74,000 dollars", uncertainty: "" },
    { level: "NATIONAL", name: "United States", value: null, valueText: "not published", uncertainty: "" },
  ],
  ...overrides,
});

describe("frames", () => {
  for (const format of Object.keys(FRAME_FORMATS)) {
    it(`draws the ${format} frame at its size with the footer`, () => {
      const svg = renderFrameSvg(spec(format));
      const { width, height } = FRAME_FORMATS[format];
      expect(svg).toContain(`width="${width}" height="${height}"`);
      const footer = svg.slice(svg.indexOf('<g data-frame-footer="true">'));
      expect(footer).toContain("Median household income (CENSUS_ACS:acs5:B19013_001)");
      expect(footer).toContain("Period: 2020-01-01 – 2024-12-31");
      expect(footer).toContain("Source: CENSUS_ACS");
      expect(footer).toContain("Uncertainty: Dane County margin of error ±1,200");
      expect(footer).toContain(SITE_NAME);
    });
  }

  it("has no way to leave the footer out", () => {
    expect(renderFrameSvg.length).toBe(1);
    const stripped = renderFrameSvg({ ...spec(), footer: false, showFooter: false });
    expect(stripped).toContain('data-frame-footer="true"');
    expect(footerLines(spec("1:1", { period: "", source: "" }))).toEqual([
      "Median household income (CENSUS_ACS:acs5:B19013_001)",
      "Period: not published · Source: not published",
      "Uncertainty: Dane County margin of error ±1,200",
      `ACS estimates carry a margin of error · ${SITE_NAME}`,
    ]);
  });

  it("escapes text and draws no bar for an unpublished value", () => {
    const svg = renderFrameSvg(spec("16:9", { title: "A <b> & \"c\"" }));
    expect(svg).toContain("A &lt;b&gt; &amp; &quot;c&quot;");
    expect(svg).not.toContain("<b>");
    expect((svg.match(/<rect /g) || []).length).toBe(1 + 2);
    expect(wrapText("one two three four", 9)).toEqual(["one two", "three", "four"]);
  });
});

describe("frame records", () => {
  const requests = [
    { level: "COUNTY", geoId: "state:55|county:025", url: "/api/v1/observations?metric_code=M&scope=latest&geo_id=state%3A55%7Ccounty%3A025&limit=1&newest_per_geography=true" },
    { level: "STATE", geoId: "state:55", url: "/api/v1/observations?metric_code=M&scope=latest&geo_id=state%3A55&limit=1&newest_per_geography=true" },
  ];
  const record = frameRecord({ spec: spec(), placePath: "/us/wisconsin/dane-county", chapterId: "work-money", measureId: "median-household-income", requests, releases: { "state:55": "2024-acs5" }, newestPerGeography: true });

  it("stores the exact requests and the release pin, as observations series", () => {
    expect(record.kind).toBe("workbench");
    expect(record.series.map((series) => series.filters.geo_id)).toEqual(["state:55|county:025", "state:55"]);
    expect(record.series.every((series) => series.newest_per_geography && series.scope === "latest")).toBe(true);
    const unreduced = frameRecord({ spec: spec(), placePath: "/us", chapterId: "c", measureId: "m", requests, releases: {}, newestPerGeography: false });
    expect(unreduced.series.every((series) => series.newest_per_geography === false)).toBe(true);
    expect(record.visualization.studio.requests).toEqual(requests);
    expect(record.visualization.studio.releases).toEqual({ "state:55": "2024-acs5" });
    expect(JSON.stringify(record)).not.toMatch(/token|authorization/i);
  });

  it("reads a stored record back, and refuses a document that is not a frame", () => {
    expect(studioRecord(record)).toMatchObject({ format: "16:9", placePath: "/us/wisconsin/dane-county", measureId: "median-household-income" });
    expect(studioRecord({ kind: "observations", visualization: {} })).toBeNull();
    expect(studioRecord({ visualization: { studio: { version: 1, format: "4:3", requests: [] } } })).toBeNull();
  });
});

describe("script notes", () => {
  it("uses a reviewed definition where one exists", () => {
    expect(scriptNotes({ harvestedLabel: "Estimate!!Median", definition: { text: "The middle household income.", reviewedOn: "2026-10-01" }, caveats: ["a", ""], period: "2024" }))
      .toEqual({ definition: "The middle household income.", reviewState: "reviewed", caveats: ["a"], period: "These figures describe 2024.", explainer: null });
  });

  it("falls back to the harvested label, marked not reviewed", () => {
    const notes = scriptNotes({ harvestedLabel: "Estimate!!Median", definition: null, caveats: [], period: "" });
    expect(notes.definition).toBe("Estimate!!Median");
    expect(notes.reviewState).toBe("not reviewed");
    expect(notes.period).toBe("No period was published for these figures.");
  });
});
