import { describe, expect, test } from "vitest";

// Covers: WEB-082 — the workbench's series model refuses what it cannot draw
// honestly: a duplicate, a composition past its ceiling, a series with no
// geography, and a stratified source whose declared dimensions are not each
// pinned to one value. Units decide axes and nothing is normalised to share
// one; a third unit becomes small multiples rather than a third axis.
// Covers: WEB-083 — a presentation is offered only where the selection can
// answer it, every unoffered one names its reason, and a plotted series
// counts the periods that published no value rather than drawing them as
// zero.

import {
  MAX_VALUE_AXES,
  MAX_WORKBENCH_SERIES,
  UNPUBLISHED_UNIT_LABEL,
  WORKBENCH_PRESENTATIONS,
  admitSeries,
  assignValueAxes,
  availablePresentations,
  buildPlottedSeries,
  describeChart,
  describeSeries,
  presentationOffer,
  sameSeries,
  seriesKey,
  unavailablePresentations,
  unpinnedDimensions,
} from "../../../apps/web/lib/workbench";
import { buildExplorerSources } from "../../../apps/web/lib/explorerSources";

import { servedParameters } from "../support/servedContract.js";

// Shaped exactly like the served CapabilityListResponse items: the neutral
// routes with the parameters the reviewed OpenAPI snapshot declares, and each
// source's own `observation_filters`, which is what the dimension filters are
// derived from by subtraction.
const NEUTRAL_ROUTES = [
  {
    path: "/api/v1/observations",
    parameters: servedParameters("/api/v1/observations"),
  },
  {
    path: "/api/v1/observations/releases",
    parameters: servedParameters("/api/v1/observations/releases"),
  },
];

const CAPABILITIES = [
  {
    source_code: "FRED",
    display_name: "Federal Reserve Economic Data",
    route_segment: "fred",
    served_by_neutral_routes: true,
    observation_filters: ["geo_id", "geo_level", "state_fips"],
    observation_routes: NEUTRAL_ROUTES,
  },
  {
    source_code: "CDC",
    display_name: "Centers for Disease Control and Prevention",
    route_segment: "cdc",
    served_by_neutral_routes: true,
    observation_filters: ["geo_id", "stratum_id"],
    observation_routes: NEUTRAL_ROUTES,
  },
];

function sources() {
  return buildExplorerSources(CAPABILITIES);
}

function sourceFor(code) {
  return sources().find((entry) => entry.sourceCode === code) || null;
}

function series(overrides = {}) {
  return {
    sourceKey: "fred",
    sourceCode: "FRED",
    metricCode: "FRED:UNRATE",
    scope: "latest",
    geoLevel: "NATIONAL",
    geoId: "us:1",
    filters: {},
    ...overrides,
  };
}

describe("a series' identity", () => {
  test("is every field that decides which published rows it is", () => {
    expect(sameSeries(series(), series())).toBe(true);
    expect(sameSeries(series(), series({ geoId: "state:06" }))).toBe(false);
    expect(sameSeries(series(), series({ scope: "as_released" }))).toBe(false);
    expect(sameSeries(series(), series({ release: "2024-01-05" }))).toBe(false);
  });

  test("does not depend on the order the dimensions were pinned", () => {
    const one = series({ filters: { stratum_id: "OVR", sex: "F" } });
    const other = series({ filters: { sex: "F", stratum_id: "OVR" } });
    expect(seriesKey(one)).toBe(seriesKey(other));
  });
});

describe("which series may join a composition", () => {
  test("a duplicate is refused, not silently added twice", () => {
    const verdict = admitSeries({
      source: sourceFor("FRED"),
      candidate: series(),
      existing: [series()],
    });
    expect(verdict.admitted).toBe(false);
    expect(verdict.reason).toMatch(/already on the chart/);
  });

  test("the same measure at another geography is a different series", () => {
    const verdict = admitSeries({
      source: sourceFor("FRED"),
      candidate: series({ geoId: "state:06", geoLevel: "STATE" }),
      existing: [series()],
    });
    expect(verdict.admitted).toBe(true);
  });

  test("the ceiling is stated rather than enforced by truncation", () => {
    const existing = Array.from({ length: MAX_WORKBENCH_SERIES }, (_, index) =>
      series({ geoId: `state:${String(index).padStart(2, "0")}` }),
    );
    const verdict = admitSeries({
      source: sourceFor("FRED"),
      candidate: series({ geoId: "state:99" }),
      existing,
    });
    expect(verdict.admitted).toBe(false);
    expect(verdict.reason).toContain(String(MAX_WORKBENCH_SERIES));
  });

  test("a series with no geography names what is missing", () => {
    const verdict = admitSeries({
      source: sourceFor("FRED"),
      candidate: series({ geoId: "" }),
      existing: [],
    });
    expect(verdict.admitted).toBe(false);
    expect(verdict.reason).toMatch(/geography/);
    expect(verdict.reason).toMatch(/rolled up/);
  });

  test("a stratified source is refused until its dimension is pinned", () => {
    const cdc = sourceFor("CDC");
    expect(unpinnedDimensions(cdc, {})).toEqual(["stratum_id"]);

    const unpinned = admitSeries({
      source: cdc,
      candidate: series({
        sourceKey: "cdc",
        sourceCode: "CDC",
        metricCode: "CDC:cdi:X:crude",
      }),
      existing: [],
    });
    expect(unpinned.admitted).toBe(false);
    expect(unpinned.reason).toContain("stratum_id");
    expect(unpinned.reason).toMatch(/publishes separately|separately/);

    const pinned = admitSeries({
      source: cdc,
      candidate: series({
        sourceKey: "cdc",
        sourceCode: "CDC",
        metricCode: "CDC:cdi:X:crude",
        filters: { stratum_id: "OVR" },
      }),
      existing: [],
    });
    expect(pinned.admitted).toBe(true);
  });

  test("a dimension pinned to an empty value is not pinned", () => {
    expect(unpinnedDimensions(sourceFor("CDC"), { stratum_id: "" })).toEqual([
      "stratum_id",
    ]);
  });
});

describe("units decide axes", () => {
  test("one unit is one axis on the left", () => {
    const assignment = assignValueAxes([
      { key: "a", unit: "Percent" },
      { key: "b", unit: "Percent" },
    ]);
    expect(assignment.axes).toHaveLength(1);
    expect(assignment.axes[0].side).toBe("left");
    expect(assignment.axes[0].seriesKeys).toEqual(["a", "b"]);
    expect(assignment.smallMultiples).toBe(false);
  });

  test("two spellings of one unit are one unit, reported as published", () => {
    const assignment = assignValueAxes([
      { key: "a", unit: "Percent" },
      { key: "b", unit: "percent" },
    ]);
    expect(assignment.axes).toHaveLength(1);
    expect(assignment.axes[0].unit).toBe("Percent");
  });

  test("two units are two axes, and the note says the scales differ", () => {
    const assignment = assignValueAxes([
      { key: "a", unit: "Percent" },
      { key: "b", unit: "People" },
    ]);
    expect(assignment.axes.map((axis) => axis.side)).toEqual(["left", "right"]);
    expect(assignment.note).toMatch(/relative heights carry no meaning/);
    expect(assignment.smallMultiples).toBe(false);
  });

  test("a measure publishing no unit gets its own axis, labelled as such", () => {
    const assignment = assignValueAxes([
      { key: "a", unit: "Percent" },
      { key: "b", unit: null },
    ]);
    expect(assignment.axes).toHaveLength(2);
    const unpublished = assignment.axes.find((axis) => axis.unpublished);
    expect(unpublished.unit).toBe(UNPUBLISHED_UNIT_LABEL);
  });

  test("a third unit becomes small multiples, never a third axis", () => {
    const assignment = assignValueAxes([
      { key: "a", unit: "Percent" },
      { key: "b", unit: "People" },
      { key: "c", unit: "Dollars" },
    ]);
    expect(assignment.axes.length).toBeGreaterThan(MAX_VALUE_AXES);
    expect(assignment.smallMultiples).toBe(true);
    expect(assignment.note).toMatch(/small multiples/);
    expect(assignment.note).toMatch(/indexed or normalised/);
  });
});

describe("which presentations the selection can answer", () => {
  test("nothing is offered with no series, and each says so", () => {
    const offer = presentationOffer({ series: [], facts: [] });
    expect(availablePresentations(offer)).toEqual([]);
    expect(unavailablePresentations(offer)).toHaveLength(
      WORKBENCH_PRESENTATIONS.length,
    );
    for (const entry of unavailablePresentations(offer)) {
      expect(entry.reason).not.toBe("");
    }
  });

  test("a line needs two periods per series; a bar needs one", () => {
    const offer = presentationOffer({
      series: [series(), series({ geoId: "state:06" })],
      facts: [
        { key: "a", metricCode: "FRED:UNRATE", periodCount: 12 },
        { key: "b", metricCode: "FRED:CIVPART", periodCount: 1 },
      ],
    });
    expect(offer.line.available).toBe(false);
    expect(offer.line.reason).toContain("FRED:CIVPART");
    expect(offer.bar.available).toBe(true);
  });

  test("a selection that published nothing draws nothing, and says why", () => {
    const offer = presentationOffer({
      series: [series()],
      facts: [{ key: "a", metricCode: "FRED:UNRATE", periodCount: 0 }],
    });
    expect(offer.line.available).toBe(false);
    expect(offer.bar.available).toBe(false);
    expect(offer.bar.reason).toMatch(/published no value|nothing to draw/);
  });

  test("the cross-sectional reason is the caller's, presented unchanged", () => {
    const refusal = "choose which pair to draw";
    const offer = presentationOffer({
      series: [series(), series({ geoId: "state:06" }), series({ geoId: "state:36" })],
      facts: [
        { key: "a", metricCode: "A", periodCount: 4 },
        { key: "b", metricCode: "B", periodCount: 4 },
        { key: "c", metricCode: "C", periodCount: 4 },
      ],
      crossSectionalReason: refusal,
    });
    expect(offer.scatter.reason).toBe(refusal);
    expect(offer.ranking.reason).toBe(refusal);
    expect(offer.line.available).toBe(true);
  });

  test("the correlation carries its own reason, not the scatter's", () => {
    // A scatter and a ranking read `/comparison` for one chosen pair; a
    // correlation reads `/comparison/matrix` for up to eight measures at
    // once. One reason for both would have to be the stricter of the two,
    // which would withhold a correlation the API would serve.
    const offer = presentationOffer({
      series: [series(), series({ geoId: "state:06" }), series({ geoId: "state:36" })],
      facts: [
        { key: "a", metricCode: "A", periodCount: 4 },
        { key: "b", metricCode: "B", periodCount: 4 },
        { key: "c", metricCode: "C", periodCount: 4 },
      ],
      crossSectionalReason: "choose which pair to draw",
      correlationReason: "",
    });
    expect(offer.scatter.available).toBe(false);
    expect(offer.correlation.available).toBe(true);

    const declined = presentationOffer({
      series: [series(), series({ geoId: "state:06" })],
      facts: [
        { key: "a", metricCode: "A", periodCount: 4 },
        { key: "b", metricCode: "B", periodCount: 4 },
      ],
      crossSectionalReason: "",
      correlationReason: "source 'CDC' publishes stratified observations",
    });
    expect(declined.scatter.available).toBe(true);
    expect(declined.correlation.available).toBe(false);
    expect(declined.correlation.reason).toMatch(/stratified/);
  });
});

describe("rows become points, and nothing absent becomes a zero", () => {
  const rows = [
    { period_start: "2023-03-01", period_end: "2023-03-01", value: "4.2" },
    { period_start: "2023-01-01", period_end: "2023-01-01", value: "3.9" },
    { period_start: "2023-02-01", period_end: "2023-02-01", value: null },
    { period_start: "2023-04-01", period_end: "2023-04-01", value: "" },
  ];

  test("an unpublished period is counted, not plotted", () => {
    const plotted = buildPlottedSeries({
      series: series(),
      rows,
      label: "Unemployment rate",
      unit: "Percent",
    });
    expect(plotted.points.map((point) => point.value)).toEqual([3.9, 4.2]);
    expect(plotted.droppedPeriods).toBe(2);
    expect(plotted.points.every((point) => point.value !== 0)).toBe(true);
  });

  test("points are ordered by published period, not by arrival", () => {
    const plotted = buildPlottedSeries({ series: series(), rows, unit: "Percent" });
    expect(plotted.points.map((point) => point.period)).toEqual([
      "2023-01-01",
      "2023-03-01",
    ]);
  });

  test("a measure with no published unit says so rather than borrowing one", () => {
    const plotted = buildPlottedSeries({ series: series(), rows, unit: "  " });
    expect(plotted.unit).toBe(UNPUBLISHED_UNIT_LABEL);
    expect(plotted.unitUnpublished).toBe(true);
  });
});

describe("what the chart and its legend say", () => {
  test("a legend entry names source, measure, grain and geography", () => {
    const plotted = buildPlottedSeries({
      series: series({ geoLevel: "COUNTY", geoId: "county:06001" }),
      rows: [{ period_start: "2023-01-01", value: "1" }],
      label: "Unemployment rate",
      unit: "Percent",
    });
    const sentence = describeSeries(plotted, "Alameda County");
    expect(sentence).toContain("FRED");
    expect(sentence).toContain("Unemployment rate");
    expect(sentence).toContain("County");
    expect(sentence).toContain("Alameda County");
  });

  test("a pinned release and pinned dimensions ride the legend", () => {
    const plotted = buildPlottedSeries({
      series: series({ release: "2024-01-05", filters: { stratum_id: "OVR" } }),
      rows: [],
      unit: "Percent",
    });
    const sentence = describeSeries(plotted);
    expect(sentence).toContain("release 2024-01-05");
    expect(sentence).toContain("stratum_id: OVR");
  });

  test("the accessible label says what is drawn and what is not", () => {
    const drawn = buildPlottedSeries({
      series: series(),
      rows: [
        { period_start: "2023-01-01", value: "1" },
        { period_start: "2023-02-01", value: null },
      ],
      unit: "Percent",
    });
    const truncated = buildPlottedSeries({
      series: series({ geoId: "state:06" }),
      rows: [{ period_start: "2023-01-01", value: "2" }],
      unit: "Percent",
      truncated: true,
    });
    const label = describeChart("line", [drawn, truncated]);
    expect(label).toContain("2 series");
    expect(label).toContain("Percent");
    expect(label).toMatch(/1 period published no value/);
    expect(label).toMatch(/cut short by the page bound/);
  });
});
