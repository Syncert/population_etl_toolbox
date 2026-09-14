import { describe, expect, test } from "vitest";

// Covers: WEB-087 — the grains a set of measures can be read at together are
// the intersection of what they publish, computed once for two measures and
// for eight, and each absent grain names the measures that removed it. The
// cross-sectional presentations refuse, with a reason, for fewer than two
// measures, for more than two until the matrix route lands, and where the
// set shares no grain.
// Covers: WEB-088 — a national measure joins a cross-sectional chart only as
// a reference line, only where its sole published grain is NATIONAL and its
// unit is the axis's, and never on a time axis.
// Covers: WEB-089 — the geography × period heatmap draws a cell for every
// pair in its rectangle, hatches the ones that published no value rather
// than colouring them, and states its own cap instead of truncating quietly.

import {
  MAX_HEATMAP_GEOGRAPHIES,
  MAX_HEATMAP_PERIODS,
  crossSectionalPair,
  crossSectionalRefusal,
  heatmapModel,
  referenceLineOffer,
} from "../../../apps/web/lib/workbench";
import {
  comparisonGrainOffer,
  sharedGrainOffer,
} from "../../../apps/web/lib/comparison";

function metric(code, grains) {
  return { metric_code: code, valid_geo_grains: grains };
}

function series(metricCode, geoId, geoLevel = "COUNTY") {
  return {
    sourceKey: "fred",
    sourceCode: "FRED",
    metricCode,
    scope: "latest",
    geoLevel,
    geoId,
    filters: {},
  };
}

describe("the grains a set of measures can be read at", () => {
  test("are the intersection, in the vocabulary's order", () => {
    const offer = sharedGrainOffer({
      metrics: [
        metric("A", ["NATIONAL", "STATE", "COUNTY"]),
        metric("B", ["STATE", "COUNTY", "PLACE"]),
        metric("C", ["COUNTY", "STATE"]),
      ],
    });
    expect(offer.levels).toEqual(["STATE", "COUNTY"]);
    expect(offer.narrowed).toBe(true);
  });

  test("name, per absent grain, the measures that do not publish it", () => {
    const offer = sharedGrainOffer({
      metrics: [metric("A", ["STATE", "COUNTY"]), metric("B", ["STATE"])],
    });
    const county = offer.absent.find((entry) => entry.level === "COUNTY");
    expect(county.withoutIt).toEqual(["B"]);
    const national = offer.absent.find((entry) => entry.level === "NATIONAL");
    expect(national.withoutIt).toEqual(["A", "B"]);
  });

  test("a measure declaring no grains does not narrow the offer", () => {
    const offer = sharedGrainOffer({
      metrics: [metric("A", ["STATE", "COUNTY"]), metric("B", [])],
    });
    expect(offer.levels).toEqual(["STATE", "COUNTY"]);
    // Unknown is not none, so B is not reported as having removed anything.
    for (const entry of offer.absent) {
      expect(entry.withoutIt).not.toContain("B");
    }
  });

  test("no shared grain is a stated verdict, not an empty control", () => {
    const offer = sharedGrainOffer({
      metrics: [metric("A", ["COUNTY"]), metric("B", ["AGENCY"])],
    });
    expect(offer.levels).toEqual([]);
    expect(offer.note).toMatch(/publishers' declaration/);
  });

  test("a link asking for a grain the set does not publish says so", () => {
    const offer = sharedGrainOffer({
      metrics: [metric("A", ["COUNTY"]), metric("B", ["COUNTY"])],
      requested: "PLACE",
    });
    expect(offer.unavailable).toMatch(/do not all publish/);
    expect(offer.unavailable).toMatch(/showing County/);
  });

  test("the pair's own offer is the same decision, in the pair's words", () => {
    const pair = comparisonGrainOffer({
      metricA: metric("A", ["NATIONAL", "STATE", "COUNTY"]),
      metricB: metric("B", ["STATE", "COUNTY"]),
    });
    const shared = sharedGrainOffer({
      metrics: [
        metric("A", ["NATIONAL", "STATE", "COUNTY"]),
        metric("B", ["STATE", "COUNTY"]),
      ],
    });
    expect(pair.levels).toEqual(shared.levels);
    expect(pair.narrowed).toBe(shared.narrowed);
    // The wording stays the workspace's: a screen about a pair does not start
    // talking about a set.
    expect(pair.note).toMatch(/These two measures|both published at/);
  });
});

describe("when the cross-sectional presentations are refused", () => {
  test("one measure is not a cross-section", () => {
    const reason = crossSectionalRefusal({
      series: [series("A", "county:06001"), series("A", "county:06003")],
      sharedGrains: ["COUNTY"],
    });
    expect(reason).toMatch(/at least two measures/);
  });

  test("three measures name the matrix route, and the count", () => {
    const reason = crossSectionalRefusal({
      series: [
        series("A", "county:06001"),
        series("B", "county:06001"),
        series("C", "county:06001"),
      ],
      sharedGrains: ["COUNTY"],
    });
    expect(reason).toMatch(/matrix route/);
    expect(reason).toContain("3 ");
  });

  test("two measures with no shared grain name the publishers", () => {
    const reason = crossSectionalRefusal({
      series: [series("A", "county:06001"), series("B", "agency:X")],
      sharedGrains: [],
    });
    expect(reason).toMatch(/no geography grain in common/);
  });

  test("two measures at a shared grain are not refused", () => {
    expect(
      crossSectionalRefusal({
        series: [series("A", "county:06001"), series("B", "county:06001")],
        sharedGrains: ["COUNTY"],
      }),
    ).toBe("");
  });

  test("the pair is the distinct measures, in selection order", () => {
    expect(
      crossSectionalPair([
        series("B", "county:06001"),
        series("A", "county:06001"),
        series("B", "county:06003"),
      ]),
    ).toEqual(["B", "A"]);
    expect(crossSectionalPair([series("A", "county:06001")])).toBeNull();
    expect(
      crossSectionalPair([
        series("A", "x"),
        series("B", "x"),
        series("C", "x"),
      ]),
    ).toBeNull();
  });
});

describe("when a national measure may be a reference line", () => {
  test("only where NATIONAL is its only published grain", () => {
    expect(
      referenceLineOffer({
        grains: ["NATIONAL"],
        unit: "Percent",
        axisUnit: "Percent",
        presentation: "ranking",
      }).eligible,
    ).toBe(true);

    const alsoState = referenceLineOffer({
      grains: ["NATIONAL", "STATE"],
      unit: "Percent",
      axisUnit: "Percent",
      presentation: "ranking",
    });
    expect(alsoState.eligible).toBe(false);
    expect(alsoState.reason).toMatch(/belongs on the axis as those geographies/);
  });

  test("only where its unit is the axis's", () => {
    const mismatch = referenceLineOffer({
      grains: ["NATIONAL"],
      unit: "People",
      axisUnit: "Percent",
      presentation: "ranking",
    });
    expect(mismatch.eligible).toBe(false);
    expect(mismatch.reason).toMatch(/units refuse/);

    // Two spellings of one unit are one unit, as they are for the axes.
    expect(
      referenceLineOffer({
        grains: ["NATIONAL"],
        unit: "percent",
        axisUnit: "Percent",
        presentation: "ranking",
      }).eligible,
    ).toBe(true);
  });

  test("an unpublished unit on either side is not an agreement", () => {
    const offer = referenceLineOffer({
      grains: ["NATIONAL"],
      unit: null,
      axisUnit: "Percent",
      presentation: "ranking",
    });
    expect(offer.eligible).toBe(false);
    expect(offer.reason).toMatch(/publishes no unit/);
  });

  test("never on a time axis, where it is an ordinary series", () => {
    const offer = referenceLineOffer({
      grains: ["NATIONAL"],
      unit: "Percent",
      axisUnit: "Percent",
      presentation: "line",
    });
    expect(offer.eligible).toBe(false);
    expect(offer.reason).toMatch(/own published history/);
  });

  test("a measure publishing no grain is not thereby national", () => {
    const offer = referenceLineOffer({
      grains: [],
      unit: "Percent",
      axisUnit: "Percent",
      presentation: "ranking",
    });
    expect(offer.eligible).toBe(false);
    expect(offer.reason).toMatch(/Unknown is not national/);
  });
});

describe("the geography by period heatmap", () => {
  const rows = [
    { geo_id: "state:06", period_start: "2022", period_end: "2022", value: "4.1", release: "r1" },
    { geo_id: "state:06", period_start: "2023", period_end: "2023", value: "3.9", release: "r1" },
    { geo_id: "state:36", period_start: "2022", period_end: "2022", value: "5.2", release: "r1" },
    // 2023 for New York published no number, with the source's own reason.
    {
      geo_id: "state:36",
      period_start: "2023",
      period_end: "2023",
      value: null,
      value_status: "suppressed",
      release: "r1",
    },
  ];

  test("draws a cell for every pair in its rectangle", () => {
    const model = heatmapModel({ rows });
    expect(model.geographies.map((entry) => entry.geoId)).toEqual([
      "state:06",
      "state:36",
    ]);
    expect(model.periods).toEqual(["2022", "2023"]);
    expect(model.cells).toHaveLength(4);
  });

  test("a cell with no published value is null, with the source's reason", () => {
    const model = heatmapModel({ rows });
    const withheld = model.cells.find(
      (cell) => cell.geoId === "state:36" && cell.period === "2023",
    );
    expect(withheld.value).toBeNull();
    expect(withheld.valueStatus).toBe("suppressed");
    expect(model.unpublishedCount).toBe(1);
    expect(model.valueCount).toBe(3);
  });

  test("a pair no row arrived for is still a cell, not a ragged edge", () => {
    const model = heatmapModel({
      rows: [
        { geo_id: "state:06", period_start: "2022", value: "1" },
        { geo_id: "state:36", period_start: "2023", value: "2" },
      ],
    });
    expect(model.cells).toHaveLength(4);
    expect(model.unpublishedCount).toBe(2);
    const absent = model.cells.find(
      (cell) => cell.geoId === "state:06" && cell.period === "2023",
    );
    expect(absent.value).toBeNull();
    expect(absent.valueStatus).toBeNull();
  });

  test("the scale is measured over the published cells only", () => {
    const model = heatmapModel({ rows });
    expect(model.minValue).toBe(3.9);
    expect(model.maxValue).toBe(5.2);
  });

  test("rows are ordered by published name where one is known", () => {
    const model = heatmapModel({
      rows,
      geographyNames: { "state:06": "California", "state:36": "New York" },
    });
    expect(model.geographies.map((entry) => entry.name)).toEqual([
      "California",
      "New York",
    ]);
  });

  test("the cap is stated, with the narrowing that would fit", () => {
    const many = [];
    for (let index = 0; index < MAX_HEATMAP_GEOGRAPHIES + 5; index += 1) {
      many.push({
        geo_id: `county:${String(index).padStart(5, "0")}`,
        period_start: "2023",
        value: "1",
      });
    }
    const model = heatmapModel({ rows: many });
    expect(model.geographies).toHaveLength(MAX_HEATMAP_GEOGRAPHIES);
    expect(model.capped).toBe(true);
    expect(model.capNote).toMatch(/Narrow to a state/);
    expect(model.capNote).toContain(String(MAX_HEATMAP_GEOGRAPHIES + 5));
  });

  test("a period cap names the year range instead", () => {
    const many = [];
    for (let index = 0; index < MAX_HEATMAP_PERIODS + 3; index += 1) {
      many.push({
        geo_id: "state:06",
        period_start: String(1900 + index),
        value: "1",
      });
    }
    const model = heatmapModel({ rows: many });
    expect(model.periods).toHaveLength(MAX_HEATMAP_PERIODS);
    expect(model.capNote).toMatch(/Narrow the year range/);
  });

  test("nothing published is an empty model, not a grid of zeroes", () => {
    const model = heatmapModel({ rows: [] });
    expect(model.cells).toEqual([]);
    expect(model.minValue).toBeNull();
  });

  test("a second row for one cell is ignored, never aggregated", () => {
    const model = heatmapModel({
      rows: [
        { geo_id: "state:06", period_start: "2023", value: "10" },
        { geo_id: "state:06", period_start: "2023", value: "20" },
      ],
    });
    expect(model.cells).toHaveLength(1);
    expect(model.cells[0].value).toBe(10);
  });
});
