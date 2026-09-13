// A link that names a measure opens on that measure, or says why it did not.
//
// Covers: WEB-072 — the explorer and the comparison workspace both looked for
// the requested metric_code in one source's catalog and, when it was absent,
// selected something else in silence: `pickPreferredMetric` on the explorer,
// `items[0]` per side on the comparison. A reader who clicked "Explore" on
// `BLS:LAU:UNEMP_RATE` was shown Census ACS total population.

import { describe, expect, test } from "vitest";

import { requestedMetricState } from "../../../apps/web/lib/requestedMetric";
import { buildExplorerSources, findExplorerSource } from "../../../apps/web/lib/explorerSources";
import { metricQualityRows } from "../../../apps/web/lib/dataQuality";

const items = [
  { metric_code: "CENSUS_ACS:acs5:B01003_001" },
  { metric_code: "CENSUS_ACS:acs1:B01003_001" },
];

describe("a link that names a measure", () => {
  test("opens on it when the source publishes it", () => {
    const state = requestedMetricState({
      requested: "CENSUS_ACS:acs5:B01003_001",
      items,
      sourceTitle: "Census American Community Survey",
    });
    expect(state).toEqual({
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      chooseDefault: false,
      notice: "",
    });
  });

  test("names what it asked for when the source does not publish it", () => {
    const state = requestedMetricState({
      requested: "BLS:LAU:UNEMP_RATE",
      items,
      sourceTitle: "Census American Community Survey",
    });
    expect(state.metricCode).toBe("");
    // Not a default, either: substituting is the one thing a link must not do.
    expect(state.chooseDefault).toBe(false);
    expect(state.notice).toContain("BLS:LAU:UNEMP_RATE");
    expect(state.notice).toContain("Census American Community Survey");
    expect(state.notice).toContain("Nothing was substituted");
  });

  test("a screen opened without a link still chooses its own default", () => {
    const state = requestedMetricState({ requested: "", items });
    expect(state).toEqual({ metricCode: "", chooseDefault: true, notice: "" });
    expect(requestedMetricState({ requested: null, items }).chooseDefault).toBe(true);
  });

  test("the notice names the source even when the caller has no title", () => {
    const state = requestedMetricState({ requested: "A:B", items, sourceTitle: "  " });
    expect(state.notice).toContain("the source shown");
  });
});

describe("a published source identity resolves either way it is spelled", () => {
  const sources = buildExplorerSources([
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      datasets: [],
      observation_routes: [
        { path: "/api/v1/observations", parameters: ["metric_code", "geo_level"] },
      ],
      observation_filters: ["geo_level"],
      observation_dimensions: [],
    },
  ]);

  test("the route segment the tabs use, and the source code a metric row publishes", () => {
    // A link built from a metric row can only carry `source_code`: that is the
    // one source identity a metric publishes. The tab key is the route
    // segment. Both come from the API, so one resolver accepts both rather
    // than making every caller learn that CENSUS_ACS is reached at `census`.
    expect(findExplorerSource(sources, "census")?.sourceCode).toBe("CENSUS_ACS");
    expect(findExplorerSource(sources, "CENSUS_ACS")?.key).toBe("census");
    expect(findExplorerSource(sources, "census_acs")?.key).toBe("census");
    expect(findExplorerSource(sources, "BLS")).toBeNull();
    expect(findExplorerSource(sources, "")).toBeNull();
  });
});

describe("the quality table's rows", () => {
  test("carry the source the metric row publishes, not the screen's selection", () => {
    const rows = metricQualityRows([
      {
        metric_code: "BLS:LAU:UNEMP_RATE",
        source_code: "BLS",
        metric_display_name: "Unemployment rate",
        freshness_state: "current",
      },
    ]);
    expect(rows[0].sourceCode).toBe("BLS");
  });
});
