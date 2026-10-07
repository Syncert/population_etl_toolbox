import { describe, expect, test } from "vitest";

// Covers: WEB-095 — a saved workbench stores the composition and not its
// values: each series becomes exactly an observations request, a
// contradiction the live route would refuse is dropped rather than stored,
// the alignment rides only a cross-sectional presentation, and the document
// round-trips back into the link that reopens the same chart.
// Covers: WEB-096 — a stale saved workbench is shown unrepaired, and the
// library row describes what it asks for without opening it.

import {
  CONFIGURATION_KINDS,
  describeDocument,
  reopenHref,
  validationState,
  workbenchDocument,
} from "../../../apps/web/lib/savedAnalysis";
import { parseWorkbenchState } from "../../../apps/web/lib/urlState";

const COMPOSITION = {
  series: [
    {
      metricCode: "FRED:UNRATE",
      geoLevel: "NATIONAL",
      geoId: "us:1",
    },
    {
      metricCode: "CDC:cdi:X:crude",
      scope: "as_released",
      geoLevel: "STATE",
      geoId: "state:55",
      filters: { stratum_id: "OVR" },
    },
  ],
  presentation: "line",
};

describe("what a workbench saves as", () => {
  test("is the workbench kind, which the API now serves", () => {
    expect(CONFIGURATION_KINDS).toContain("workbench");
    expect(workbenchDocument(COMPOSITION).kind).toBe("workbench");
  });

  test("each series is exactly an observations request", () => {
    const document = workbenchDocument(COMPOSITION);
    expect(document.series).toHaveLength(2);
    expect(document.series[0]).toEqual({
      metric_code: "FRED:UNRATE",
      scope: "latest",
      release: null,
      newest_per_geography: false,
      newest_release_per_period: false,
      filters: { geo_level: "NATIONAL", geo_id: "us:1" },
    });
  });

  test("the geography rides the filters the capability contract governs", () => {
    const document = workbenchDocument(COMPOSITION);
    expect(document.series[1].filters).toEqual({
      geo_level: "STATE",
      geo_id: "state:55",
      stratum_id: "OVR",
    });
  });

  test("a settled history is stored as the as-released read it is", () => {
    const document = workbenchDocument(COMPOSITION);
    expect(document.series[1].scope).toBe("as_released");
    expect(document.series[1].newest_release_per_period).toBe(true);
    expect(document.series[1].release).toBeNull();
  });

  test("a release pinned under the latest scope is dropped, not stored", () => {
    // The live route refuses the pair, so a document carrying it is one the
    // reader could not reopen.
    const document = workbenchDocument({
      ...COMPOSITION,
      series: [
        {
          metricCode: "FRED:UNRATE",
          scope: "latest",
          release: "2024-01-05",
          geoLevel: "NATIONAL",
        },
      ],
    });
    expect(document.series[0].release).toBeNull();
    expect(document.series[0].newest_release_per_period).toBe(false);
  });

  test("a pinned release and a settled history are never both carried", () => {
    const document = workbenchDocument({
      ...COMPOSITION,
      series: [
        {
          metricCode: "FRED:UNRATE",
          scope: "as_released",
          release: "2024-01-05",
          geoLevel: "NATIONAL",
        },
      ],
    });
    expect(document.series[0].release).toBe("2024-01-05");
    expect(document.series[0].newest_release_per_period).toBe(false);
  });

  test("no top-level measure, filter or reduction is carried", () => {
    const document = workbenchDocument(COMPOSITION);
    expect(document.metric_code).toBeUndefined();
    expect(document.scope).toBeUndefined();
    expect(document.filters).toEqual({});
  });

  test("the alignment is carried only for a cross-sectional presentation", () => {
    expect(
      workbenchDocument({
        ...COMPOSITION,
        alignment: { geoLevel: "COUNTY", stateFips: "06", year: 2023 },
      }).alignment,
    ).toEqual({ geo_level: "COUNTY", state_fips: "06", year: 2023 });

    // No alignment given: a longitudinal composition has no shared grain, and
    // inventing one would be the roll-up this surface refuses.
    expect(workbenchDocument(COMPOSITION).alignment).toBeNull();
  });

  test("a national alignment carries no state scope", () => {
    expect(
      workbenchDocument({
        ...COMPOSITION,
        alignment: { geoLevel: "NATIONAL", stateFips: "06" },
      }).alignment.state_fips,
    ).toBeNull();
  });

  test("the presentation's options are carried verbatim", () => {
    const options = { palette: "mono", nested: { any: [1, 2] } };
    expect(
      workbenchDocument({
        ...COMPOSITION,
        presentationOptions: options,
      }).presentation,
    ).toEqual({ type: "line", options });
  });
});

describe("reopening a saved workbench", () => {
  test("the link carries every series, its pins and the presentation", () => {
    const document = workbenchDocument({
      ...COMPOSITION,
      presentation: "scatter",
      alignment: { geoLevel: "STATE", stateFips: "55", year: 2023 },
    });
    const state = parseWorkbenchState(
      reopenHref(document).split("?")[1] || "",
    );

    expect(state.series).toHaveLength(2);
    expect(state.series[0].metricCode).toBe("FRED:UNRATE");
    expect(state.series[0].geoLevel).toBe("NATIONAL");
    expect(state.series[0].geoId).toBe("us:1");
    expect(state.series[1].scope).toBe("as_released");
    expect(state.series[1].filters).toEqual({ stratum_id: "OVR" });
    expect(state.presentation).toBe("scatter");
    expect(state.alignmentGeoLevel).toBe("STATE");
    expect(state.stateFips).toBe("55");
    expect(state.year).toBe(2023);
  });

  test("the link carries no name, id, version or owner", () => {
    const href = reopenHref(workbenchDocument(COMPOSITION));
    expect(href).toMatch(/^\/workbench\?/);
    for (const forbidden of ["name", "configuration_id", "version", "owner", "token"]) {
      expect(href).not.toContain(forbidden);
    }
  });

  test("a workbench reopens on the workbench, not the explorer", () => {
    expect(reopenHref(workbenchDocument(COMPOSITION))).toContain("/workbench");
    expect(reopenHref({ kind: "observations", metric_code: "A" })).toContain(
      "/explore",
    );
  });
});

describe("what the library row says about a saved workbench", () => {
  test("names the count, the presentation, the grain and the measures", () => {
    const description = describeDocument(
      workbenchDocument({
        ...COMPOSITION,
        presentation: "ranking",
        alignment: { geoLevel: "STATE" },
      }),
    );
    expect(description).toContain("2 series");
    expect(description).toContain("ranking");
    expect(description).toContain("at STATE");
    expect(description).toContain("FRED:UNRATE");
  });

  test("a stale document is a caution shown unrepaired, never an ok", () => {
    const stale = validationState({
      valid: false,
      reason:
        "series 2 metric_code 'CDC:cdi:X:crude' is not a published metric",
    });
    expect(stale.state).toBe("warn");
    expect(stale.message).toContain("series 2");
    // And the document itself is untouched: the reason is reported, the
    // content is the reader's.
    expect(validationState({ valid: true }).state).toBe("ok");
  });
});

// Covers: WEB-141 — a series' calendar grain or window is part of what is
// saved and linked: a stored document carries the route's own `time_grain` or
// `window` (and nothing for a native read), the time view is part of the
// series' identity, and the document reopens the same view.
describe("a series' time view", () => {
  test("is stored as the route's own parameter, and native stores nothing new", async () => {
    const document = workbenchDocument({
      series: [
        { metricCode: "BLS:CUUR0000SA0", geoLevel: "NATIONAL", geoId: "us:1", timeView: "annual" },
        { metricCode: "BLS:CUUR0000SA0", geoLevel: "NATIONAL", geoId: "us:1", timeView: "trailing_12" },
        { metricCode: "FRED:UNRATE", geoLevel: "NATIONAL", geoId: "us:1" },
      ],
      presentation: "line",
    });
    expect(document.series[0]).toMatchObject({ time_grain: "annual", window: null, scope: "latest" });
    expect(document.series[1]).toMatchObject({ time_grain: "native", window: "trailing_12" });
    expect(document.series[2].time_grain).toBeUndefined();
    expect(document.series[2].window).toBeUndefined();
  });

  test("a time view is a latest read with no release or reduction", () => {
    const document = workbenchDocument({
      series: [
        {
          metricCode: "BLS:CUUR0000SA0",
          scope: "as_released",
          release: "2025-01-15",
          geoLevel: "NATIONAL",
          geoId: "us:1",
          timeView: "quarterly",
        },
      ],
      presentation: "line",
    });
    expect(document.series[0]).toMatchObject({
      scope: "latest",
      release: null,
      newest_release_per_period: false,
      time_grain: "quarterly",
    });
  });

  test("reopens the same view through the link", () => {
    const document = workbenchDocument({
      series: [{ metricCode: "BLS:CUUR0000SA0", geoLevel: "NATIONAL", geoId: "us:1", timeView: "ytd" }],
      presentation: "line",
    });
    const href = reopenHref(document);
    expect(href).toContain("tv%3Aytd");
    const state = parseWorkbenchState(href.slice(href.indexOf("?")));
    expect(state.series[0].timeView).toBe("ytd");
  });
});
