import { describe, expect, test } from "vitest";

// Covers: WEB-010 — explorer URL-state parse/serialize keeps existing
// supported links valid and reproduces the same analysis request.
// Covers: WEB-016 — the scope and pinned release travel in the link, so a
// shared as-released URL reproduces the same as-released analysis.
// Covers: WEB-019 — the comparison link names both measures and the scope,
// and deliberately carries no compatibility verdict.

import {
  GEO_GRAIN_ALIASES,
  GEO_LEVELS,
  MAX_WORKBENCH_URL_SERIES,
  comparisonHref,
  explorerHref,
  normalizeGeoLevel,
  parseComparisonState,
  parseExplorerState,
  parseWorkbenchState,
  serializeComparisonState,
  serializeExplorerState,
  serializeWorkbenchState,
  workbenchHref,
  workbenchLinkCeiling,
} from "../../../apps/web/lib/urlState";

describe("explorer URL state", () => {
  test("parses every currently supported link parameter", () => {
    const parsed = parseExplorerState(
      "?source=pep&metric=CENSUS_ACS%3Aacs5%3AB01003_001&state=55&geo=state%3A55%7Ccounty%3A025&geo_level=COUNTY&map_mode=extrusion&value_scale=log",
    );
    expect(parsed).toEqual({
      source: "pep",
      metric: "CENSUS_ACS:acs5:B01003_001",
      stateFips: "55",
      geoId: "state:55|county:025",
      geoLevel: "COUNTY",
      mapMode: "extrusion",
      valueScale: "log",
    });
  });

  // Covers: WEB-038 — every word the API's grain vocabulary publishes
  // survives a link. A shared view of a place- or agency-grain measure is
  // otherwise not reproducible: the grain is dropped as invalid and the
  // explorer opens on one the measure does not publish.
  test("carries every published geography grain through a link", () => {
    for (const grain of ["NATIONAL", "STATE", "COUNTY", "PLACE", "AGENCY"]) {
      expect(parseExplorerState(`?geo_level=${grain}`)).toEqual({ geoLevel: grain });
      expect(serializeExplorerState({ geoLevel: grain })).toContain(`geo_level=${grain}`);
    }
  });

  // Covers: WEB-073 — a link carried the source, measure, geography, grain,
  // view mode and scope of a view and none of its dimension narrowing. The
  // explorer itself refuses to chart a CDC series until the reader narrows to
  // one stratum, so the link copied *from that view* reopened stratified with
  // a blank map, while the saved-view document carried the same narrowing:
  // two records of one view that disagreed.
  test("carries the dimension narrowing under the source's own filter names", () => {
    const parsed = parseExplorerState(
      "?metric=CDC%3Acdi%3AALC06%3AAGEADJPREV&stratum_id=OVR&adjustment_status=crude",
    );
    expect(parsed.dimensions).toEqual({ stratum_id: "OVR", adjustment_status: "crude" });

    // The same names the saved document's `filters` uses, and sorted, so two
    // equivalent selections produce the same link.
    expect(
      serializeExplorerState({
        metric: "CDC:cdi:ALC06:AGEADJPREV",
        dimensions: { stratum_id: "OVR", adjustment_status: "crude" },
      }),
    ).toBe("metric=CDC%3Acdi%3AALC06%3AAGEADJPREV&adjustment_status=crude&stratum_id=OVR");

    // An empty selection is not a narrowing, and a key the explorer's own
    // controls own is never overwritten by one.
    expect(serializeExplorerState({ dimensions: { stratum_id: "" } })).toBe("");
    expect(serializeExplorerState({ source: "cdc", dimensions: { source: "bls" } })).toBe(
      "source=cdc",
    );
    expect(parseExplorerState("?stratum_id=")).toEqual({});
  });

  test("drops invalid values instead of propagating them", () => {
    expect(
      parseExplorerState("?geo_level=PLANET&map_mode=hologram&value_scale=cubic&state=5x5&source=Not%2FValid"),
    ).toEqual({});
    expect(parseExplorerState("")).toEqual({});
  });

  test("carries the published identity of a source with no route segment", () => {
    // FBI UCR publishes no route segment, so its glossary source code is the
    // shareable identity; a link naming it must survive the round trip.
    expect(parseExplorerState("?source=FBI_UCR")).toEqual({ source: "FBI_UCR" });
    expect(serializeExplorerState({ source: "FBI_UCR" }, { source: "census" }))
      .toBe("source=FBI_UCR");
  });

  test("round-trips state through serialize and parse", () => {
    const state = {
      source: "usda-nass",
      metric: "BLS:LAU:UNEMP_RATE",
      geoLevel: "STATE",
      mapMode: "extrusion",
      valueScale: "log",
      stateFips: "55",
      geoId: "state:55",
    };
    expect(parseExplorerState(`?${serializeExplorerState(state)}`)).toEqual(state);
  });

  test("carries the scope and a pinned release so an as-released link reproduces", () => {
    expect(parseExplorerState("?scope=as_released&release=2022")).toEqual({
      scope: "as_released",
      release: "2022",
    });
    expect(
      parseExplorerState(`?${serializeExplorerState({ scope: "as_released", release: "2022" })}`),
    ).toEqual({ scope: "as_released", release: "2022" });
  });

  test("a release without the as-released scope is dropped, not propagated", () => {
    // The API answers `release` without `scope=as_released` with a 422, so a
    // link carrying one alone must not reproduce that request.
    expect(parseExplorerState("?release=2022")).toEqual({});
    expect(parseExplorerState("?scope=latest&release=2022")).toEqual({ scope: "latest" });
    expect(serializeExplorerState({ release: "2022" })).toBe("");
    expect(serializeExplorerState({ scope: "latest", release: "2022" }, { scope: "latest" }))
      .toBe("");
    expect(parseExplorerState("?scope=whenever")).toEqual({});
  });

  test("omits defaults so equivalent selections share one URL", () => {
    const defaults = {
      source: "census",
      geoLevel: "COUNTY",
      mapMode: "choropleth",
      scope: "latest",
    };
    expect(
      serializeExplorerState(
        {
          source: "census",
          metric: "M",
          geoLevel: "COUNTY",
          mapMode: "choropleth",
          scope: "latest",
        },
        defaults,
      ),
    ).toBe("metric=M");
    expect(
      serializeExplorerState({ source: "pep", metric: "M" }, defaults),
    ).toBe("source=pep&metric=M");
    expect(explorerHref({}, defaults)).toBe("/explore");
    expect(explorerHref({ metric: "M" }, defaults)).toBe("/explore?metric=M");
  });
});

describe("comparison URL state", () => {
  test("names both measures, their sources, and the scope", () => {
    const state = {
      metricA: "CENSUS_ACS:acs5:B01003_001",
      metricB: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      sourceA: "census",
      sourceB: "pep",
      geoLevel: "STATE",
      stateFips: "55",
    };
    expect(parseComparisonState(`?${serializeComparisonState(state)}`)).toEqual(state);
    expect(comparisonHref(state, { geoLevel: "COUNTY" })).toContain("/compare?a=CENSUS_ACS");
    expect(comparisonHref({}, { geoLevel: "COUNTY" })).toBe("/compare");
  });

  test("carries no compatibility verdict", () => {
    // The verdict belongs to the API and is re-asked on open, so a link can
    // never reproduce a stale "comparable" for a pair whose published
    // semantics have since changed.
    const query = serializeComparisonState({
      metricA: "A",
      metricB: "B",
      geoLevel: "COUNTY",
    });
    expect(query).not.toContain("comparable");
    expect(parseComparisonState("?a=A&b=B&comparable=true")).toEqual({
      metricA: "A",
      metricB: "B",
    });
  });

  test("drops invalid scope values instead of propagating them", () => {
    expect(parseComparisonState("?geo_level=PLANET&state=5x5&source_a=Not%2FValid")).toEqual({});
    expect(parseComparisonState("")).toEqual({});
    expect(serializeComparisonState({ geoLevel: "COUNTY" }, { geoLevel: "COUNTY" })).toBe("");
  });
});

// Covers: WEB-076 — a grain the vocabulary replaced still opens the view it
// was saved or shared with.
describe("the grain vocabulary's aliases", () => {
  const aliased = Object.entries(GEO_GRAIN_ALIASES);

  test("every alias resolves to its vocabulary word, in any case", () => {
    expect(aliased.length).toBeGreaterThan(0);
    for (const [alias, word] of aliased) {
      for (const sent of [alias, alias.toLowerCase(), `  ${alias}  `]) {
        expect(normalizeGeoLevel(sent)).toBe(word);
      }
    }
  });

  test("a word that is not a grain comes back unchanged, not repaired", () => {
    // Normalising is not validating: the readers below decide what to do
    // with a word that is not a grain, and each drops it.
    expect(normalizeGeoLevel("COUNTRY")).toBe("COUNTRY");
    expect(normalizeGeoLevel(null)).toBe("");
    expect(parseExplorerState("?geo_level=COUNTRY")).toEqual({});
    expect(parseComparisonState("?geo_level=COUNTRY")).toEqual({});
  });

  test("an aliased link opens on the grain it names", () => {
    for (const [alias, word] of aliased) {
      expect(parseExplorerState(`?geo_level=${alias}`)).toEqual({ geoLevel: word });
      expect(parseComparisonState(`?geo_level=${alias}`)).toEqual({ geoLevel: word });
    }
  });

  test("a state carrying an alias serializes as the vocabulary word", () => {
    // Which is what makes a link shareable onward: an alias in, the
    // vocabulary word out, and the same link for the same selection.
    for (const [alias, word] of aliased) {
      expect(serializeExplorerState({ geoLevel: alias })).toBe(`geo_level=${word}`);
      expect(serializeComparisonState({ geoLevel: alias })).toBe(`geo_level=${word}`);
      expect(serializeExplorerState({ geoLevel: alias }, { geoLevel: word })).toBe("");
    }
  });

  test("every vocabulary word survives the round trip", () => {
    for (const word of GEO_LEVELS) {
      expect(normalizeGeoLevel(word)).toBe(word);
      expect(parseExplorerState(`?geo_level=${word}`)).toEqual({ geoLevel: word });
    }
  });
});

// Covers: WEB-084 — a workbench link carries the whole composition and
// nothing a reader's browser should not hold: every series' source, measure,
// scope, pinned release, grain, geography and dimension pins, plus the
// presentation, the shared grain and the correlation toggle. Parse drops an
// invalid series rather than sending it; serialize omits defaults; and the
// link states its own ceiling rather than truncating past it.

describe("the workbench link carries the composition", () => {
  const composition = {
    series: [
      {
        sourceKey: "fred",
        metricCode: "FRED:UNRATE",
        geoLevel: "NATIONAL",
        geoId: "us:1",
      },
      {
        sourceKey: "cdc",
        metricCode: "CDC:cdi:X:crude",
        scope: "as_released",
        release: "2024-01-05",
        geoLevel: "COUNTY",
        geoId: "county:06001",
        filters: { stratum_id: "OVR" },
      },
    ],
    presentation: "line",
    alignmentGeoLevel: "COUNTY",
    stateFips: "06",
    year: 2023,
    correlation: true,
  };

  test("round-trips every field of every series", () => {
    const reopened = parseWorkbenchState(serializeWorkbenchState(composition));
    expect(reopened).toEqual(composition);
  });

  test("omits the defaults it was given", () => {
    const query = serializeWorkbenchState(
      { series: composition.series, presentation: "line", alignmentGeoLevel: "COUNTY" },
      { presentation: "line", alignmentGeoLevel: "COUNTY" },
    );
    expect(query).not.toContain("view=");
    expect(query).not.toContain("grain=");
    expect(query).toContain("s=");
  });

  test("a series' latest scope and an off correlation are not carried", () => {
    const query = serializeWorkbenchState({
      series: [
        {
          sourceKey: "fred",
          metricCode: "FRED:UNRATE",
          scope: "latest",
          geoLevel: "NATIONAL",
          geoId: "us:1",
        },
      ],
      correlation: false,
    });
    expect(query).not.toContain("scope");
    expect(query).not.toContain("corr");
  });

  test("an unreadable series is dropped and the rest of the link survives", () => {
    const state = parseWorkbenchState(
      "s=src:fred;m:FRED%3AUNRATE;lvl:NATIONAL;geo:us%3A1&s=m:only-a-measure&s=src:cdc&view=bar",
    );
    expect(state.series).toHaveLength(1);
    expect(state.series[0].metricCode).toBe("FRED:UNRATE");
    expect(state.presentation).toBe("bar");
  });

  test("a grain, scope, presentation or year that is not one is dropped", () => {
    const state = parseWorkbenchState(
      "s=src:fred;m:FRED%3AUNRATE;lvl:PLANET;scope:whenever&view=pie&grain=PLANET&year=23&state=x",
    );
    expect(state.series[0].geoLevel).toBeUndefined();
    expect(state.series[0].scope).toBeUndefined();
    expect(state.presentation).toBeUndefined();
    expect(state.alignmentGeoLevel).toBeUndefined();
    expect(state.year).toBeUndefined();
    expect(state.stateFips).toBeUndefined();
  });

  test("a grain the vocabulary replaced still opens the composition", () => {
    const state = parseWorkbenchState(
      "s=src:fred;m:FRED%3AUNRATE;lvl:NATION&grain=US",
    );
    expect(state.series[0].geoLevel).toBe("NATIONAL");
    expect(state.alignmentGeoLevel).toBe("NATIONAL");
    // And a state built from that alias serializes as the vocabulary word.
    expect(decodeURIComponent(serializeWorkbenchState(state))).toContain(
      "lvl:NATIONAL",
    );
  });

  test("a reserved field name cannot be smuggled in as a dimension pin", () => {
    const state = parseWorkbenchState(
      "s=src:fred;m:FRED%3AUNRATE;geo:us%3A1;metric_code:OTHER",
    );
    expect(state.series[0].filters).toBeUndefined();
  });

  test("the link ceiling is stated, and a longer composition is not truncated silently", () => {
    const ceiling = workbenchLinkCeiling(MAX_WORKBENCH_URL_SERIES);
    expect(ceiling.fits).toBe(true);

    const over = workbenchLinkCeiling(MAX_WORKBENCH_URL_SERIES + 1);
    expect(over.fits).toBe(false);
    expect(over.reason).toMatch(/Save it instead/);
  });

  test("the href is the page's own path", () => {
    expect(workbenchHref({})).toBe("/workbench");
    expect(workbenchHref({ presentation: "bar" })).toBe("/workbench?view=bar");
  });
});
