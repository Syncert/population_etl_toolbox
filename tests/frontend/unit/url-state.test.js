import { describe, expect, test } from "vitest";

// Covers: WEB-010 — explorer URL-state parse/serialize keeps existing
// supported links valid and reproduces the same analysis request.
// Covers: WEB-016 — the scope and pinned release travel in the link, so a
// shared as-released URL reproduces the same as-released analysis.
// Covers: WEB-019 — the comparison link names both measures and the scope,
// and deliberately carries no compatibility verdict.

import {
  comparisonHref,
  explorerHref,
  parseComparisonState,
  parseExplorerState,
  serializeComparisonState,
  serializeExplorerState,
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
      "?metric=CDC%3Acdi%3AALC1_1%3Acrude&stratum_id=OVR&adjustment_status=crude",
    );
    expect(parsed.dimensions).toEqual({ stratum_id: "OVR", adjustment_status: "crude" });

    // The same names the saved document's `filters` uses, and sorted, so two
    // equivalent selections produce the same link.
    expect(
      serializeExplorerState({
        metric: "CDC:cdi:ALC1_1:crude",
        dimensions: { stratum_id: "OVR", adjustment_status: "crude" },
      }),
    ).toBe("metric=CDC%3Acdi%3AALC1_1%3Acrude&adjustment_status=crude&stratum_id=OVR");

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
