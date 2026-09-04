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
      "?source=pep&metric=ACS%3Aacs5%3AB01003_001&state=55&geo=state%3A55%7Ccounty%3A025&geo_level=COUNTY&map_mode=extrusion",
    );
    expect(parsed).toEqual({
      source: "pep",
      metric: "ACS:acs5:B01003_001",
      stateFips: "55",
      geoId: "state:55|county:025",
      geoLevel: "COUNTY",
      mapMode: "extrusion",
    });
  });

  test("drops invalid values instead of propagating them", () => {
    expect(
      parseExplorerState("?geo_level=PLANET&map_mode=hologram&state=5x5&source=Not%2FValid"),
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
      metricA: "ACS:acs5:B01003_001",
      metricB: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      sourceA: "census",
      sourceB: "pep",
      geoLevel: "STATE",
      stateFips: "55",
    };
    expect(parseComparisonState(`?${serializeComparisonState(state)}`)).toEqual(state);
    expect(comparisonHref(state, { geoLevel: "COUNTY" })).toContain("/compare?a=ACS");
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
