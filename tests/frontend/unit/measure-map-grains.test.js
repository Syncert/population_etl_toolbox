// Covers: WEB-148 — web-place-maps: the one-measure map offers exactly the
// grains the catalog publishes a measure at, draws each on the layer
// /catalog/tile-layers lists for it, always on the most recent boundary
// vintage held, marked with an asterisk and the year it was last updated when
// that is not the period's year, and narrows city and tract layers to one state.

import { describe, expect, it } from "vitest";

import {
  MAP_GRAINS,
  STATE_SCOPED_GRAINS,
  boundaryForGrain,
  boundaryNote,
  boundarySentence,
  grainLayerFilter,
  initialMapGrain,
  mapGrainChoices,
  newestPeriodYear,
  rowInState,
  tileMetadataForBoundary,
} from "../../../apps/web/lib/measureMapGrains.ts";
import { buildMeasureMapView, currentLatestPeriods } from "../../../apps/web/lib/oneMeasureMap.ts";

const layer = (overrides) => ({
  layer_id: "places",
  source_layer: "places",
  grains: ["PLACE"],
  versioned: true,
  minzoom: 6,
  maxzoom: 14,
  join_key: "geo_id",
  name_field: "place_name",
  tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024",
  vintages: [
    { boundary_vintage: 2024, feature_count: 32350, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024" },
    { boundary_vintage: 2020, feature_count: 31900, is_default: false, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2020" },
  ],
  default_vintage: 2024,
  available: true,
  ...overrides,
});
const CATALOG = {
  uncoloured_rule: "A geography with no published value is left uncoloured, never drawn as zero.",
  layers: [
    layer({ layer_id: "counties", source_layer: "counties", grains: ["STATE", "COUNTY"], versioned: false, minzoom: 0, maxzoom: 12, tile_template: "/tiles/counties/{z}/{x}/{y}", vintages: [], default_vintage: null }),
    layer({ layer_id: "tracts", source_layer: "tracts", grains: ["TRACT"], versioned: false, tile_template: "/tiles/tracts/{z}/{x}/{y}", vintages: [], default_vintage: null }),
    layer({}),
    layer({
      layer_id: "metros", source_layer: "metros", grains: ["METRO"], minzoom: 0, maxzoom: 12,
      tile_template: "/tiles/metros/{z}/{x}/{y}?vintage=2023",
      vintages: [{ boundary_vintage: 2023, feature_count: 925, is_default: true, tile_template: "/tiles/metros/{z}/{x}/{y}?vintage=2023" }],
      default_vintage: 2023,
    }),
  ],
};

describe("which grains a measure is offered at", () => {
  it("offers only the catalog's grains, in the map's order", () => {
    expect(mapGrainChoices(["NATIONAL", "STATE", "PLACE", "COUNTY", "METRO", "TRACT"])).toEqual(["COUNTY", "METRO", "PLACE", "TRACT"]);
    expect(mapGrainChoices(["METRO", "STATE"])).toEqual(["METRO"]);
    expect(mapGrainChoices(["STATE", "NATIONAL"])).toEqual([]);
    // A catalog row without grains is the county map it always was.
    expect(mapGrainChoices(undefined)).toEqual(["COUNTY"]);
    expect(MAP_GRAINS).toEqual(["COUNTY", "METRO", "PLACE", "TRACT"]);
  });

  it("opens on the grain asked for only when it is offered", () => {
    expect(initialMapGrain(["COUNTY", "PLACE"], "place")).toBe("PLACE");
    expect(initialMapGrain(["COUNTY", "PLACE"], "TRACT")).toBe("COUNTY");
    expect(initialMapGrain(["METRO"], null)).toBe("METRO");
    expect(initialMapGrain([], "COUNTY")).toBeNull();
  });

  it("reads cities and tracts one state at a time, counties and metros nationally", () => {
    expect([...STATE_SCOPED_GRAINS].sort()).toEqual(["PLACE", "TRACT"]);
    expect(grainLayerFilter("PLACE", "48")).toEqual(["all", ["==", ["get", "geo_level"], "PLACE"], ["==", ["to-string", ["get", "state_fips"]], "48"]]);
    expect(grainLayerFilter("METRO", "48")).toEqual(["==", ["get", "geo_level"], "METRO"]);
    expect(rowInState("state:48|place:05000", "48")).toBe(true);
    expect(rowInState("state:04|place:48000", "48")).toBe(false);
  });
});

describe("which boundary a grain is drawn on", () => {
  it("draws the most recent held vintage, and names it", () => {
    const boundary = boundaryForGrain(CATALOG, "PLACE", 2024);
    expect(boundary).toMatchObject({ drawn: true, vintage: 2024, marked: false, tileTemplate: "/tiles/places/{z}/{x}/{y}?vintage=2024" });
    expect(boundarySentence(boundary, "PLACE", 2024)).toBe("Drawn on the 2024 city and town boundaries.");
    expect(boundaryNote(boundary, "PLACE")).toBe("City and town boundaries last updated 2024, the most recent held.");
    // An older period is drawn on the newest boundaries too, never an older set.
    expect(boundaryForGrain(CATALOG, "PLACE", 2022)).toMatchObject({ drawn: true, vintage: 2024, marked: true });
    expect(boundaryForGrain(CATALOG, "PLACE", 2020)).toMatchObject({ drawn: true, vintage: 2024, marked: true });
    expect(boundaryForGrain(CATALOG, "METRO", 2023)).toMatchObject({ drawn: true, vintage: 2023, marked: false });
  });

  it("marks a map whose boundaries are not from the period's year, and never withholds it", () => {
    // OFFICE_LAB holds only the 2025 place boundaries; ACS 2020–2024 ends in 2024.
    const only2025 = { ...CATALOG, layers: CATALOG.layers.map((item) => (item.layer_id === "places"
      ? { ...item, vintages: [{ boundary_vintage: 2025, feature_count: 32629, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2025" }] }
      : item)) };
    const boundary = boundaryForGrain(only2025, "PLACE", 2024);
    expect(boundary).toMatchObject({ drawn: true, vintage: 2025, marked: true, tileTemplate: "/tiles/places/{z}/{x}/{y}?vintage=2025" });
    expect(boundarySentence(boundary, "PLACE", 2024)).toBe(
      "* Drawn on the 2025 city and town boundaries, the most recent held; the values are for 2024, so a few cities and towns may differ from the shapes they were measured on.",
    );
    expect(boundaryNote(boundary, "PLACE")).toBe("City and town boundaries last updated 2025, the most recent held.");
    // However far the period is from the boundaries, the map is drawn and marked.
    expect(boundaryForGrain(only2025, "PLACE", 2009)).toMatchObject({ drawn: true, vintage: 2025, marked: true });
    const metros = boundaryForGrain(CATALOG, "METRO", 2024);
    expect(metros).toMatchObject({ drawn: true, vintage: 2023, marked: true });
    expect(boundarySentence(metros, "METRO", 2024)).toBe(
      "* Drawn on the 2023 metro area boundaries, the most recent held; the values are for 2024, so a few metro areas may differ from the shapes they were measured on.",
    );
  });

  it("draws nothing only when a versioned layer holds no vintage at all", () => {
    const none = { ...CATALOG, layers: CATALOG.layers.map((item) => (item.layer_id === "places" ? { ...item, vintages: [] } : item)) };
    const boundary = boundaryForGrain(none, "PLACE", 2024);
    expect(boundary).toMatchObject({ drawn: false, reason: "no-vintage" });
    expect(boundarySentence(boundary, "PLACE", 2024)).toBe(
      "No city and town boundaries are held yet, so the map is not drawn. Every value is in the table below.",
    );
  });

  it("draws unversioned layers on their current boundaries", () => {
    const boundary = boundaryForGrain(CATALOG, "TRACT", 2015);
    expect(boundary).toMatchObject({ drawn: true, vintage: null, tileTemplate: "/tiles/tracts/{z}/{x}/{y}" });
    expect(boundary.marked).toBe(false);
    expect(boundarySentence(boundary, "TRACT", 2015)).toBe("Drawn on the current tract boundaries.");
    expect(boundaryNote(boundary, "TRACT")).toBe("Tract boundaries: current.");
  });

  it("says when no layer draws a grain", () => {
    const withoutMetros = { ...CATALOG, layers: CATALOG.layers.map((item) => (item.layer_id === "metros" ? { ...item, available: false, vintages: [] } : item)) };
    const boundary = boundaryForGrain(withoutMetros, "METRO", 2024);
    expect(boundary).toMatchObject({ drawn: false, reason: "no-layer" });
    expect(boundarySentence(boundary, "METRO", 2024)).toBe("No map layer draws metro areas here; every value is in the table below.");
  });

  it("hands the choropleth the layer's own source layer, join key and vintage template", () => {
    expect(tileMetadataForBoundary(boundaryForGrain(CATALOG, "METRO", 2024))).toEqual({
      layerId: "metros",
      sourceLayer: "metros",
      sourceLayerCandidates: ["metros"],
      joinKey: "geo_id",
      fields: [],
      tileJsonUrl: "/tiles/metros",
      tileTemplate: "/tiles/metros/{z}/{x}/{y}?vintage=2023",
    });
  });

  it("reads the period's year from the rows' period ends", () => {
    expect(newestPeriodYear([{ period_start: "2020-01-01", period_end: "2024-12-31" }, { period_start: "2019-01-01", period_end: "2023-12-31" }])).toBe(2024);
    expect(newestPeriodYear([{ period_start: "2023-01-01" }])).toBe(2023);
    expect(newestPeriodYear([])).toBeNull();
  });
});

describe("a value for a geography not on the list", () => {
  it("is counted, not dropped silently and not tabled", () => {
    const places = [
      { geo_id: "state:48|place:05000", geo_level: "PLACE", state_fips: "48", geo_name: "Austin city" },
      { geo_id: "state:48|place:35000", geo_level: "PLACE", state_fips: "48", geo_name: "Houston city" },
    ];
    const rows = [
      { metric_code: "M", geo_id: "state:48|place:05000", value: "90000" },
      { metric_code: "M", geo_id: "state:48|place:99999", value: "40000" },
    ];
    const view = buildMeasureMapView("M", rows, places);
    expect(view.withValue).toBe(1);
    expect(view.withoutValue).toBe(1);
    expect(view.offBoundary).toBe(1);
    expect(view.ranked.map((entry) => entry.county.geo_id)).toEqual(["state:48|place:05000", "state:48|place:35000"]);
  });
});

describe("an older period only a few geographies still hold", () => {
  const releases = [
    { release: "2024", newest_period_start: "2020-01-01", newest_period_end: "2024-12-31" },
    { release: "2022", newest_period_start: "2018-01-01", newest_period_end: "2022-12-31" },
  ];

  it("is read from the release that published it for every geography", () => {
    const periods = [
      { period_start: "2020-01-01", period_end: "2024-12-31", observation_count: 30000 },
      { period_start: "2018-01-01", period_end: "2022-12-31", observation_count: 12 },
      { period_start: "2015-01-01", period_end: "2019-12-31", observation_count: 4 },
    ];
    const { periods: kept, readAsReleased } = currentLatestPeriods(periods, releases);
    expect(kept.map((item) => item.period_start)).toEqual(["2020-01-01", "2015-01-01"]);
    expect(Object.fromEntries(readAsReleased)).toEqual({ "2018-01-01": "release:2022" });
  });

  it("stays a latest period when the latest publication holds it in full", () => {
    // A population-estimates vintage re-estimates every year since the census.
    const pep = [
      { period_start: "2024-01-01", observation_count: 3200 },
      { period_start: "2023-01-01", observation_count: 3200 },
    ];
    const vintages = [{ release: "v2024", newest_period_start: "2024-01-01" }, { release: "v2023", newest_period_start: "2023-01-01" }];
    const { periods: kept, readAsReleased } = currentLatestPeriods(pep, vintages);
    expect(kept).toHaveLength(2);
    expect(readAsReleased.size).toBe(0);
    // Without a release listing nothing is moved.
    expect(currentLatestPeriods(pep, []).periods).toHaveLength(2);
  });
});
