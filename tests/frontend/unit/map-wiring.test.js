import { describe, expect, test } from "vitest";

// Covers: WEB-033 — one MapLibre wiring. The explorer and the comparison
// workspace bring their maps up the same way and keep their layers in sync
// the same way, through one module: a sync touches every listed layer that
// exists and none that does not, reports what it touched, and the explorer's
// two modes are one layer-state function rather than two copies of it. The
// boundary's tile template keeps its placeholders when resolved to this
// origin.

import {
  COMPARISON_LAYER,
  EXPLORER_CHOROPLETH_LAYERS,
  US_MAP_VIEW,
  baseMapStyle,
  boundaryTileUrl,
  pitchForMapMode,
  syncExplorerMapMode,
  syncLayerFilter,
  syncLayerPaint,
  syncLayerVisibility,
} from "../../../apps/web/lib/mapWiring";

/** A map that knows which layers exist and records what was set on them. */
function fakeMap(existing) {
  const calls = [];
  return {
    calls,
    getLayer: (id) => (existing.includes(id) ? { id } : undefined),
    setPaintProperty: (id, name, value) => calls.push(["paint", id, name, value]),
    setLayoutProperty: (id, name, value) => calls.push(["layout", id, name, value]),
    setFilter: (id, filter) => calls.push(["filter", id, filter]),
  };
}

describe("the shared map construction", () => {
  test("opens on the contiguous United States with no sources", () => {
    const style = baseMapStyle();
    expect(style.version).toBe(8);
    expect(style.sources).toEqual({});
    // Only a background: every data layer is the caller's, added once the
    // boundary is discovered, so a map can never draw a source it invented.
    expect(style.layers.map((layer) => layer.type)).toEqual(["background"]);
    expect(US_MAP_VIEW.zoom).toBe(3);
  });

  test("resolves the boundary template to this origin and keeps its placeholders", () => {
    const url = boundaryTileUrl("/tiles/counties/{z}/{x}/{y}?key=v", "http://localhost:3100");
    // The template the server advertised is the one substituted (WEB-026);
    // an encoded placeholder would 404 every tile.
    expect(url).toBe("http://localhost:3100/tiles/counties/{z}/{x}/{y}?key=v");
  });
});

describe("keeping layers in sync", () => {
  test("a paint sync touches every listed layer that exists and none that does not", () => {
    const map = fakeMap(["choropleth-fill", "choropleth-outline"]);
    const touched = syncLayerPaint(map, EXPLORER_CHOROPLETH_LAYERS, "fill-color", ["match"]);
    // The extrusion layer is not there yet; it is skipped, not created and
    // not thrown on, so a sync before the boundary arrives is harmless.
    expect(touched).toEqual(["choropleth-fill", "choropleth-outline"]);
    expect(map.calls).toEqual([
      ["paint", "choropleth-fill", "fill-color", ["match"]],
      ["paint", "choropleth-outline", "fill-color", ["match"]],
    ]);
  });

  test("a sync before any layer exists reports that it painted nothing", () => {
    const map = fakeMap([]);
    expect(syncLayerPaint(map, [COMPARISON_LAYER], "fill-color", "#000")).toEqual([]);
    expect(syncLayerFilter(map, EXPLORER_CHOROPLETH_LAYERS, ["==", "a", "b"])).toEqual([]);
    expect(syncLayerVisibility(map, "choropleth-extrusion", true)).toBe(false);
    expect(map.calls).toEqual([]);
  });

  test("one filter reaches every drawn choropleth layer", () => {
    const map = fakeMap([...EXPLORER_CHOROPLETH_LAYERS]);
    const filter = ["all", ["==", ["get", "geo_level"], "COUNTY"]];
    expect(syncLayerFilter(map, EXPLORER_CHOROPLETH_LAYERS, filter)).toEqual([
      ...EXPLORER_CHOROPLETH_LAYERS,
    ]);
    // A state selection that filtered the fill but not the outline would
    // draw every other state's borders over an empty map.
    expect(map.calls.every(([kind, , value]) => kind === "filter" && value === filter)).toBe(true);
  });
});

describe("the explorer's two modes are one layer state", () => {
  test("choropleth mode shows the fill and hides the extrusion outright", () => {
    const map = fakeMap(["choropleth-fill", "choropleth-extrusion"]);
    syncExplorerMapMode(map, "choropleth");
    expect(map.calls).toEqual([
      ["paint", "choropleth-fill", "fill-opacity", 0.95],
      ["layout", "choropleth-extrusion", "visibility", "none"],
    ]);
    expect(pitchForMapMode("choropleth")).toBe(0);
  });

  test("extrusion mode fades the fill and shows the columns, tilted", () => {
    const map = fakeMap(["choropleth-fill", "choropleth-extrusion"]);
    syncExplorerMapMode(map, "extrusion");
    // The flat fill fades almost out so the columns read; hidden rather than
    // flattened on the way back, because a zero-height extrusion still draws
    // its top in the choropleth's colour.
    expect(map.calls).toEqual([
      ["paint", "choropleth-fill", "fill-opacity", 0.08],
      ["layout", "choropleth-extrusion", "visibility", "visible"],
    ]);
    expect(pitchForMapMode("extrusion")).toBe(55);
  });
});
