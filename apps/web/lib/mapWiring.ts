// The one MapLibre wiring, in the parts that do not need MapLibre.
//
// The explorer and the comparison workspace draw different maps -- the
// explorer decodes tile previews into GeoJSON for a fill, an extrusion, an
// outline, and a selection ring, with hover, click, keyboard, and a state
// fit; the comparison draws one fill straight off the vector source -- but
// they were built the same way and kept in sync the same way, in two copies.
// Two copies of "set this paint property on whichever of these layers exist"
// is two places for one of them to stop checking whether the layer exists.
//
// Everything here takes the smallest slice of a map it needs, so it can be
// exercised against a fake in a unit test and used by both screens against
// the real thing. Nothing here computes an analytical value: the colouring
// expressions come from `explorerViewModel`, which already owns the rule that
// a geography without a published number is left uncoloured.

/** The map's opening view: the contiguous United States. */
export const US_MAP_VIEW = Object.freeze({
  center: [-98.5795, 39.8283] as [number, number],
  zoom: 3,
});

/** Where the explorer eases back to when no state is selected. */
export const US_OVERVIEW_VIEW = Object.freeze({
  center: [-98.5, 38.5] as [number, number],
  zoom: 3.05,
});

export const MAP_BACKGROUND_COLOR = "#dfe8ed";

/** A style with no sources: every layer is added once the boundary is known. */
export function baseMapStyle() {
  return {
    version: 8 as const,
    sources: {},
    layers: [
      {
        id: "background",
        type: "background" as const,
        paint: { "background-color": MAP_BACKGROUND_COLOR },
      },
    ],
  };
}

/**
 * The boundary's tile template as an absolute URL on this origin.
 *
 * Resolved textually, never through `new URL(...)`. The WHATWG path
 * percent-encode set includes braces, and Chromium honours it: `new URL`
 * turns `{z}/{x}/{y}` into `%7Bz%7D/%7Bx%7D/%7By%7D` (verified in the
 * browser the suite runs). MapLibre fills a template with
 * `.replace(/{z}/g, ...)` -- literal braces (verified in the installed
 * bundle) -- so an encoded template is never filled and every tile request
 * asks the server for the row `%7By%7D`. The comparison map built its source
 * URL through `new URL` before this module existed. That the tiles then
 * 404ed is inferred from those two verified premises rather than observed
 * end to end: headless Chromium has no GL context, so MapLibre requests no
 * tiles in the browser tier at all, and the legend and coloured count --
 * which come from the rows -- stay green either way. `tiles.js` already
 * resolves templates textually for discovery for the same reason (WEB-026);
 * this keeps the rule at the source URL too. An absolute template is
 * returned as advertised.
 */
export function boundaryTileUrl(tileTemplate: string, origin: string): string {
  if (/^https?:\/\//i.test(tileTemplate)) {
    return tileTemplate;
  }
  const base = origin.replace(/\/+$/, "");
  return tileTemplate.startsWith("/") ? `${base}${tileTemplate}` : `${base}/${tileTemplate}`;
}

/** The explorer's choropleth layers, in the order they are drawn. */
export const EXPLORER_CHOROPLETH_LAYERS = Object.freeze([
  "choropleth-fill",
  "choropleth-extrusion",
  "choropleth-outline",
] as const);

/** The comparison map's single layer. */
export const COMPARISON_LAYER = "comparison-choropleth";
export const COMPARISON_SOURCE = "comparison-boundary";

/**
 * The slice of a MapLibre map the sync helpers need. Narrow on purpose: a
 * unit test supplies a fake, and a helper cannot reach for anything else.
 */
export interface LayerHost {
  getLayer(id: string): unknown;
  setPaintProperty(id: string, name: string, value: unknown): unknown;
  setLayoutProperty(id: string, name: string, value: unknown): unknown;
  setFilter(id: string, filter: unknown): unknown;
}

/**
 * Set one paint property on every listed layer that exists.
 *
 * A layer that is not there is skipped, not created: the layers are built
 * once the boundary is known, and a sync that ran before then must not
 * throw. Returns the ids it touched so a caller can tell "nothing to paint
 * yet" from "painted".
 */
export function syncLayerPaint(
  map: LayerHost,
  layerIds: readonly string[],
  property: string,
  value: unknown,
): string[] {
  const touched: string[] = [];
  for (const id of layerIds) {
    if (map.getLayer(id)) {
      map.setPaintProperty(id, property, value);
      touched.push(id);
    }
  }
  return touched;
}

/** Apply one filter to every listed layer that exists. */
export function syncLayerFilter(
  map: LayerHost,
  layerIds: readonly string[],
  filter: unknown,
): string[] {
  const touched: string[] = [];
  for (const id of layerIds) {
    if (map.getLayer(id)) {
      map.setFilter(id, filter);
      touched.push(id);
    }
  }
  return touched;
}

/** Show or hide one layer, if it exists. */
export function syncLayerVisibility(map: LayerHost, layerId: string, visible: boolean): boolean {
  if (!map.getLayer(layerId)) {
    return false;
  }
  map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
  return true;
}

export type ExplorerMapMode = "choropleth" | "extrusion";

/**
 * The explorer's two modes as layer state.
 *
 * In extrusion mode the flat fill fades almost out so the columns read; in
 * choropleth mode the extrusion layer is hidden outright rather than
 * flattened, because a zero-height extrusion still draws its top in the
 * choropleth's colour and the two modes become indistinguishable. The camera
 * pitch is the caller's, because it is an animation, not layer state.
 */
export function syncExplorerMapMode(map: LayerHost, mode: ExplorerMapMode): void {
  syncLayerPaint(map, ["choropleth-fill"], "fill-opacity", mode === "choropleth" ? 0.95 : 0.08);
  syncLayerVisibility(map, "choropleth-extrusion", mode === "extrusion");
}

/** The pitch each mode is viewed at: overhead for a choropleth, tilted for columns. */
export function pitchForMapMode(mode: ExplorerMapMode): number {
  return mode === "extrusion" ? 55 : 0;
}
