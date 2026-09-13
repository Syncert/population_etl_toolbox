// The one declaration of which geography grains the tile boundary can draw.
//
// Three places used to answer that question, and they did not agree. The
// view-mode check read which fips fields the layer publishes; the layer
// filter matched the published `geo_level` and said in its own comment that
// this replaced inferring a grain from fips columns; and the preview decoder
// still inferred it from `county_fips`, which meant the state grain handed
// the map some 32k place polygons that the layer filter then had to hide.
// A grain added to one of the three and not the others is a mode offered
// where nothing can be drawn, or another grain's polygons drawn in its
// place. It is declared once here and read everywhere.

/** One grain the boundary can draw, and the published evidence for it. */
export interface DrawableGrain {
  /** The grain as `geo_level` publishes it and the URL state selects it. */
  grain: string;
  /**
   * The layer field whose presence in the published schema says this grain
   * is attributed. It is a capability check against the layer's schema, not
   * a claim that the boundary carries any row at the grain.
   */
  attributionField: string;
}

export const DRAWABLE_TILE_GRAINS: readonly DrawableGrain[] = [
  { grain: "STATE", attributionField: "state_fips" },
  { grain: "COUNTY", attributionField: "county_fips" },
];

/**
 * Whether the boundary can draw a grain at all. `NATIONAL`, `PLACE` and
 * `AGENCY` are selectable grains (`GEO_LEVELS`) that it cannot: the answer
 * is no before any observation is read, which is why a series at such a
 * grain gets no map rather than a map that failed to colour.
 */
export function isDrawableTileGrain(grain: string | null | undefined): boolean {
  const level = String(grain || "").toUpperCase();
  return DRAWABLE_TILE_GRAINS.some((entry) => entry.grain === level);
}
