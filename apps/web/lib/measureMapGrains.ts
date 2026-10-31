// Which grains the one-measure map offers, and which boundary it draws each
// on (web-place-maps).
//
// Nothing here decides what a source publishes: the grains come from the
// catalog's `valid_geo_grains` and the layers and their vintages from
// `/catalog/tile-layers`. What is local is presentation only -- the order the
// grains are offered in, which of them are read one state at a time because
// a national read would be tens of thousands of shapes, and the sentence
// that names the boundary drawn.

import type { TileLayer, TileLayerCatalogResponse } from "./api/types";
import { tileLayerForGrain, versionedTileTemplate } from "./tiles";
import type { TileMetadata } from "./tiles";
import { GEOGRAPHY_NOUNS } from "./uncoloured";
import type { GeographyNoun } from "./uncoloured";

/** The grains the one-measure map can offer, in the order it offers them. */
export const MAP_GRAINS = ["COUNTY", "METRO", "PLACE", "TRACT"] as const;
export type MapGrain = (typeof MAP_GRAINS)[number];

/**
 * Grains read and drawn one state at a time. There are about 32,000 places
 * and 85,000 tracts; a national read of either is a page bound, not a map.
 */
export const STATE_SCOPED_GRAINS: ReadonlySet<MapGrain> = new Set<MapGrain>(["PLACE", "TRACT"]);

export function isMapGrain(value: unknown): value is MapGrain {
  return typeof value === "string" && (MAP_GRAINS as readonly string[]).includes(value.toUpperCase());
}

export function nounFor(grain: MapGrain): GeographyNoun {
  return GEOGRAPHY_NOUNS[grain];
}

/** "Cities and towns", for a heading or a column. */
export function capitalised(text: string): string {
  return text ? `${text[0]!.toUpperCase()}${text.slice(1)}` : text;
}

/**
 * The grains a measure is offered at: those the catalog says it is published
 * at, in the map's order. A measure whose catalog row lists no grains is
 * treated as the county map it always was.
 */
export function mapGrainChoices(validGeoGrains: readonly string[] | null | undefined): MapGrain[] {
  if (!Array.isArray(validGeoGrains)) return ["COUNTY"];
  const published = new Set(validGeoGrains.map((grain) => String(grain).toUpperCase()));
  return MAP_GRAINS.filter((grain) => published.has(grain));
}

/** The grain a page opens on: the one asked for if offered, else the first. */
export function initialMapGrain(choices: readonly MapGrain[], requested?: string | null): MapGrain | null {
  const wanted = String(requested || "").toUpperCase();
  const match = choices.find((grain) => grain === wanted);
  return match || choices[0] || null;
}

/** The newest year a set of rows covers, from their period ends (or starts). */
export function newestPeriodYear(rows: readonly Record<string, unknown>[]): number | null {
  let newest: number | null = null;
  for (const row of rows) {
    const year = Number(String(row.period_end || row.period_start || "").slice(0, 4));
    if (Number.isInteger(year) && year > 0 && (newest === null || year > newest)) newest = year;
  }
  return newest;
}

/** The boundary a grain is drawn on, or why it is not drawn. */
export type MapBoundary =
  | { drawn: true; layer: TileLayer; vintage: number | null; tileTemplate: string }
  | { drawn: false; reason: "no-layer" | "no-vintage"; layer: TileLayer | null };

/**
 * The boundary one grain's map is drawn on, for a period ending in `year`.
 *
 * A versioned layer draws the newest vintage it holds that is not later than
 * the period: the boundaries in force when the value was measured, or the
 * nearest earlier set the layer holds. A layer holding nothing that old draws
 * nothing -- a value is never drawn on a later period's shapes -- and an
 * unversioned layer draws the current boundaries it carries.
 */
export function boundaryForGrain(
  catalog: TileLayerCatalogResponse | null | undefined,
  grain: MapGrain,
  year: number | null,
): MapBoundary {
  const layer = tileLayerForGrain(catalog, grain);
  if (!layer) return { drawn: false, reason: "no-layer", layer: null };
  if (!layer.versioned) {
    const template = versionedTileTemplate(layer);
    return template ? { drawn: true, layer, vintage: null, tileTemplate: template } : { drawn: false, reason: "no-layer", layer };
  }
  const held = [...layer.vintages]
    .filter((entry) => year === null || entry.boundary_vintage <= year)
    .sort((left, right) => right.boundary_vintage - left.boundary_vintage);
  const vintage = held[0]?.boundary_vintage;
  if (vintage === undefined) return { drawn: false, reason: "no-vintage", layer };
  const template = versionedTileTemplate(layer, vintage);
  return template ? { drawn: true, layer, vintage, tileTemplate: template } : { drawn: false, reason: "no-vintage", layer };
}

/** The sentence that names the boundary a map is drawn on, or why there is none. */
export function boundarySentence(boundary: MapBoundary, grain: MapGrain, year: number | null): string {
  const noun = nounFor(grain);
  const singular = grain === "PLACE" ? "city and town" : noun.one;
  if (boundary.drawn) {
    return boundary.vintage === null
      ? `Drawn on the current ${singular} boundaries.`
      : `Drawn on the ${boundary.vintage} ${singular} boundaries.`;
  }
  if (boundary.reason === "no-vintage") {
    const held = (boundary.layer?.vintages || []).map((entry) => entry.boundary_vintage).sort((a, b) => a - b);
    return `No ${singular} boundaries ${year ? `from ${year} or earlier ` : ""}are held${held.length ? ` (held: ${held.join(", ")})` : ""}, and a value is never drawn on another period's shapes. Every value is in the table below.`;
  }
  return `No map layer draws ${noun.many} here; every value is in the table below.`;
}

/** A layer from `/catalog/tile-layers` in the shape the choropleth draws. */
export function tileMetadataForBoundary(boundary: Extract<MapBoundary, { drawn: true }>): TileMetadata {
  const { layer } = boundary;
  return {
    layerId: layer.layer_id,
    sourceLayer: layer.source_layer,
    sourceLayerCandidates: [layer.source_layer],
    joinKey: layer.join_key || "geo_id",
    fields: [],
    tileJsonUrl: `/tiles/${layer.layer_id}`,
    tileTemplate: boundary.tileTemplate,
  };
}

/**
 * The layer filter for one grain, narrowed to one state where the grain is
 * read a state at a time. Features carry `geo_level` (and `state_fips` where
 * they have a state); tiles carry no values, so this only chooses shapes.
 */
export function grainLayerFilter(grain: MapGrain, stateFips?: string | null): unknown {
  const level = ["==", ["get", "geo_level"], grain];
  if (!stateFips || !STATE_SCOPED_GRAINS.has(grain)) return level;
  return ["all", level, ["==", ["to-string", ["get", "state_fips"]], stateFips]];
}

/** Whether a row belongs to the state a state-scoped read asked for. */
export function rowInState(geoId: unknown, stateFips: string): boolean {
  return String(geoId || "").startsWith(`state:${stateFips}|`);
}
