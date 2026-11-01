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
  | {
      drawn: true;
      layer: TileLayer;
      vintage: number | null;
      tileTemplate: string;
      /**
       * True when the boundaries are from a year other than the period's, so
       * the page marks the map with an asterisk and says which year they are.
       */
      marked: boolean;
    }
  | { drawn: false; reason: "no-layer" | "no-vintage"; layer: TileLayer | null };

/**
 * The boundary one grain's map is drawn on, for a period ending in `year`.
 *
 * A versioned layer always draws the most recent vintage it holds (Nick,
 * 2026-10-31: "City maps should be the most recent map available", marked
 * with when the map was last updated). Boundaries change slowly, and a map
 * on the newest shapes is more useful than no map; when that vintage is not
 * the period's year the boundary is `marked`, and the page puts an asterisk
 * on the legend and names the year in the legend note and the map's tooltip.
 * A versioned layer holding no vintage at all draws nothing. An unversioned
 * layer draws the current boundaries it carries.
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
    return template
      ? { drawn: true, layer, vintage: null, tileTemplate: template, marked: false }
      : { drawn: false, reason: "no-layer", layer };
  }
  const years = layer.vintages.map((entry) => entry.boundary_vintage);
  if (!years.length) return { drawn: false, reason: "no-vintage", layer };
  const vintage = Math.max(...years);
  const template = versionedTileTemplate(layer, vintage);
  if (!template) return { drawn: false, reason: "no-vintage", layer };
  return { drawn: true, layer, vintage, tileTemplate: template, marked: year !== null && vintage !== year };
}

function singularFor(grain: MapGrain): string {
  return grain === "PLACE" ? "city and town" : nounFor(grain).one;
}

/**
 * The short line naming when a drawn map's boundaries were last updated: the
 * legend's note and the map's tooltip.
 */
export function boundaryNote(boundary: Extract<MapBoundary, { drawn: true }>, grain: MapGrain): string {
  const singular = capitalised(singularFor(grain));
  if (boundary.vintage === null) return `${singular} boundaries: current.`;
  return `${singular} boundaries last updated ${boundary.vintage}, the most recent held.`;
}

/** The sentence that names the boundary a map is drawn on, or why there is none. */
export function boundarySentence(boundary: MapBoundary, grain: MapGrain, year: number | null): string {
  const noun = nounFor(grain);
  const singular = singularFor(grain);
  if (boundary.drawn) {
    if (boundary.vintage === null) return `Drawn on the current ${singular} boundaries.`;
    if (boundary.marked && year !== null) {
      return `* Drawn on the ${boundary.vintage} ${singular} boundaries, the most recent held; the values are for ${year}, so a few ${noun.many} may differ from the shapes they were measured on.`;
    }
    return `Drawn on the ${boundary.vintage} ${singular} boundaries.`;
  }
  if (boundary.reason === "no-vintage") {
    return `No ${singular} boundaries are held yet, so the map is not drawn. Every value is in the table below.`;
  }
  return `No map layer draws ${noun.many} here; every value is in the table below.`;
}

/**
 * Grains whose tile layer can be asked for one state's shapes (`?state=NN`).
 *
 * Cities only: `gold.tile_places` takes the argument. A state map opens at the
 * layer's minzoom, where one tile spans several states, so without it a Texas
 * map was sent every Oklahoma, Louisiana, Arkansas and New Mexico city in its
 * border tiles only for the layer filter to hide them. Tracts are a table
 * source, which cannot take an argument, and keep the client filter alone.
 */
export const STATE_TILE_GRAINS: ReadonlySet<MapGrain> = new Set<MapGrain>(["PLACE"]);

/**
 * A tile template narrowed to one state, where the grain's layer takes it.
 *
 * The narrowing is a saving, not a rule: the layer filter still selects the
 * state's shapes, so a tile server that ignores the argument draws the same
 * map. A place never crosses a state line, so a narrowed tile draws exactly
 * the shapes the filter would have kept.
 */
export function stateScopedTileTemplate(template: string, grain: MapGrain, stateFips?: string | null): string {
  if (!stateFips || !/^\d{2}$/.test(stateFips) || !STATE_TILE_GRAINS.has(grain)) return template;
  return `${template}${template.includes("?") ? "&" : "?"}state=${stateFips}`;
}

/**
 * A layer from `/catalog/tile-layers` in the shape the choropleth draws, its
 * tiles narrowed to `stateFips` where the grain's layer can be.
 */
export function tileMetadataForBoundary(
  boundary: Extract<MapBoundary, { drawn: true }>,
  grain?: MapGrain,
  stateFips?: string | null,
): TileMetadata {
  const { layer } = boundary;
  return {
    layerId: layer.layer_id,
    sourceLayer: layer.source_layer,
    sourceLayerCandidates: [layer.source_layer],
    joinKey: layer.join_key || "geo_id",
    fields: [],
    tileJsonUrl: `/tiles/${layer.layer_id}`,
    tileTemplate: grain ? stateScopedTileTemplate(boundary.tileTemplate, grain, stateFips) : boundary.tileTemplate,
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
