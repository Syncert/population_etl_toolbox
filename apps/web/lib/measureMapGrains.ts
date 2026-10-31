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

/**
 * How many years later than the period a boundary vintage may be and still be
 * drawn. Boundaries change slowly -- a few annexations, a handful of new or
 * dissolved places a year -- so a value drawn on boundaries one to three
 * years newer than its period is nearly the right shape, and the page says
 * which. Beyond that the map is withheld and the table carries the values.
 */
export const MAX_LATER_VINTAGE_GAP = 3;

/** The boundary a grain is drawn on, or why it is not drawn. */
export type MapBoundary =
  | {
      drawn: true;
      layer: TileLayer;
      vintage: number | null;
      tileTemplate: string;
      /** True when no vintage at or before the period is held and a later one is drawn. */
      later: boolean;
    }
  | { drawn: false; reason: "no-layer" | "no-vintage"; layer: TileLayer | null };

/**
 * The boundary one grain's map is drawn on, for a period ending in `year`.
 *
 * A versioned layer draws the newest vintage it holds that is not later than
 * the period: the boundaries in force when the value was measured, or the
 * nearest earlier set the layer holds. When it holds none that old, it draws
 * the earliest later vintage within `MAX_LATER_VINTAGE_GAP` years, marked
 * `later` so the page says so; past that gap it draws nothing. An
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
    return template
      ? { drawn: true, layer, vintage: null, tileTemplate: template, later: false }
      : { drawn: false, reason: "no-layer", layer };
  }
  const years = layer.vintages.map((entry) => entry.boundary_vintage).sort((left, right) => left - right);
  const atOrBefore = years.filter((vintage) => year === null || vintage <= year);
  let vintage = atOrBefore[atOrBefore.length - 1];
  let later = false;
  if (vintage === undefined && year !== null) {
    const nextLater = years.find((held) => held > year);
    if (nextLater !== undefined && nextLater - year <= MAX_LATER_VINTAGE_GAP) {
      vintage = nextLater;
      later = true;
    }
  }
  if (vintage === undefined) return { drawn: false, reason: "no-vintage", layer };
  const template = versionedTileTemplate(layer, vintage);
  return template ? { drawn: true, layer, vintage, tileTemplate: template, later } : { drawn: false, reason: "no-vintage", layer };
}

/** The sentence that names the boundary a map is drawn on, or why there is none. */
export function boundarySentence(boundary: MapBoundary, grain: MapGrain, year: number | null): string {
  const noun = nounFor(grain);
  const singular = grain === "PLACE" ? "city and town" : noun.one;
  if (boundary.drawn) {
    if (boundary.vintage === null) return `Drawn on the current ${singular} boundaries.`;
    if (boundary.later && year !== null) {
      return `Drawn on the ${boundary.vintage} ${singular} boundaries; ${year} boundaries are not yet held, so a few ${noun.many} may differ from the shapes the values were measured on.`;
    }
    return `Drawn on the ${boundary.vintage} ${singular} boundaries.`;
  }
  if (boundary.reason === "no-vintage") {
    const held = (boundary.layer?.vintages || []).map((entry) => entry.boundary_vintage).sort((a, b) => a - b);
    return `No ${singular} boundaries ${year ? `from ${year} or up to ${MAX_LATER_VINTAGE_GAP} years after it ` : ""}are held${held.length ? ` (held: ${held.join(", ")})` : ""}, so the map is not drawn on shapes that far from the period. Every value is in the table below.`;
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
