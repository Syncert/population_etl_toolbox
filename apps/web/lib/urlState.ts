// Explorer URL-state contract: parse and serialize the shareable public
// exploration state. Every link shape supported by the current explorer
// (`source`, `metric`, `state`, `geo`, `geo_level`, `map_mode`) stays
// valid; unknown or invalid values are dropped rather than propagated
// into requests.

export const GEO_LEVELS = ["NATIONAL", "STATE", "COUNTY"] as const;
export const MAP_MODES = ["choropleth", "extrusion"] as const;

export type GeoLevel = (typeof GEO_LEVELS)[number];
export type MapMode = (typeof MAP_MODES)[number];

export interface ExplorerState {
  /** API route segment of the explored source, from capability discovery. */
  source?: string;
  metric?: string;
  geoLevel?: GeoLevel;
  mapMode?: MapMode;
  stateFips?: string;
  geoId?: string;
}

/** Defaults are omitted from serialized links. */
export type ExplorerStateDefaults = Pick<
  ExplorerState,
  "source" | "metric" | "geoLevel" | "mapMode"
>;

const STATE_FIPS_PATTERN = /^\d{2}$/;
// Route segments as the API spells them (e.g. "census", "usda-nass").
const SOURCE_SEGMENT_PATTERN = /^[a-z][a-z0-9-]{0,49}$/;

function isGeoLevel(value: string): value is GeoLevel {
  return (GEO_LEVELS as readonly string[]).includes(value);
}

function isMapMode(value: string | null): value is MapMode {
  return value !== null && (MAP_MODES as readonly string[]).includes(value);
}

export function parseExplorerState(search: string | null | undefined): ExplorerState {
  const params = new URLSearchParams(search || "");
  const state: ExplorerState = {};

  const source = params.get("source");
  if (source && SOURCE_SEGMENT_PATTERN.test(source)) {
    state.source = source;
  }

  const metric = params.get("metric");
  if (metric) {
    state.metric = metric;
  }

  const geoLevel = (params.get("geo_level") || "").toUpperCase();
  if (isGeoLevel(geoLevel)) {
    state.geoLevel = geoLevel;
  }

  const mapMode = params.get("map_mode");
  if (isMapMode(mapMode)) {
    state.mapMode = mapMode;
  }

  const stateFips = params.get("state");
  if (stateFips && STATE_FIPS_PATTERN.test(stateFips)) {
    state.stateFips = stateFips;
  }

  const geoId = params.get("geo");
  if (geoId) {
    state.geoId = geoId;
  }

  return state;
}

// Serializes only non-default values so shared URLs stay minimal and two
// equivalent selections produce the same link.
export function serializeExplorerState(
  state: ExplorerState = {},
  defaults: ExplorerStateDefaults = {},
): string {
  const params = new URLSearchParams();

  if (
    state.source &&
    SOURCE_SEGMENT_PATTERN.test(state.source) &&
    state.source !== defaults.source
  ) {
    params.set("source", state.source);
  }
  if (state.metric && state.metric !== defaults.metric) {
    params.set("metric", state.metric);
  }
  if (state.geoLevel && isGeoLevel(state.geoLevel) && state.geoLevel !== defaults.geoLevel) {
    params.set("geo_level", state.geoLevel);
  }
  if (state.mapMode && isMapMode(state.mapMode) && state.mapMode !== defaults.mapMode) {
    params.set("map_mode", state.mapMode);
  }
  if (state.stateFips && STATE_FIPS_PATTERN.test(state.stateFips)) {
    params.set("state", state.stateFips);
  }
  if (state.geoId) {
    params.set("geo", state.geoId);
  }

  return params.toString();
}

export function explorerHref(
  state: ExplorerState = {},
  defaults: ExplorerStateDefaults = {},
): string {
  const query = serializeExplorerState(state, defaults);
  return query ? `/explore?${query}` : "/explore";
}
