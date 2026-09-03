// Explorer URL-state contract: parse and serialize the shareable public
// exploration state. Every link shape supported by the current explorer
// (`source`, `metric`, `state`, `geo`, `geo_level`, `map_mode`, `scope`,
// `release`) stays valid; unknown or invalid values are dropped rather than
// propagated into requests.

export const GEO_LEVELS = ["NATIONAL", "STATE", "COUNTY"] as const;
export const MAP_MODES = ["choropleth", "extrusion"] as const;
export const OBSERVATION_SCOPES = ["latest", "as_released"] as const;

export type GeoLevel = (typeof GEO_LEVELS)[number];
export type MapMode = (typeof MAP_MODES)[number];
export type ObservationScope = (typeof OBSERVATION_SCOPES)[number];

export interface ExplorerState {
  /** Published identity of the explored source, from capability discovery. */
  source?: string;
  metric?: string;
  geoLevel?: GeoLevel;
  mapMode?: MapMode;
  stateFips?: string;
  geoId?: string;
  /** `latest` (the source's own latest publication) or `as_released`. */
  scope?: ObservationScope;
  /**
   * A release identity from `/observations/releases`. Only meaningful with
   * `scope=as_released`: the API rejects `release` without it with a 422, so
   * a link carrying one alone would not reproduce a valid request.
   */
  release?: string;
}

/** Defaults are omitted from serialized links. */
export type ExplorerStateDefaults = Pick<
  ExplorerState,
  "source" | "metric" | "geoLevel" | "mapMode" | "scope"
>;

const STATE_FIPS_PATTERN = /^\d{2}$/;
// Published source identities as the API spells them: a route segment
// ("census", "usda-nass") or, for a source that publishes none, its
// glossary source code ("FBI_UCR").
const SOURCE_KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,49}$/;

function isGeoLevel(value: string): value is GeoLevel {
  return (GEO_LEVELS as readonly string[]).includes(value);
}

function isMapMode(value: string | null): value is MapMode {
  return value !== null && (MAP_MODES as readonly string[]).includes(value);
}

function isScope(value: string | null): value is ObservationScope {
  return value !== null && (OBSERVATION_SCOPES as readonly string[]).includes(value);
}

export function parseExplorerState(search: string | null | undefined): ExplorerState {
  const params = new URLSearchParams(search || "");
  const state: ExplorerState = {};

  const source = params.get("source");
  if (source && SOURCE_KEY_PATTERN.test(source)) {
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

  const scope = params.get("scope");
  if (isScope(scope)) {
    state.scope = scope;
  }

  // A pinned release only reproduces an analysis under `scope=as_released`;
  // carried alone it would build a request the API answers with a 422, so
  // it is dropped rather than propagated.
  const release = params.get("release");
  if (release && state.scope === "as_released") {
    state.release = release;
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
    SOURCE_KEY_PATTERN.test(state.source) &&
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
  if (state.scope && isScope(state.scope) && state.scope !== defaults.scope) {
    params.set("scope", state.scope);
  }
  if (state.release && state.scope === "as_released") {
    params.set("release", state.release);
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
