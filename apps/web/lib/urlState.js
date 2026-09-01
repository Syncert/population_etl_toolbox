// Explorer URL-state contract: parse and serialize the shareable public
// exploration state. Every link shape supported by the current explorer
// (`metric`, `state`, `geo`, `geo_level`, `map_mode`) stays valid; unknown
// or invalid values are dropped rather than propagated into requests.

export const GEO_LEVELS = Object.freeze(["NATIONAL", "STATE", "COUNTY"]);
export const MAP_MODES = Object.freeze(["choropleth", "extrusion"]);

const STATE_FIPS_PATTERN = /^\d{2}$/;

export function parseExplorerState(search) {
  const params = new URLSearchParams(search || "");
  const state = {};

  const metric = params.get("metric");
  if (metric) {
    state.metric = metric;
  }

  const geoLevel = (params.get("geo_level") || "").toUpperCase();
  if (GEO_LEVELS.includes(geoLevel)) {
    state.geoLevel = geoLevel;
  }

  const mapMode = params.get("map_mode");
  if (MAP_MODES.includes(mapMode)) {
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
export function serializeExplorerState(state = {}, defaults = {}) {
  const params = new URLSearchParams();

  if (state.metric && state.metric !== defaults.metric) {
    params.set("metric", state.metric);
  }
  if (
    state.geoLevel &&
    GEO_LEVELS.includes(state.geoLevel) &&
    state.geoLevel !== defaults.geoLevel
  ) {
    params.set("geo_level", state.geoLevel);
  }
  if (
    state.mapMode &&
    MAP_MODES.includes(state.mapMode) &&
    state.mapMode !== defaults.mapMode
  ) {
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

export function explorerHref(state = {}, defaults = {}) {
  const query = serializeExplorerState(state, defaults);
  return query ? `/explore?${query}` : "/explore";
}
