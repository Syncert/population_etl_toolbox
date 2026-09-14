// URL-state contract for the public analysis surfaces: parse and serialize
// the shareable exploration and comparison state.
//
// Explorer state comes first; the comparison workspace's own state is at the
// bottom of the file and shares this module's validation vocabulary.
//
// Explorer: parse and serialize the shareable public exploration state. Every link shape supported by the current explorer
// (`source`, `metric`, `state`, `geo`, `geo_level`, `map_mode`, `value_scale`,
// `scope`, `release`) stays valid; unknown or invalid values are dropped rather than
// propagated into requests.

// The published grain vocabulary, all five words of it. `geo_level` on a
// served row is always one of these, and a metric's `valid_geo_grains` uses
// the same words, so a grain read from the catalog can be sent straight back
// as the filter (API_CONSUMER_GUIDE.md). PLACE is Census PEP's and AGENCY is
// FBI UCR's; dropping them here made a shared link to either kind of view
// open on a grain the measure does not publish (WEB-038).
export const GEO_LEVELS = ["NATIONAL", "STATE", "COUNTY", "PLACE", "AGENCY"] as const;

// The words the vocabulary replaced, and what each one means now. The catalog
// published `NATION` for CDC, PEP and USDA NASS before the grains were
// unified, and ADR-0002 promises a saved configuration or a shared link
// holding one keeps answering -- which is why the API accepts them, from the
// same mapping under the same name (`registry.GEO_GRAIN_ALIASES`). This
// application honoured case and not the aliases, so a grain read from outside
// it -- a link, a stored document's `filters.geo_level`, a metric's
// `valid_geo_grains` -- was dropped entirely when it carried one, and the
// view opened on a default grain the measure may not publish (WEB-076).
export const GEO_GRAIN_ALIASES: Readonly<Record<string, GeoLevel>> = {
  NATION: "NATIONAL",
  US: "NATIONAL",
};
export const MAP_MODES = ["choropleth", "extrusion"] as const;
export const VALUE_SCALES = ["linear", "log"] as const;
export const OBSERVATION_SCOPES = ["latest", "as_released"] as const;

export type GeoLevel = (typeof GEO_LEVELS)[number];
export type MapMode = (typeof MAP_MODES)[number];
export type ValueScale = (typeof VALUE_SCALES)[number];
export type ObservationScope = (typeof OBSERVATION_SCOPES)[number];

/**
 * The vocabulary word for a grain read from outside this application.
 *
 * Trimmed, upper-cased, and de-aliased -- the same three steps as the API's
 * `registry.normalize_geo_level`, and for the same stated reason: normalising
 * is not validating, so a word that is not a grain comes back unchanged and
 * the caller decides what to do about it. Every grain that enters from a URL,
 * a stored document, or the catalog goes through here, so the aliases are
 * honoured in one place rather than at each reader.
 */
export function normalizeGeoLevel(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }
  const word = value.trim().toUpperCase();
  return GEO_GRAIN_ALIASES[word] || word;
}

export interface ExplorerState {
  /** Published identity of the explored source, from capability discovery. */
  source?: string;
  metric?: string;
  geoLevel?: GeoLevel;
  mapMode?: MapMode;
  /** How values map to colour and height: equal-width bins, or logarithmic. */
  valueScale?: ValueScale;
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
  /**
   * The per-source dimension narrowing the view was reading under, keyed by
   * the source's own declared filter name — `stratum_id`,
   * `adjustment_status`, `domain_desc` — which is the same key the saved
   * document's `filters` uses.
   *
   * A copied link carried none of it. The screen itself refuses to chart a
   * CDC series until the reader narrows to one stratum, so the link that was
   * copied *from that view* reopened stratified, with a blank map, while the
   * saved-view path carried the same narrowing: two records of one view that
   * disagreed (WEB-073).
   */
  dimensions?: Record<string, string>;
}

// URL keys the explorer's own controls own. A declared filter spelled like
// one of these is not carried as a dimension, because writing it would
// overwrite a control rather than narrow anything. No source declares one
// today; the guard is here so adding one cannot silently break a link.
const RESERVED_EXPLORER_PARAMS: ReadonlySet<string> = new Set([
  "source",
  "metric",
  "geo_level",
  "map_mode",
  "value_scale",
  "state",
  "geo",
  "scope",
  "release",
]);
// The API's own filter-name shape, and a bound on what a link may carry.
const DIMENSION_NAME_PATTERN = /^[a-z][a-z0-9_]{0,49}$/;
const DIMENSION_VALUE_MAX_LENGTH = 200;

function isCarriableDimension(name: string, value: unknown): value is string {
  return (
    !RESERVED_EXPLORER_PARAMS.has(name) &&
    DIMENSION_NAME_PATTERN.test(name) &&
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= DIMENSION_VALUE_MAX_LENGTH
  );
}

/** Defaults are omitted from serialized links. */
export type ExplorerStateDefaults = Pick<
  ExplorerState,
  "source" | "metric" | "geoLevel" | "mapMode" | "valueScale" | "scope"
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

function isValueScale(value: string | null): value is ValueScale {
  return value !== null && (VALUE_SCALES as readonly string[]).includes(value);
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

  const geoLevel = normalizeGeoLevel(params.get("geo_level"));
  if (isGeoLevel(geoLevel)) {
    state.geoLevel = geoLevel;
  }

  const mapMode = params.get("map_mode");
  if (isMapMode(mapMode)) {
    state.mapMode = mapMode;
  }
  const valueScale = params.get("value_scale");
  if (isValueScale(valueScale)) {
    state.valueScale = valueScale;
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

  // Anything else shaped like a filter name is carried as a dimension. The
  // parser does not know which names a source declares -- the capability
  // entry does, and the screen applies only the declared ones -- so this
  // stays a pure read of the URL and the declaration check stays in the one
  // place that already makes it (WEB-073).
  const dimensions: Record<string, string> = {};
  for (const [name, value] of params.entries()) {
    if (isCarriableDimension(name, value)) {
      dimensions[name] = value;
    }
  }
  if (Object.keys(dimensions).length > 0) {
    state.dimensions = dimensions;
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
  // Serialized as the vocabulary word, so a state built from a document or a
  // link that carries an alias produces a link the rest of this application
  // reads.
  const geoLevel = normalizeGeoLevel(state.geoLevel);
  if (geoLevel && isGeoLevel(geoLevel) && geoLevel !== defaults.geoLevel) {
    params.set("geo_level", geoLevel);
  }
  if (state.mapMode && isMapMode(state.mapMode) && state.mapMode !== defaults.mapMode) {
    params.set("map_mode", state.mapMode);
  }
  if (
    state.valueScale &&
    isValueScale(state.valueScale) &&
    state.valueScale !== defaults.valueScale
  ) {
    params.set("value_scale", state.valueScale);
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
  // Sorted, so two equivalent selections produce the same link -- the rule
  // this serializer is built on.
  for (const name of Object.keys(state.dimensions || {}).sort()) {
    const value = (state.dimensions || {})[name];
    if (isCarriableDimension(name, value)) {
      params.set(name, value);
    }
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

// --- Comparison workspace ---
//
// The comparison link names both measures, the geography grain, and the
// state scope. It deliberately does not carry a compatibility verdict: the
// verdict belongs to the API and is re-asked on open, so a link can never
// reproduce a stale "comparable" for a pair whose published semantics have
// since changed.

export interface ComparisonUrlState {
  metricA?: string;
  metricB?: string;
  sourceA?: string;
  sourceB?: string;
  geoLevel?: GeoLevel;
  stateFips?: string;
}

export type ComparisonUrlDefaults = Pick<ComparisonUrlState, "geoLevel">;

export function parseComparisonState(
  search: string | null | undefined,
): ComparisonUrlState {
  const params = new URLSearchParams(search || "");
  const state: ComparisonUrlState = {};

  const metricA = params.get("a");
  if (metricA) {
    state.metricA = metricA;
  }
  const metricB = params.get("b");
  if (metricB) {
    state.metricB = metricB;
  }

  const sourceA = params.get("source_a");
  if (sourceA && SOURCE_KEY_PATTERN.test(sourceA)) {
    state.sourceA = sourceA;
  }
  const sourceB = params.get("source_b");
  if (sourceB && SOURCE_KEY_PATTERN.test(sourceB)) {
    state.sourceB = sourceB;
  }

  const geoLevel = normalizeGeoLevel(params.get("geo_level"));
  if (isGeoLevel(geoLevel)) {
    state.geoLevel = geoLevel;
  }

  const stateFips = params.get("state");
  if (stateFips && STATE_FIPS_PATTERN.test(stateFips)) {
    state.stateFips = stateFips;
  }

  return state;
}

export function serializeComparisonState(
  state: ComparisonUrlState = {},
  defaults: ComparisonUrlDefaults = {},
): string {
  const params = new URLSearchParams();

  if (state.metricA) {
    params.set("a", state.metricA);
  }
  if (state.metricB) {
    params.set("b", state.metricB);
  }
  if (state.sourceA && SOURCE_KEY_PATTERN.test(state.sourceA)) {
    params.set("source_a", state.sourceA);
  }
  if (state.sourceB && SOURCE_KEY_PATTERN.test(state.sourceB)) {
    params.set("source_b", state.sourceB);
  }
  // Serialized as the vocabulary word, so a state built from a document or a
  // link that carries an alias produces a link the rest of this application
  // reads.
  const geoLevel = normalizeGeoLevel(state.geoLevel);
  if (geoLevel && isGeoLevel(geoLevel) && geoLevel !== defaults.geoLevel) {
    params.set("geo_level", geoLevel);
  }
  if (state.stateFips && STATE_FIPS_PATTERN.test(state.stateFips)) {
    params.set("state", state.stateFips);
  }

  return params.toString();
}

export function comparisonHref(
  state: ComparisonUrlState = {},
  defaults: ComparisonUrlDefaults = {},
): string {
  const query = serializeComparisonState(state, defaults);
  return query ? `/compare?${query}` : "/compare";
}

// --- Product profiles ---
//
// A profile link names the template and the place. It carries no measure
// values: reopening it re-asks the catalog and the observations, so a shared
// profile can never show what a place looked like when the link was made
// while presenting it as current.

export interface ProfileUrlState {
  template?: string;
  geoId?: string;
}

export type ProfileUrlDefaults = Pick<ProfileUrlState, "template">;

const TEMPLATE_ID_PATTERN = /^[a-z][a-z0-9-]{0,49}$/;

export function parseProfileState(search: string | null | undefined): ProfileUrlState {
  const params = new URLSearchParams(search || "");
  const state: ProfileUrlState = {};

  const template = params.get("template");
  if (template && TEMPLATE_ID_PATTERN.test(template)) {
    state.template = template;
  }
  const geoId = params.get("place");
  if (geoId) {
    state.geoId = geoId;
  }
  return state;
}

export function serializeProfileState(
  state: ProfileUrlState = {},
  defaults: ProfileUrlDefaults = {},
): string {
  const params = new URLSearchParams();
  if (
    state.template &&
    TEMPLATE_ID_PATTERN.test(state.template) &&
    state.template !== defaults.template
  ) {
    params.set("template", state.template);
  }
  if (state.geoId) {
    params.set("place", state.geoId);
  }
  return params.toString();
}

export function profileHref(
  state: ProfileUrlState = {},
  defaults: ProfileUrlDefaults = {},
): string {
  const query = serializeProfileState(state, defaults);
  return query ? `/profiles?${query}` : "/profiles";
}

// --- Workbench ---
//
// A workbench link carries the composition: each series' source, measure,
// scope, pinned release, grain, geography and dimension pins, plus the shared
// presentation and the cross-sectional grain. It carries no value, no
// configuration id and no token — the privacy boundary the first-wave handoff
// states, which holds here for the same reason it holds on the explorer: a
// link that carried values would show a reader what the warehouse said when
// the link was made while presenting it as current.
//
// Each series is one `s` parameter, so the shape is flat, order is the
// composition's own, and a single malformed series is dropped without taking
// the rest of the link with it. Within one `s`, fields are `key:value`
// separated by `;` — chosen over JSON because a link a reader can read is a
// link a reader can edit, and over positional fields because a composition
// with no pinned release should not carry an empty slot for one.

export const WORKBENCH_PRESENTATION_WORDS = [
  "line",
  "bar",
  "scatter",
  "ranking",
  "correlation",
  "heatmap",
] as const;

export type WorkbenchPresentationWord =
  (typeof WORKBENCH_PRESENTATION_WORDS)[number];

export interface WorkbenchSeriesUrlState {
  sourceKey: string;
  metricCode: string;
  scope?: ObservationScope;
  release?: string;
  geoLevel?: GeoLevel;
  geoId?: string;
  filters?: Record<string, string>;
}

export interface WorkbenchUrlState {
  series?: WorkbenchSeriesUrlState[];
  presentation?: WorkbenchPresentationWord;
  /** The shared grain a cross-sectional presentation reads at. */
  alignmentGeoLevel?: GeoLevel;
  stateFips?: string;
  /** The same-year pin, off unless a reader asked for it. */
  year?: number;
  correlation?: boolean;
}

export type WorkbenchUrlDefaults = Pick<
  WorkbenchUrlState,
  "presentation" | "alignmentGeoLevel"
>;

/**
 * The ceiling a link carries. Beyond it the share control explains why the
 * link cannot be made and points at saving, rather than producing a URL that
 * some browser, proxy or chat client will truncate into a different
 * composition. Kept equal to `MAX_WORKBENCH_SERIES`; stated here rather than
 * imported because `urlState` must not depend on the workbench module that
 * depends on it.
 */
export const MAX_WORKBENCH_URL_SERIES = 8;

const METRIC_CODE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_:.\-/]{0,199}$/;
const GEO_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_:.\-]{0,99}$/;
const RELEASE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_:.\- ]{0,99}$/;
/**
 * Field names inside one `s` that are the series' own, plus every name the
 * observation request already spends on something else.
 *
 * The second half is the part that matters. `isCarriableDimension` screens
 * against the *explorer's* reserved parameter names, which are that page's
 * short spellings (`metric`, `geo`, `state`) and not the API's — so
 * `metric_code`, `limit` or `offset` written into a series by hand passed
 * that screen and would have been sent as a dimension filter, either refused
 * by the resource as undeclared or, worse, overriding the measure the rest
 * of the link names. A dimension pin is one of the source's own declared
 * filters; nothing else may ride in as one.
 */
const SERIES_RESERVED_KEYS = new Set([
  "src",
  "m",
  "scope",
  "rel",
  "lvl",
  "geo",
  "metric_code",
  "geo_id",
  "geo_level",
  "state_fips",
  "county_fips",
  "year_from",
  "year_to",
  "release",
  "limit",
  "offset",
  "newest_per_geography",
  "newest_release_per_period",
]);

function isWorkbenchPresentation(
  value: string | null,
): value is WorkbenchPresentationWord {
  return (
    value !== null &&
    (WORKBENCH_PRESENTATION_WORDS as readonly string[]).includes(value)
  );
}

function parseWorkbenchSeries(
  raw: string,
): WorkbenchSeriesUrlState | null {
  const fields = new Map<string, string>();
  for (const part of raw.split(";")) {
    const separator = part.indexOf(":");
    if (separator <= 0) {
      continue;
    }
    const name = part.slice(0, separator);
    const value = part.slice(separator + 1);
    if (name && value && !fields.has(name)) {
      fields.set(name, value);
    }
  }

  const sourceKey = fields.get("src") || "";
  const metricCode = fields.get("m") || "";
  // Both are required: a series naming no measure is not a series, and a
  // series naming no source cannot be re-resolved to the access shape that
  // reads it. Dropped rather than half-restored.
  if (!SOURCE_KEY_PATTERN.test(sourceKey)) {
    return null;
  }
  if (!METRIC_CODE_PATTERN.test(metricCode)) {
    return null;
  }

  const series: WorkbenchSeriesUrlState = { sourceKey, metricCode };

  const scope = fields.get("scope") || null;
  if (isScope(scope)) {
    series.scope = scope;
  }
  const release = fields.get("rel");
  if (release && RELEASE_PATTERN.test(release)) {
    series.release = release;
  }
  const geoLevel = normalizeGeoLevel(fields.get("lvl"));
  if (isGeoLevel(geoLevel)) {
    series.geoLevel = geoLevel;
  }
  const geoId = fields.get("geo");
  if (geoId && GEO_ID_PATTERN.test(geoId)) {
    series.geoId = geoId;
  }

  const filters: Record<string, string> = {};
  for (const [name, value] of fields) {
    if (SERIES_RESERVED_KEYS.has(name)) {
      continue;
    }
    if (isCarriableDimension(name, value)) {
      filters[name] = value;
    }
  }
  if (Object.keys(filters).length > 0) {
    series.filters = filters;
  }

  return series;
}

export function parseWorkbenchState(
  search: string | null | undefined,
): WorkbenchUrlState {
  const params = new URLSearchParams(search || "");
  const state: WorkbenchUrlState = {};

  const series = params
    .getAll("s")
    .slice(0, MAX_WORKBENCH_URL_SERIES)
    .map(parseWorkbenchSeries)
    .filter((entry): entry is WorkbenchSeriesUrlState => entry !== null);
  if (series.length > 0) {
    state.series = series;
  }

  const presentation = params.get("view");
  if (isWorkbenchPresentation(presentation)) {
    state.presentation = presentation;
  }

  const alignment = normalizeGeoLevel(params.get("grain"));
  if (isGeoLevel(alignment)) {
    state.alignmentGeoLevel = alignment;
  }

  const stateFips = params.get("state");
  if (stateFips && STATE_FIPS_PATTERN.test(stateFips)) {
    state.stateFips = stateFips;
  }

  const year = params.get("year");
  if (year && /^\d{4}$/.test(year)) {
    state.year = Number(year);
  }

  // Present and "1" is on; present and anything else is off rather than
  // dropped, so a link someone edited by hand cannot turn the panel on by
  // accident.
  const correlation = params.get("corr");
  if (correlation !== null) {
    state.correlation = correlation === "1";
  }

  return state;
}

function serializeWorkbenchSeries(
  series: WorkbenchSeriesUrlState,
): string | null {
  if (!SOURCE_KEY_PATTERN.test(series.sourceKey || "")) {
    return null;
  }
  if (!METRIC_CODE_PATTERN.test(series.metricCode || "")) {
    return null;
  }
  const parts = [`src:${series.sourceKey}`, `m:${series.metricCode}`];
  // `latest` is the resource's own default and is omitted, like every other
  // default this module leaves out of a link.
  if (series.scope && series.scope !== "latest") {
    parts.push(`scope:${series.scope}`);
  }
  if (series.release && RELEASE_PATTERN.test(series.release)) {
    parts.push(`rel:${series.release}`);
  }
  const geoLevel = normalizeGeoLevel(series.geoLevel);
  if (isGeoLevel(geoLevel)) {
    parts.push(`lvl:${geoLevel}`);
  }
  if (series.geoId && GEO_ID_PATTERN.test(series.geoId)) {
    parts.push(`geo:${series.geoId}`);
  }
  for (const name of Object.keys(series.filters || {}).sort()) {
    const value = (series.filters || {})[name];
    if (!SERIES_RESERVED_KEYS.has(name) && isCarriableDimension(name, value)) {
      parts.push(`${name}:${value}`);
    }
  }
  return parts.join(";");
}

export function serializeWorkbenchState(
  state: WorkbenchUrlState = {},
  defaults: WorkbenchUrlDefaults = {},
): string {
  const params = new URLSearchParams();

  for (const series of (state.series || []).slice(
    0,
    MAX_WORKBENCH_URL_SERIES,
  )) {
    const encoded = serializeWorkbenchSeries(series);
    if (encoded) {
      params.append("s", encoded);
    }
  }

  if (
    state.presentation &&
    (WORKBENCH_PRESENTATION_WORDS as readonly string[]).includes(
      state.presentation,
    ) &&
    state.presentation !== defaults.presentation
  ) {
    params.set("view", state.presentation);
  }

  const alignment = normalizeGeoLevel(state.alignmentGeoLevel);
  if (
    alignment &&
    isGeoLevel(alignment) &&
    alignment !== defaults.alignmentGeoLevel
  ) {
    params.set("grain", alignment);
  }

  if (state.stateFips && STATE_FIPS_PATTERN.test(state.stateFips)) {
    params.set("state", state.stateFips);
  }

  if (
    typeof state.year === "number" &&
    Number.isInteger(state.year) &&
    state.year >= 1000 &&
    state.year <= 9999
  ) {
    params.set("year", String(state.year));
  }

  // Off is the default and is omitted; only the on state is carried.
  if (state.correlation === true) {
    params.set("corr", "1");
  }

  return params.toString();
}

export function workbenchHref(
  state: WorkbenchUrlState = {},
  defaults: WorkbenchUrlDefaults = {},
): string {
  const query = serializeWorkbenchState(state, defaults);
  return query ? `/workbench?${query}` : "/workbench";
}

/**
 * Whether a composition fits in a link, and what to say when it does not.
 *
 * The share control asks this rather than producing a URL and hoping: a link
 * silently truncated by a chat client reopens as a different composition,
 * which is worse than no link.
 */
export function workbenchLinkCeiling(seriesCount: number): {
  fits: boolean;
  reason: string;
} {
  if (seriesCount <= MAX_WORKBENCH_URL_SERIES) {
    return { fits: true, reason: "" };
  }
  return {
    fits: false,
    reason:
      `A link carries at most ${MAX_WORKBENCH_URL_SERIES} series and this ` +
      `composition has ${seriesCount}. Save it instead — a saved workbench ` +
      "carries every series and reopens against the live publication.",
  };
}
