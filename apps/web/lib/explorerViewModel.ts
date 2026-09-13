// Pure explorer view models: deterministic display transformations over
// API observations, catalog metrics, distribution bins, and MapLibre
// expressions. No React, no fetch, no browser state.

import type { DistributionResponse, MetricSummary } from "./api/types";
import type { ValueScale } from "./urlState";

export const CHOROPLETH_FALLBACK_COLOR = "#9fb0ba";
export const CHOROPLETH_PALETTE = ["#edcf63", "#9dc57d", "#419261", "#2f7fa6", "#594a9b"];
export const DEFAULT_POPULATION_VARIABLE = "B01003_001";

// A MapLibre style expression / filter, kept structural: the view models
// build them as plain arrays and the map boundary owns the cast.
export type MapExpression = unknown[];
export type TileFilter = boolean | unknown[];

/**
 * One observation row as the explorer consumes it. Loose by design — the
 * per-source routes publish source-specific fields under their own names —
 * but `value` follows the guide's guarantee: text (or a number from older
 * shapes), never coerced, `null`/absent when nothing was published.
 */
export interface ObservationRow {
  geo_id?: string | null;
  geo_level?: string | null;
  metric_code?: string | null;
  value?: string | number | null;
  [key: string]: unknown;
}

export interface MetricOption {
  value: string;
  label: string;
  source: string | null | undefined;
}

export interface DatasetFacetOption {
  value: string;
  label: string;
}

export interface DistributionBinModel {
  binIndex: number;
  color: string;
  lowerBound: number;
  upperBound: number;
  count: number;
}

export interface LegendItem {
  color: string;
  label: string;
  count?: number;
}

export interface ChoroplethModel {
  expression: MapExpression;
  legendItems: LegendItem[];
  minValue: number | null;
  maxValue: number | null;
  usesDistribution: boolean;
  valueCount: number;
  /** The scale the colours were assigned on; "linear" when log was asked for but nothing positive was published. */
  scale: ValueScale;
}

export function metricDataset(metricCode: unknown): string {
  const parts = typeof metricCode === "string" ? metricCode.split(":") : [];
  return parts.length >= 3 ? (parts[1] || "").toLowerCase() : "";
}

export function metricVariable(metricCode: unknown): string {
  const parts = typeof metricCode === "string" ? metricCode.split(":") : [];
  return parts.length >= 3 ? parts.slice(2).join(":") : "";
}

// Presentation vocabulary for published dataset facets the application
// documents coverage for; unlisted facets fall back to their published
// spelling. This labels known facets — it does not decide which exist.
const DATASET_FACET_LABELS: Record<string, string> = {
  acs5: "ACS 5-year — complete county coverage",
  acs1: "ACS 1-year — partial county coverage",
};

/**
 * Distinct dataset facets carried by the loaded metrics' own published
 * codes (`SOURCE:dataset:variable`), sorted for deterministic rendering.
 * Sources whose metric codes embed no facet get an empty list, which the
 * explorer renders as "no dataset selector".
 */
export function datasetFacetOptions(
  metrics: MetricSummary[] | null | undefined,
): DatasetFacetOption[] {
  const facets = new Set<string>();
  for (const metric of metrics || []) {
    const facet = metricDataset(metric.metric_code);
    if (facet) {
      facets.add(facet);
    }
  }
  return [...facets].sort().map((facet) => ({
    value: facet,
    label: DATASET_FACET_LABELS[facet] || facet.toUpperCase(),
  }));
}

/** The default facet: the complete-coverage ACS facet when published, else the first. */
export function preferredDatasetFacet(
  metrics: MetricSummary[] | null | undefined,
): string {
  const options = datasetFacetOptions(metrics);
  const acs5 = options.find((option) => option.value === "acs5");
  return (acs5 || options[0])?.value || "";
}

export function pickPreferredMetric(
  metrics: MetricSummary[] | null | undefined,
  dataset: string,
  preferredVariable: string = DEFAULT_POPULATION_VARIABLE,
): string {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return "";
  }

  // No facet selected means "the whole list", which is what the explorer
  // renders when a source publishes fewer than two facets. Filtering on the
  // empty string instead selected exactly the metrics whose codes carry no
  // facet -- for BLS, the 56 national series and none of the LAUS measures.
  const datasetMetrics = dataset
    ? metrics.filter((item) => metricDataset(item.metric_code) === dataset)
    : metrics;
  const candidates = datasetMetrics.length > 0 ? datasetMetrics : metrics;
  const matchingVariable = candidates.find(
    (item) => metricVariable(item.metric_code) === preferredVariable,
  );
  const canonicalPopulation = candidates.find(
    (item) => metricVariable(item.metric_code) === DEFAULT_POPULATION_VARIABLE,
  );
  // Nothing named the measure, so fall back on what the catalog says the
  // measures cover. Opening a source on a national-only series would land the
  // user on a selection its map can never draw, and BLS lists 56 of them
  // ahead of every LAUS measure. The grains are the published ones; this
  // prefers a spatial measure, it does not decide which measures are spatial.
  const firstSpatial = candidates.find((item) =>
    metricSupportedGeoLevels(item).some((level) => level !== "NATIONAL"),
  );

  return (matchingVariable || canonicalPopulation || firstSpatial || candidates[0]!)
    .metric_code;
}

export function metricOptions(metrics: MetricSummary[] | null | undefined): MetricOption[] {
  return (metrics || []).map((metric) => ({
    value: metric.metric_code,
    label: `${String(metric.metric_display_name).replaceAll("!!", " › ")} (${metric.metric_code})`,
    source: metric.source_code,
  }));
}

export function normalizeGeoLevel(value: unknown): string {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim().toUpperCase();
}

export function metricSupportedGeoLevels(metric: MetricSummary | null | undefined): string[] {
  const grains = Array.isArray(metric?.valid_geo_grains)
    ? metric.valid_geo_grains
    : [];
  return grains
    .map((value) => normalizeGeoLevel(value))
    .filter(Boolean);
}

export function preferredGeoLevelForMetric(
  metric: MetricSummary | null | undefined,
  fallbackGeoLevel: string = "COUNTY",
): string {
  const supported = metricSupportedGeoLevels(metric);
  if (supported.length === 0) {
    // Unknown grains, not none: the caller's fallback still applies.
    return fallbackGeoLevel;
  }

  if (supported.includes("COUNTY")) {
    return "COUNTY";
  }
  if (supported.includes("STATE")) {
    return "STATE";
  }
  if (supported.includes("NATIONAL")) {
    return "NATIONAL";
  }

  // The measure publishes at a grain outside the spatial three -- Census
  // PEP's PLACE, FBI UCR's AGENCY. Falling back to the caller's default here
  // preferred a grain the measure never claimed, so the explorer asked for
  // it, received nothing, and reported "0 COUNTY records published" as
  // though the measure published none (WEB-038). Its own first declared
  // grain is the only honest preference.
  return supported[0]!;
}

export interface ObservationPointFeature {
  type: "Feature";
  properties: Record<string, unknown>;
  geometry: { type: "Point"; coordinates: [number, number] };
}

export function observationToFeature(
  item: ObservationRow | null | undefined,
): ObservationPointFeature | null {
  const longitude = Number(item?.geo_longitude);
  const latitude = Number(item?.geo_latitude);

  if (!item || !Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    return null;
  }

  return {
    type: "Feature",
    properties: {
      geo_id: item.geo_id,
      geo_level: item.geo_level,
      metric_code: item.metric_code,
      value: item.value,
      name: item.county_name || item.state_name || item.geo_id,
    },
    geometry: {
      type: "Point",
      coordinates: [longitude, latitude],
    },
  };
}

export function isCountyObservation(item: ObservationRow | null | undefined): boolean {
  if (!item) {
    return false;
  }

  if (typeof item.geo_level === "string" && item.geo_level.toUpperCase() === "COUNTY") {
    return true;
  }

  if (item.county_fips) {
    return true;
  }

  return typeof item.geo_id === "string" && item.geo_id.toLowerCase().includes("|county:");
}

export function tileFilterForGeoLevel(geoLevel: string): TileFilter {
  // The layer carries every geography with a shape -- some 32k places among
  // them, which have no county_fips either -- so a level is matched on the
  // published geo_level rather than inferred from which fips columns a
  // feature happens to carry. The national view keeps states and counties
  // as its backdrop.
  if (geoLevel === "NATIONAL") {
    return ["in", ["get", "geo_level"], ["literal", ["STATE", "COUNTY"]]];
  }
  return ["==", ["get", "geo_level"], geoLevel === "STATE" ? "STATE" : "COUNTY"];
}

/**
 * The geo-level filter narrowed to one state when a state is selected: a
 * selected state is the whole map, so every other state's geometry leaves
 * the choropleth layers rather than staying behind in the no-data colour.
 */
export function tileFilterForSelection(
  geoLevel: string,
  stateFips: string | null | undefined,
): TileFilter {
  const levelFilter = tileFilterForGeoLevel(geoLevel);
  if (!stateFips) {
    return levelFilter;
  }
  const stateFilter = ["==", ["to-string", ["get", "state_fips"]], stateFips];
  return levelFilter === true ? stateFilter : ["all", levelFilter, stateFilter];
}

export type LngLatBoundsArray = [[number, number], [number, number]];

/** The subset of a GeoJSON feature the extent needs; the tile decoder's shape. */
export interface FeatureLike {
  properties?: Record<string, unknown> | null;
  geometry?: { type?: string; coordinates?: unknown } | null;
}

function extendBounds(bounds: number[], coordinates: unknown): void {
  if (!Array.isArray(coordinates)) {
    return;
  }
  if (typeof coordinates[0] === "number" && typeof coordinates[1] === "number") {
    const longitude = Number(coordinates[0]);
    const latitude = Number(coordinates[1]);
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
      return;
    }
    bounds[0] = Math.min(bounds[0]!, longitude);
    bounds[1] = Math.min(bounds[1]!, latitude);
    bounds[2] = Math.max(bounds[2]!, longitude);
    bounds[3] = Math.max(bounds[3]!, latitude);
    return;
  }
  for (const child of coordinates) {
    extendBounds(bounds, child);
  }
}

/**
 * The extent of the features in one state (every feature when no state is
 * named) as [[west, south], [east, north]], or null when nothing matched.
 * Read from the drawn polygons rather than catalog centroids, so an
 * outlying county is not clipped and the fit does not depend on which
 * coordinate fields the catalog happens to publish.
 */
export function boundsOfFeatures(
  features: FeatureLike[] | null | undefined,
  stateFips?: string | null,
): LngLatBoundsArray | null {
  const bounds = [Infinity, Infinity, -Infinity, -Infinity];
  for (const feature of features || []) {
    const properties = (feature?.properties || {}) as Record<string, unknown>;
    if (stateFips && String(properties.state_fips ?? "") !== stateFips) {
      continue;
    }
    extendBounds(bounds, feature?.geometry?.coordinates);
  }
  if (!Number.isFinite(bounds[0]!)) {
    return null;
  }
  return [[bounds[0]!, bounds[1]!], [bounds[2]!, bounds[3]!]];
}

/**
 * A published number, or `null` when the source published none.
 *
 * The API publishes `value: null` whenever a source published no usable
 * number, with `value_status` saying why, and an empty string carries the
 * same meaning. `Number(null)` and `Number("")` are both `0`, and
 * `Number.isFinite(0)` is true — so every numeric path has to reject the
 * absent value before coercing, or a suppressed observation silently
 * becomes a published zero on a map, in a height, or in a formatted label.
 */
export function publishedNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

// MapLibre draws extrusion heights to scale, in metres. At the national zoom
// (~3) one pixel spans about 15 km, so the 12 km ceiling below is under a
// pixel tall and the mode reads as a flat choropleth; at a state zoom (~7) a
// pixel is ~1 km and the same column is a dozen pixels. Scaling by
// 2^(REFERENCE_ZOOM - zoom) keeps the tallest column about the same size on
// screen across the zooms the explorer moves between. `zoom` may only feed a
// top-level step/interpolate, so the per-feature match is bound once with
// `let` and referenced from every stop rather than repeated per stop.
const EXTRUSION_REFERENCE_ZOOM = 10;
const EXTRUSION_ZOOM_STOPS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

function scaleExtrusionByZoom(perFeatureHeight: MapExpression): MapExpression {
  const stops = EXTRUSION_ZOOM_STOPS.flatMap((zoom) => [
    zoom,
    ["*", ["var", "height"], 2 ** (EXTRUSION_REFERENCE_ZOOM - zoom)],
  ]);
  return ["let", "height", perFeatureHeight, ["interpolate", ["linear"], ["zoom"], ...stops]];
}

/** log10 bounds over the published positive values, or null when there are none. */
function logRange(values: number[]): { logMin: number; logMax: number } | null {
  let logMin = Infinity;
  let logMax = -Infinity;
  for (const value of values) {
    if (value > 0) {
      const logValue = Math.log10(value);
      logMin = Math.min(logMin, logValue);
      logMax = Math.max(logMax, logValue);
    }
  }
  return Number.isFinite(logMin) ? { logMin, logMax } : null;
}

/**
 * Where a value sits on a log10 scale, 0..1. A value that is not positive
 * has no logarithm; it sits at the bottom of the scale rather than
 * vanishing, because a county of zero is still a county.
 */
function logPosition(value: number, logMin: number, logMax: number): number {
  if (!(value > 0)) {
    return 0;
  }
  const span = logMax - logMin;
  if (span <= 0) {
    return 0;
  }
  return Math.min(1, Math.max(0, (Math.log10(value) - logMin) / span));
}

function binIndexFor(position: number, binCount: number): number {
  return Math.max(0, Math.min(binCount - 1, Math.floor(position * binCount)));
}

export function buildExtrusionHeightExpression(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
  valueScale: ValueScale = "linear",
): MapExpression {
  if (!Array.isArray(observations) || observations.length === 0) {
    return ["literal", 0];
  }

  const keyedValues: (string | number)[] = [];
  const values = observations
    .map((item) => publishedNumber(item.value))
    .filter((value): value is number => value !== null);

  if (values.length === 0) {
    return ["literal", 0];
  }

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = maxValue - minValue || 1;
  const logBounds = valueScale === "log" ? logRange(values) : null;

  for (const item of observations) {
    const joinValue = observationJoinValue(item, joinKey);
    const numericValue = publishedNumber(item.value);
    if (!joinValue || numericValue === null) {
      continue;
    }

    const normalized = logBounds
      ? logPosition(numericValue, logBounds.logMin, logBounds.logMax)
      : (numericValue - minValue) / span;
    const height = Math.round(200 + normalized * 12000);
    keyedValues.push(String(joinValue), height);
  }

  if (keyedValues.length === 0) {
    return ["literal", 0];
  }

  return scaleExtrusionByZoom(["match", ["to-string", ["get", joinKey]], ...keyedValues, 0]);
}

export function observationJoinValue(
  item: ObservationRow,
  joinKey: string | null | undefined,
): unknown {
  const normalizedJoinKey = typeof joinKey === "string" ? joinKey.toLowerCase() : "geo_id";

  if (normalizedJoinKey === "geo_id") {
    return item.geo_id || null;
  }

  if (normalizedJoinKey === "geoid") {
    if (item.state_fips && item.county_fips) {
      return `${item.state_fips}${item.county_fips}`;
    }
    if (item.state_fips) {
      return item.state_fips;
    }
    if (typeof item.geo_id === "string") {
      const countyMatch = item.geo_id.match(/^state:(\d{2})\|county:(\d{3})$/i);
      if (countyMatch) {
        return `${countyMatch[1]}${countyMatch[2]}`;
      }

      const stateMatch = item.geo_id.match(/^state:(\d{2})$/i);
      if (stateMatch) {
        return stateMatch[1];
      }
    }
  }

  if (normalizedJoinKey === "county_fips") {
    return item.county_fips || null;
  }

  if (normalizedJoinKey === "state_fips") {
    return item.state_fips || null;
  }

  return (joinKey ? item[joinKey] : null) || item.geo_id || null;
}

export function colorForValue(value: number, minValue: number, maxValue: number): string {
  if (!Number.isFinite(value) || !Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return CHOROPLETH_FALLBACK_COLOR;
  }

  const span = maxValue - minValue;
  const ratio = span <= 0 ? 0 : (value - minValue) / span;
  const index = Math.max(0, Math.min(CHOROPLETH_PALETTE.length - 1, Math.floor(ratio * CHOROPLETH_PALETTE.length)));
  return CHOROPLETH_PALETTE[index]!;
}

export function distributionBins(
  payload: DistributionResponse | null | undefined,
): DistributionBinModel[] {
  const minValue = Number(payload?.min_value);
  const maxValue = Number(payload?.max_value);
  const binCount = Number(payload?.bin_count);

  if (
    !Number.isFinite(minValue) ||
    !Number.isFinite(maxValue) ||
    !Number.isInteger(binCount) ||
    binCount < 1 ||
    binCount > CHOROPLETH_PALETTE.length ||
    Number(payload?.total) < 1
  ) {
    return [];
  }

  const counts = new Map(
    (payload?.items || []).map((item) => [Number(item.bin_index), Number(item.count) || 0]),
  );
  const width = (maxValue - minValue) / binCount;

  return CHOROPLETH_PALETTE.slice(0, binCount).map((color, index) => ({
    binIndex: index + 1,
    color,
    lowerBound: minValue + index * width,
    upperBound: index === binCount - 1 ? maxValue : minValue + (index + 1) * width,
    count: counts.get(index + 1) || 0,
  }));
}

export function colorForDistributionValue(
  value: number,
  bins: DistributionBinModel[],
): string {
  if (!Number.isFinite(value) || bins.length === 0) {
    return CHOROPLETH_FALLBACK_COLOR;
  }

  const matched = bins.find(
    (bin, index) => index === bins.length - 1 || value < bin.upperBound,
  );
  return matched?.color || CHOROPLETH_FALLBACK_COLOR;
}

export function formatLegendValue(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    notation: Math.abs(value) >= 10000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 10000 ? 1 : 0,
  }).format(value);
}

export function formatObservationValue(
  value: unknown,
  maximumFractionDigits: number = 1,
): string {
  const numericValue = publishedNumber(value);
  if (numericValue === null) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(numericValue);
}

export function observationName(item: ObservationRow | null | undefined): string {
  if (!item) {
    return "Unknown county";
  }

  const name = String(item.geo_name || item.county_name || item.geo_id || "Unknown county");
  // A state-level row carries its own name as state_name too.
  return item.state_name && item.state_name !== name ? `${name}, ${item.state_name}` : name;
}

export function observationUnit(item: ObservationRow | null | undefined): string {
  return String(item?.unit || item?.units || "value");
}

export function marginOfErrorText(item: ObservationRow | null | undefined): string {
  const marginOfError = Number(item?.margin_of_error);
  if (Number.isFinite(marginOfError) && marginOfError >= 0) {
    const marginPct = Number(item?.margin_of_error_pct);
    const pctText = Number.isFinite(marginPct) && marginPct >= 0
      ? ` (${formatObservationValue(marginPct, 2)}%)`
      : "";
    return `±${formatObservationValue(marginOfError)}${pctText}`;
  }

  if (marginOfError === -555555555) {
    return "0 (Census-controlled estimate)";
  }
  if (marginOfError === -222222222) {
    return "Not computed (insufficient sample)";
  }
  if (marginOfError === -333333333) {
    return "Not computed (open-ended median)";
  }
  if (marginOfError === -666666666 || marginOfError === -888888888) {
    return "Not applicable";
  }
  if (marginOfError === -999999999) {
    return "Suppressed (sample too small)";
  }

  return "Not provided";
}

export function buildObservationIndex<T extends ObservationRow>(
  observations: T[] | null | undefined,
  joinKey: string | null | undefined,
): Map<string, T> {
  const index = new Map<string, T>();

  for (const item of observations || []) {
    const joinValue = observationJoinValue(item, joinKey);
    if (joinValue !== null && joinValue !== undefined && joinValue !== "") {
      index.set(String(joinValue), item);
    }
  }

  return index;
}

export function buildSelectionFilter(
  joinValue: unknown,
  joinKey: string,
): MapExpression {
  return [
    "==",
    ["to-string", ["get", joinKey]],
    joinValue === null || joinValue === undefined ? "__no_selected_county__" : String(joinValue),
  ];
}

/**
 * Colours on a log10 scale: five bins of equal width in log space between
 * the smallest and largest positive value, each legend row counted locally.
 * The API's equal-width bins put nearly every county of a long-tailed
 * measure such as population in the first bin; this is the alternative.
 */
function buildLogChoroplethModel(
  keyedMap: Map<string, number>,
  joinKey: string,
  bounds: { logMin: number; logMax: number },
  missingValueLabel: string,
): ChoroplethModel {
  const binCount = CHOROPLETH_PALETTE.length;
  const counts = new Array<number>(binCount).fill(0);
  const keyedValues: string[] = [];
  for (const [key, numericValue] of keyedMap.entries()) {
    const index = binIndexFor(logPosition(numericValue, bounds.logMin, bounds.logMax), binCount);
    counts[index] = (counts[index] ?? 0) + 1;
    keyedValues.push(key, CHOROPLETH_PALETTE[index]!);
  }

  const edge = (index: number): number =>
    10 ** (bounds.logMin + ((bounds.logMax - bounds.logMin) * index) / binCount);
  const legendItems: LegendItem[] = CHOROPLETH_PALETTE.map((color, index) => ({
    color,
    label:
      bounds.logMax <= bounds.logMin
        ? formatLegendValue(edge(0))
        : index === 0
          ? `Up to ${formatLegendValue(edge(1))}`
          : index === binCount - 1
            ? `${formatLegendValue(edge(index))} and above`
            : `${formatLegendValue(edge(index))} - ${formatLegendValue(edge(index + 1))}`,
    count: counts[index],
  }));
  legendItems.push({ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel });

  return {
    expression: ["match", ["to-string", ["get", joinKey]], ...keyedValues, CHOROPLETH_FALLBACK_COLOR],
    legendItems,
    minValue: 10 ** bounds.logMin,
    maxValue: 10 ** bounds.logMax,
    usesDistribution: false,
    valueCount: keyedMap.size,
    scale: "log",
  };
}

export function buildChoroplethModel(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
  distribution: DistributionResponse | null = null,
  missingValueLabel: string = "No observation",
  valueScale: ValueScale = "linear",
): ChoroplethModel {
  if (!Array.isArray(observations) || observations.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [{ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel }],
      minValue: null,
      maxValue: null,
      usesDistribution: false,
      valueCount: 0,
      scale: valueScale,
    };
  }

  const keyedValues: string[] = [];
  const keyedMap = new Map<string, number>();

  for (const item of observations) {
    const joinValue = observationJoinValue(item, joinKey);
    const numericValue = publishedNumber(item.value);
    if (!joinValue || numericValue === null) {
      continue;
    }

    keyedMap.set(String(joinValue), numericValue);
  }

  const values = [...keyedMap.values()];
  if (values.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [{ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel }],
      minValue: null,
      maxValue: null,
      usesDistribution: false,
      valueCount: 0,
      scale: valueScale,
    };
  }

  const logBounds = valueScale === "log" ? logRange(values) : null;
  if (logBounds) {
    return buildLogChoroplethModel(keyedMap, joinKey, logBounds, missingValueLabel);
  }

  const apiBins = distributionBins(distribution);
  const usesDistribution = apiBins.length > 0;
  const minValue = usesDistribution ? apiBins[0]!.lowerBound : Math.min(...values);
  const maxValue = usesDistribution
    ? apiBins[apiBins.length - 1]!.upperBound
    : Math.max(...values);
  const span = maxValue - minValue;

  for (const [key, numericValue] of keyedMap.entries()) {
    keyedValues.push(
      key,
      usesDistribution
        ? colorForDistributionValue(numericValue, apiBins)
        : colorForValue(numericValue, minValue, maxValue),
    );
  }

  const legendItems: LegendItem[] = usesDistribution
    ? apiBins.map((bin, index) => ({
        color: bin.color,
        label: apiBins.length === 1
          ? "All numeric values"
          : index === 0
            ? `Up to ${formatLegendValue(bin.upperBound)}`
            : index === apiBins.length - 1
              ? `${formatLegendValue(bin.lowerBound)} and above`
              : `${formatLegendValue(bin.lowerBound)} - ${formatLegendValue(bin.upperBound)}`,
        count: bin.count,
      }))
    : CHOROPLETH_PALETTE.map((color, index) => {
    if (span <= 0) {
      return {
        color,
        label: formatLegendValue(minValue),
      };
    }

    const start = minValue + (span * index) / CHOROPLETH_PALETTE.length;
    const end = index === CHOROPLETH_PALETTE.length - 1
      ? maxValue
      : minValue + (span * (index + 1)) / CHOROPLETH_PALETTE.length;

    return {
      color,
      label: `${formatLegendValue(start)} - ${formatLegendValue(end)}`,
    };
  });

  legendItems.push({
    color: CHOROPLETH_FALLBACK_COLOR,
    label: missingValueLabel,
  });

  return {
    expression: ["match", ["to-string", ["get", joinKey]], ...keyedValues, CHOROPLETH_FALLBACK_COLOR],
    legendItems,
    minValue,
    maxValue,
    usesDistribution,
    valueCount: values.length,
    scale: "linear",
  };
}

export function buildChoroplethMatchExpression(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
  distribution: DistributionResponse | null = null,
  missingValueLabel: string = "No observation",
  valueScale: ValueScale = "linear",
): MapExpression {
  return buildChoroplethModel(
    observations,
    joinKey,
    distribution,
    missingValueLabel,
    valueScale,
  ).expression;
}
