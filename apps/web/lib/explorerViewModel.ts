// Pure explorer view models: deterministic display transformations over
// API observations, catalog metrics, distribution bins, and MapLibre
// expressions. No React, no fetch, no browser state.

import type { DistributionResponse, MetricSummary } from "./api/types";

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

  const datasetMetrics = metrics.filter(
    (item) => metricDataset(item.metric_code) === dataset,
  );
  const candidates = datasetMetrics.length > 0 ? datasetMetrics : metrics;
  const matchingVariable = candidates.find(
    (item) => metricVariable(item.metric_code) === preferredVariable,
  );
  const canonicalPopulation = candidates.find(
    (item) => metricVariable(item.metric_code) === DEFAULT_POPULATION_VARIABLE,
  );

  return (matchingVariable || canonicalPopulation || candidates[0]!).metric_code;
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

  return fallbackGeoLevel;
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
  if (geoLevel === "NATIONAL") {
    return true;
  }
  return geoLevel === "STATE"
    ? ["!", ["has", "county_fips"]]
    : ["has", "county_fips"];
}

export function buildExtrusionHeightExpression(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
): MapExpression {
  if (!Array.isArray(observations) || observations.length === 0) {
    return ["literal", 0];
  }

  const keyedValues: (string | number)[] = [];
  const values = observations
    .map((item) => Number(item.value))
    .filter((value) => Number.isFinite(value));

  if (values.length === 0) {
    return ["literal", 0];
  }

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = maxValue - minValue || 1;

  for (const item of observations) {
    const joinValue = observationJoinValue(item, joinKey);
    const numericValue = Number(item.value);
    if (!joinValue || !Number.isFinite(numericValue)) {
      continue;
    }

    const normalized = (numericValue - minValue) / span;
    const height = Math.round(200 + normalized * 12000);
    keyedValues.push(String(joinValue), height);
  }

  if (keyedValues.length === 0) {
    return ["literal", 0];
  }

  return ["match", ["to-string", ["get", joinKey]], ...keyedValues, 0];
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
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
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

  const county = item.geo_name || item.county_name || item.geo_id || "Unknown county";
  return item.state_name ? `${county}, ${item.state_name}` : String(county);
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

export function buildChoroplethModel(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
  distribution: DistributionResponse | null = null,
  missingValueLabel: string = "No observation",
): ChoroplethModel {
  if (!Array.isArray(observations) || observations.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [{ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel }],
      minValue: null,
      maxValue: null,
      usesDistribution: false,
      valueCount: 0,
    };
  }

  const keyedValues: string[] = [];
  const keyedMap = new Map<string, number>();

  for (const item of observations) {
    const joinValue = observationJoinValue(item, joinKey);
    const numericValue = Number(item.value);
    if (!joinValue || !Number.isFinite(numericValue)) {
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
    };
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
  };
}

export function buildChoroplethMatchExpression(
  observations: ObservationRow[] | null | undefined,
  joinKey: string,
  distribution: DistributionResponse | null = null,
  missingValueLabel: string = "No observation",
): MapExpression {
  return buildChoroplethModel(
    observations,
    joinKey,
    distribution,
    missingValueLabel,
  ).expression;
}
