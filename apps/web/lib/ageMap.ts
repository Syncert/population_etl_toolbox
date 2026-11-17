// Age indicators on the map (web-age-and-sex-structure, ADR-0013; API-191).
//
// The indicators are not catalog measures: they are computed by the API from
// the published B01001 lines under the five-year grouping and served by
// `/age-structure/map`, one indicator for every county or metro area, or
// every city or tract in one state. This module names them, reads them, and
// puts the answer in the row shape the one-measure map's helpers and the
// choropleth read, so the bands, legend and tables are the same as every
// other map's. A value the API withholds keeps its reason and is never zero.

import { apiFetch } from "./api/client";
import type { RequestOptions } from "./api/client";
import type { ObservationRow } from "./explorerViewModel";
import type { AgeScheme } from "./ageStructure";
import { marginText, unavailableText } from "./ageStructure";
import { formatNumber } from "./format";
import type { MapGrain } from "./measureMapGrains";

export type AgeMapUnit = "percent" | "ratio" | "years";

export interface AgeMapIndicator {
  id: string;
  label: string;
  unit: AgeMapUnit;
  /** What the number counts, after the value. */
  per: string;
  /** Words a reader might search the measure switcher with. */
  words: string;
}

/** The indicators `/age-structure/map` serves, in the order they are offered. */
export const AGE_MAP_INDICATORS: readonly AgeMapIndicator[] = [
  { id: "median_age", label: "Median age", unit: "years", per: "years", words: "median age old young" },
  { id: "median_age_men", label: "Median age of men", unit: "years", per: "years", words: "median age men male" },
  { id: "median_age_women", label: "Median age of women", unit: "years", per: "years", words: "median age women female" },
  { id: "under_18", label: "People under 18", unit: "percent", per: "of everyone", words: "under 18 children kids young age" },
  { id: "age_18_64", label: "People aged 18 to 64", unit: "percent", per: "of everyone", words: "18 to 64 working age adults" },
  { id: "age_65_plus", label: "People 65 and over", unit: "percent", per: "of everyone", words: "65 and over older seniors elderly retirement age" },
  { id: "age_85_plus", label: "People 85 and over", unit: "percent", per: "of everyone", words: "85 and over oldest elderly age" },
  { id: "child_dependency_ratio", label: "Child dependency ratio", unit: "ratio", per: "under 18 per 100 aged 18 to 64", words: "child dependency ratio children age" },
  { id: "old_age_dependency_ratio", label: "Old-age dependency ratio", unit: "ratio", per: "65 and over per 100 aged 18 to 64", words: "old age dependency ratio seniors elderly" },
  { id: "total_dependency_ratio", label: "Total dependency ratio", unit: "ratio", per: "under 18 and 65 and over per 100 aged 18 to 64", words: "total dependency ratio age" },
  { id: "sex_ratio", label: "Sex ratio", unit: "ratio", per: "men per 100 women", words: "sex ratio men women gender" },
];

export const DEFAULT_AGE_MAP_INDICATOR = "median_age";

export function ageMapIndicator(id: string | null | undefined): AgeMapIndicator | null {
  return AGE_MAP_INDICATORS.find((item) => item.id === id) || null;
}

/** The key the map's one-measure helpers carry for an indicator; never a catalog id. */
export function ageMapKey(indicator: string): string {
  return `age-structure:${indicator}`;
}

/** The page that maps an indicator, at a grain and, for a city or tract map, a state. */
export function ageMapPath(indicator: string, grain?: MapGrain | null, stateFips?: string | null): string {
  const params = new URLSearchParams();
  if (grain && grain !== "COUNTY") params.set("grain", grain);
  if (stateFips && (grain === "PLACE" || grain === "TRACT")) params.set("state", stateFips);
  const query = params.toString();
  return `/map/age/${encodeURIComponent(indicator)}${query ? `?${query}` : ""}`;
}

/** The indicators a search in the measure switcher finds: three letters or more, every word matched. */
export function searchAgeMapIndicators(query: string): AgeMapIndicator[] {
  const terms = query.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  if (query.trim().length < 3 || !terms.length) return [];
  return AGE_MAP_INDICATORS.filter((item) => {
    const haystack = `${item.label} ${item.words}`.toLowerCase();
    return terms.every((term) => haystack.includes(term));
  });
}

export interface AgeMapItem {
  geo_id: string;
  value?: string | null;
  moe?: string | null;
  unavailable_reason?: string | null;
}

export interface AgeMapResponse {
  indicator: string;
  label: string;
  unit: string;
  derived?: boolean;
  dataset: string;
  geo_level: string;
  state_fips?: string | null;
  release?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  scheme: AgeScheme;
  total?: number;
  items: AgeMapItem[];
}

export function getAgeMap(
  params: { indicator: string; geo_level: MapGrain | "NATIONAL"; state_fips?: string | null; release?: string | null },
  options: RequestOptions = {},
): Promise<AgeMapResponse> {
  return apiFetch<AgeMapResponse>("/age-structure/map", {
    ...options,
    params: {
      indicator: params.indicator,
      geo_level: params.geo_level,
      state_fips: params.state_fips || undefined,
      release: params.release || undefined,
    },
  });
}

/** The unit a value is shown with: "%" after a share, nothing after a ratio, "years" after an age. */
export function ageMapUnitText(unit: string | null | undefined): string {
  if (unit === "percent") return "%";
  if (unit === "years") return "years";
  return "";
}

/** A value with its unit, as the legend and the tables print it. */
export function ageMapValueText(value: number | null, unit: string | null | undefined): string {
  if (value === null || !Number.isFinite(value)) return "not available";
  const text = formatNumber(value, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const suffix = ageMapUnitText(unit);
  return suffix === "%" ? `${text}%` : suffix ? `${text} ${suffix}` : text;
}

/** A 90 % margin as served: two decimals on a share, one on a ratio or an age. */
export function ageMapMarginText(moe: string | null | undefined, unit: string | null | undefined): string {
  return marginText(moe ?? null, unit === "percent" ? 2 : 1);
}

/** "2019–2023", from the answer's period. */
export function ageMapPeriodText(answer: Pick<AgeMapResponse, "period_start" | "period_end" | "release">): string {
  const start = answer.period_start?.slice(0, 4);
  const end = answer.period_end?.slice(0, 4);
  if (start && end) return start === end ? end : `${start}–${end}`;
  return answer.release || "";
}

/**
 * The answer as the rows the one-measure map ranks and the choropleth colours.
 * A withheld value stays `null` with its reason in words; nothing becomes zero.
 */
export function ageMapRows(answer: AgeMapResponse): ObservationRow[] {
  const key = ageMapKey(answer.indicator);
  return answer.items.map((item) => ({
    geo_id: item.geo_id,
    geo_level: answer.geo_level,
    metric_code: key,
    value: item.value ?? null,
    margin_of_error: item.moe ?? null,
    period_start: answer.period_start ?? null,
    period_end: answer.period_end ?? null,
    unit: answer.unit,
    value_status: item.value === null || item.value === undefined ? unavailableText(item.unavailable_reason) : null,
  }));
}

/**
 * The age map a place page links to: the place's own grain and, for a city
 * or tract, its state. A state or the nation links to the county map.
 */
export function ageMapPathForPlace(indicator: string, geoId: string, geoLevel: string | null | undefined): string {
  const level = String(geoLevel || "").toUpperCase();
  const grain: MapGrain = level === "PLACE" || level === "TRACT" || level === "METRO" ? (level as MapGrain) : "COUNTY";
  const state = /^state:(\d{2})\|/.exec(geoId)?.[1] || null;
  return ageMapPath(indicator, grain, state);
}
