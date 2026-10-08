// One measure, every county (one-measure-map).
//
// The view model for `/map/<metric_code>`: one metric, one period, every
// county the catalog publishes, and nothing else. It refuses a second metric
// outright, because the page's whole claim is that it never combines
// measures. Counties without a published number are counted and kept in the
// table as missing or withheld; they are never painted and never printed as
// zero. Bins are equal-width over the published values, and the map, the
// legend and the table read the same rows.

import type { DistributionResponse, GeographySummary } from "./api/types";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { observationPeriodLabel } from "./observationAccess";

export const MAP_BIN_COUNT = 5;

export interface RankedCounty {
  county: GeographySummary;
  row: ObservationRow | null;
  value: number | null;
  /** "withheld: <status>" when a row carried no number, "missing" when none came. */
  gap: string;
}

export interface MeasureBin {
  binIndex: number;
  lowerBound: number;
  upperBound: number;
  count: number;
}

export interface MeasureMapView {
  metricCode: string;
  period: string;
  ranked: RankedCounty[];
  withValue: number;
  withoutValue: number;
  bins: MeasureBin[];
  /** The bins in the shape the explorer's choropleth reads. */
  distribution: DistributionResponse | null;
}

/** Refuse anything but one measure: the page never combines two. */
export function assertOneMeasure(metricCode: string, rows: readonly ObservationRow[]): void {
  if (!metricCode) throw new Error("A map shows exactly one measure; none was named.");
  const others = [...new Set(rows.map((row) => row.metric_code).filter((code) => code && code !== metricCode))];
  if (others.length) {
    throw new Error(`A map shows exactly one measure; the response also carried ${others.join(", ")}.`);
  }
}

/** Equal-width bins over the published values, with their counts. */
export function equalWidthBins(values: readonly number[], binCount = MAP_BIN_COUNT): MeasureBin[] {
  if (!values.length) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (max === min) return [{ binIndex: 1, lowerBound: min, upperBound: max, count: values.length }];
  const width = (max - min) / binCount;
  const bins = Array.from({ length: binCount }, (_, index) => ({
    binIndex: index + 1,
    lowerBound: min + width * index,
    upperBound: index === binCount - 1 ? max : min + width * (index + 1),
    count: 0,
  }));
  for (const value of values) {
    const index = Math.min(binCount - 1, Math.floor((value - min) / width));
    bins[index]!.count += 1;
  }
  return bins;
}

/**
 * Join one period's rows to the catalog's counties, rank the published
 * values highest first, and keep every county without one at the end.
 */
export function buildMeasureMapView(
  metricCode: string,
  rows: readonly ObservationRow[],
  counties: readonly GeographySummary[],
): MeasureMapView {
  assertOneMeasure(metricCode, rows);
  const byGeo = new Map<string, ObservationRow>();
  for (const row of rows) {
    if (row.geo_id) byGeo.set(String(row.geo_id), row);
  }
  const periods = [...new Set([...byGeo.values()].map((row) => observationPeriodLabel(row)))];
  const ranked: RankedCounty[] = counties.map((county) => {
    const row = byGeo.get(county.geo_id) || null;
    const value = row ? publishedNumber(row.value) : null;
    return {
      county,
      row,
      value,
      gap: value !== null ? "" : row ? `withheld${row.value_status ? `: ${String(row.value_status)}` : ""}` : "missing",
    };
  });
  ranked.sort((left, right) => {
    if (left.value === null && right.value === null) return 0;
    if (left.value === null) return 1;
    if (right.value === null) return -1;
    return right.value - left.value;
  });
  const values = ranked.flatMap((entry) => (entry.value === null ? [] : [entry.value]));
  const bins = equalWidthBins(values);
  return {
    metricCode,
    period: periods.length === 1 ? periods[0]! : periods.length ? "several periods" : "",
    ranked,
    withValue: values.length,
    withoutValue: ranked.length - values.length,
    bins,
    distribution: bins.length
      ? {
          total: values.length,
          bin_count: bins.length,
          items: bins.map((bin) => ({
            bin_index: bin.binIndex,
            lower_bound: bin.lowerBound,
            upper_bound: bin.upperBound,
            count: bin.count,
          })),
        }
      : null,
  };
}

/** A page of the ranked table, highest first. */
export function rankedPage(view: MeasureMapView, page: number, pageSize: number): RankedCounty[] {
  return view.ranked.slice(page * pageSize, (page + 1) * pageSize);
}
