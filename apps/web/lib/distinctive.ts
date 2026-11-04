// "What stands out" on a place page (what-makes-this-place-distinctive).
//
// The API ranks each reviewed measure on its own (`/place/distinctive`); this
// only splits the ranked measures into the few highest and the few lowest and
// words each one. Nothing here sums, averages, or scores across measures.

import { displayMetricName } from "./format";

export interface DistinctiveMeasure {
  metric_code: string;
  metric_display_name?: string | null;
  metric_label?: string | null;
  source_code?: string | null;
  units?: string | null;
  period_start?: string | null;
  value: number;
  siblings_with_value: number;
  siblings_withheld: number;
  siblings_missing: number;
  siblings_below: number;
  siblings_tied: number;
  percentile_rank: number;
  caveats: string[];
  request: string;
}

export interface DistinctiveResponse {
  derived: true;
  geo_id: string;
  geo_level?: string | null;
  parent_scope?: string | null;
  minimum_siblings: number;
  method: string;
  ranked: DistinctiveMeasure[];
  not_ranked: { metric_code: string; reason: string }[];
}

/** How many of each end the section shows. */
export const STANDOUT_COUNT = 3;

export function standouts(response: DistinctiveResponse | null): {
  highest: DistinctiveMeasure[];
  lowest: DistinctiveMeasure[];
  rankedCount: number;
} {
  const ranked = [...(response?.ranked || [])].sort((left, right) => right.percentile_rank - left.percentile_rank);
  const highest = ranked.slice(0, STANDOUT_COUNT);
  const lowest = ranked
    .slice(Math.max(STANDOUT_COUNT, ranked.length - STANDOUT_COUNT))
    .reverse();
  return { highest, lowest, rankedCount: ranked.length };
}

/**
 * "Higher than 4 of 10 Wisconsin counties with a published value", with the
 * siblings that withheld or published nothing named after it, never folded in.
 */
export function rankSentence(measure: DistinctiveMeasure, siblingsName: string): string {
  const base = `Higher than ${measure.siblings_below} of ${measure.siblings_with_value} ${siblingsName} with a published value`;
  const tied = measure.siblings_tied ? `, equal to ${measure.siblings_tied}` : "";
  const gaps = [
    measure.siblings_withheld ? `${measure.siblings_withheld} withheld a value` : "",
    measure.siblings_missing ? `${measure.siblings_missing} published none` : "",
  ].filter(Boolean);
  return `${base}${tied}${gaps.length ? ` (${gaps.join("; ")})` : ""}`;
}

/**
 * The name a rank row shows. The API's plain label when it has one; a
 * measure with no registry entry falls back to the provider's label, and for
 * a bulk file that is the bare variable ("Rdeath" for CENSUS_PEP:RDEATH),
 * which a reader cannot read. A name that is only the code says so in words
 * instead; the code stays in the row's hover text.
 */
export function standoutName(measure: DistinctiveMeasure): string {
  const name = displayMetricName(measure);
  const code = measure.metric_code || "";
  const variable = code.split(":").pop() || code;
  const bare = name.trim().toLowerCase();
  return !/\s/.test(bare) && (bare === code.toLowerCase() || bare === variable.toLowerCase())
    ? "Measure not yet named in plain words"
    : name;
}
