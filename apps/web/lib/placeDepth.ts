// A chapter's depth rows and industry tables, said once and drawn to scale
// (web-compact-depth).
//
// The depth grid used to repeat the source, the five-year period and
// "margin of error ±x (y%)" in every tile, so the figure itself was the
// smallest thing in its own cell. Here what the rows share is stated once,
// above them, and each row keeps only what is its own: its universe, its
// value, its margin as a share, and its source or period only where they
// differ from the rest. Industry tables become bars sorted largest first,
// with the sectors published without a value gathered in one line.

import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { formatNumber } from "./format";
import { measureBasis, trendPeriodLabel } from "./placeChapters";

/** z for the Census Bureau's 90 percent margins. */
const Z90 = 1.645;
/**
 * A coefficient of variation above this marks an estimate to use with care;
 * 30 percent is the cutoff the Census Bureau's ACS guidance names for
 * estimates of low reliability.
 */
export const CARE_CV = 0.3;

export interface DepthShared {
  /** The basis most rows share ("Survey estimate (American Community Survey)"), or "". */
  basis: string;
  /** The period most rows share, in words ("2020–2024"), or "". */
  period: string;
  /** Whether any row carries a published margin, so the header explains "±". */
  margins: boolean;
}

function mostCommon(values: string[]): string {
  const counts = new Map<string, number>();
  for (const value of values) if (value) counts.set(value, (counts.get(value) || 0) + 1);
  let best = "";
  let most = 0;
  for (const [value, count] of counts) {
    if (count > most) {
      best = value;
      most = count;
    }
  }
  return best;
}

/** A row's published margin as a number, or null when none is usable. */
export function rowMargin(row: ObservationRow): number | null {
  const raw = row.margin_of_error;
  if (raw === undefined || raw === null || raw === "") return null;
  const number = Number(raw);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

/** "±1.7%": the margin as a share of the value, as published when it is. */
export function marginShare(row: ObservationRow): string {
  const margin = rowMargin(row);
  if (margin === null) return "";
  const published = parseFloat(row.margin_of_error_pct as string);
  const value = publishedNumber(row.value);
  const percent = Number.isFinite(published) && published >= 0
    ? published
    : value
      ? (margin / Math.abs(value)) * 100
      : null;
  if (percent === null) return `±${formatNumber(margin, { maximumFractionDigits: 1 })}`;
  return `±${percent < 10 ? percent.toFixed(1) : Math.round(percent)}%`;
}

/** Whether the estimate's coefficient of variation is above `CARE_CV`. */
export function needsCare(row: ObservationRow): boolean {
  const margin = rowMargin(row);
  const value = publishedNumber(row.value);
  return margin !== null && Boolean(value) && margin / Z90 / Math.abs(value!) > CARE_CV;
}

/** What the published rows share, to say once above them. */
export function depthShared(rows: readonly { metricCode: string; row: ObservationRow | null }[]): DepthShared {
  const published = rows.filter((entry) => entry.row && publishedNumber(entry.row.value) !== null);
  return {
    basis: mostCommon(published.map((entry) => measureBasis(entry.metricCode) || "")),
    period: mostCommon(published.map((entry) => trendPeriodLabel(entry.row!))),
    margins: published.some((entry) => rowMargin(entry.row!) !== null),
  };
}

/** "American Community Survey · 2020–2024 · ± is the 90% margin of error". */
export function sharedLine(shared: DepthShared): string {
  return [
    shared.basis,
    shared.period,
    shared.margins ? "± is the 90% margin of error, as a share of the value" : "",
  ].filter(Boolean).join(" · ");
}

export interface IndustryEntry {
  id: string;
  label: string;
  value: number | null;
  /** Why there is no value, when there is none. */
  message: string;
}

/** Statuses the publishers use for a figure held back to protect employers. */
const WITHHELD = new Set(["withheld", "suppressed", "confidential", "d", "(d)"]);

/** The reason a sector has no bar, in the reader's words. */
export function gapReason(row: ObservationRow | null, fallback: string): string {
  const status = String(row?.value_status || "").toLowerCase();
  if (WITHHELD.has(status)) return "Withheld by the publisher to protect confidential employer data";
  if (row && status) return "Published without a value";
  return fallback;
}

/**
 * Bars largest first, and the sectors with no bar grouped by their reason,
 * each group in the table's own order.
 */
export function industryBars(entries: readonly IndustryEntry[]): {
  bars: (IndustryEntry & { value: number })[];
  max: number;
  gaps: { reason: string; entries: IndustryEntry[] }[];
} {
  const bars = entries
    .filter((entry): entry is IndustryEntry & { value: number } => entry.value !== null)
    .sort((left, right) => right.value - left.value);
  const gaps = new Map<string, IndustryEntry[]>();
  for (const entry of entries) {
    if (entry.value === null) gaps.set(entry.message, [...(gaps.get(entry.message) || []), entry]);
  }
  return {
    bars,
    max: Math.max(0, ...bars.map((bar) => bar.value)),
    gaps: [...gaps].map(([reason, grouped]) => ({ reason, entries: grouped })),
  };
}
