// Where public money goes (where-public-money-goes slice 5).
//
// The view model behind the Work and Money chapter's "Where the money goes"
// group. Everything here arranges what `/where-money-goes` served: one layer
// per government level, each line's share of that layer's own total
// computed in gold. Nothing here adds one layer to another; a null amount is
// "not published", never zero.

import { formatNumber } from "./format";

export type LayerKey =
  | "federal_outlays"
  | "state_local_spending"
  | "federal_awards_by_type"
  | "federal_awards_by_agency";

export interface SpendingLine {
  key: string;
  label: string;
  amount: string | null;
  share_pct: string | null;
  unavailable_reason?: string | null;
}

export interface SpendingLayer {
  layer: LayerKey;
  title: string;
  spender: string;
  source: string;
  geo_id: string;
  fiscal_year: number | null;
  period_label: string | null;
  unit: string;
  total: string | null;
  total_label: string;
  lines: SpendingLine[];
  notes: string[];
  unavailable_reason?: string | null;
}

export interface WhereMoneyGoesAnswer {
  geo_id: string;
  geo_level: "nation" | "state" | "county";
  layers: SpendingLayer[];
  caveats: string[];
}

/** The grains `/where-money-goes` answers; anything else is its 422, never asked. */
export const WHERE_MONEY_GEO_ID = /^(us:1|state:\d{2}|state:\d{2}\|county:\d{3})$/;

/** Lines drawn as bars before the rest is one "Everything else" bar. */
export const SHOWN_LINES = 6;
export const EVERYTHING_ELSE = "Everything else";
/** The rest of the total when every line is drawn: what no line holds (an agency USAspending did not name, a negative line). */
export const NOT_IN_A_LINE = "Not in any line here";

/** Dollars from a stored amount in the layer's unit (Census stores thousands). */
export function dollars(amount: string | null, unit: string): number | null {
  if (amount === null || amount === undefined || amount === "") return null;
  const value = Number(amount);
  if (!Number.isFinite(value)) return null;
  return /^thousands/i.test(unit) ? value * 1000 : value;
}

/**
 * A dollar amount in words a reader scans: "$1.41 billion", "$853,504",
 * "−$5.0 million". Three significant figures above a million; the stored
 * amount stays in the table's title text.
 */
export function moneyText(value: number | null): string {
  if (value === null) return "not published";
  const sign = value < 0 ? "−" : "";
  const size = Math.abs(value);
  const scaled = (divisor: number, word: string) => {
    const number = size / divisor;
    const decimals = number >= 100 ? 0 : number >= 10 ? 1 : 2;
    return `${sign}$${number.toFixed(decimals)} ${word}`;
  };
  if (size >= 1e12) return scaled(1e12, "trillion");
  if (size >= 1e9) return scaled(1e9, "billion");
  if (size >= 1e6) return scaled(1e6, "million");
  return `${sign}$${formatNumber(Math.round(size), { maximumFractionDigits: 0 })}`;
}

export function shareText(share: string | null): string {
  if (share === null || share === undefined) return "not published";
  return `${Number(share).toFixed(1)}%`;
}

export interface ShareBar {
  key: string;
  label: string;
  share: number;
  amount: number | null;
  rest?: boolean;
}

/**
 * The largest lines with a positive share, in the served order, then one
 * "Everything else" bar: the rest of the layer's own total (100 less the
 * shown shares), which holds the smaller lines, any negative line and, for
 * agencies, obligations USAspending names no agency for. When every line
 * is drawn the rest is named for what it is: the part of the total no line
 * holds. It is never drawn when nothing is left or no share was published.
 */
export function shareBars(layer: SpendingLayer, shown = SHOWN_LINES): ShareBar[] {
  const positive = layer.lines.filter((line) => line.share_pct !== null && Number(line.share_pct) > 0);
  if (!positive.length) return [];
  const top: ShareBar[] = positive.slice(0, shown).map((line) => ({
    key: line.key,
    label: line.label,
    share: Number(line.share_pct),
    amount: dollars(line.amount, layer.unit),
  }));
  const used = top.reduce((sum, bar) => sum + bar.share, 0);
  const rest = Math.round((100 - used) * 100) / 100;
  const total = dollars(layer.total, layer.unit);
  if (rest >= 0.05) {
    const shownDollars = top.reduce((sum, bar) => sum + (bar.amount ?? 0), 0);
    top.push({
      key: "rest",
      label: layer.lines.length > top.length ? EVERYTHING_ELSE : NOT_IN_A_LINE,
      share: rest,
      amount: total === null ? null : total - shownDollars,
      rest: true,
    });
  }
  return top;
}

/** The lines a layer serves with no share: the reason, once, when they share one. */
export function unsharedReason(layer: SpendingLayer): string | null {
  const reasons = new Set(
    layer.lines.filter((line) => line.share_pct === null && line.amount !== null).map((line) => line.unavailable_reason || ""),
  );
  if (!reasons.size) return null;
  return [...reasons].filter(Boolean).join("; ") || "share not published";
}

/** "What state and local governments spend, fiscal year ending in 2022". */
export function layerHeading(layer: SpendingLayer): string {
  return layer.period_label ? `${layer.title}, ${layer.period_label}` : layer.title;
}

/** The layer's total in one sentence, or null when it is not published. */
export function totalSentence(layer: SpendingLayer): string | null {
  const total = dollars(layer.total, layer.unit);
  if (total === null) return null;
  return `${layer.total_label}: ${moneyText(total)}.`;
}
