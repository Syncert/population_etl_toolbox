// A small trend inside a headline card (web-card-sparkline).
//
// The card's number used to stand alone, its history drawn in a separate
// chart below, indexed to 100. The sparkline draws the place's own published
// values -- not an index -- over the record's last ten years, marks the
// newest point, and one line says how it moved, by the record page's rules
// (ADR-0011): a monthly series is compared with the same month of an earlier
// year, a five-year survey estimate with one that shares none of its years,
// and a survey change is a rise or a fall only beyond the published margins.

import { RECORD_SPAN_YEARS, changeVerb, chooseSpan, recordPeriodLabel, surveyMargin } from "./checkTheRecord";
import type { ObservationRow } from "./explorerViewModel";
import { formatObservationValue, publishedNumber } from "./explorerViewModel";

export interface SparkPoint {
  x: number;
  y: number;
  row: ObservationRow;
}

export interface Sparkline {
  points: SparkPoint[];
  /** "2015" and "2024": the first and last periods drawn, in words. */
  first: string;
  last: string;
  /** "Fell from 2.9 in September 2025." or "", when the record cannot say. */
  change: string;
}

const time = (row: ObservationRow) => Date.parse(String(row.period_start || ""));
const year = (row: ObservationRow) => Number(String(row.period_start || "").slice(0, 4));

/** Whether a row is a multi-year survey estimate, and how many years it spans. */
function spanYears(row: ObservationRow): number {
  const start = Number(String(row.period_start || "").slice(0, 4));
  const end = Number(String(row.period_end || "").slice(0, 4));
  return Number.isFinite(start) && Number.isFinite(end) && end > start ? end - start + 1 : 1;
}

/**
 * The points to draw in a `width` by `height` box, and the change line, or
 * null when fewer than three published periods fall in the last ten years.
 */
export function sparkline(rows: readonly ObservationRow[], width: number, height: number): Sparkline | null {
  const numbered = rows
    .filter((row) => publishedNumber(row.value) !== null && Number.isFinite(time(row)))
    .sort((left, right) => time(left) - time(right));
  const newest = numbered.at(-1);
  if (!newest) return null;
  const drawn = numbered.filter((row) => year(row) >= year(newest) - RECORD_SPAN_YEARS);
  if (drawn.length < 3) return null;
  const values = drawn.map((row) => publishedNumber(row.value)!);
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const t0 = time(drawn[0]!);
  const t1 = time(newest);
  const pad = 3;
  const points = drawn.map((row, index) => ({
    x: t1 > t0 ? ((time(row) - t0) / (t1 - t0)) * width : (index / (drawn.length - 1)) * width,
    y: hi > lo ? pad + ((hi - values[index]!) / (hi - lo)) * (height - 2 * pad) : height / 2,
    row,
  }));
  return {
    points,
    first: recordPeriodLabel(drawn[0]!),
    last: recordPeriodLabel(newest),
    change: changeLine(drawn),
  };
}

/** One line on how the series moved, in ADR-0011's verbs, or "". */
export function changeLine(rows: readonly ObservationRow[]): string {
  const newest = rows.at(-1);
  if (!newest) return "";
  const span = chooseSpan(rows, spanYears(newest));
  if (!span) return "";
  const from = publishedNumber(span.from.value)!;
  const to = publishedNumber(span.to.value)!;
  const fromMargin = surveyMargin(span.from);
  const toMargin = surveyMargin(span.to);
  const surveyed = fromMargin !== null || toMargin !== null;
  const verb = changeVerb(from, to, surveyed ? { from: fromMargin, to: toMargin } : null);
  if (!verb) return "";
  const period = recordPeriodLabel(span.from);
  const value = formatObservationValue(from);
  switch (verb) {
    case "rose":
      return `Rose from ${value} in ${period}.`;
    case "fell":
      return `Fell from ${value} in ${period}.`;
    case "was unchanged":
      return `Unchanged from ${period}.`;
    default:
      return `Not distinguishable from ${value} in ${period} at the published margins of error.`;
  }
}
