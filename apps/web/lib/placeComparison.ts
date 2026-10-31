// Two places side by side, chapter by chapter (compare-two-places).
//
// The place page folded in half: the same chapters in the same order, one
// row per headline measure, both places in one shared period with their
// parents as reference ticks. What may be compared is the API's decision:
// each measure's `/comparison/preflight` verdict is carried through
// unchanged, and a measure it refuses, or one the two places do not publish
// for the same period, is listed under "Not comparable here" with the reason.
// Nothing is computed here beyond formatting and bar lengths.

import type { ComparisonPreflight, ComparisonRule } from "./api/types";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { observationPeriodLabel } from "./observationAccess";
import type { CompareLevel } from "./crossLevelComparison";

export interface ComparedPlace {
  geoId: string;
  name: string;
  level: CompareLevel;
}

export interface ComparisonTick {
  name: string;
  value: number;
}

export interface ComparedRow {
  measureId: string;
  label: string;
  metricCode: string;
  period: string;
  a: { row: ObservationRow; value: number };
  b: { row: ObservationRow; value: number };
  ticks: ComparisonTick[];
  /** The preflight verdict, exactly as the API published it. */
  preflight: ComparisonPreflight;
}

export interface NotComparableRow {
  measureId: string;
  label: string;
  metricCode: string;
  reasons: string[];
  /** The preflight verdict, exactly as the API published it, when one came. */
  preflight: ComparisonPreflight | null;
}

export interface MeasureInputs {
  measureId: string;
  label: string;
  metricCode: string;
  preflight: ComparisonPreflight | null;
  /** An error reading the verdict, stated rather than treated as a pass. */
  preflightError?: string;
  a: ObservationRow | null;
  b: ObservationRow | null;
  parents: { name: string; row: ObservationRow | null }[];
}

function failedRules(rules: ComparisonRule[] | undefined): ComparisonRule[] {
  return (rules || []).filter((rule) => rule.status !== "pass");
}

/**
 * One measure's row, or why it cannot be one. The order of the checks is
 * the order a reader needs: the API's verdict first, then whether both
 * places published, then whether they published for the same period.
 */
export function compareMeasure(
  inputs: MeasureInputs,
  a: ComparedPlace,
  b: ComparedPlace,
): { row: ComparedRow | null; refused: NotComparableRow | null } {
  const base = { measureId: inputs.measureId, label: inputs.label, metricCode: inputs.metricCode };
  if (inputs.preflightError || !inputs.preflight) {
    return { row: null, refused: { ...base, reasons: [`The comparison check could not be read: ${inputs.preflightError || "no answer"}`], preflight: inputs.preflight } };
  }
  if (inputs.preflight.comparable === false) {
    const reasons = failedRules(inputs.preflight.rules).map((rule) => String(rule.reason || rule.rule));
    return { row: null, refused: { ...base, reasons: [...new Set(reasons)], preflight: inputs.preflight } };
  }
  const valueA = inputs.a ? publishedNumber(inputs.a.value) : null;
  const valueB = inputs.b ? publishedNumber(inputs.b.value) : null;
  const missing = [valueA === null ? a.name : "", valueB === null ? b.name : ""].filter(Boolean);
  if (missing.length) {
    return { row: null, refused: { ...base, reasons: [`No published value for ${missing.join(" or ")}.`], preflight: inputs.preflight } };
  }
  const periodA = observationPeriodLabel(inputs.a);
  const periodB = observationPeriodLabel(inputs.b);
  if (periodA !== periodB) {
    return {
      row: null,
      refused: { ...base, reasons: [`${a.name}'s newest published period is ${periodA} and ${b.name}'s is ${periodB}; they are not shown as one year.`], preflight: inputs.preflight },
    };
  }
  const ticks = inputs.parents.flatMap((parent) => {
    const value = parent.row ? publishedNumber(parent.row.value) : null;
    return value !== null && observationPeriodLabel(parent.row) === periodA ? [{ name: parent.name, value }] : [];
  });
  return {
    row: {
      ...base,
      period: periodA,
      a: { row: inputs.a!, value: valueA! },
      b: { row: inputs.b!, value: valueB! },
      ticks,
      preflight: inputs.preflight,
    },
    refused: null,
  };
}

/**
 * Where a value sits along a bar shared by the two places, 0-100, from zero
 * to the larger of the two. The bar is scaled to the places and not to
 * their reference marks: scaled to the nation, two counties' populations
 * were both a sliver, and the comparison the page exists for was invisible.
 * A reference mark beyond the bar is pinned to its end; the marks' own
 * values are printed beneath the bar.
 */
export function barPosition(value: number, row: ComparedRow): number {
  const values = [row.a.value, row.b.value, 0];
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (max === min) return 100;
  return Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
}

/**
 * Whether two published values' margin-of-error intervals overlap, so the
 * page can say the places may not truly differ. Null when either side
 * publishes no usable margin: silence, not a claim either way.
 */
export function intervalsOverlap(a: ObservationRow, b: ObservationRow): boolean | null {
  const valueA = publishedNumber(a.value);
  const valueB = publishedNumber(b.value);
  const marginA = Number(a.margin_of_error);
  const marginB = Number(b.margin_of_error);
  if (valueA === null || valueB === null) return null;
  if (!Number.isFinite(marginA) || !Number.isFinite(marginB) || marginA < 0 || marginB < 0) return null;
  return Math.abs(valueA - valueB) <= marginA + marginB;
}

/** The address of a pair, and of the same pair swapped. */
export function pairPath(a: string, b: string): string {
  return `${a}/vs${b}`;
}

/** Geography identities a compare address may name (`/compare/places`). */
export const COMPARE_GEO_ID = /^(?:us:1|cbsa:\d{5}|state:\d{2}(?:\|county:\d{3}|\|place:\d{5})?)$/;

/**
 * The address of a comparison between two places. Two places with pages
 * share the place-page form (`/us/<a>/vs/us/<b>`); a metro has no page yet,
 * so a pair with one is addressed by both identities instead.
 */
export function compareHref(a: { geoId: string; path: string | null }, b: { geoId: string; path: string | null }): string {
  if (a.path && b.path) return pairPath(a.path, b.path);
  return `/compare/places?a=${encodeURIComponent(a.geoId)}&b=${encodeURIComponent(b.geoId)}`;
}

/**
 * When the two sides are not the same grain, what to offer instead: the
 * finer side's parent, at the coarser side's grain.
 */
export function grainMismatch(a: ComparedPlace, b: ComparedPlace): string | null {
  if (a.level === b.level) return null;
  return `${a.name} is a ${a.level.toLowerCase()} and ${b.name} is a ${b.level.toLowerCase()}. Places are compared at the same grain.`;
}
