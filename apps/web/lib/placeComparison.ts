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
import type { PlaceLevel } from "./placeChapters";

export interface ComparedPlace {
  geoId: string;
  name: string;
  level: PlaceLevel;
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

/** Where a value sits along a bar shared by both places and their ticks, 0-100. */
export function barPosition(value: number, row: ComparedRow): number {
  const values = [row.a.value, row.b.value, ...row.ticks.map((tick) => tick.value), 0];
  const min = Math.min(...values);
  const max = Math.max(...values);
  return max === min ? 100 : ((value - min) / (max - min)) * 100;
}

/** The address of a pair, and of the same pair swapped. */
export function pairPath(a: string, b: string): string {
  return `${a}/vs${b}`;
}

/**
 * When the two sides are not the same grain, what to offer instead: the
 * finer side's parent, at the coarser side's grain.
 */
export function grainMismatch(a: ComparedPlace, b: ComparedPlace): string | null {
  if (a.level === b.level) return null;
  return `${a.name} is a ${a.level.toLowerCase()} and ${b.name} is a ${b.level.toLowerCase()}. Places are compared at the same grain.`;
}
