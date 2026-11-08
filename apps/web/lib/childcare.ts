// Childcare prices on a county page (county-childcare-prices): the U.S.
// Department of Labor Women's Bureau's National Database of Childcare
// Prices, a median weekly full-time price for each care type and age group.
//
// The Women's Bureau publishes them for counties only, so a county page
// shows its own prices and every other page says where to find them; no
// state or nation figure is made up from counties. A price the Women's
// Bureau filled in itself (imputed) is shown as published and marked, with
// the reasons its five-digit code gives; a blank price is said to be
// missing, never zero.

import type { ObservationRow } from "./explorerViewModel";
import { formatNumber } from "./format";

export const CHILDCARE_SOURCE = "DOL_CHILDCARE";
export const CHILDCARE_CREDIT =
  "U.S. Department of Labor Women's Bureau, National Database of Childcare Prices";

export type CareType = "center" | "home";
export type AgeGroup = "infant" | "toddler" | "preschool" | "school_age";

export const CARE_TYPES: readonly { key: CareType; title: string }[] = [
  { key: "center", title: "Childcare center" },
  { key: "home", title: "Home-based (family childcare)" },
];

export const AGE_GROUPS: readonly { key: AgeGroup; title: string; ages: string }[] = [
  { key: "infant", title: "Infant", ages: "birth to 23 months" },
  { key: "toddler", title: "Toddler", ages: "24 to 35 months" },
  { key: "preschool", title: "Preschool", ages: "3 to 4 years" },
  { key: "school_age", title: "School age", ages: "5 to 12 years" },
];

export const NOT_A_COUNTY =
  "The Women's Bureau publishes childcare prices for counties only: each county's page shows its own.";

export function childcareMetricCode(care: CareType, age: AgeGroup): string {
  return `${CHILDCARE_SOURCE}:${care}_${age}`;
}

/** One cell of the table: the county's newest published row for one price. */
export interface ChildcareCell {
  care: CareType;
  age: AgeGroup;
  /** Null when the Women's Bureau left the price blank for the table's year. */
  value: number | null;
  year: number | null;
  imputed: boolean;
  /** The five-digit code as published, "00000" when nothing was imputed. */
  code: string;
}

export interface ChildcareTable {
  /** The newest study year any price was published for; null when none was. */
  year: number | null;
  cells: ChildcareCell[];
  /** The largest price shown, the one scale every bar is drawn on. */
  max: number;
}

const yearOf = (row: ObservationRow | null | undefined): number | null => {
  const year = Number(String(row?.period_start || "").slice(0, 4));
  return Number.isInteger(year) && year > 0 ? year : null;
};

const dimension = (row: ObservationRow | null | undefined, key: string): string => {
  const dimensions = (row?.dimensions || {}) as Record<string, unknown>;
  const value = dimensions[key];
  return value === null || value === undefined ? "" : String(value);
};

/**
 * The table for one county from each price's newest row. One study year for
 * the whole table, the newest any price was published for: a price whose
 * newest row is older was not published that year, and says so rather than
 * show an older year's price beside the others.
 */
export function childcareTable(newest: Partial<Record<string, ObservationRow | null>>): ChildcareTable {
  const years = Object.values(newest).map(yearOf).filter((year): year is number => year !== null);
  const year = years.length ? Math.max(...years) : null;
  const cells: ChildcareCell[] = [];
  for (const care of CARE_TYPES) {
    for (const age of AGE_GROUPS) {
      const row = newest[childcareMetricCode(care.key, age.key)];
      const sameYear = row && yearOf(row) === year;
      const raw = sameYear ? row.value : null;
      const value = raw === null || raw === undefined || raw === "" ? null : Number(raw);
      const code = sameYear ? dimension(row, "imputation_code") : "";
      cells.push({
        care: care.key,
        age: age.key,
        value: value !== null && Number.isFinite(value) ? value : null,
        year: sameYear ? year : null,
        imputed: Boolean(sameYear) && dimension(row, "imputed") === "true",
        code,
      });
    }
  }
  const max = Math.max(0, ...cells.map((cell) => cell.value ?? 0));
  return { year, cells, max };
}

/** "$293 a week", "$258.75 a week": the price as the Women's Bureau publishes it. */
export function formatWeekly(value: number): string {
  const whole = Number.isInteger(value);
  return `$${formatNumber(value, { minimumFractionDigits: whole ? 0 : 2, maximumFractionDigits: 2 })} a week`;
}

/**
 * What each position of the Women's Bureau's imputation code says, in its
 * own order. A position is 1 when that step was used for the price.
 */
export const IMPUTATION_REASONS: readonly string[] = [
  "taken from another age group",
  "converted to a weekly price",
  "the statewide price given to the county",
  "filled in where the survey's median or 75th percentile was missing",
  "filled in for a year between the state's surveys",
];

export function imputationReasons(code: string): string[] {
  if (!/^[01]{5}$/.test(code)) return [];
  return IMPUTATION_REASONS.filter((_reason, index) => code[index] === "1");
}

/** Every reason any shown imputed price gives, each once, in the code's order. */
export function footnoteReasons(cells: readonly ChildcareCell[]): string[] {
  const codes = cells.filter((cell) => cell.imputed && cell.value !== null).map((cell) => cell.code);
  return IMPUTATION_REASONS.filter((reason) => codes.some((code) => imputationReasons(code).includes(reason)));
}
