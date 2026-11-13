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

/** The eight prices' metric codes, center first, in the table's order. */
export const CHILDCARE_CODES: readonly string[] = CARE_TYPES.flatMap((care) =>
  AGE_GROUPS.map((age) => childcareMetricCode(care.key, age.key)),
);

/**
 * Which of the prices to read, from the place page's one catalog read: only
 * the codes the catalog holds, so a price not loaded is never asked for (and
 * never answers 404). `null` while that read is pending; `undefined` when the
 * page has no catalog read, and then every price is read.
 */
export function catalogedCodes(
  catalogued: ReadonlyMap<string, unknown> | null | undefined,
): readonly string[] | null {
  if (catalogued === undefined) return CHILDCARE_CODES;
  if (catalogued === null) return null;
  return CHILDCARE_CODES.filter((code) => catalogued.has(code));
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

// ---------------------------------------------------------------------------
// Hours of work (Nick, 2026-11-08): each price over the county's own average
// hourly wage, the QCEW weekly wage over 40 hours, read through the API's
// reviewed `minutes_of_work_childcare_*` pairings. The dollar price stays
// the figure; the hours are shown beside it with the arithmetic stated.
// Only a wage of the price's own year is used: a price is never divided by
// another year's wage (no extrapolation), and the gap is said instead.
// ---------------------------------------------------------------------------

export const HOURS_PER_WEEK = 40;

export function childcareRatioId(care: CareType, age: AgeGroup): string {
  return `minutes_of_work_childcare_${care}_${age}`;
}

/** A served minutes-of-work row, the fields this page reads. */
export interface ChildcareMinutes {
  value?: string | null;
  unavailable_reason?: string | null;
  wage_geo_name?: string | null;
  wage_year?: "same_year" | "newest_published_year" | null;
  denominator?: { value?: string | null; period_start?: string | null } | null;
}

/**
 * What the county card read for center-based infant care, handed to
 * "Starting a family" so the same price and hours are not asked twice.
 * `row` undefined while reading; `minutes` undefined until its read ends.
 */
export interface InfantCareRead {
  row?: ObservationRow | null;
  error?: string;
  minutes?: ChildcareMinutes | null;
  /** The national price change since the study year, once read; see `nationalChildcareTrend`. */
  national?: NationalTrend | null;
}

const dollars = (value: number, cents = false): string =>
  `$${formatNumber(value, { minimumFractionDigits: cents ? 2 : 0, maximumFractionDigits: cents ? 2 : 0 })}`;

/**
 * "9 hours 23 minutes of work", or "" when there is no value or the wage is
 * not the price's own year: never zero, never another year's wage.
 */
export function workText(row: ChildcareMinutes | null | undefined): string {
  const minutes = row?.value === null || row?.value === undefined || row.value === "" ? NaN : Number(row.value);
  if (!row || row.unavailable_reason || row.wage_year !== "same_year" || !Number.isFinite(minutes)) return "";
  const whole = Math.round(minutes);
  const hours = Math.floor(whole / 60);
  const rest = whole % 60;
  const part = (count: number, word: string) => `${count} ${word}${count === 1 ? "" : "s"}`;
  const text = !hours ? part(rest, "minute") : rest ? `${part(hours, "hour")} ${part(rest, "minute")}` : part(hours, "hour");
  return `${text} of work`;
}

/**
 * The arithmetic behind the hours, worked once for one price: the county's
 * wage and its year, the hourly wage over a 40-hour week, and the division.
 */
export function workFormula(row: ChildcareMinutes | null | undefined, price: number, what: string, county: string): string {
  const weekly = Number(row?.denominator?.value);
  const year = String(row?.denominator?.period_start || "").slice(0, 4);
  if (!row || !workText(row) || !Number.isFinite(weekly) || weekly <= 0 || !/^\d{4}$/.test(year)) return "";
  const hourly = weekly / HOURS_PER_WEEK;
  const hours = price / hourly;
  const name = row.wage_geo_name || county;
  return (
    `How the hours are worked out: ${name}'s average wage in ${year} was ${dollars(weekly)} a week ` +
    `(Bureau of Labor Statistics, QCEW, all jobs located in the county), or ${dollars(hourly, true)} an hour over a ${HOURS_PER_WEEK}-hour week. ` +
    `${dollars(price, !Number.isInteger(price))} a week for ${what} ÷ ${dollars(hourly, true)} an hour = ` +
    `${formatNumber(hours, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} hours of work. Each price is divided the same way.`
  );
}

/**
 * Why no hours are shown for a price that has one, in the words the other
 * gaps use; "" when hours are shown or the price itself is missing.
 */
export function workGap(row: ChildcareMinutes | null | undefined, year: number | null, county: string): string {
  if (!row || workText(row) || year === null) return "";
  if (row.unavailable_reason === "denominator_missing") return `Hours of work: no wage is loaded for ${county} yet.`;
  if (row.wage_year === "newest_published_year" || row.unavailable_reason === "different_period") {
    return `Hours of work: ${county}'s wage for ${year} is not published yet.`;
  }
  return "";
}

// ---------------------------------------------------------------------------
// The national price change since the study year (Nick, 2026-11-10: option
// 2 of the childcare recency options). The Women's Bureau has no release
// schedule and its newest study year is 2022, so the card adds one more true
// figure beside the county's price: the Bureau of Labor Statistics' monthly
// CPI for day care and preschool, U.S. city average, not seasonally
// adjusted, read as its own change from the same month of the study year to
// the newest month published. It is the nation's change, stated as such;
// the county's price is never scaled by it (no extrapolation).
// ---------------------------------------------------------------------------

export const CHILDCARE_CPI_SERIES = "CUUR0000SEEB03";
export const CHILDCARE_CPI_CODE = `BLS:${CHILDCARE_CPI_SERIES}`;
export const NATION_GEO_ID = "us:1";
export const CHILDCARE_CPI_CREDIT =
  "Bureau of Labor Statistics, Consumer Price Index for All Urban Consumers, day care and preschool, U.S. city average, not seasonally adjusted";

/** What the card says about the national change; every field "" when there is nothing to say. */
export interface NationalTrend {
  /** "Nationally, ... rose 18.4% from September 2022 to September 2026." */
  text: string;
  /** The two published index values and the division, stated once. */
  formula: string;
  /** Why no change is shown, in the words the other gaps use. */
  gap: string;
  percent: number | null;
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/** "September 2022" from "2022-09". */
export function monthName(month: string): string {
  const index = Number(month.slice(5, 7)) - 1;
  return MONTHS[index] ? `${MONTHS[index]} ${month.slice(0, 4)}` : month;
}

const indexValue = (row: ObservationRow | null | undefined): number | null => {
  if (!row || row.value === null || row.value === undefined || row.value === "") return null;
  const value = Number(row.value);
  return Number.isFinite(value) && value > 0 ? value : null;
};

const EMPTY_TREND: NationalTrend = { text: "", formula: "", gap: "", percent: null };

/**
 * The national index's change from the same month of the study year to the
 * newest month published: two published values divided, nothing filled in.
 * Nothing is said when no month after the study year is published; a base
 * month that is not published is said, and no other month stands in for it.
 */
export function nationalChildcareTrend(
  rows: readonly ObservationRow[],
  studyYear: number | null,
  county: string,
): NationalTrend {
  if (studyYear === null) return EMPTY_TREND;
  const monthly = rows.filter((row) => /^\d{4}-\d{2}/.test(String(row.period_start || "")));
  const monthOfRow = (row: ObservationRow) => String(row.period_start).slice(0, 7);
  const newest = [...monthly]
    .filter((row) => indexValue(row) !== null)
    .sort((left, right) => monthOfRow(right).localeCompare(monthOfRow(left)))[0];
  if (!newest) return { ...EMPTY_TREND, gap: "The national price index for day care and preschool has no month published yet." };
  const month = monthOfRow(newest);
  if (Number(month.slice(0, 4)) <= studyYear) return EMPTY_TREND;
  const baseMonth = `${studyYear}${month.slice(4)}`;
  const baseRow = monthly.find((row) => monthOfRow(row) === baseMonth);
  const then = indexValue(baseRow);
  if (then === null) {
    return {
      ...EMPTY_TREND,
      gap: `Nationally: ${monthName(baseMonth)} is not published in the price index for day care and preschool, so its change since ${studyYear} is not shown.`,
    };
  }
  const now = indexValue(newest)!;
  const percent = Math.round((now / then - 1) * 1000) / 10;
  const verb = percent > 0 ? `rose ${percent.toFixed(1)}%` : percent < 0 ? `fell ${Math.abs(percent).toFixed(1)}%` : "did not change";
  // Each index value as stored: as many decimals as the published value has, no more.
  const index = (row: ObservationRow | undefined, value: number) => {
    const decimals = (String(row?.value ?? "").split(".")[1] || "").length;
    return formatNumber(value, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  };
  const ratio = formatNumber(now / then, { minimumFractionDigits: 3, maximumFractionDigits: 3 });
  const unit = String(newest.unit || newest.units || "").trim();
  return {
    text: `Nationally, prices for day care and preschool ${verb} from ${monthName(baseMonth)} to ${monthName(month)}.`,
    formula:
      `How the national change is worked out: the ${CHILDCARE_CPI_CREDIT} (series ${CHILDCARE_CPI_SERIES}${unit ? `, ${unit}` : ""}), ` +
      `was ${index(newest, now)} in ${monthName(month)} and ${index(baseRow, then)} in ${monthName(baseMonth)}; ${index(newest, now)} ÷ ${index(baseRow, then)} = ${ratio}, ` +
      `a change of ${percent > 0 ? "+" : ""}${percent.toFixed(1)}%. It is the nation's change, not ${county}'s: ` +
      `the county's prices above are the Women's Bureau's for ${studyYear}, as published, and are not adjusted by it.`,
    gap: "",
    percent,
  };
}
