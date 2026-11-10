// "Starting a family" on a place page (starting-a-family-collection).
//
// Eight published measures a family weighs when deciding where to have and
// raise children, gathered from the sources the other chapters read. Each row
// stands alone: its own source, period, levels and rank. Nothing here adds
// rows together, scores a place or says whether a value helps or hinders a
// family (Nick, 2026-11-10: "skip the words and go with the information,
// because it's subjective"). A figure published only for a larger area is
// that area's, named; a level the source does not publish says so.

import type { PlaceLevel } from "./placeChapters";
import { formatNumber } from "./format";

export type FamilyGroup = "care" | "home" | "money" | "surroundings";

export const FAMILY_GROUPS: readonly { key: FamilyGroup; title: string }[] = [
  { key: "care", title: "Paying for care" },
  { key: "home", title: "A home with room" },
  { key: "money", title: "Money coming in" },
  { key: "surroundings", title: "A child's surroundings" },
];

export interface FamilyRow {
  id: string;
  group: FamilyGroup;
  label: string;
  /** A published metric, read as the other chapters read it. */
  metricCode?: string;
  /** A reviewed derived view (ADR-0009), read from `/observations/derived`. */
  ratioId?: string;
  /** The levels the source publishes this row for. */
  levels: readonly PlaceLevel[];
  /** The unit, when the row carries none. */
  unit?: string;
  /** Said in place of a level the source does not publish. */
  gap?: string;
  /** The time cost dividing this price by the county's own wage. */
  hoursRatioId?: string;
  /** What one unit of the price buys, for the arithmetic ("a week of infant care"). */
  hoursWhat?: string;
  /** "a week", "a month": the price's period. */
  per?: string;
  /**
   * Hours only at a wage of the price's own year (childcare, as its own
   * table shows them). Otherwise the newest wage stands in for a price newer
   * than QCEW's newest year, and the arithmetic says so.
   */
  hoursSameYearOnly?: boolean;
  note?: string;
}

const COUNTY_ONLY: readonly PlaceLevel[] = ["COUNTY"];
const COUNTY_STATE_NATION: readonly PlaceLevel[] = ["COUNTY", "STATE", "NATIONAL"];
const ACS_LEVELS: readonly PlaceLevel[] = ["PLACE", "COUNTY", "METRO", "STATE", "NATIONAL"];

export const FAMILY_ROWS: readonly FamilyRow[] = [
  {
    id: "infant-care",
    group: "care",
    label: "Infant care at a childcare center",
    metricCode: "DOL_CHILDCARE:center_infant",
    levels: COUNTY_ONLY,
    unit: "dollars per week",
    gap: "Published by county; the Women's Bureau publishes no state or national figure",
    hoursRatioId: "minutes_of_work_childcare_center_infant",
    hoursWhat: "a week of infant care at a center",
    per: "a week",
    hoursSameYearOnly: true,
    note: "Median full-time price, birth to 23 months. The other seven childcare prices are in Cost of living.",
  },
  {
    id: "rent-3br",
    group: "home",
    label: "Fair Market Rent, three bedrooms",
    metricCode: "HUD_FMR_IL:fmr_3br",
    levels: COUNTY_ONLY,
    unit: "dollars per month",
    gap: "Set by HUD for each county's HUD area; shown on county pages",
    hoursRatioId: "minutes_of_work_rent_3br",
    hoursWhat: "a month's three-bedroom rent",
    per: "a month",
    note: "HUD's reference rent, including utilities, for the county's HUD area; not the survey's median rent.",
  },
  {
    id: "home-value",
    group: "home",
    label: "Median home value",
    metricCode: "CENSUS_ACS:acs5:B25077_001",
    levels: ACS_LEVELS,
    unit: "Dollars",
    note: "Owner-occupied homes, as their owners value them.",
  },
  {
    id: "weekly-wage",
    group: "money",
    label: "Average weekly wage of jobs located here",
    metricCode: "BLS_QCEW:annual_avg_weekly_wage:10:0",
    levels: COUNTY_STATE_NATION,
    unit: "dollars per week",
    gap: "Published for counties, states and the nation",
    note: "Annual average over all covered jobs, including part-time jobs; the wage the hours of work are counted in.",
  },
  {
    id: "child-poverty",
    group: "money",
    label: "Children under 18 in poverty",
    metricCode: "CENSUS_SAIPE_SAHIE:saipe:SAEPOVRT0_17",
    levels: COUNTY_STATE_NATION,
    unit: "percent",
    gap: "Published for counties, states and the nation",
    note: "The Census Bureau's model-based single-year estimate (SAIPE).",
  },
  {
    id: "children-uninsured",
    group: "surroundings",
    label: "Children under 19 with no health insurance",
    ratioId: "uninsured_under_19_share:acs5",
    levels: ACS_LEVELS,
    note: "Share of civilian noninstitutionalized people under 19, from the survey.",
  },
  {
    id: "students-per-teacher",
    group: "surroundings",
    label: "Public school students per teacher",
    ratioId: "students_per_teacher",
    levels: ["COUNTY", "STATE"],
    gap: "Published for counties and states",
    note: "Students over full-time-equivalent teachers in the public schools placed here that reported both figures.",
  },
  {
    id: "households-with-children",
    group: "surroundings",
    label: "Households with children under 18",
    ratioId: "households_with_children_share:acs5",
    levels: ACS_LEVELS,
    note: "Share of households with one or more people under 18, from the survey.",
  },
];

/** Every metric the rows read directly; `lib/familyCodes.ts` names the same, pinned by its test. */
export const FAMILY_ROW_CODES: readonly string[] = FAMILY_ROWS.flatMap((row) => (row.metricCode ? [row.metricCode] : []));

export const FAMILY_INTRO =
  "What having and raising children here costs and looks like, from published figures. Each line stands alone, with its own source and year; the lines are not added up, and none is called good or bad.";

export const HOURS_PER_WEEK = 40;

/** A served time-cost row, the fields read here. */
export interface FamilyMinutes {
  value?: string | null;
  unavailable_reason?: string | null;
  wage_geo_name?: string | null;
  wage_year?: "same_year" | "newest_published_year" | null;
  denominator?: { value?: string | null; period_start?: string | null } | null;
  numerator?: { value?: string | null; period_start?: string | null } | null;
}

const minutesOf = (row: FamilyMinutes | null | undefined): number | null => {
  if (!row || row.unavailable_reason || row.value === null || row.value === undefined || row.value === "") return null;
  const minutes = Number(row.value);
  return Number.isFinite(minutes) ? minutes : null;
};

/**
 * "8 hours 42 minutes of work", or "" when there is no value, or when the row
 * takes only its own year's wage and was given another: never zero.
 */
export function hoursText(row: FamilyMinutes | null | undefined, sameYearOnly = false): string {
  if (sameYearOnly && row?.wage_year !== "same_year") return "";
  const minutes = minutesOf(row);
  if (minutes === null) return "";
  const whole = Math.round(minutes);
  const hours = Math.floor(whole / 60);
  const rest = whole % 60;
  const part = (count: number, word: string) => `${formatNumber(count)} ${word}${count === 1 ? "" : "s"}`;
  const text = !hours ? part(rest, "minute") : rest ? `${part(hours, "hour")} ${part(rest, "minute")}` : part(hours, "hour");
  return `${text} of work`;
}

const dollars = (value: number, cents = false): string =>
  `$${formatNumber(value, { minimumFractionDigits: cents ? 2 : 0, maximumFractionDigits: cents ? 2 : 0 })}`;

/**
 * The arithmetic behind one row's hours: the wage, its year, the hourly
 * wage over a 40-hour week, and the division. When the price is newer than
 * QCEW's newest year, the wage's year is the newest and that is said.
 */
export function hoursFormula(row: FamilyMinutes | null | undefined, family: FamilyRow, county: string): string {
  const weekly = Number(row?.denominator?.value);
  const price = Number(row?.numerator?.value);
  const year = String(row?.denominator?.period_start || "").slice(0, 4);
  if (!hoursText(row, family.hoursSameYearOnly) || !Number.isFinite(weekly) || weekly <= 0 || !Number.isFinite(price) || !/^\d{4}$/.test(year)) return "";
  const hourly = weekly / HOURS_PER_WEEK;
  const name = row?.wage_geo_name || county;
  const newest = row?.wage_year === "newest_published_year" ? `, the newest year QCEW has published` : "";
  return (
    `${dollars(price, !Number.isInteger(price))} ${family.per} ÷ (${name}'s average wage in ${year}${newest}, ${dollars(weekly)} a week ÷ ${HOURS_PER_WEEK} hours = ${dollars(hourly, true)} an hour) = ` +
    `${formatNumber(price / hourly, { minimumFractionDigits: 1, maximumFractionDigits: 1 })} hours for ${family.hoursWhat}.`
  );
}

/** Why a price that has a value shows no hours; "" when hours are shown. */
export function hoursGap(row: FamilyMinutes | null | undefined, family: FamilyRow, county: string): string {
  if (!row || hoursText(row, family.hoursSameYearOnly)) return "";
  if (row.unavailable_reason === "denominator_missing") return `Hours of work: no wage is loaded for ${county} yet.`;
  const year = String(row.numerator?.period_start || "").slice(0, 4);
  if (row.unavailable_reason === "different_period" || (family.hoursSameYearOnly && row.wage_year === "newest_published_year")) {
    return `Hours of work: ${county}'s wage for ${/^\d{4}$/.test(year) ? year : "this year"} is not published.`;
  }
  return "";
}

/** One level's school coverage for a ratio of sums over schools. */
export interface SchoolCoverage {
  name: string;
  withValue?: string | null;
  withoutValue?: string | null;
}

/**
 * How many of each place's schools a ratio over schools covers, when any
 * did not report: "From the schools that reported both students and
 * teachers: 112 of 118 in Dane County and all 2,190 in Wisconsin." "" when
 * every school reported, so a whole figure carries no note.
 */
export function schoolsText(levels: readonly SchoolCoverage[]): string {
  const counted = levels
    .filter((level) => level.withValue && level.withoutValue)
    .map((level) => ({ name: level.name, reported: Number(level.withValue), missing: Number(level.withoutValue) }))
    .filter((level) => Number.isInteger(level.reported) && Number.isInteger(level.missing) && level.reported > 0);
  if (!counted.some((level) => level.missing > 0)) return "";
  const parts = counted.map((level) =>
    level.missing
      ? `${formatNumber(level.reported)} of ${formatNumber(level.reported + level.missing)} in ${level.name}`
      : `all ${formatNumber(level.reported)} in ${level.name}`,
  );
  const list = parts.length > 1 ? `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}` : parts[0];
  return `From the schools that reported both students and teachers: ${list}.`;
}

/**
 * The level a row's headline value comes from: the page's own place when the
 * source publishes it there, otherwise the nearest larger level that does
 * (a city shows its county's childcare price, named). Null when none does.
 */
export function headlineLevel<T extends { level: PlaceLevel }>(row: FamilyRow, chain: readonly T[]): T | null {
  return chain.find((entry) => row.levels.includes(entry.level)) || null;
}

/**
 * A row's value in the reader's terms: "$293 a week", "$385,000", "12.4%",
 * "15.2 students per full-time-equivalent teacher". "" when there is no number.
 */
export function familyValueText(value: unknown, unit: string): string {
  const number = value === null || value === undefined || value === "" ? NaN : Number(value);
  if (!Number.isFinite(number)) return "";
  const whole = Number.isInteger(number);
  const money = (amount: number) =>
    `$${formatNumber(amount, { minimumFractionDigits: whole ? 0 : 2, maximumFractionDigits: whole ? 0 : 2 })}`;
  const trimmed = unit.trim();
  const per = /^dollars per (week|month|year)$/i.exec(trimmed);
  if (per) return `${money(number)} a ${per[1]!.toLowerCase()}`;
  if (/^dollars$/i.test(trimmed)) return money(number);
  if (/^(percent|%)$/i.test(trimmed)) return `${formatNumber(number, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`;
  return `${formatNumber(number, { maximumFractionDigits: 1 })} ${trimmed}`.trim();
}
