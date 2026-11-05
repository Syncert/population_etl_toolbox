// Taxes and public money (taxes-and-public-money T-6, ADR-0014 fiscal section).
//
// The view model behind the Work and Money chapter's "Taxes and public money"
// group and a state's governor table. Everything here arranges what
// `/public-money` and `/governor-terms` served: the API computed every share
// under an approved pairing (ADR-0009), and nothing here adds measures
// together, scores or ranks them. A null value is "not published", never zero.

export type LevelRole = "this place" | "state" | "nation";

export interface PublicMoneyLevel {
  geo_id: string;
  role: LevelRole;
}

export interface ProgramShare {
  program: string;
  label: string;
  share_pct: string | null;
}

export interface PaymentsByProgram {
  geo_id: string;
  year: number;
  total_pct: string | null;
  programs: ProgramShare[];
  unavailable_reason?: string | null;
}

export interface IncomeSources {
  geo_id: string;
  year: number;
  parts: { source: string; label: string; share_pct: string | null }[];
  unavailable_reason?: string | null;
}

export interface SeriesPoint {
  year: number;
  value: string | null;
}

export interface LevelSeries {
  geo_id: string;
  points: SeriesPoint[];
}

export interface MeasureSeries {
  measure: string;
  label: string;
  unit: string;
  source: string;
  derived_from: string;
  caveats: string[];
  series: LevelSeries[];
}

export interface PointValue {
  geo_id: string;
  value: string | null;
  margin_of_error?: string | null;
  unavailable_reason?: string | null;
}

export interface PeriodValues {
  measure: string;
  label: string;
  unit: string;
  source: string;
  period: string | null;
  caveats: string[];
  values: PointValue[];
}

export interface PublicMoneyAnswer {
  derived: true;
  geo_id: string;
  levels: PublicMoneyLevel[];
  payments: PaymentsByProgram[];
  payments_source: string;
  income_sources: IncomeSources[];
  federal_tax_rate: MeasureSeries;
  tax_burden: MeasureSeries;
  property_tax: PeriodValues;
  caveats: string[];
}

export interface TenureFiscalChange {
  measure: string;
  label: string;
  window_start_year: number;
  window_end_year: number | null;
  place_change: string | null;
  nation_change: string | null;
  net_change: string | null;
  unavailable_reason: string | null;
}

export type TaxGroup = "property" | "general_sales" | "income" | "other";

export interface GovernorTenure {
  tenure_key: string;
  holder_name: string;
  party_code: string;
  party_text: string | null;
  start_date: string;
  end_date: string | null;
  so_far: boolean;
  term_count: number;
  fiscal: TenureFiscalChange[];
  tax_mix_year: number | null;
  tax_mix: { tax_group: TaxGroup; label: string; share_pct: string | null }[];
}

export interface GovernorTermsAnswer {
  derived: true;
  geo_id: string;
  state_code: string | null;
  measures: { measure: string; label: string; change_unit: string; source: string; basis: string }[];
  caveats: string[];
  items: GovernorTenure[];
}

/** An empty group's wording, the site's rule for a source not loaded. */
export const NOT_LOADED = "Not loaded into this site yet.";

/** The levels a place page asks about: the nation, a state, a county. */
export const PUBLIC_MONEY_GEO_ID = /^(us:1|state:\d{2}|state:\d{2}\|county:\d{3})$/;
export const STATE_GEO_ID = /^state:\d{2}$/;

/** The same colours the chapter's trends give each level (PlaceTrend). */
export const LEVEL_COLOURS: Record<LevelRole, string> = {
  "this place": "#0b6b57",
  state: "#8c3b17",
  nation: "#2b4c8c",
};

/**
 * The tax mix in one neutral hue, darkest first, in Census's order (an
 * ordinal grey ramp, validated: monotone lightness, light end 2.2:1 on the
 * card). Fiscal figures are never drawn in good or bad colours (ADR-0014).
 */
export const TAX_MIX_COLOURS: Record<TaxGroup, string> = {
  property: "#2f2f2c",
  general_sales: "#5c5b56",
  income: "#86857f",
  other: "#acaba4",
};
export const TAX_MIX_ORDER: readonly TaxGroup[] = ["property", "general_sales", "income", "other"];
export const TAX_MIX_LABELS: Record<TaxGroup, string> = {
  property: "Property",
  general_sales: "General sales",
  income: "Income",
  other: "Other",
};

/** Change bars in the governor table: neutral grey, never good or bad. */
export const CHANGE_GREY = "#86857f";

export function numberOrNull(value: string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

/** "13.1%" -- one decimal, the precision the shares are read at. */
export function percentText(value: string | number | null | undefined): string {
  const number = typeof value === "number" ? value : numberOrNull(value);
  return number === null ? "Not published" : `${number.toFixed(1)}%`;
}

/** "+0.4 points" / "−0.6 points" / "0.0 points", with a true minus sign. */
export function pointsText(value: string | number | null | undefined): string {
  const number = typeof value === "number" ? value : numberOrNull(value);
  if (number === null) return "Not published";
  const rounded = Math.round(number * 10) / 10;
  const sign = rounded > 0 ? "+" : rounded < 0 ? "−" : "";
  return `${sign}${Math.abs(rounded).toFixed(1)}`;
}

/** Why a value is missing, in the site's words. */
export function gapText(reason: string | null | undefined): string {
  switch (reason) {
    case "not_published_that_year":
    case "different_period":
      return "Not published that year";
    case "start_year_not_published":
      return "Not published for the first year compared";
    case "end_year_not_published":
      return "Not published for the last year compared";
    case "no_year_published_since_start":
      return "Not published since the term began";
    case "tenure_shorter_than_a_full_year":
      return "No full year in office";
    case "suppressed_input":
      return "Withheld";
    default:
      return "Not published";
  }
}

export interface StripRow {
  id: string;
  label: string;
  /** One value per level, in the page's level order; null is not published. */
  values: { geoId: string; role: LevelRole; value: number | null }[];
  emphasis?: boolean;
}

/** A shared 0-based scale for a group of dot strips, rounded up to a clean step. */
export function stripMax(rows: StripRow[]): number {
  const values = rows.flatMap((row) => row.values.map((entry) => entry.value ?? 0));
  const top = Math.max(0, ...values);
  if (top <= 0) return 1;
  const step = top <= 5 ? 1 : top <= 20 ? 5 : top <= 50 ? 10 : 25;
  return Math.ceil(top / step) * step;
}

/** The payments rows: the total first, then each program group. */
export function paymentRows(answer: PublicMoneyAnswer): StripRow[] {
  const byGeo = new Map(answer.payments.map((entry) => [entry.geo_id, entry]));
  const levels = answer.levels.filter((level) => byGeo.has(level.geo_id));
  if (!levels.length) return [];
  const first = byGeo.get(levels[0]!.geo_id)!;
  const total: StripRow = {
    id: "total",
    label: "All payments",
    emphasis: true,
    values: levels.map((level) => ({ geoId: level.geo_id, role: level.role, value: numberOrNull(byGeo.get(level.geo_id)!.total_pct) })),
  };
  const programs = first.programs.map((program) => ({
    id: program.program,
    label: program.label,
    values: levels.map((level) => ({
      geoId: level.geo_id,
      role: level.role,
      value: numberOrNull(byGeo.get(level.geo_id)!.programs.find((entry) => entry.program === program.program)?.share_pct),
    })),
  }));
  return [total, ...programs];
}

/** "Where income comes from": each part on its own row, every level on it. */
export function incomeRows(answer: PublicMoneyAnswer): StripRow[] {
  const byGeo = new Map(answer.income_sources.map((entry) => [entry.geo_id, entry]));
  const levels = answer.levels.filter((level) => byGeo.has(level.geo_id));
  if (!levels.length) return [];
  return byGeo.get(levels[0]!.geo_id)!.parts.map((part) => ({
    id: part.source,
    label: part.label,
    values: levels.map((level) => ({
      geoId: level.geo_id,
      role: level.role,
      value: numberOrNull(byGeo.get(level.geo_id)!.parts.find((entry) => entry.source === part.source)?.share_pct),
    })),
  }));
}

export function propertyRow(answer: PublicMoneyAnswer): StripRow | null {
  const roles = new Map(answer.levels.map((level) => [level.geo_id, level.role]));
  const values = answer.property_tax.values
    .filter((entry) => roles.has(entry.geo_id))
    .map((entry) => ({ geoId: entry.geo_id, role: roles.get(entry.geo_id)!, value: numberOrNull(entry.value) }));
  if (!values.some((entry) => entry.value !== null)) return null;
  return { id: "property", label: answer.property_tax.label, values };
}

/** The place's gap from the nation, in points, said once under a group. */
export function nationGapSentence(row: StripRow, placeName: string, subject: string): string | null {
  const own = row.values[0];
  const nation = row.values.find((entry) => entry.role === "nation");
  if (!own || !nation || own === nation || own.value === null || nation.value === null) return null;
  const gap = Math.round((own.value - nation.value) * 10) / 10;
  if (gap === 0) return `${placeName}: ${subject} the same share as the nation.`;
  return `${placeName}: ${subject} ${Math.abs(gap).toFixed(1)} points ${gap > 0 ? "above" : "below"} the nation.`;
}

export interface LinePoint {
  year: number;
  value: number;
}

export interface LineSeries {
  geoId: string;
  role: LevelRole;
  points: LinePoint[];
}

/** Series the levels publish, in level order; a level with no point is left out. */
export function lineSeries(measure: MeasureSeries, levels: PublicMoneyLevel[], only?: (geoId: string) => boolean): LineSeries[] {
  return levels
    .filter((level) => !only || only(level.geo_id))
    .map((level) => {
      const series = measure.series.find((entry) => entry.geo_id === level.geo_id);
      const points = (series?.points || [])
        .map((point) => ({ year: point.year, value: numberOrNull(point.value) }))
        .filter((point): point is LinePoint => point.value !== null)
        .sort((a, b) => a.year - b.year);
      return { geoId: level.geo_id, role: level.role, points };
    })
    .filter((series) => series.points.length > 0);
}

export interface LineGeometry {
  years: [number, number];
  values: [number, number];
  ticks: number[];
  x: (year: number) => number;
  y: (value: number) => number;
}

/** A y-scale that starts at zero (shares of income are read against it). */
export function lineGeometry(series: LineSeries[], width: number, height: number, pad = { left: 36, right: 8, top: 8, bottom: 20 }): LineGeometry | null {
  const points = series.flatMap((entry) => entry.points);
  if (!points.length) return null;
  const firstYear = Math.min(...points.map((point) => point.year));
  const lastYear = Math.max(...points.map((point) => point.year));
  const top = Math.max(...points.map((point) => point.value));
  const step = top <= 4 ? 1 : top <= 10 ? 2 : top <= 25 ? 5 : 10;
  const max = Math.max(step, Math.ceil(top / step) * step);
  const ticks: number[] = [];
  for (let tick = 0; tick <= max + 1e-9; tick += step) ticks.push(Math.round(tick * 100) / 100);
  const span = Math.max(1, lastYear - firstYear);
  return {
    years: [firstYear, lastYear],
    values: [0, max],
    ticks,
    x: (year) => pad.left + ((year - firstYear) / span) * (width - pad.left - pad.right),
    y: (value) => pad.top + (1 - value / max) * (height - pad.top - pad.bottom),
  };
}

/** An SVG path through a series' points, broken where a year is missing. */
export function linePath(points: LinePoint[], geometry: LineGeometry): string {
  let path = "";
  points.forEach((point, index) => {
    const joined = index > 0 && point.year - points[index - 1]!.year === 1;
    path += `${joined ? "L" : "M"}${geometry.x(point.year).toFixed(1)},${geometry.y(point.value).toFixed(1)}`;
  });
  return path;
}

/**
 * End-label heights pushed apart so no two overlap: each label keeps its
 * line's height unless that is within `gap` of the one above, in which case
 * it moves down just enough. Returned in the input order.
 */
export function spreadLabels(ys: number[], gap = 13): number[] {
  const order = ys.map((y, index) => ({ y, index })).sort((a, b) => a.y - b.y);
  const placed = new Array<number>(ys.length);
  let previous = -Infinity;
  for (const { y, index } of order) {
    const at = Math.max(y, previous + gap);
    placed[index] = at;
    previous = at;
  }
  return placed;
}

/** "2011–2018" or "2019–2023 so far"; a window with no end says why. */
export function windowText(change: TenureFiscalChange, soFar: boolean): string {
  if (change.window_end_year === null) return gapText(change.unavailable_reason);
  return `${change.window_start_year}–${change.window_end_year}${soFar ? " so far" : ""}`;
}

/** The years compared for a tenure: the widest window any measure has. */
export function tenureYears(tenure: GovernorTenure): string {
  const windows = tenure.fiscal.filter((change) => change.window_end_year !== null);
  if (!windows.length) return tenure.so_far ? "In office, no full year yet" : gapText(tenure.fiscal[0]?.unavailable_reason);
  const start = Math.min(...windows.map((change) => change.window_start_year));
  const end = Math.max(...windows.map((change) => change.window_end_year!));
  return `${start}–${end}${tenure.so_far ? " so far" : ""}`;
}

/** The largest net change in the table, so every grey bar shares one scale. */
export function changeScale(items: GovernorTenure[]): number {
  const values = items.flatMap((tenure) => tenure.fiscal.map((change) => Math.abs(numberOrNull(change.net_change) ?? 0)));
  return Math.max(0.5, ...values);
}

export function partyLetter(code: string): string {
  return code === "REP" ? "R" : code === "DEM" ? "D" : code === "IND" ? "I" : "O";
}

/** The years of a tenure as the reader knows them: "2011 to 2019", "2019 to today". */
export function tenureSpan(tenure: GovernorTenure): string {
  const start = tenure.start_date.slice(0, 4);
  return `${start} to ${tenure.end_date && !tenure.so_far ? tenure.end_date.slice(0, 4) : "today"}`;
}

/** The group's glossary: the four terms its measures rest on (T-6). */
export const PUBLIC_MONEY_TERMS: readonly { term: string; definition: string }[] = [
  {
    term: "Adjusted gross income",
    definition:
      "Income on a federal tax return after a few adjustments such as retirement contributions and student loan interest, before the standard or itemized deduction. People who file no return are not counted.",
  },
  {
    term: "Payments from government and others (transfer receipts)",
    definition:
      "Money residents receive without working for it in that year: Social Security, Medicare and Medicaid spending on their care, income support, unemployment insurance, veterans' benefits, and smaller payments from nonprofits and businesses. BEA counts them where the people receiving them live.",
  },
  {
    term: "Fiscal year",
    definition:
      "A government's budget year. It is shown as the calendar year it ends in: fiscal 2022 ran from July 2021 to June 2022 in 46 states.",
  },
  {
    term: "Effective tax rate",
    definition:
      "Tax actually paid as a share of income, after deductions and credits, rather than the rate written in law. The federal rate here is income tax after credits over adjusted gross income, for all of a place's returns together.",
  },
];
