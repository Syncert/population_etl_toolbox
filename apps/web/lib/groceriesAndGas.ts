// "Cost of living" on a place page (groceries-and-gas-cards, extended by
// cost-of-living-section with everyday prices people recognise).
//
// No source publishes a county's grocery or gas prices. Each figure on the
// card is the containing area's: the area is found through the reference's
// `part_of` rows (never by name), and the card names it beside the number.
// The metric identities below are built from those areas' Census codes,
// which is how BLS and BEA number them; nothing is matched by name.

import type { ObservationRow } from "./explorerViewModel";
import { formatNumber } from "./format";
import type { RelatedResponse } from "./placeRelationships";
import type { PlaceLevel } from "./placeChapters";

export interface Area {
  geoId: string;
  level: "NATIONAL" | "STATE" | "METRO" | "CENSUS_DIVISION" | "CENSUS_REGION" | "COUNTY" | "PLACE" | "PROVIDER_AREA";
  name: string;
}

export interface ContainingAreas {
  nation: Area | null;
  state: Area | null;
  metro: Area | null;
  division: Area | null;
  region: Area | null;
}

export type CostMeasure = "gas" | "food" | "parity" | HeatMeasure | "diesel" | PriceItem;

/**
 * Home heating fuels (home-heating-fuel-prices), each EIA's residential price:
 * natural gas monthly by state; heating oil and propane weekly, October to
 * March, by state where EIA surveys it, else by EIA's own petroleum region.
 */
export type HeatMeasure = "heat_gas" | "heating_oil" | "propane";

/**
 * Everyday items with a dollar price (BLS Average Price, `ap`), in the order
 * the section lists them. BLS publishes them for the nation and the four
 * Census regions only, so every page shows its region's price, named.
 */
export type PriceItem = "eggs" | "milk" | "bread" | "beef" | "chicken" | "electricity";

export interface PriceItemSpec {
  item: PriceItem;
  /** BLS's item code in the `ap` series id. */
  code: string;
  title: string;
  /** "a dozen", "a gallon": what one price buys. */
  per: string;
  /** Shown in cents: a price well under a dollar a unit (electricity). */
  cents?: boolean;
}

export const PRICE_ITEMS: readonly PriceItemSpec[] = [
  { item: "eggs", code: "708111", title: "Eggs, grade A large", per: "a dozen" },
  { item: "milk", code: "709112", title: "Whole milk", per: "a gallon" },
  { item: "bread", code: "702111", title: "White bread", per: "a pound" },
  { item: "beef", code: "FC1101", title: "Ground beef", per: "a pound" },
  { item: "chicken", code: "FF1101", title: "Chicken breast, boneless", per: "a pound" },
  { item: "electricity", code: "72610", title: "Electricity", per: "a kilowatt-hour", cents: true },
];

/** One way to answer a card line: this metric, published for this area. */
export interface Reading {
  measure: CostMeasure;
  metricCode: string;
  area: Area;
  source: string;
  /** What one published unit is in the line's unit: EIA's electricity is cents, the line dollars. */
  scale?: number;
}

export const SOURCE_NAMES: Record<string, string> = {
  EIA: "U.S. Energy Information Administration",
  BLS: "Bureau of Labor Statistics",
  BEA: "Bureau of Economic Analysis",
};

export const NO_LOCAL_PRICES =
  "No source publishes grocery or gas prices for a county or a city. Each price here is for the larger area named beside it.";

/** On a metro's page: the price level is its own, the rest a larger area's. */
export const NO_METRO_PRICES =
  "No source publishes grocery or gas prices for a metro area as the Census Bureau draws it. The price level is BEA's figure for this metro; each other figure is for the larger area named beside it.";

const LEVEL_WORDS: Partial<Record<Area["level"], string>> = {
  NATIONAL: "the nation",
  STATE: "the state",
  METRO: "the metropolitan area",
  CENSUS_DIVISION: "the Census division",
  CENSUS_REGION: "the Census region",
};

function areaName(row: { geo_id: string; geo_level?: string | null; geo_name?: string | null }): string {
  if (row.geo_level === "NATIONAL") return "United States";
  const name = String(row.geo_name || "").trim();
  // A catalog that has not named the area answers with its id; say what kind
  // of area it is rather than print the id.
  return name && name !== row.geo_id ? name : LEVEL_WORDS[row.geo_level as Area["level"]] || row.geo_id;
}

/**
 * The areas this place lies in, from the reference's own rows. On a state or
 * the nation's page the place is its own state or nation.
 */
export function containingAreas(
  level: PlaceLevel,
  place: { geo_id: string; geo_name?: string | null } | null,
  related: RelatedResponse | null,
): ContainingAreas {
  const areas: ContainingAreas = { nation: null, state: null, metro: null, division: null, region: null };
  const own = (areaLevel: Area["level"]): Area | null =>
    place ? { geoId: place.geo_id, level: areaLevel, name: areaName({ ...place, geo_level: areaLevel }) } : null;
  if (level === "NATIONAL") areas.nation = own("NATIONAL");
  if (level === "STATE") areas.state = own("STATE");
  if (level === "METRO") areas.metro = own("METRO");
  for (const row of related?.items || []) {
    if (row.relationship !== "part_of") continue;
    const area = { geoId: row.geo_id, level: row.geo_level as Area["level"], name: areaName(row) };
    if (row.geo_level === "NATIONAL") areas.nation ||= area;
    else if (row.geo_level === "STATE" && level !== "STATE") areas.state ||= area;
    else if (row.geo_level === "METRO" && /^cbsa:\d{5}$/.test(row.geo_id)) areas.metro ||= area;
    else if (row.geo_level === "CENSUS_DIVISION" && /^division:[1-9]$/.test(row.geo_id)) areas.division ||= area;
    else if (row.geo_level === "CENSUS_REGION" && /^region:[1-4]$/.test(row.geo_id)) areas.region ||= area;
  }
  return areas;
}

const code = (area: Area | null) => (area ? area.geoId.split(":")[1] : "");

/**
 * EIA's petroleum regions (PADDs), by the state FIPS codes EIA assigns to
 * each; the East Coast is read by its three sub-districts. EIA's own
 * definition, not a Census geography.
 */
const PADDS: readonly [string, string, readonly string[]][] = [
  ["R1X", "New England (PADD 1A)", ["09", "23", "25", "33", "44", "50"]],
  ["R1Y", "Central Atlantic (PADD 1B)", ["10", "11", "24", "34", "36", "42"]],
  ["R1Z", "Lower Atlantic (PADD 1C)", ["12", "13", "37", "45", "51", "54"]],
  ["R20", "Midwest (PADD 2)", ["17", "18", "19", "20", "21", "26", "27", "29", "31", "38", "39", "40", "46", "47", "55"]],
  ["R30", "Gulf Coast (PADD 3)", ["01", "05", "22", "28", "35", "48"]],
  ["R40", "Rocky Mountain (PADD 4)", ["08", "16", "30", "49", "56"]],
  ["R50", "West Coast (PADD 5)", ["02", "04", "06", "15", "32", "41", "53"]],
];

/** The EIA petroleum region a state lies in, as the provider area EIA publishes. */
export function paddOf(state: Area | null): Area | null {
  const fips = code(state) || "";
  const padd = PADDS.find(([, , states]) => states.includes(fips));
  return padd ? { geoId: `area:eia:${padd[0]}`, level: "PROVIDER_AREA", name: padd[1] } : null;
}

/**
 * What to ask for each line, most local first. Each list is tried in order
 * and the first area that publishes a value answers.
 *
 * - Gas: EIA's regular gasoline where EIA publishes the state, else BLS's
 *   average price of regular gasoline for the Census region, else EIA's
 *   national price. Both are dollars per gallon.
 * - Food at home: the BLS CPI for the division, else the region, else the
 *   nation, read as that area's own change over a year. Index levels are
 *   relative to each area's base, so they are never shown or compared.
 * - Price parity: BEA's all-items parity for the metro area, else the state.
 *   The nation is 100 by definition and is stated, not read.
 */
export function readingsFor(areas: ContainingAreas): Record<CostMeasure, Reading[]> {
  const region = code(areas.region);
  const division = code(areas.division);
  const gas: Reading[] = [];
  const food: Reading[] = [];
  const parity: Reading[] = [];
  if (areas.state) gas.push({ measure: "gas", metricCode: "EIA:EPMR", area: areas.state, source: "EIA" });
  if (areas.region) gas.push({ measure: "gas", metricCode: `BLS:APU0${region}0074714`, area: areas.region, source: "BLS" });
  if (areas.nation) gas.push({ measure: "gas", metricCode: "EIA:EPMR", area: areas.nation, source: "EIA" });
  if (areas.division && areas.region) food.push({ measure: "food", metricCode: `BLS:CUUR0${region}${division}0SAF11`, area: areas.division, source: "BLS" });
  if (areas.region) food.push({ measure: "food", metricCode: `BLS:CUUR0${region}00SAF11`, area: areas.region, source: "BLS" });
  if (areas.nation) food.push({ measure: "food", metricCode: "BLS:CUUR0000SAF11", area: areas.nation, source: "BLS" });
  if (areas.metro) parity.push({ measure: "parity", metricCode: "BEA:MARPP:1", area: areas.metro, source: "BEA" });
  if (areas.state) parity.push({ measure: "parity", metricCode: "BEA:SARPP:1", area: areas.state, source: "BEA" });
  const padd = paddOf(areas.state);
  const eia = (measure: CostMeasure, metricCode: string, within: (Area | null)[]): Reading[] =>
    within.filter((area): area is Area => Boolean(area)).map((area) => ({ measure, metricCode, area, source: "EIA" }));
  const heat = {
    heat_gas: eia("heat_gas", "EIA:NG_RES", [areas.state, areas.nation]),
    heating_oil: eia("heating_oil", "EIA:EPD2F", [areas.state, padd, areas.nation]),
    propane: eia("propane", "EIA:EPLLPA", [areas.state, padd, areas.nation]),
  };
  const diesel = eia("diesel", "EIA:EPD2D", [areas.state, padd, areas.nation]);
  const prices = {} as Record<PriceItem, Reading[]>;
  for (const spec of PRICE_ITEMS) {
    const readings: Reading[] = [];
    // EIA publishes each state's own residential electricity price (cents a
    // kilowatt-hour); BLS only the region's urban average.
    if (spec.item === "electricity" && areas.state) {
      readings.push({ measure: spec.item, metricCode: "EIA:ELEC_RES", area: areas.state, source: "EIA", scale: 0.01 });
    }
    if (areas.region) readings.push({ measure: spec.item, metricCode: `BLS:APU0${region}00${spec.code}`, area: areas.region, source: "BLS" });
    if (areas.nation) readings.push({ measure: spec.item, metricCode: `BLS:APU0000${spec.code}`, area: areas.nation, source: "BLS" });
    prices[spec.item] = readings;
  }
  return { gas, food, parity, diesel, ...heat, ...prices };
}

/** Therms in a thousand cubic feet: EIA's average heat content, 1,037 Btu a cubic foot. */
export const THERMS_PER_MCF = 10.37;

export const HEATING_SEASON = "EIA surveys heating oil and propane prices weekly from October to March.";

/** "$15.20 per thousand cubic feet ($1.47 a therm)". */
export function formatGasHeat(value: number): string {
  return `$${value.toFixed(2)} per thousand cubic feet ($${(value / THERMS_PER_MCF).toFixed(2)} a therm)`;
}

/**
 * A weekly price's change from the same week a year earlier (EIA's weeks
 * are Mondays, so 364 days back). An unpublished week is said, not filled.
 */
export function changeFromYearEarlierWeek(rows: readonly ObservationRow[]): YearChange {
  const dated = rows.filter((row) => /^\d{4}-\d{2}-\d{2}/.test(String(row.period_start || "")) && numberOf(row) !== null);
  const newest = [...dated].sort((left, right) => String(right.period_start).localeCompare(String(left.period_start)))[0];
  if (!newest) return { percent: null, period: "", reason: "No week is published for this area." };
  const week = String(newest.period_start).slice(0, 10);
  const back = new Date(`${week}T00:00:00Z`);
  back.setUTCDate(back.getUTCDate() - 364);
  const earlierWeek = back.toISOString().slice(0, 10);
  const then = numberOf(dated.find((row) => String(row.period_start).slice(0, 10) === earlierWeek));
  if (then === null || then === 0) return { percent: null, period: week, reason: `The week of ${earlierWeek} is not published for this area.` };
  return { percent: Math.round((numberOf(newest)! / then - 1) * 1000) / 10, period: `${earlierWeek} to ${week}`, reason: "" };
}

/** "$3.12 a dozen", "17.9\u00a2 a kilowatt-hour". */
export function formatPrice(value: number, spec: PriceItemSpec): string {
  if (spec.cents) return `${(value * 100).toFixed(1)}\u00a2 ${spec.per}`;
  return `$${value.toFixed(2)} ${spec.per}`;
}


function numberOf(row: ObservationRow | null | undefined): number | null {
  if (!row || row.value === null || row.value === undefined || row.value === "") return null;
  const value = Number(row.value);
  return Number.isFinite(value) ? value : null;
}

function monthOf(row: ObservationRow): string {
  return String(row.period_start || "").slice(0, 7);
}

export interface YearChange {
  percent: number | null;
  period: string;
  reason: string;
}

/**
 * An area's change over a year, from its own index: the newest published
 * month against the same month a year earlier. A month that is not
 * published, or a value withheld, is said, never filled in.
 */
export function changeOverYear(rows: readonly ObservationRow[]): YearChange {
  const dated = rows.filter((row) => /^\d{4}-\d{2}/.test(String(row.period_start || "")));
  const newest = [...dated].sort((left, right) => monthOf(right).localeCompare(monthOf(left)))[0];
  if (!newest) return { percent: null, period: "", reason: "No month is published for this area." };
  const month = monthOf(newest);
  const now = numberOf(newest);
  if (now === null) return { percent: null, period: month, reason: withheldReason(newest) };
  const earlierMonth = `${Number(month.slice(0, 4)) - 1}${month.slice(4)}`;
  const earlier = dated.find((row) => monthOf(row) === earlierMonth);
  const then = numberOf(earlier);
  if (then === null || then === 0) {
    return { percent: null, period: month, reason: `${earlierMonth} is not published for this area, so no change over the year can be read.` };
  }
  return { percent: Math.round((now / then - 1) * 1000) / 10, period: `${earlierMonth} to ${month}`, reason: "" };
}

/**
 * The month a price series last carried a value, when it is more than a
 * year before the newest month the series publishes (rows without a value
 * after it): BLS stopped pricing the item there. Otherwise "".
 */
export function stoppedSince(rows: readonly ObservationRow[], newestValued: ObservationRow): string {
  const valued = monthOf(newestValued);
  const latest = rows.map(monthOf).filter((month) => /^\d{4}-\d{2}$/.test(month)).sort().at(-1) || valued;
  if (!/^\d{4}-\d{2}$/.test(valued)) return "";
  const gap = (Number(latest.slice(0, 4)) - Number(valued.slice(0, 4))) * 12 + Number(latest.slice(5, 7)) - Number(valued.slice(5, 7));
  return gap > 12 ? valued : "";
}

export function withheldReason(row: ObservationRow | null | undefined): string {
  const status = String(row?.value_status || "").trim();
  return status && status !== "valid" ? `Published without a value (${status}).` : "Published without a value.";
}

/** "Midwest Region figure" when the area is not the page's own place. */
export function areaLabel(area: Area, placeGeoId: string): string {
  return area.geoId === placeGeoId ? area.name : `${area.name} figure`;
}

export function formatGas(value: number): string {
  return `$${value.toFixed(2)} a gallon`;
}

export function formatChange(percent: number): string {
  if (percent === 0) return "no change over the year";
  return `${percent > 0 ? "up" : "down"} ${Math.abs(percent).toFixed(1)}% over the year`;
}

export function formatParity(value: number): string {
  return `${value.toFixed(1)} (nation = 100)`;
}

// ---------------------------------------------------------------------------
// Minutes of work (cost-of-living-section CL-3; ADR-0009 amendment of
// 2026-11-06). The API divides each price by the hourly wage of the page's
// own county, state or nation; this module only names the pairing and words
// the answer. QCEW publishes no wage for a metro area or a city.
// ---------------------------------------------------------------------------

/** The page levels QCEW publishes an average weekly wage for. */
export const WAGE_LEVELS: readonly PlaceLevel[] = ["COUNTY", "STATE", "NATIONAL"];

export const NO_METRO_WAGE =
  "Minutes of work needs the area's own wage, and QCEW publishes wages for counties, states and the nation, not metro areas.";

/** The served pairing for one price reading: `minutes_of_work_eggs:0200`. */
export function minutesRatioId(reading: Pick<Reading, "measure" | "metricCode">): string | null {
  const match = /^BLS:APU([0-9A-Z]{4})/.exec(reading.metricCode);
  return match && PRICE_ITEMS.some((spec) => spec.item === reading.measure) ? `minutes_of_work_${reading.measure}:${match[1]}` : null;
}

/** "6 minutes", "1 minute", "20 seconds", "1 hour 5 minutes". */
export function formatMinutes(minutes: number): string {
  if (!Number.isFinite(minutes) || minutes < 0) return "";
  if (minutes < 1) {
    const seconds = Math.max(1, Math.round(minutes * 60));
    return `${seconds} ${seconds === 1 ? "second" : "seconds"}`;
  }
  const whole = Math.round(minutes);
  const hours = Math.floor(whole / 60);
  const rest = whole % 60;
  const part = (count: number, word: string) => `${count} ${word}${count === 1 ? "" : "s"}`;
  if (!hours) return part(rest, "minute");
  return rest ? `${part(hours, "hour")} ${part(rest, "minute")}` : part(hours, "hour");
}

export interface MinutesRow {
  value?: string | null;
  unavailable_reason?: string | null;
  wage_geo_id?: string | null;
  wage_geo_name?: string | null;
  wage_year?: "same_year" | "newest_published_year" | null;
  denominator?: { value?: string | null; period_start?: string | null } | null;
}

/** The wage behind every minutes figure, stated once: "Dane County's average wage, $1,200 a week in 2025". */
export function wageText(row: MinutesRow, placeName: string): string {
  const weekly = Number(row.denominator?.value);
  const year = String(row.denominator?.period_start || "").slice(0, 4);
  if (!row.denominator || !Number.isFinite(weekly) || !/^\d{4}$/.test(year)) return "";
  const name = row.wage_geo_name || placeName;
  const newest = row.wage_year === "newest_published_year" ? ", the newest year published" : "";
  return `${name}'s average wage, $${formatNumber(Math.round(weekly), { maximumFractionDigits: 0 })} a week in ${year}${newest}, over a 40-hour week. Source: ${SOURCE_NAMES.BLS}, QCEW, all jobs.`;
}

/** A minutes row's cell text, or why there is none. Never zero for missing. */
export function minutesText(row: MinutesRow | null | undefined): string {
  if (!row) return "";
  const value = row.value === null || row.value === undefined || row.value === "" ? NaN : Number(row.value);
  if (!row.unavailable_reason && Number.isFinite(value)) return formatMinutes(value);
  if (row.unavailable_reason === "denominator_missing") return "No wage loaded for this place yet.";
  if (row.unavailable_reason === "different_period") return "No wage published for this price's year.";
  return "Not computed: an input is withheld.";
}
