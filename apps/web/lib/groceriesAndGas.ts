// "Groceries and gas" on a place page (groceries-and-gas-cards).
//
// No source publishes a county's grocery or gas prices. Each figure on the
// card is the containing area's: the area is found through the reference's
// `part_of` rows (never by name), and the card names it beside the number.
// The metric identities below are built from those areas' Census codes,
// which is how BLS and BEA number them; nothing is matched by name.

import type { ObservationRow } from "./explorerViewModel";
import type { RelatedResponse } from "./placeRelationships";
import type { PlaceLevel } from "./placeChapters";

export interface Area {
  geoId: string;
  level: "NATIONAL" | "STATE" | "METRO" | "CENSUS_DIVISION" | "CENSUS_REGION";
  name: string;
}

export interface ContainingAreas {
  nation: Area | null;
  state: Area | null;
  metro: Area | null;
  division: Area | null;
  region: Area | null;
}

export type CostMeasure = "gas" | "food" | "parity";

/** One way to answer a card line: this metric, published for this area. */
export interface Reading {
  measure: CostMeasure;
  metricCode: string;
  area: Area;
  source: string;
}

export const SOURCE_NAMES: Record<string, string> = {
  EIA: "U.S. Energy Information Administration",
  BLS: "Bureau of Labor Statistics",
  BEA: "Bureau of Economic Analysis",
};

export const NO_LOCAL_PRICES =
  "No source publishes grocery or gas prices for a county. Each figure here is for the larger area named beside it.";

const LEVEL_WORDS: Record<Area["level"], string> = {
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
  return { gas, food, parity };
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
