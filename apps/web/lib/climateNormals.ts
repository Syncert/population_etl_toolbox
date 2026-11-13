// Climate averages on a county page (place-climate-normals): NOAA's U.S.
// Climate Normals 1991-2020, monthly, for the county.
//
// NCEI publishes normals for weather stations, not places. The warehouse's
// county figure is the unweighted mean of the standard or representative
// stations it places in the county (gold_noaa_normals.observation_revision),
// so a county page shows its own twelve months and every other page says
// where the figures are; no state, metro or city figure is made from
// counties. A month with no row is "not published", never zero, and the
// months are never added or averaged into a year here.

import type { ObservationRow } from "./explorerViewModel";

export const NORMALS_SOURCE = "NOAA_NORMALS";
export const NORMALS_CREDIT = "NOAA National Centers for Environmental Information, U.S. Climate Normals 1991-2020";

export type NormalsMeasure = "mean" | "high" | "low" | "precipitation";

/** The four monthly normals the card reads, in the table's column order. */
/** `short` is the column title on a phone, where the full one would break mid-word. */
export const NORMALS_MEASURES: readonly { key: NormalsMeasure; code: string; title: string; short: string }[] = [
  { key: "mean", code: `${NORMALS_SOURCE}:monthly_mean_temperature`, title: "Mean temperature", short: "Mean" },
  { key: "high", code: `${NORMALS_SOURCE}:monthly_mean_maximum_temperature`, title: "Mean daily high", short: "High" },
  { key: "low", code: `${NORMALS_SOURCE}:monthly_mean_minimum_temperature`, title: "Mean daily low", short: "Low" },
  { key: "precipitation", code: `${NORMALS_SOURCE}:monthly_precipitation`, title: "Precipitation", short: "Precip." },
];
import { NORMALS_CODES } from "./climateCodes";
export { NORMALS_CODES };

export const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
] as const;

export const NOT_A_COUNTY =
  "NOAA publishes climate normals for weather stations; this site shows them for counties, from the stations in each county.";

/** Which normals to read, from the place page's one catalog read (as childcare's). */
export function catalogedNormals(catalogued: ReadonlyMap<string, unknown> | null | undefined): readonly string[] | null {
  if (catalogued === undefined) return NORMALS_CODES;
  if (catalogued === null) return null;
  return NORMALS_CODES.filter((code) => catalogued.has(code));
}

export interface NormalsMonth {
  month: number;
  name: string;
  /** The stored value text for each measure, or null when no row. */
  values: Record<NormalsMeasure, string | null>;
}

export interface NormalsTable {
  months: NormalsMonth[];
  /** Fewest and most stations behind any shown temperature / precipitation value. */
  temperatureStations: [number, number] | null;
  precipitationStations: [number, number] | null;
  /** Each distinct placement note the rows carry (stations placed by the nearest-county rule). */
  placementNotes: string[];
  /** The lowest low and highest high shown, for the range bars' one scale. */
  scale: [number, number] | null;
  /** The largest precipitation shown, for the precipitation bars' scale. */
  maxPrecipitation: number;
  shown: number;
}

function monthOf(row: ObservationRow): number | null {
  const dimensions = (row.dimensions || {}) as Record<string, unknown>;
  const fromDimension = Number(dimensions.month);
  if (Number.isInteger(fromDimension) && fromDimension >= 1 && fromDimension <= 12) return fromDimension;
  const match = /^\d{4}-(\d{2})/.exec(String(row.period_start || ""));
  return match ? Number(match[1]) : null;
}

function valueText(row: ObservationRow): string | null {
  if (row.value === null || row.value === undefined || row.value === "") return null;
  if (row.value_status && row.value_status !== "valid") return null;
  return String(row.value);
}

function stationRange(counts: number[]): [number, number] | null {
  return counts.length ? [Math.min(...counts), Math.max(...counts)] : null;
}

/** Twelve months of the four normals, from each measure's rows for one county. */
export function normalsTable(rowsByCode: Readonly<Record<string, readonly ObservationRow[]>>, geoId: string): NormalsTable {
  const months: NormalsMonth[] = MONTHS.map((name, index) => ({
    month: index + 1,
    name,
    values: { mean: null, high: null, low: null, precipitation: null },
  }));
  const temperatureCounts: number[] = [];
  const precipitationCounts: number[] = [];
  const notes = new Set<string>();
  for (const measure of NORMALS_MEASURES) {
    for (const row of rowsByCode[measure.code] || []) {
      if (row.geo_id !== geoId) continue;
      const month = monthOf(row);
      const value = valueText(row);
      if (month === null || value === null) continue;
      months[month - 1]!.values[measure.key] = value;
      const dimensions = (row.dimensions || {}) as Record<string, unknown>;
      const stations = Number(dimensions.station_count);
      if (Number.isFinite(stations) && stations > 0) {
        (measure.key === "precipitation" ? precipitationCounts : temperatureCounts).push(stations);
      }
      const note = dimensions.placement_note;
      if (typeof note === "string" && note.trim()) notes.add(note.trim());
    }
  }
  const lows = months.map((month) => month.values.low).filter((value): value is string => value !== null).map(Number);
  const highs = months.map((month) => month.values.high).filter((value): value is string => value !== null).map(Number);
  const temps = [...lows, ...highs, ...months.map((month) => month.values.mean).filter((value): value is string => value !== null).map(Number)];
  const precipitation = months.map((month) => month.values.precipitation).filter((value): value is string => value !== null).map(Number);
  return {
    months,
    temperatureStations: stationRange(temperatureCounts),
    precipitationStations: stationRange(precipitationCounts),
    placementNotes: [...notes].sort(),
    scale: temps.length ? [Math.min(...temps), Math.max(...temps)] : null,
    maxPrecipitation: precipitation.length ? Math.max(...precipitation) : 0,
    shown: months.filter((month) => Object.values(month.values).some((value) => value !== null)).length,
  };
}

/** NCEI's own precision for a station normal: 0.1 F and 0.01 in. */
export const NORMALS_DECIMALS: Readonly<Record<NormalsMeasure, number>> = { mean: 1, high: 1, low: 1, precipitation: 2 };

/**
 * A county figure at the precision NCEI publishes its station normals:
 * temperature to 0.1 F, precipitation to 0.01 in, always with that many
 * decimals ("57.0°F", "4.40 in"). The county figure is this warehouse's
 * unweighted mean of station normals, stored to two decimals; its second
 * temperature decimal comes from the averaging, not from NCEI, so it is
 * rounded (half away from zero) to the stations' own precision.
 */
export function formatNormal(value: string, measure: NormalsMeasure): string {
  const decimals = NORMALS_DECIMALS[measure];
  const scale = 10 ** decimals;
  const number = Number(value);
  // The epsilon keeps a stored half (18.65) from flooring through binary error.
  const rounded = (Math.sign(number) * Math.round(Math.abs(number) * scale + 1e-9)) / scale;
  const text = (rounded === 0 ? 0 : rounded).toFixed(decimals);
  return measure === "precipitation" ? `${text} in` : `${text}°F`;
}

/** The months with the highest and lowest mean temperature and the most precipitation, ties named together. */
export function normalsHeadline(table: NormalsTable, place: string): string {
  const extreme = (key: NormalsMeasure, pick: (values: number[]) => number) => {
    const shown = table.months.filter((month) => month.values[key] !== null);
    if (!shown.length) return null;
    const target = pick(shown.map((month) => Number(month.values[key])));
    const named = shown.filter((month) => Number(month.values[key]) === target);
    return { names: named.map((month) => month.name).join(" and "), value: named[0]!.values[key]! };
  };
  const warm = extreme("mean", (values) => Math.max(...values));
  const cold = extreme("mean", (values) => Math.min(...values));
  const wet = extreme("precipitation", (values) => Math.max(...values));
  const parts: string[] = [];
  if (warm && cold) {
    parts.push(
      `In ${place}, the month with the highest mean temperature is ${warm.names} (${formatNormal(warm.value, "mean")}) and the lowest is ${cold.names} (${formatNormal(cold.value, "mean")})`,
    );
  }
  if (wet) {
    parts.push(`${parts.length ? "the" : `In ${place}, the`} month with the most precipitation is ${wet.names} (${formatNormal(wet.value, "precipitation")})`);
  }
  return parts.length ? `${parts.join("; ")}.` : "";
}

/** "3 stations" or "2 to 4 stations". */
export function stationText([fewest, most]: [number, number]): string {
  if (fewest === most) return `${fewest} station${fewest === 1 ? "" : "s"}`;
  return `${fewest} to ${most} stations`;
}
