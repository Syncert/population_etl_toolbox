import { describe, expect, it } from "vitest";

import {
  NORMALS_CODES,
  catalogedNormals,
  formatNormal,
  normalsHeadline,
  normalsTable,
  stationText,
} from "../../../apps/web/lib/climateNormals";

// Covers: WEB-175 — place-climate-normals: a county's monthly NOAA normals
// are laid out by month as stored, a month with no row is missing (never
// zero), station counts and placement notes come from the rows, and no
// annual figure is made from the months.

const GEO = "state:55|county:025";
const row = (code, month, value, dimensions = {}) => ({
  metric_code: code, geo_id: GEO, value, value_status: "valid",
  period_start: `1991-${String(month).padStart(2, "0")}-01`,
  dimensions: { month: String(month), station_count: "4", ...dimensions },
});
const [MEAN, HIGH, LOW, PRCP] = NORMALS_CODES;

describe("climate normals", () => {
  it("names the four monthly normals and reads only the catalogued ones", () => {
    expect(NORMALS_CODES).toEqual([
      "NOAA_NORMALS:monthly_mean_temperature",
      "NOAA_NORMALS:monthly_mean_maximum_temperature",
      "NOAA_NORMALS:monthly_mean_minimum_temperature",
      "NOAA_NORMALS:monthly_precipitation",
    ]);
    expect(catalogedNormals(null)).toBeNull();
    expect(catalogedNormals(undefined)).toEqual(NORMALS_CODES);
    expect(catalogedNormals(new Map([[PRCP, {}]]))).toEqual([PRCP]);
  });

  it("keeps the stored value at the source's precision", () => {
    expect(formatNormal("18.60", "mean")).toBe("18.6°F");
    expect(formatNormal("18.63", "mean")).toBe("18.63°F");
    expect(formatNormal("-3.00", "low")).toBe("-3.0°F");
    expect(formatNormal("4.40", "precipitation")).toBe("4.40 in");
    expect(formatNormal("1.5", "precipitation")).toBe("1.50 in");
  });

  it("lays out twelve months, leaving a month with no row missing", () => {
    const table = normalsTable({
      [MEAN]: [row(MEAN, 1, "18.60", { station_count: "3" }), row(MEAN, 7, "71.50")],
      [HIGH]: [row(HIGH, 1, "27.30"), row(HIGH, 7, "82.20")],
      [LOW]: [row(LOW, 1, "9.90"), row(LOW, 7, "60.60")],
      [PRCP]: [row(PRCP, 1, "1.56", { station_count: "6", placement_note: "Includes stations ... (212 m)." }), { ...row(PRCP, 7, null), value_status: "missing" }],
      other: [{ ...row(MEAN, 2, "99"), geo_id: "state:55" }],
    }, GEO);
    expect(table.months).toHaveLength(12);
    expect(table.months[0].values).toEqual({ mean: "18.60", high: "27.30", low: "9.90", precipitation: "1.56" });
    expect(table.months[6].values.precipitation).toBeNull();
    expect(table.months[1].values.mean).toBeNull();
    expect(table.shown).toBe(2);
    expect(table.temperatureStations).toEqual([3, 4]);
    expect(table.precipitationStations).toEqual([6, 6]);
    expect(table.placementNotes).toEqual(["Includes stations ... (212 m)."]);
    expect(table.scale).toEqual([9.9, 82.2]);
    expect(normalsHeadline(table, "Dane County")).toBe(
      "In Dane County, the month with the highest mean temperature is July (71.5°F) and the lowest is January (18.6°F); the month with the most precipitation is January (1.56 in).",
    );
    expect(stationText([3, 4])).toBe("3 to 4 stations");
    expect(stationText([1, 1])).toBe("1 station");
  });

  it("says nothing when no month is published", () => {
    const table = normalsTable({}, GEO);
    expect(table.shown).toBe(0);
    expect(table.scale).toBeNull();
    expect(normalsHeadline(table, "X County")).toBe("");
  });
});
