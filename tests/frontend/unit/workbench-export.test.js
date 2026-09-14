import { describe, expect, test } from "vitest";

// Covers: WEB-098 — the workbench's CSV carries one row per plotted value
// with the full observation envelope, plus the four columns a composition
// needs: which series the row belongs to, its grain, and a `derived` flag
// that is true only for the API-computed coefficients. A read the page bound
// cut short names itself a prefix in the file name.

import {
  workbenchExport,
  workbenchExportFilename,
} from "../../../apps/web/lib/observationExport";
import { buildPlottedSeries } from "../../../apps/web/lib/workbench";
import {
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_FIELDS,
} from "../../../apps/web/lib/observationAccess";

function series(overrides = {}) {
  return {
    sourceKey: "census",
    sourceCode: "CENSUS_ACS",
    metricCode: "CENSUS_ACS:acs5:B01003_001",
    scope: "latest",
    geoLevel: "COUNTY",
    geoId: "county:06001",
    filters: {},
    ...overrides,
  };
}

const ACS_ROWS = [
  {
    geo_id: "county:06001",
    geo_level: "COUNTY",
    county_name: "Alameda",
    metric_code: "CENSUS_ACS:acs5:B01003_001",
    period_start: "2023",
    period_end: "2023",
    value: "1648556",
    value_status: null,
    unit: "People",
    source: "CENSUS_ACS",
    dataset: "acs5",
    release: "acs5:2023",
    as_of: "2024-01-05",
    uncertainty: { margin_of_error: "1200", margin_of_error_pct: "0.07" },
  },
  {
    geo_id: "county:06001",
    geo_level: "COUNTY",
    metric_code: "CENSUS_ACS:acs5:B01003_001",
    period_start: "2022",
    period_end: "2022",
    // A period the source published without a number: not a row in the file,
    // because it is not a plotted value.
    value: null,
    value_status: "suppressed",
    release: "acs5:2022",
  },
];

function plottedAcs(overrides = {}) {
  return buildPlottedSeries({
    series: series(),
    rows: ACS_ROWS,
    label: "Total population",
    unit: "People",
    ...overrides,
  });
}

describe("what the workbench's file carries", () => {
  test("the full observation envelope, plus the composition's own columns", () => {
    const { headings } = workbenchExport([plottedAcs()]);
    for (const field of [
      ...OBSERVATION_UNCERTAINTY_FIELDS,
      ...OBSERVATION_COVERAGE_FIELDS,
    ]) {
      expect(headings).toContain(field);
    }
    for (const own of ["series", "geo_level", "derived", "scope", "release", "as_of"]) {
      expect(headings).toContain(own);
    }
  });

  test("one row per plotted value, and none for an unpublished period", () => {
    const { rows } = workbenchExport([plottedAcs()]);
    expect(rows).toHaveLength(1);
  });

  test("a row carries the published value, never the parsed number", () => {
    const { headings, rows } = workbenchExport([plottedAcs()]);
    expect(rows[0][headings.indexOf("value")]).toBe("1648556");
  });

  test("a published uncertainty travels", () => {
    const { headings, rows } = workbenchExport([plottedAcs()]);
    expect(rows[0][headings.indexOf("margin_of_error")]).toBe("1200");
  });

  test("the series column separates several measures in one file", () => {
    const other = buildPlottedSeries({
      series: series({
        sourceCode: "FRED",
        metricCode: "FRED:UNRATE",
        geoLevel: "NATIONAL",
        geoId: "NATIONAL",
      }),
      rows: [{ geo_id: "NATIONAL", period_start: "2023", value: "3.6" }],
      label: "Unemployment rate",
      unit: "Percent",
    });
    const { headings, rows } = workbenchExport([plottedAcs(), other]);
    const labels = rows.map((row) => row[headings.indexOf("series")]);
    expect(labels[0]).toContain("CENSUS_ACS");
    expect(labels[1]).toContain("FRED");
    expect(new Set(labels).size).toBe(2);
  });

  test("the grain rides every row, because a composition mixes them", () => {
    const { headings, rows } = workbenchExport([plottedAcs()]);
    expect(rows[0][headings.indexOf("geo_level")]).toBe("COUNTY");
  });

  test("a published value is never marked derived", () => {
    const { headings, rows } = workbenchExport([plottedAcs()]);
    expect(rows[0][headings.indexOf("derived")]).toBe(false);
  });
});

describe("the coefficients in the file", () => {
  const correlation = {
    metricCodeA: "A",
    metricCodeB: "B",
    readings: [
      { label: "Paired geographies", value: "48", derived: false },
      { label: "Pearson r", value: "0.412", derived: true },
      { label: "Spearman ρ", value: "0.377", derived: true },
    ],
  };

  test("are marked derived, and only they are", () => {
    const { headings, rows } = workbenchExport([plottedAcs()], { correlation });
    const derived = headings.indexOf("derived");
    expect(rows[0][derived]).toBe(false);
    const coefficients = rows.filter((row) => row[derived] === true);
    expect(coefficients).toHaveLength(2);
  });

  test("come after every published value, so sorting keeps the file usable", () => {
    const { headings, rows } = workbenchExport([plottedAcs()], { correlation });
    const series = headings.indexOf("series");
    expect(rows[0][series]).toContain("CENSUS_ACS");
    expect(rows[rows.length - 1][series]).toBe("A against B");
  });

  test("carry no period or release: a coefficient is not a publication", () => {
    const { headings, rows } = workbenchExport([plottedAcs()], { correlation });
    const last = rows[rows.length - 1];
    expect(last[headings.indexOf("period")]).toBe("");
    expect(last[headings.indexOf("release")]).toBe("");
  });

  test("are absent when no correlation was asked for", () => {
    const { headings, rows } = workbenchExport([plottedAcs()]);
    expect(rows.every((row) => row[headings.indexOf("derived")] === false)).toBe(
      true,
    );
  });
});

describe("the file's name", () => {
  test("names the presentation and the series count", () => {
    expect(
      workbenchExportFilename({
        presentation: "line",
        plotted: [plottedAcs(), plottedAcs()],
      }),
    ).toBe("workbench-line-2-series.csv");
  });

  test("says so when any series was cut short by the page bound", () => {
    const name = workbenchExportFilename({
      presentation: "line",
      plotted: [plottedAcs(), plottedAcs({ truncated: true })],
    });
    expect(name).toContain("partial");
    expect(name).toContain("1-truncated");
  });
});
