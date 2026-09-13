import { describe, expect, test } from "vitest";

// Covers: WEB-051, WEB-053 — what the observation export actually carries.
//
// Both rows are about the same rule: every field `ObservationCoverage` and
// `ObservationUncertainty` publish travels, whether or not a given source
// publishes it, because "a file that carried a subset would be this client
// deciding which part of a source's participation basis a reader may have".
// Neither had a node asserting the file's contents -- the only export
// coverage in the suite was that the button is enabled -- which is how the
// same `margin_of_error`-only defect survived in the profile product until
// WEB-060. The export is a pure function now, so the columns are asserted.

import {
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_FIELDS,
} from "../../../apps/web/lib/observationAccess";
import { observationExport } from "../../../apps/web/lib/observationExport";

// The shapes these fields actually arrive in: the neutral resource nests
// them, and the source-scoped shapes carry the margin at the top level with
// no envelope at all.
const nestedRow = {
  geo_id: "state:55|county:025",
  county_name: "Dane County",
  state_name: "Wisconsin",
  metric_code: "USDA_NASS:corn:YIELD",
  source_code: "USDA_NASS",
  value: "181.4",
  value_status: "valid",
  unit: "bu / acre",
  period_start: "2024-01-01",
  period_end: "2024-12-31",
  release: "2025-01-10",
  as_of: "2025-01-10",
  uncertainty: { cv_value: "14.7", cv_status: "unreliable", cv_symbol: "(D)" },
  coverage: {
    participation_status: "did not report",
    participated_population: "0",
    population: "5900000",
  },
  dimensions: { domain_desc: "TOTAL", domaincat_desc: "NOT SPECIFIED" },
};

const flatRow = {
  geo_id: "state:55",
  state_name: "Wisconsin",
  metric_code: "CENSUS_ACS:acs5:B01003_001",
  source: "CENSUS_ACS",
  value: "5900000",
  value_status: "valid",
  units: "people",
  period: "2023",
  margin_of_error: "1200",
  margin_of_error_pct: "0.02",
};

const cell = (headings, row, name) => row[headings.indexOf(name)];

describe("the observation export carries every published qualifier", () => {
  test("every uncertainty and coverage field is a column, published or not", () => {
    const { headings } = observationExport([flatRow], { scope: "latest" });
    for (const field of OBSERVATION_UNCERTAINTY_FIELDS) {
      expect(headings).toContain(field);
    }
    for (const field of OBSERVATION_COVERAGE_FIELDS) {
      expect(headings).toContain(field);
    }
  });

  test("a nested envelope is read, not skipped", () => {
    const { headings, rows } = observationExport([nestedRow], { scope: "latest" });
    expect(cell(headings, rows[0], "cv_value")).toBe("14.7");
    expect(cell(headings, rows[0], "cv_status")).toBe("unreliable");
    // The flag NASS publishes precisely to say an estimate is unreliable.
    expect(cell(headings, rows[0], "cv_symbol")).toBe("(D)");
    expect(cell(headings, rows[0], "participation_status")).toBe("did not report");
    expect(cell(headings, rows[0], "population")).toBe("5900000");
    // A participated population of zero is a published fact, not an absence.
    expect(cell(headings, rows[0], "participated_population")).toBe("0");
    // This source publishes no margin; the column is empty, never a zero.
    expect(cell(headings, rows[0], "margin_of_error")).toBe("");
  });

  test("a top-level margin is read for a source with no envelope", () => {
    const { headings, rows } = observationExport([flatRow], { scope: "latest" });
    expect(cell(headings, rows[0], "margin_of_error")).toBe("1200");
    expect(cell(headings, rows[0], "margin_of_error_pct")).toBe("0.02");
    expect(cell(headings, rows[0], "confidence_lower")).toBe("");
  });

  test("the read's own scope and each row's release identity travel", () => {
    const { headings, rows } = observationExport([nestedRow], { scope: "as_released" });
    expect(cell(headings, rows[0], "scope")).toBe("as_released");
    expect(cell(headings, rows[0], "release")).toBe("2025-01-10");
    expect(cell(headings, rows[0], "as_of")).toBe("2025-01-10");
  });

  test("a declared dimension becomes a column under the source's own name", () => {
    const { headings, rows } = observationExport([nestedRow], {
      scope: "latest",
      dimensionFilters: ["domain_desc", "domaincat_desc"],
    });
    expect(headings.at(-2)).toBe("domain_desc");
    expect(headings.at(-1)).toBe("domaincat_desc");
    expect(cell(headings, rows[0], "domain_desc")).toBe("TOTAL");
    expect(cell(headings, rows[0], "domaincat_desc")).toBe("NOT SPECIFIED");
  });

  test("a suppressed value stays absent and says why", () => {
    const { headings, rows } = observationExport(
      [{ ...flatRow, value: null, value_status: "suppressed" }],
      { scope: "latest" },
    );
    expect(cell(headings, rows[0], "value")).toBe(null);
    expect(cell(headings, rows[0], "value_status")).toBe("suppressed");
  });

  test("no rows is no rows, and the columns still stand", () => {
    const { headings, rows } = observationExport(null, { scope: "latest" });
    expect(rows).toEqual([]);
    expect(headings).toContain("cv_symbol");
  });
});
