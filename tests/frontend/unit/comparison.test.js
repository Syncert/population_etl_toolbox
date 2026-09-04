import { describe, expect, test } from "vitest";

// Covers: WEB-019 — the comparison workspace presents the API's own
// compatibility verdict and never substitutes its own. A pair the declared
// policy blocks is explained and never queried; an unverifiable rule is a
// caution rather than a rejection; each side's published value, period, and
// identity survive into the table and the export; and every API-computed
// field is labelled derived wherever it appears.

import {
  DEFAULT_COMPARISON_SELECTION,
  comparisonCells,
  comparisonColumns,
  comparisonExport,
  comparisonRequestParams,
  comparisonRowName,
  comparisonValueText,
  compatibilityState,
  describePreflight,
  incompatibleAlternatives,
  isDerivedField,
  mayRequestComparison,
  periodsDiffer,
  preflightRequestParams,
  selectionIsComplete,
} from "../../../apps/web/lib/comparison";

// Shaped exactly like the served ComparisonPreflightResponse.
const comparablePreflight = {
  metric_code_a: "ACS:acs5:B01003_001",
  metric_code_b: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  comparable: true,
  derivations: ["difference", "ratio"],
  rules: [
    {
      rule: "source_analysis_ready",
      status: "pass",
      reason: "measure A is served by source 'CENSUS_ACS'",
    },
    {
      rule: "units",
      status: "unknown",
      reason: "Census ACS publishes no units for measure A",
    },
    { rule: "time_grains", status: "pass", reason: "both publish ANNUAL" },
    { rule: "geo_grains", status: "pass", reason: "both publish COUNTY" },
  ],
  caveats: ["units could not be verified"],
};

const blockedPreflight = {
  metric_code_a: "CDC:cdc_places_county:OBESITY",
  metric_code_b: "ACS:acs5:B01003_001",
  source_code_a: "CDC",
  source_code_b: "CENSUS_ACS",
  comparable: false,
  derivations: [],
  rules: [
    {
      rule: "source_analysis_ready",
      status: "fail",
      reason:
        "measure A: source 'CDC' publishes stratified observations an aligned analysis would collapse",
    },
    { rule: "units", status: "fail", reason: "percent cannot be compared with people" },
    { rule: "time_grains", status: "pass", reason: "both publish ANNUAL" },
  ],
  caveats: [],
};

// Shaped exactly like the served ComparisonResponse.
const comparison = {
  metric_code_a: "ACS:acs5:B01003_001",
  metric_code_b: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  units_a: null,
  units_b: "people",
  derivations: ["difference", "ratio"],
  caveats: ["units could not be verified"],
  total: 2,
  limit: 1000,
  offset: 0,
  items: [
    {
      geo_id: "state:55|county:025",
      geo_level: "COUNTY",
      state_name: "Wisconsin",
      county_name: "Dane County",
      metric_code_a: "ACS:acs5:B01003_001",
      metric_code_b: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      period_a: "2023",
      period_b: "2024",
      value_a: 561504,
      value_b: 568203,
      difference: -6699,
      ratio: 0.98821,
    },
    {
      geo_id: "state:55|county:001",
      geo_level: "COUNTY",
      state_name: "Wisconsin",
      county_name: "Adams County",
      period_a: "2023",
      period_b: "2023",
      value_a: null,
      value_b: 20567,
      difference: null,
      ratio: null,
    },
  ],
};

describe("the API owns the compatibility verdict", () => {
  test("the verdict is read, never inferred from the rule list", () => {
    const model = describePreflight(comparablePreflight);
    expect(model.comparable).toBe(true);
    expect(model.blocking).toEqual([]);
    expect(model.unverified.map((rule) => rule.rule)).toEqual(["units"]);
    expect(model.passed.map((rule) => rule.rule)).toEqual([
      "source_analysis_ready",
      "time_grains",
      "geo_grains",
    ]);
    expect(model.derivations).toEqual(["difference", "ratio"]);

    // A response claiming comparability with no rules this client recognises
    // is still comparable: the API decides, not a rule allowlist here.
    expect(describePreflight({ comparable: true, rules: [{ rule: "future_rule", status: "pass", reason: "" }] })
      .comparable).toBe(true);
    // And a response the client never received decides nothing.
    expect(describePreflight(null).comparable).toBe(false);
    expect(describePreflight(undefined).blocking).toEqual([]);
  });

  test("an unverified rule is a caution, not a rejection", () => {
    // Where a source publishes nothing to check, the comparison is served
    // and the unverified rule travels as a caveat.
    expect(compatibilityState(comparablePreflight)).toEqual({
      state: "warn",
      message: "comparable; 1 rule could not be verified",
    });
    expect(mayRequestComparison(comparablePreflight)).toBe(true);

    const allVerified = {
      ...comparablePreflight,
      rules: comparablePreflight.rules.map((rule) => ({ ...rule, status: "pass" })),
    };
    expect(compatibilityState(allVerified)).toEqual({
      state: "ok",
      message: "comparable; every declared rule passed",
    });
  });

  test("a blocked pair reads as a failure-shaped state and is never queried", () => {
    expect(compatibilityState(blockedPreflight)).toEqual({
      state: "incompatible",
      message: "not comparable: 2 declared rules failed",
    });
    // /comparison answers an incompatible pair with a 422, so asking anyway
    // would turn a stated explanation into a request failure — and would
    // move data for a pair the policy rejected.
    expect(mayRequestComparison(blockedPreflight)).toBe(false);
    expect(mayRequestComparison(null)).toBe(false);
    expect(compatibilityState(null)).toEqual({
      state: "idle",
      message: "select two measures",
    });
  });

  test("a blocked pair gets actionable alternatives from its failed rules", () => {
    const alternatives = incompatibleAlternatives(blockedPreflight);
    expect(alternatives[0]).toContain("Explore each measure on its own");
    expect(alternatives.join(" ")).toContain("stratified");
    expect(alternatives.join(" ")).toContain("same unit");
    // Nothing here proposes a weakened comparison or one the policy declined.
    expect(alternatives.join(" ")).not.toContain("anyway");
    expect(incompatibleAlternatives(comparablePreflight)).toEqual([]);
  });
});

describe("comparison requests carry only declared parameters", () => {
  test("preflight names both measures", () => {
    expect(
      preflightRequestParams({
        a: { sourceCode: "CENSUS_ACS", metricCode: "ACS:acs5:B01003_001" },
        b: { sourceCode: "CENSUS_PEP", metricCode: "PEP:X" },
        geoLevel: "COUNTY",
        stateFips: "55",
      }),
    ).toEqual({
      metric_code_a: "ACS:acs5:B01003_001",
      metric_code_b: "PEP:X",
    });
  });

  test("state scope is dropped at the national grain", () => {
    const selection = {
      a: { sourceCode: "CENSUS_ACS", metricCode: "A" },
      b: { sourceCode: "CENSUS_PEP", metricCode: "B" },
      geoLevel: "COUNTY",
      stateFips: "55",
    };
    expect(comparisonRequestParams(selection, 1000)).toEqual({
      metric_code_a: "A",
      metric_code_b: "B",
      geo_level: "COUNTY",
      state_fips: "55",
      limit: "1000",
    });
    // Scoping a national selection to one state would contradict it.
    expect(
      comparisonRequestParams({ ...selection, geoLevel: "NATIONAL" }, 100).state_fips,
    ).toBeUndefined();
  });

  test("a selection is incomplete until both sides name a measure", () => {
    expect(selectionIsComplete(DEFAULT_COMPARISON_SELECTION)).toBe(false);
    expect(
      selectionIsComplete({ ...DEFAULT_COMPARISON_SELECTION, a: { sourceCode: "S", metricCode: "A" } }),
    ).toBe(false);
    expect(
      selectionIsComplete({
        a: { sourceCode: "S", metricCode: "A" },
        b: { sourceCode: "T", metricCode: "B" },
        geoLevel: "COUNTY",
        stateFips: "",
      }),
    ).toBe(true);
  });
});

describe("published inputs and derived values stay distinct", () => {
  test("columns preserve each side's identity and mark derived fields", () => {
    const columns = comparisonColumns(comparison);
    expect(columns.map((column) => column.key)).toEqual([
      "geography",
      "value_a",
      "period_a",
      "value_b",
      "period_b",
      "difference",
      "ratio",
    ]);
    // Each side's column is headed by its own metric code, so the two
    // published inputs can never be read as one measure.
    expect(columns[1].label).toBe("ACS:acs5:B01003_001");
    expect(columns[3].label).toBe("CENSUS_PEP:pep_cty_alldata:POPESTIMATE");
    expect(columns.filter((column) => column.derived).map((column) => column.key)).toEqual([
      "difference",
      "ratio",
    ]);

    // A derivation this client has never heard of is labelled, not dropped.
    const future = comparisonColumns({ ...comparison, derivations: ["difference", "z_score"] });
    expect(future.at(-1)).toEqual({ key: "z_score", label: "z_score", derived: true });
    expect(isDerivedField(comparison, "ratio")).toBe(true);
    expect(isDerivedField(comparison, "value_a")).toBe(false);
  });

  test("a side that published nothing is never rendered as zero", () => {
    const cells = comparisonCells(comparison, comparison.items[1]);
    expect(cells.value_a).toBe("Not published");
    expect(cells.value_b).toBe("20,567");
    // The derived fields the API could not compute are equally explicit.
    expect(cells.difference).toBe("Not published");
    expect(cells.ratio).toBe("Not published");
    expect(comparisonValueText(0)).toBe("0");
    expect(comparisonValueText(null)).toBe("Not published");
    expect(comparisonValueText(undefined)).toBe("Not published");
  });

  test("differing as-of periods are visible on the row that has them", () => {
    // The API combines each side's own newest value rather than aligning
    // them, so the pair is not contemporaneous and must not read as if it is.
    expect(periodsDiffer(comparison.items[0])).toBe(true);
    expect(periodsDiffer(comparison.items[1])).toBe(false);
    expect(periodsDiffer({ period_a: "2023" })).toBe(false);
    const cells = comparisonCells(comparison, comparison.items[0]);
    expect(cells.period_a).toBe("2023");
    expect(cells.period_b).toBe("2024");
  });

  test("geography names come from the row's own published attribution", () => {
    expect(comparisonRowName(comparison.items[0])).toBe("Dane County, Wisconsin");
    expect(comparisonRowName({ state_name: "Wisconsin" })).toBe("Wisconsin");
    expect(comparisonRowName({ geo_id: "state:55" })).toBe("state:55");
    expect(comparisonRowName(null)).toBe("");
  });
});

describe("the export carries its own interpretation envelope", () => {
  test("both identities, units, periods, derived markers, and caveats travel", () => {
    const exported = comparisonExport(comparison, comparablePreflight);
    expect(exported.headings).toEqual([
      "geo_id",
      "geo_name",
      "geo_level",
      "metric_code_a",
      "source_code_a",
      "units_a",
      "period_a",
      "value_a",
      "metric_code_b",
      "source_code_b",
      "units_b",
      "period_b",
      "value_b",
      "difference (API-derived)",
      "ratio (API-derived)",
      "caveats",
    ]);
    expect(exported.filename).toBe(
      "comparison-ACS-acs5-B01003_001-vs-CENSUS_PEP-pep_cty_alldata-POPESTIMATE.csv",
    );

    const [first, second] = exported.rows;
    expect(first[1]).toBe("Dane County, Wisconsin");
    expect(first[6]).toBe("2023");
    expect(first[11]).toBe("2024");
    expect(first[13]).toBe("-6699");
    // A value the source did not publish exports as empty, never as zero.
    expect(second[7]).toBe("");
    expect(second[13]).toBe("");
    // The response's caveats and the unverified rules both travel.
    expect(first.at(-1)).toContain("units could not be verified");
    expect(first.at(-1)).toContain("unverified units");
  });

  test("an absent response exports nothing rather than an invented file", () => {
    const exported = comparisonExport(null, null);
    expect(exported.rows).toEqual([]);
    expect(exported.headings).toContain("caveats");
  });
});
