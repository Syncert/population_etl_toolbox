import { describe, expect, test } from "vitest";

// Covers: WEB-019 and WEB-020 — the comparison workspace presents the API's own
// compatibility verdict and never substitutes its own. A pair the declared
// policy blocks is explained and never queried; an unverifiable rule is a
// caution rather than a rejection; each side's published value, period, and
// identity survive into the table and the export; and every API-computed
// field is labelled derived wherever it appears. WEB-020 adds the aligned
// presentations: a scatter of the two published inputs and a choropleth of
// one API-derived field, each offered only where the pair can answer it, and
// neither plotting nor colouring a geography whose side published nothing.

import {
  DEFAULT_COMPARISON_SELECTION,
  comparisonCells,
  comparisonColumns,
  comparisonExport,
  comparisonMapRows,
  comparisonRequestParams,
  comparisonRowName,
  comparisonScatterModel,
  comparisonValueText,
  compatibilityState,
  defaultDerivedField,
  describePreflight,
  incompatibleAlternatives,
  isDerivedField,
  mayRequestComparison,
  describeComparisonCoverage,
  mapPeriodMismatchNote,
  periodsDiffer,
  preflightRequestParams,
  selectionIsComplete,
} from "../../../apps/web/lib/comparison";

// Shaped exactly like the served ComparisonPreflightResponse.
const comparablePreflight = {
  metric_code_a: "CENSUS_ACS:acs5:B01003_001",
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
  metric_code_b: "CENSUS_ACS:acs5:B01003_001",
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
  metric_code_a: "CENSUS_ACS:acs5:B01003_001",
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
      metric_code_a: "CENSUS_ACS:acs5:B01003_001",
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
        a: { sourceCode: "CENSUS_ACS", metricCode: "CENSUS_ACS:acs5:B01003_001" },
        b: { sourceCode: "CENSUS_PEP", metricCode: "PEP:X" },
        geoLevel: "COUNTY",
        stateFips: "55",
      }),
    ).toEqual({
      metric_code_a: "CENSUS_ACS:acs5:B01003_001",
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
    expect(columns[1].label).toBe("CENSUS_ACS:acs5:B01003_001");
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
    // The name a complete read keeps. An export handed no load at all is
    // read as complete, which is what every caller before WEB-067 did.
    expect(exported.filename).toBe(
      "comparison-CENSUS_ACS-acs5-B01003_001-vs-CENSUS_PEP-pep_cty_alldata-POPESTIMATE.csv",
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

  test("a file of a page-bounded read says so in its name and its caveats", () => {
    // Covers: WEB-067 — the workspace pages `/comparison`, states the
    // shortfall in its pill ("loaded 8,000 of 12,400 aligned geographies;
    // the page bound cut the answer short"), and handed the export neither
    // the count nor the flag. The file outlives the pill, which is the
    // argument WEB-059 makes for the explorer's own file.
    const exported = comparisonExport(comparison, comparablePreflight, {
      loaded: 8000,
      total: 12400,
      complete: false,
    });
    expect(exported.filename).toBe(
      "comparison-CENSUS_ACS-acs5-B01003_001-vs-CENSUS_PEP-pep_cty_alldata-POPESTIMATE" +
        "-partial-8000-of-12400.csv",
    );
    // A bounded read is the first thing a reader needs, so it leads the
    // caveats rather than trailing what the API published.
    const caveats = exported.rows[0].at(-1);
    expect(caveats.startsWith("incomplete: 8000 of 12400 aligned geographies")).toBe(
      true,
    );
    expect(caveats).toContain("the page bound cut the answer short");
    // And nothing the API said is displaced by it.
    expect(caveats).toContain("units could not be verified");
  });

  test("a prefix of an unreported total still says it is a prefix", () => {
    // `fetchComparisonPages` counts a read with no published total as
    // incomplete, because without one the client cannot know whether more
    // exist -- and inventing a total would assert a count the API withheld.
    const exported = comparisonExport(comparison, comparablePreflight, {
      loaded: 8000,
      total: null,
      complete: false,
    });
    expect(exported.filename).toContain("-partial-8000.csv");
    expect(exported.filename).not.toContain("-of-");
    expect(exported.rows[0].at(-1)).toContain("no total published");
  });

  test("a complete read keeps the name it always had", () => {
    // The load is not a caveat when there is nothing short about it.
    const exported = comparisonExport(comparison, comparablePreflight, {
      loaded: 2,
      total: 2,
      complete: true,
    });
    expect(exported.filename).toBe(
      "comparison-CENSUS_ACS-acs5-B01003_001-vs-CENSUS_PEP-pep_cty_alldata-POPESTIMATE.csv",
    );
    expect(exported.rows[0].at(-1)).not.toContain("incomplete");
  });

  test("an absent response exports nothing rather than an invented file", () => {
    const exported = comparisonExport(null, null);
    expect(exported.rows).toEqual([]);
    expect(exported.headings).toContain("caveats");
  });
});

describe("aligned presentations read the same rows without inventing values", () => {
  test("the scatter plots each geography's own published pair", () => {
    const model = comparisonScatterModel(comparison);
    // A scatter of the two inputs needs no shared axis or unit, and shows
    // each geography's own pair rather than a series implying one scale.
    expect(model.points).toEqual([
      {
        geoId: "state:55|county:025",
        name: "Dane County, Wisconsin",
        x: 561504,
        y: 568203,
        // Each point carries the period each side describes, so the chart can
        // say what the table already marks (WEB-049).
        periodA: "2023",
        periodB: "2024",
        periodsDiffer: true,
      },
    ]);
    // The geography missing measure A is excluded and counted — plotting it
    // at zero would state a value neither source published.
    expect(model.excluded).toBe(1);
    expect(model.minX).toBe(561504);
    expect(model.maxY).toBe(568203);
  });

  test("a response with no plottable pair yields no points, not a zeroed one", () => {
    const nonePlottable = {
      ...comparison,
      items: comparison.items.map((row) => ({ ...row, value_a: null })),
    };
    const model = comparisonScatterModel(nonePlottable);
    expect(model.points).toEqual([]);
    expect(model.excluded).toBe(2);
    expect(comparisonScatterModel(null).points).toEqual([]);
    expect(comparisonScatterModel({ items: [] }).excluded).toBe(0);
  });

  test("map rows carry one derived field, and only a field the API named", () => {
    expect(defaultDerivedField(comparison)).toBe("difference");
    const rows = comparisonMapRows(comparison, "difference");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      geo_id: "state:55|county:025",
      geo_level: "COUNTY",
      value: "-6699",
    });
    // A geography the API could not derive stays null, so the shared
    // choropleth model leaves it uncoloured rather than colouring a zero.
    expect(rows[1].value).toBeNull();
    expect(rows[1].value_status).toBe("not published on both sides");

    // A field the response never named as derived is not mappable: colouring
    // by a published input would present one side as the comparison.
    expect(comparisonMapRows(comparison, "value_a")).toEqual([]);
    expect(comparisonMapRows(comparison, "")).toEqual([]);
    expect(defaultDerivedField({ ...comparison, derivations: [] })).toBe("");
  });
});

describe("an aligned view says when a pair is not contemporaneous", () => {
  // Covers: WEB-049 — `periodsDiffer` had one call site, in the table body.
  // The scatter drew a 2023-with-2019 pair as a point like any other and the
  // map coloured it by a difference computed across those years, while both
  // panels were otherwise careful about units, derivation, and missing
  // values. The API carries both periods precisely so the difference is
  // visible rather than implied away.

  const pairs = {
    metric_code_a: "A",
    metric_code_b: "B",
    derivations: ["difference"],
    items: [
      { geo_id: "g1", value_a: 10, value_b: 20, period_a: "2023", period_b: "2019", difference: -10 },
      { geo_id: "g2", value_a: 30, value_b: 40, period_a: "2023", period_b: "2023", difference: -10 },
      { geo_id: "g3", value_a: 50, value_b: 60, period_a: "2023", period_b: null, difference: -10 },
      { geo_id: "g4", value_a: null, value_b: 70, period_a: "2023", period_b: "2019", difference: null },
    ],
  };

  test("the scatter counts and marks the points that are not contemporaneous", () => {
    const model = comparisonScatterModel(pairs);
    // g4 has no usable pair and is excluded, as it already was.
    expect(model.points.map((point) => point.geoId)).toEqual(["g1", "g2", "g3"]);
    expect(model.excluded).toBe(1);
    // Only g1 pairs two published periods that differ.
    expect(model.differingPeriods).toBe(1);
    expect(model.points.map((point) => point.periodsDiffer)).toEqual([true, false, false]);
    // The periods travel with the point so the reader can see which two.
    expect(model.points[0].periodA).toBe("2023");
    expect(model.points[0].periodB).toBe("2019");
  });

  test("an absent period is not a mismatch", () => {
    // g3 publishes one period and not the other. That is incompleteness, and
    // asserting a mismatch from it would state something the row does not.
    const model = comparisonScatterModel(pairs);
    const g3 = model.points.find((point) => point.geoId === "g3");
    expect(g3.periodsDiffer).toBe(false);
    expect(g3.periodB).toBe("");
  });

  test("a comparison whose sides share a period says nothing extra", () => {
    const model = comparisonScatterModel({
      ...pairs,
      items: [pairs.items[1]],
    });
    expect(model.differingPeriods).toBe(0);
    expect(model.points[0].periodsDiffer).toBe(false);
  });

  test("the map reports how many coloured geographies are not contemporaneous", () => {
    // The map is the sharper half: it colours one number per polygon, and
    // that number is a subtraction between two publications years apart.
    expect(mapPeriodMismatchNote(pairs, "difference")).toBe(
      "1 of 3 coloured geographies combine values published for different periods; " +
        "each row's two periods are in the table below.",
    );
    // Nothing extra where nothing differs, and nothing at all where the map
    // is not drawn.
    expect(mapPeriodMismatchNote({ ...pairs, items: [pairs.items[1]] }, "difference")).toBe("");
    expect(mapPeriodMismatchNote(pairs, "not_a_derived_field")).toBe("");
    expect(mapPeriodMismatchNote(null, "difference")).toBe("");
  });
});

describe("the screen says what its geographies are an intersection of", () => {
  // Covers: WEB-050 — the route joins its two reduced sides on geography
  // identity with an inner join, so `total` is the size of the intersection.
  // The screen reported "N aligned geographies", which reads as the universe.

  const paired = {
    metric_code_a: "CENSUS_ACS:acs5:B01003_001",
    metric_code_b: "BLS:LAU:UNEMP_RATE",
    total: 500,
    geographies_a: 3143,
    geographies_b: 500,
    items: [],
  };

  test("a side that published more than was paired is named", () => {
    expect(describeComparisonCoverage(paired)).toBe(
      "500 geographies are paired here. CENSUS_ACS:acs5:B01003_001 publishes 3,143 " +
        "and BLS:LAU:UNEMP_RATE publishes 500 under these filters; a geography only " +
        "one of the two publishes is not in this comparison.",
    );
  });

  test("a comparison that paired everything says nothing extra", () => {
    expect(
      describeComparisonCoverage({ ...paired, geographies_a: 500, geographies_b: 500 }),
    ).toBe("");
  });

  test("an API that publishes neither count reports no shortfall", () => {
    // An older deployment serves no coverage. Reading an absent count as zero
    // would report every geography as dropped.
    expect(describeComparisonCoverage({ metric_code_a: "A", metric_code_b: "B", total: 7 })).toBe(
      "",
    );
    expect(describeComparisonCoverage(null)).toBe("");
    expect(describeComparisonCoverage({ ...paired, total: undefined })).toBe("");
  });
});
