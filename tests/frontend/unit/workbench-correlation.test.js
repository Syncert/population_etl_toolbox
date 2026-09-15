import { describe, expect, test } from "vitest";

// Covers: WEB-091 — a correlation is offered only where an API route will
// serve one: never for a longitudinal composition, never outside the matrix
// route's two-to-eight bound, never for a source the analysis routes decline,
// and never for a pair the preflight blocked. Each refusal carries the API's
// or the publication's own reason rather than a paraphrase.
// Covers: WEB-092 — the panel reads `n` first, labels every coefficient
// API-derived, and shows a null coefficient's reason rather than a blank.
// Covers: WEB-093 — the correlation matrix keeps three kinds of cell off its
// diverging scale: the diagonal, a declined pair, and a comparable pair whose
// data cannot carry a coefficient.
// Covers: WEB-103 — the route a correlation would be asked on must be one the
// capability entry declares. The panel used to read `/comparison/preflight`
// and send to `/comparison/correlation`, which is an inference about one
// route standing in for a contract about another.

import {
  CORRELATION_IS_ACROSS_GEOGRAPHIES,
  MAX_MATRIX_METRICS,
  correlationEligibility,
  correlationMatrixModel,
  correlationReadings,
  crossSectionalPair,
  crossSectionalRefusal,
  formatCoefficient,
  selectablePairs,
} from "../../../apps/web/lib/workbench";
import {
  CORRELATION_DIVERGING_SCALE,
  correlationColor,
} from "../../../apps/web/components/CorrelationMatrixChart";

/**
 * Which correlation routes each source in these cases declares.
 *
 * Spelled rather than defaulted, because `correlationEligibility` treats a
 * source it has no entry for as declaring neither: an unknown capability is
 * not a capability. FRED and Census ACS are analysis-ready sources whose
 * capability entries carry both routes; CDC is one the analysis surface
 * declines, so it declares neither.
 */
const DECLARED_ROUTES = {
  FRED: { correlation: true, matrix: true },
  CENSUS_ACS: { correlation: true, matrix: true },
  CDC: { correlation: false, matrix: false },
};

function series(metricCode, sourceCode = "FRED", geoId = "county:06001") {
  return {
    sourceKey: sourceCode.toLowerCase(),
    sourceCode,
    metricCode,
    scope: "latest",
    geoLevel: "COUNTY",
    geoId,
    filters: {},
  };
}

describe("when a correlation may be asked for", () => {
  test("does not depend on the presentation on screen", () => {
    // The plan asks the control to be absent for "a longitudinal
    // composition". Read literally that makes it unreachable, because the
    // correlation *is* one of the presentations a reader selects: it would be
    // refused while a line is shown, and a line is shown until it is not
    // refused. The statistic the plan declines to offer — a correlation of
    // two histories of one geography — is instead named on the panel, so it
    // is stated rather than silently absent.
    expect(
      correlationEligibility({
        series: [series("A"), series("B")],
        declaredRoutes: DECLARED_ROUTES,
      }).eligible,
    ).toBe(true);
    expect(CORRELATION_IS_ACROSS_GEOGRAPHIES).toMatch(/shared time trend/);
    expect(CORRELATION_IS_ACROSS_GEOGRAPHIES).toMatch(/across geographies/);
  });

  test("a pair goes to the correlation route, three go to the matrix", () => {
    expect(
      correlationEligibility({
        series: [series("A"), series("B")],
        declaredRoutes: DECLARED_ROUTES,
      }),
    ).toEqual({ eligible: true, route: "correlation", reason: "" });

    expect(
      correlationEligibility({
        series: [series("A"), series("B"), series("C")],
        declaredRoutes: DECLARED_ROUTES,
      }).route,
    ).toBe("matrix");
  });

  test("the matrix route's upper bound is stated, with the count", () => {
    const many = Array.from({ length: MAX_MATRIX_METRICS + 1 }, (_, index) =>
      series(`M${index}`),
    );
    const verdict = correlationEligibility({
      series: many,
      declaredRoutes: DECLARED_ROUTES,
    });
    expect(verdict.eligible).toBe(false);
    expect(verdict.reason).toContain(String(MAX_MATRIX_METRICS + 1));
  });

  test("a declined source is named before any request, in the API's words", () => {
    const refusal =
      "source 'CDC' publishes stratified observations that an aligned one-value-per-geography analysis would silently collapse";
    const verdict = correlationEligibility({
      series: [series("A"), series("CDC:x", "CDC")],
      analysisRefusals: { CDC: refusal },
      declaredRoutes: DECLARED_ROUTES,
    });
    expect(verdict.eligible).toBe(false);
    // Presented unchanged, not paraphrased.
    expect(verdict.reason).toBe(refusal);
  });

  test("a blocked pair carries the preflight's failed rules", () => {
    const verdict = correlationEligibility({
      series: [series("A"), series("B")],
      declaredRoutes: DECLARED_ROUTES,
      preflightBlocking: [{ rule: "units", reason: "units differ" }],
    });
    expect(verdict.eligible).toBe(false);
    expect(verdict.reason).toBe("units differ");
  });

  test("an unread verdict is a wait, not a refusal", () => {
    const verdict = correlationEligibility({
      series: [series("A"), series("B")],
      declaredRoutes: DECLARED_ROUTES,
      preflightRead: false,
    });
    expect(verdict.eligible).toBe(false);
    expect(verdict.reason).toMatch(/Checking/);
  });

  test("three measures need no preflight: the matrix answers each pair", () => {
    expect(
      correlationEligibility({
        series: [series("A"), series("B"), series("C")],
        declaredRoutes: DECLARED_ROUTES,
        preflightRead: false,
        preflightBlocking: [{ rule: "units", reason: "units differ" }],
      }).eligible,
    ).toBe(true);
  });

  test("a route the capability entry does not declare is refused, by name", () => {
    // The pair route is declared for FRED and not for this source, and the
    // analysis surface has not declined it either — so nothing else in the
    // chain of refusals above would catch it. Before the routes were declared
    // separately this case could not be expressed: the panel asked about
    // `/comparison/preflight` and sent to `/comparison/correlation`.
    const verdict = correlationEligibility({
      series: [series("A"), series("B", "CENSUS_ACS")],
      declaredRoutes: {
        FRED: { correlation: true, matrix: true },
        CENSUS_ACS: { correlation: false, matrix: true },
      },
    });
    expect(verdict.eligible).toBe(false);
    expect(verdict.route).toBeNull();
    expect(verdict.reason).toContain("/comparison/correlation");
    expect(verdict.reason).toContain("CENSUS_ACS");
  });

  test("a source with no capability entry declares neither route", () => {
    // An unknown capability is not a capability: a source discovery never
    // returned cannot be assumed to serve a route this client would send to.
    const verdict = correlationEligibility({
      series: [series("A"), series("B")],
      declaredRoutes: {},
    });
    expect(verdict.eligible).toBe(false);
    expect(verdict.reason).toContain("/comparison/correlation");
  });
});

describe("the pair chooser for more than two measures", () => {
  test("every unordered pair is selectable", () => {
    expect(
      selectablePairs([series("A"), series("B"), series("C")]),
    ).toEqual([
      ["A", "B"],
      ["A", "C"],
      ["B", "C"],
    ]);
  });

  test("a scatter is refused until a pair is chosen, then offered", () => {
    const three = [series("A"), series("B"), series("C")];
    expect(
      crossSectionalRefusal({ series: three, sharedGrains: ["COUNTY"] }),
    ).toMatch(/choose which pair/);
    expect(
      crossSectionalRefusal({
        series: three,
        sharedGrains: ["COUNTY"],
        chosenPair: ["A", "C"],
      }),
    ).toBe("");
  });

  test("a choice naming a measure since removed is not honoured", () => {
    expect(
      crossSectionalPair([series("A"), series("B")], ["A", "REMOVED"]),
    ).toBeNull();
    expect(crossSectionalPair([series("A"), series("B")], ["A", "B"])).toEqual([
      "A",
      "B",
    ]);
  });
});

describe("the correlation panel's readings", () => {
  const statistic = {
    n: 3016,
    contemporaneous_pairs: 2800,
    pearson_r: 0.4123,
    spearman_rho: -0.377,
  };

  test("lead with the pair count, so the coefficient is read against it", () => {
    const readings = correlationReadings(statistic, {
      geographiesA: 3143,
      geographiesB: 3016,
    });
    expect(readings[0].label).toBe("Paired geographies");
    expect(readings[0].value).toBe((3016).toLocaleString());
  });

  test("label every coefficient API-derived, and nothing else", () => {
    const readings = correlationReadings(statistic, {});
    const derived = readings.filter((reading) => reading.derived);
    expect(derived.map((reading) => reading.label)).toEqual([
      "Pearson r",
      "Spearman ρ",
    ]);
  });

  test("show three decimal places, keeping a sign", () => {
    expect(formatCoefficient(0.4123)).toBe("0.412");
    expect(formatCoefficient(-0.377)).toBe("-0.377");
    expect(formatCoefficient(null)).toBe("");
  });

  test("a null coefficient shows the API's reason, never a blank", () => {
    const readings = correlationReadings(
      { ...statistic, pearson_r: null, spearman_rho: null },
      { nullReason: "2 paired geographies is fewer than the 3 a correlation needs" },
    );
    for (const reading of readings.filter((entry) => entry.derived)) {
      expect(reading.value).toMatch(/fewer than the 3/);
    }
  });

  test("coverage and contemporaneity are read against the pair count", () => {
    const readings = correlationReadings(statistic, {
      geographiesA: 3143,
      geographiesB: 3016,
    });
    const coverage = readings.find((entry) => entry.label === "Coverage");
    expect(coverage.value).toContain("3,143");
    const contemporaneous = readings.find(
      (entry) => entry.label === "Contemporaneous pairs",
    );
    expect(contemporaneous.value).toBe("2,800 of 3,016");
  });
});

describe("the correlation matrix", () => {
  const codes = ["A", "B", "C"];
  const pairs = [
    {
      metric_code_a: "A",
      metric_code_b: "B",
      comparable: true,
      rules: [],
      statistic: { n: 50, pearson_r: 0.8, spearman_rho: 0.7 },
    },
    {
      metric_code_a: "A",
      metric_code_b: "C",
      comparable: false,
      rules: [
        { rule: "units", status: "fail", reason: "units differ ('People' vs 'Percent')" },
      ],
      statistic: null,
    },
    {
      metric_code_a: "B",
      metric_code_b: "C",
      comparable: true,
      rules: [],
      // Comparable, but the data could not carry a coefficient.
      statistic: { n: 2, pearson_r: null, spearman_rho: null },
    },
  ];

  test("is square, and the diagonal carries no coefficient", () => {
    const model = correlationMatrixModel({ codes, pairs });
    expect(model.cells).toHaveLength(9);
    const diagonal = model.cells.filter((cell) => cell.identity);
    expect(diagonal).toHaveLength(3);
    for (const cell of diagonal) {
      expect(cell.value).toBeNull();
      expect(cell.reason).toMatch(/arithmetic, not by\s+measurement/);
    }
  });

  test("a pair is answered from both sides of the diagonal", () => {
    const model = correlationMatrixModel({ codes, pairs });
    const forward = model.cells.find(
      (cell) => cell.metricCodeA === "A" && cell.metricCodeB === "B",
    );
    const mirrored = model.cells.find(
      (cell) => cell.metricCodeA === "B" && cell.metricCodeB === "A",
    );
    expect(forward.value).toBe(0.8);
    expect(mirrored.value).toBe(0.8);
  });

  test("a declined pair carries its failed rule and no coefficient", () => {
    const model = correlationMatrixModel({ codes, pairs });
    const declined = model.cells.find(
      (cell) => cell.metricCodeA === "A" && cell.metricCodeB === "C",
    );
    expect(declined.declined).toBe(true);
    expect(declined.value).toBeNull();
    expect(declined.reason).toContain("units differ");
    expect(model.declinedCount).toBe(2);
  });

  test("a comparable pair with no measurable coefficient is not declined", () => {
    const model = correlationMatrixModel({ codes, pairs });
    const unmeasured = model.cells.find(
      (cell) => cell.metricCodeA === "B" && cell.metricCodeB === "C",
    );
    expect(unmeasured.declined).toBe(false);
    expect(unmeasured.value).toBeNull();
    expect(unmeasured.reason).toMatch(/caveats say why/);
  });

  test("the other coefficient is the same grid, differently filled", () => {
    const spearman = correlationMatrixModel({
      codes,
      pairs,
      which: "spearman_rho",
    });
    const cell = spearman.cells.find(
      (entry) => entry.metricCodeA === "A" && entry.metricCodeB === "B",
    );
    expect(cell.value).toBe(0.7);
  });

  test("only the measured cells are counted as measured", () => {
    const model = correlationMatrixModel({ codes, pairs });
    // A-B and B-A; the declined pair and the unmeasurable one are neither.
    expect(model.measuredCount).toBe(2);
  });
});

describe("the diverging scale", () => {
  test("is fixed to a coefficient's own range, not the observed one", () => {
    expect(CORRELATION_DIVERGING_SCALE[0].from).toBe(-1);
    expect(
      CORRELATION_DIVERGING_SCALE[CORRELATION_DIVERGING_SCALE.length - 1].to,
    ).toBe(1);
  });

  test("gives zero the neutral band, and the two directions differ", () => {
    const neutral = correlationColor(0);
    expect(correlationColor(0.05)).toBe(neutral);
    expect(correlationColor(0.9)).not.toBe(neutral);
    expect(correlationColor(-0.9)).not.toBe(neutral);
    expect(correlationColor(0.9)).not.toBe(correlationColor(-0.9));
  });
});
