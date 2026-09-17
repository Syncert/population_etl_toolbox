import { describe, expect, test } from "vitest";

// Covers: WEB-114 — the browser's report carries what an operator needs and
// nothing the privacy boundary keeps off the wire.
//
// The shape is closed, and the closure is the test: an extra field is refused
// rather than stripped, because a field nobody meant to send starts arriving
// exactly where one is quietly dropped.

import {
  MAX_BODY_BYTES,
  MAX_MESSAGE_LENGTH,
  REPORT_KINDS,
  formatReportLine,
  parseReport,
  reportPath,
  reportsFromBrowserPayload,
} from "../../../apps/web/lib/clientReport";

describe("the route a report names", () => {
  test("is a path, never a query string", () => {
    // What a reader is looking at is in the query. What an operator needs is
    // the route.
    expect(reportPath("/explore?metric=CENSUS_ACS:acs5:B01003_001&geo=state:55")).toBe(
      "/explore",
    );
    expect(reportPath("/compare?a=X&b=Y#chart")).toBe("/compare");
    expect(reportPath("https://studio.example/saved?name=My%20analysis")).toBe("/saved");
  });

  test("is always a path, whatever it is handed", () => {
    expect(reportPath("")).toBe("/");
    expect(reportPath(null)).toBe("/");
    expect(reportPath(undefined)).toBe("/");
    expect(reportPath("https://studio.example")).toBe("/");
    expect(reportPath("explore")).toBe("/explore");
  });

  test("is bounded, so a report cannot be a payload", () => {
    expect(reportPath(`/${"a".repeat(5000)}`).length).toBeLessThanOrEqual(200);
  });
});

describe("the closed shape", () => {
  const valid = {
    kind: "error",
    route: "/explore",
    name: "TypeError",
    message: "Cannot read properties of undefined",
    buildId: "abc123",
  };

  test("accepts exactly the fields it declares", () => {
    expect(parseReport(valid)).toEqual(valid);
  });

  test("refuses a field outside the shape rather than dropping it", () => {
    for (const extra of [
      { token: "secret-bearer-value" },
      { query: "?metric=CENSUS_ACS:acs5:B01003_001" },
      { savedAnalysisName: "My analysis" },
      { configurationId: 41 },
      { value: 1 },
    ]) {
      expect(parseReport({ ...valid, ...extra }), JSON.stringify(extra)).toBeNull();
    }
  });

  test("refuses a kind it does not know", () => {
    expect(parseReport({ ...valid, kind: "telemetry" })).toBeNull();
    expect(parseReport({ ...valid, kind: 7 })).toBeNull();
    for (const kind of REPORT_KINDS) {
      const measured = kind === "vital" ? { value: 12.5 } : {};
      expect(parseReport({ ...valid, kind, ...measured })).not.toBeNull();
    }
  });

  test("refuses what is not an object at all", () => {
    for (const payload of [null, undefined, "a string", 7, [], [valid]]) {
      expect(parseReport(payload)).toBeNull();
    }
  });

  test("strips the query out of a route even when the reporter did not", () => {
    // The sink does not trust the reporters: anything can POST here.
    const parsed = parseReport({ ...valid, route: "/explore?metric=SECRET&geo=state:55" });
    expect(parsed?.route).toBe("/explore");
  });

  test("requires a name, because a report with none names nothing", () => {
    expect(parseReport({ ...valid, name: "" })).toBeNull();
    expect(parseReport({ ...valid, name: "   " })).toBeNull();
    expect(parseReport({ ...valid, name: 42 })).toBeNull();
  });

  test("a vital carries its measurement and every other kind carries none", () => {
    const vital = parseReport({ ...valid, kind: "vital", name: "LCP", value: 1234.5678 });
    expect(vital?.value).toBe(1234.568);
    // A vital with nothing measured reports nothing.
    expect(parseReport({ ...valid, kind: "vital", name: "LCP" })).toBeNull();
    expect(
      parseReport({ ...valid, kind: "vital", name: "LCP", value: Number.NaN }),
    ).toBeNull();
    expect(parseReport({ ...valid, kind: "csp", value: 3 })).toBeNull();
  });

  test("bounds the message, so a thrown string cannot become the log", () => {
    const parsed = parseReport({ ...valid, message: "x".repeat(5000) });
    expect(parsed?.message.length).toBe(MAX_MESSAGE_LENGTH);
  });

  test("takes the control characters out, so one report cannot be two lines", () => {
    const parsed = parseReport({
      ...valid,
      message: ["first", "client_report kind=error name=Forged"].join("\n"),
    });
    expect(parsed?.message).not.toContain("\n");
    expect(formatReportLine(parsed)).toBe(formatReportLine(parsed).split("\n")[0]);
  });
});

describe("the line an operator reads", () => {
  test("names the kind, the route, the name and the build", () => {
    const line = formatReportLine({
      kind: "vital",
      route: "/explore",
      name: "LCP",
      message: "good",
      buildId: "abc123",
      value: 1234.5,
    });
    expect(line).toContain("client_report kind=vital");
    expect(line).toContain("route=/explore");
    expect(line).toContain("name=LCP");
    expect(line).toContain("build=abc123");
    expect(line).toContain("value=1234.5");
    expect(line).toContain('message="good"');
  });

  test("quotes only the message, which is the only field that can hold a space", () => {
    const line = formatReportLine({
      kind: "error",
      route: "/compare",
      name: "TypeError",
      message: "x is not a function",
      buildId: "abc123",
    });
    expect(line.split(" ").filter((part) => part.includes("="))[0]).toBe("kind=error");
    expect(line).toContain('message="x is not a function"');
    expect(line).not.toContain("value=");
  });
});

describe("a browser's own violation report", () => {
  test("reads the deprecated report-uri shape", () => {
    const [report] = reportsFromBrowserPayload(
      "application/csp-report",
      {
        "csp-report": {
          "document-uri": "https://studio.example/explore?metric=SECRET",
          "violated-directive": "script-src-elem",
          "blocked-uri": "inline",
          "script-sample": "alert(1)",
        },
      },
      "abc123",
    );
    expect(report.kind).toBe("csp");
    // The query is gone, and so is the sample: a report says which door was
    // closed, never what was behind it.
    expect(report.route).toBe("/explore");
    expect(report.name).toBe("script-src-elem");
    expect(report.message).toBe("inline");
    expect(JSON.stringify(report)).not.toContain("alert(1)");
    expect(JSON.stringify(report)).not.toContain("SECRET");
  });

  test("reads the Reporting API shape, and only its violations", () => {
    const reports = reportsFromBrowserPayload(
      "application/reports+json",
      [
        {
          type: "csp-violation",
          url: "https://studio.example/compare?a=SECRET",
          body: { effectiveDirective: "img-src", blockedURL: "https://elsewhere.example/x.png" },
        },
        { type: "deprecation", url: "https://studio.example/", body: { id: "x" } },
      ],
      "abc123",
    );
    expect(reports).toHaveLength(1);
    expect(reports[0].route).toBe("/compare");
    expect(reports[0].name).toBe("img-src");
    expect(reports[0].buildId).toBe("abc123");
  });

  test("answers nothing to a content type it does not read", () => {
    expect(reportsFromBrowserPayload("application/json", { kind: "error" }, "b")).toEqual([]);
    expect(reportsFromBrowserPayload("text/plain", "anything", "b")).toEqual([]);
    expect(reportsFromBrowserPayload("application/reports+json", { not: "an array" }, "b")).toEqual(
      [],
    );
    expect(reportsFromBrowserPayload("application/csp-report", {}, "b")).toEqual([]);
  });
});

describe("the bound on a body", () => {
  test("is small, because a report is small", () => {
    expect(MAX_BODY_BYTES).toBeLessThanOrEqual(8192);
    const biggest = JSON.stringify({
      kind: "error",
      route: "/".padEnd(200, "a"),
      name: "E".repeat(64),
      message: "m".repeat(MAX_MESSAGE_LENGTH),
      buildId: "b".repeat(64),
    });
    // Every report the shape permits fits inside the bound, so the bound
    // refuses only what the shape already would.
    expect(biggest.length).toBeLessThan(MAX_BODY_BYTES);
  });
});
