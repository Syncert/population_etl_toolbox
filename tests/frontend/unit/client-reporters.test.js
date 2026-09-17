import { describe, expect, test, vi } from "vitest";

// Covers: WEB-114 — each of the three reporters says one thing, through the
// one payload builder, with nothing the privacy boundary keeps off the wire.
//
// These are the builders rather than the component, deliberately. A component
// that held this logic could only be tested by rendering it, which means
// mocking `next/navigation` and `next/web-vitals` -- and a test that mocks
// the framework proves what the mock does. The wiring is four lines in
// `ClientReporters.tsx` and is proven end to end in the browser tier.
//
// This tier is also the only place the *vitals* path is provable at all: a
// Web Vital is reported when a page is hidden or unloaded, which a browser
// test can only make happen by ending itself.

import {
  REPORT_ENDPOINT,
  errorReport,
  rejectionReport,
  sendReport,
  violationReport,
  vitalReport,
} from "../../../apps/web/lib/clientReport";

describe("what each reporter says", () => {
  test("a web vital carries its measurement, its rating and its route", () => {
    const report = vitalReport("/explore", {
      name: "LCP",
      value: 1234.56789,
      rating: "good",
      id: "v1-17",
    });
    expect(report).toEqual({
      kind: "vital",
      route: "/explore",
      name: "LCP",
      message: "good",
      buildId: "development",
      value: 1234.568,
    });
    // The browser's own metric id is not this application's to publish.
    expect(JSON.stringify(report)).not.toContain("v1-17");
  });

  test("a vital with nothing measured is not a report", () => {
    expect(vitalReport("/explore", { name: "LCP", value: Number.NaN })).toBeNull();
    expect(vitalReport("/explore", { name: "", value: 1 })).toBeNull();
  });

  test("an uncaught error is named by its type", () => {
    const report = errorReport("/compare", {
      message: "Cannot read properties of undefined (reading 'geo_id')",
      error: new TypeError("Cannot read properties of undefined"),
    });
    expect(report.kind).toBe("error");
    expect(report.name).toBe("TypeError");
    expect(report.route).toBe("/compare");
    expect(report.value).toBeUndefined();
  });

  test("an error's assigned name cannot forge the type", () => {
    // `error.name` is assignable; the constructor's is not.
    const thrown = new RangeError("out of bounds");
    thrown.name = "SecurityError";
    expect(errorReport("/", { message: "x", error: thrown }).name).toBe("RangeError");
  });

  test("an unhandled rejection is reported, whatever was rejected with", () => {
    expect(rejectionReport("/saved", new RangeError("offset beyond the bound"))).toMatchObject({
      kind: "error",
      name: "RangeError",
      message: "offset beyond the bound",
    });
    // A rejection with a bare string still names something.
    expect(rejectionReport("/saved", "just a string")).toMatchObject({
      name: "UnhandledRejection",
      message: "just a string",
    });
    // And one with nothing to say says nothing rather than half of it.
    expect(rejectionReport("/saved", undefined)).toMatchObject({
      name: "UnhandledRejection",
      message: "",
    });
  });

  test("a policy violation names the directive and the blocked URI", () => {
    const report = violationReport("/explore", {
      effectiveDirective: "img-src",
      blockedURI: "https://blocked.invalid/pixel.png",
    });
    expect(report).toMatchObject({
      kind: "csp",
      name: "img-src",
      message: "https://blocked.invalid/pixel.png",
    });
    // The older event spelling still works.
    expect(violationReport("/", { violatedDirective: "script-src-elem" }).name).toBe(
      "script-src-elem",
    );
    expect(violationReport("/", {}).name).toBe("csp-violation");
  });

  test("every builder strips a query string it is handed", () => {
    const query = "/explore?metric=CENSUS_ACS:acs5:B01003_001&geo=state:55";
    for (const report of [
      vitalReport(query, { name: "CLS", value: 0.02 }),
      errorReport(query, { message: "x", error: new Error("x") }),
      rejectionReport(query, new Error("x")),
      violationReport(query, { effectiveDirective: "img-src" }),
    ]) {
      expect(report.route).toBe("/explore");
      expect(JSON.stringify(report)).not.toContain("B01003_001");
    }
  });
});

describe("how a report is sent", () => {
  const report = vitalReport("/explore", { name: "LCP", value: 1, rating: "good" });

  test("by beacon, to the same-origin sink", () => {
    const beacon = vi.fn(() => true);
    const fetchImpl = vi.fn();
    expect(sendReport(report, { beacon, fetchImpl })).toBe(true);
    expect(beacon).toHaveBeenCalledTimes(1);
    expect(beacon.mock.calls[0][0]).toBe(REPORT_ENDPOINT);
    // A beacon survives the navigation that a vital is reported during; a
    // fetch at that moment is cancelled by it.
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  test("by keepalive fetch when the beacon is refused", () => {
    const beacon = vi.fn(() => false);
    const fetchImpl = vi.fn(async () => new Response(null, { status: 204 }));
    expect(sendReport(report, { beacon, fetchImpl })).toBe(true);
    const [url, init] = fetchImpl.mock.calls[0];
    expect(url).toBe(REPORT_ENDPOINT);
    expect(init.method).toBe("POST");
    expect(init.keepalive).toBe(true);
    expect(JSON.parse(init.body)).toEqual(report);
  });

  test("never, when there is nothing honest to send", () => {
    const beacon = vi.fn(() => true);
    expect(sendReport(null, { beacon })).toBe(false);
    expect(beacon).not.toHaveBeenCalled();
  });

  test("and a browser that refuses to report does not break the page", () => {
    const beacon = vi.fn(() => {
      throw new Error("blocked by the user agent");
    });
    expect(() => sendReport(report, { beacon })).not.toThrow();
    expect(sendReport(report, { beacon })).toBe(false);
  });
});
