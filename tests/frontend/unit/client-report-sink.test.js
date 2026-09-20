import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

// Covers: WEB-114 — the sink writes one line an operator can read, and
// answers 204 to everything.
//
// The route handler is imported and called directly. That is the only tier
// where its *output* is observable at all: Playwright's web server runs with
// stdout ignored, and the container log the Compose smoke job reads needs a
// Docker daemon. What the browser tier proves is that the requests arrive;
// what this proves is what happens to them.

import { POST, GET } from "../../../apps/web/app/client-report/route";
import { MAX_BODY_BYTES } from "../../../apps/web/lib/clientReport";

function request(body, { type = "application/json", length = null } = {}) {
  const headers = new Headers({ "content-type": type });
  if (length !== null) {
    headers.set("content-length", String(length));
  }
  return new Request("http://localhost:3100/client-report", {
    method: "POST",
    headers,
    body,
  });
}

const VALID = {
  kind: "error",
  route: "/explore",
  name: "TypeError",
  message: "Cannot read properties of undefined",
  buildId: "abc123",
};

describe("the report sink", () => {
  let written;

  beforeEach(() => {
    written = [];
    vi.spyOn(console, "log").mockImplementation((line) => written.push(line));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test("writes one line for one report", async () => {
    const response = await POST(request(JSON.stringify(VALID)));
    expect(response.status).toBe(204);
    expect(written).toHaveLength(1);
    expect(written[0]).toContain("client_report kind=error");
    expect(written[0]).toContain("route=/explore");
    expect(written[0]).toContain("name=TypeError");
    expect(written[0]).toContain("build=abc123");
  });

  test("answers 204 to a report it refuses, and writes nothing", async () => {
    // A reporter told "400" retries, and a retrying reporter against a broken
    // sink is a loop running in every reader's browser at once.
    for (const body of [
      JSON.stringify({ ...VALID, token: "secret-bearer-value" }),
      JSON.stringify({ kind: "telemetry", name: "x" }),
      JSON.stringify([VALID]),
      JSON.stringify("a string"),
      "not json at all",
      "",
    ]) {
      const response = await POST(request(body));
      expect(response.status, body).toBe(204);
    }
    expect(written).toEqual([]);
  });

  test("re-validates rather than trusting what arrived", async () => {
    // Anything can POST to a same-origin path; the reporters are not a trust
    // boundary.
    await POST(request(JSON.stringify({ ...VALID, route: "/explore?metric=SECRET" })));
    expect(written[0]).toContain("route=/explore");
    expect(written[0]).not.toContain("SECRET");
  });

  test("refuses a body past the bound, by claim and by fact", async () => {
    const huge = JSON.stringify({ ...VALID, message: "m".repeat(MAX_BODY_BYTES * 2) });

    // A `content-length` claim past the bound: refused before it is read.
    expect((await POST(request(huge, { length: MAX_BODY_BYTES + 1 }))).status).toBe(204);
    expect(written).toEqual([]);

    // And a body past the bound whose header lied about it: `content-length`
    // is a claim, not a fact, so the length is checked again after reading.
    expect((await POST(request(huge, { length: 10 }))).status).toBe(204);
    expect(written).toEqual([]);
  });

  test("reads a browser's own violation report, in both wire formats", async () => {
    await POST(
      request(
        JSON.stringify({
          "csp-report": {
            "document-uri": "https://studio.example/compare?a=SECRET",
            "violated-directive": "img-src",
            "blocked-uri": "https://blocked.invalid/pixel.png",
          },
        }),
        { type: "application/csp-report" },
      ),
    );
    expect(written[0]).toContain("client_report kind=csp");
    expect(written[0]).toContain("route=/compare");
    expect(written[0]).not.toContain("SECRET");

    written.length = 0;
    await POST(
      request(
        JSON.stringify([
          {
            type: "csp-violation",
            url: "https://studio.example/explore",
            body: { effectiveDirective: "script-src-elem", blockedURL: "inline" },
          },
        ]),
        { type: "application/reports+json" },
      ),
    );
    expect(written[0]).toContain("name=script-src-elem");
  });

  test("one report is one line, whatever a message tries to do", async () => {
    await POST(
      request(
        JSON.stringify({
          ...VALID,
          message: 'x"\nclient_report kind=error name=Forged route=/ build=b message="y',
        }),
      ),
    );
    expect(written).toHaveLength(1);
    // One line: the newline is gone, so the forged record cannot start a
    // second one.
    expect(written[0].split("\n")).toHaveLength(1);
    // And what is left of it is inside the quoted message, where it is text
    // rather than a record: everything after `message=` is one JSON string,
    // and its quotes are escaped.
    const quoted = written[0].slice(written[0].indexOf("message=") + "message=".length);
    expect(() => JSON.parse(quoted)).not.toThrow();
    expect(JSON.parse(quoted)).toContain("client_report kind=error name=Forged");
    expect(written[0].indexOf("client_report")).toBeLessThan(written[0].indexOf("message="));
  });

  test("a GET says nothing, rather than confirming the route exists", async () => {
    const response = await GET();
    expect(response.status).toBe(204);
    expect(written).toEqual([]);
  });
});
