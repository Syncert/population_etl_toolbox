// What the browser is allowed to say, and how it says it.
//
// The deployment-observability plan recorded plainly that "the frontend
// cannot be debugged", and answered it with server-side smoke tiers. Nothing
// gave the browser itself a voice: a CSP violation in production, a chunk
// blocked under the nonce policy, a WebGL failure or a hydration mismatch
// produced no signal anywhere, and the CSP browser spec proves zero
// violations only against a build CI made seconds earlier.
//
// This module is the payload discipline for that voice, and it is the whole
// of it: the reporters build reports here, and the sink re-validates here
// rather than trusting what arrived. A report is a fixed, closed shape. A
// field outside it is refused rather than dropped, because "dropped" is how a
// field nobody meant to send starts arriving.
//
// What never travels, and why it is worth a list:
//
// - **The query string.** `/explore?metric=CENSUS_ACS:acs5:B01003_001&geo=...`
//   is what a reader is looking at. The route path is what an operator needs.
// - **Any observed value.** A report is about the application, never about
//   the data it drew.
// - **A token, an account, a saved-analysis name or any identifier.** The
//   private routes are somebody's own; a log line is not a place to publish
//   who was reading what.
//
// The one number that does travel is a Web Vital's own measurement -- a
// duration or a layout-shift score. A vitals report without it reports
// nothing at all; it is a measurement of this application's rendering and
// carries nothing about the data or the reader.

/** The kinds of thing a browser may report. Closed on purpose. */
export const REPORT_KINDS = ["vital", "error", "csp"] as const;

export type ClientReportKind = (typeof REPORT_KINDS)[number];

export interface ClientReport {
  kind: ClientReportKind;
  /** The route's path. Never its query string, never its fragment. */
  route: string;
  /** The metric name, the error's constructor name, or the violated directive. */
  name: string;
  /** A bounded description. Never a value, an id or a name from the data. */
  message: string;
  /** Which build is speaking. */
  buildId: string;
  /** A Web Vital's own measurement. Absent on every other kind. */
  value?: number;
}

/** Every key a report may carry. Anything else is a refusal, not a drop. */
const ALLOWED_KEYS = new Set(["kind", "route", "name", "message", "buildId", "value"]);

export const MAX_NAME_LENGTH = 64;
export const MAX_MESSAGE_LENGTH = 300;
export const MAX_ROUTE_LENGTH = 200;
/** A report is small. A body larger than this is refused unread. */
export const MAX_BODY_BYTES = 4096;

/** The build this bundle was made from, or `development`. */
export function buildId(): string {
  return process.env.NEXT_PUBLIC_BUILD_ID || "development";
}

/**
 * The path of a location, with the query string and fragment removed.
 *
 * Takes anything: an absolute URL, a path, an empty string. What it returns
 * is always a path or `/`, because a report that cannot say which route it
 * came from is still worth having and a half-parsed URL is not.
 */
export function reportPath(href: string | null | undefined): string {
  if (!href) {
    return "/";
  }
  const withoutFragment = String(href).split("#")[0] ?? "";
  const withoutQuery = withoutFragment.split("?")[0] ?? "";
  if (/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(withoutQuery)) {
    // An absolute URL: keep the path, drop the origin.
    const afterScheme = withoutQuery.slice(withoutQuery.indexOf("://") + 3);
    const slash = afterScheme.indexOf("/");
    return slash === -1 ? "/" : afterScheme.slice(slash).slice(0, MAX_ROUTE_LENGTH) || "/";
  }
  const path = withoutQuery.startsWith("/") ? withoutQuery : `/${withoutQuery}`;
  return path.slice(0, MAX_ROUTE_LENGTH) || "/";
}

function bounded(value: unknown, limit: number): string {
  if (typeof value !== "string") {
    return "";
  }
  // Control characters are removed before anything else: a log line that can
  // be forged into two log lines is not evidence. `\p{C}` is Unicode's own
  // "other" category, so this covers more than the ASCII control range.
  return value
    .replace(/\p{C}+/gu, " ")
    .trim()
    .slice(0, limit);
}

/**
 * A report, or `null` when what was given cannot honestly make one.
 *
 * Used by the reporters to build and by the sink to re-validate. The sink
 * does not trust the reporters: anything can POST to a same-origin path.
 */
export function parseReport(payload: unknown): ClientReport | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const candidate = payload as Record<string, unknown>;
  for (const key of Object.keys(candidate)) {
    if (!ALLOWED_KEYS.has(key)) {
      // Refused, not stripped. A field nobody meant to send starts arriving
      // exactly where one is quietly dropped.
      return null;
    }
  }
  const kind = candidate.kind;
  if (typeof kind !== "string" || !REPORT_KINDS.includes(kind as ClientReportKind)) {
    return null;
  }
  const name = bounded(candidate.name, MAX_NAME_LENGTH);
  if (!name) {
    return null;
  }
  const report: ClientReport = {
    kind: kind as ClientReportKind,
    route: reportPath(typeof candidate.route === "string" ? candidate.route : "/"),
    name,
    message: bounded(candidate.message, MAX_MESSAGE_LENGTH),
    buildId: bounded(candidate.buildId, MAX_NAME_LENGTH) || "unknown",
  };
  if (kind === "vital") {
    const value = candidate.value;
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return null;
    }
    report.value = Math.round(value * 1000) / 1000;
  } else if (candidate.value !== undefined) {
    // A measurement on a kind that has nothing to measure is a shape nobody
    // designed; it is refused rather than ignored.
    return null;
  }
  return report;
}

/**
 * One structured line, in the shape the API's own completion line uses.
 *
 * `key=value`, space separated, quoted only where a value can hold a space --
 * which is why `message` is the only quoted field and why `bounded` above
 * takes the control characters out of it.
 */
export function formatReportLine(report: ClientReport): string {
  const fields = [
    `client_report kind=${report.kind}`,
    `route=${report.route}`,
    `name=${report.name}`,
    `build=${report.buildId}`,
  ];
  if (report.value !== undefined) {
    fields.push(`value=${report.value}`);
  }
  fields.push(`message=${JSON.stringify(report.message)}`);
  return fields.join(" ");
}

/**
 * A browser's own violation report, turned into this application's shape.
 *
 * Two wire formats, because two generations of the standard are in the field:
 * `report-uri` posts `application/csp-report` with one `{"csp-report": {...}}`
 * object, and the Reporting API posts `application/reports+json` with an
 * array of `{type, url, body}`. Neither is this application's shape, and
 * neither is trusted: each is read for the four fields a report carries and
 * everything else in it is left where it was.
 */
export function reportsFromBrowserPayload(
  contentType: string,
  payload: unknown,
  build: string,
): ClientReport[] {
  const type = contentType.split(";")[0]?.trim().toLowerCase() ?? "";

  if (type === "application/csp-report") {
    const violation = (payload as { "csp-report"?: Record<string, unknown> } | null)?.[
      "csp-report"
    ];
    if (!violation) {
      return [];
    }
    return [
      {
        kind: "csp",
        route: reportPath(String(violation["document-uri"] ?? "")),
        name: bounded(violation["violated-directive"], MAX_NAME_LENGTH) || "csp-violation",
        // The blocked URI, not the blocked content: a report says which door
        // was closed, never what was behind it.
        message: bounded(violation["blocked-uri"], MAX_MESSAGE_LENGTH),
        buildId: build,
      },
    ];
  }

  if (type === "application/reports+json") {
    if (!Array.isArray(payload)) {
      return [];
    }
    const found: ClientReport[] = [];
    for (const entry of payload) {
      if (typeof entry !== "object" || entry === null) {
        continue;
      }
      const record = entry as Record<string, unknown>;
      if (record.type !== "csp-violation") {
        continue;
      }
      const body = (record.body ?? {}) as Record<string, unknown>;
      found.push({
        kind: "csp",
        route: reportPath(String(record.url ?? "")),
        name:
          bounded(body.effectiveDirective ?? body.violatedDirective, MAX_NAME_LENGTH) ||
          "csp-violation",
        message: bounded(body.blockedURL ?? body.blockedURI, MAX_MESSAGE_LENGTH),
        buildId: build,
      });
    }
    return found;
  }

  return [];
}


// ---------------------------------------------------------------------------
// What each reporter has to say, and how it is sent
// ---------------------------------------------------------------------------
//
// These are plain functions rather than code inside the component, for the
// same reason every other decision in this application is: a component that
// holds the logic can only be tested by rendering it, and rendering it means
// mocking `next/navigation` and `next/web-vitals`. The component below them
// is wiring -- three listeners and a hook -- and the browser tier proves the
// wiring end to end.

/** Where a report is posted. Same-origin, outside the rewritten prefixes. */
export const REPORT_ENDPOINT = "/client-report";

/** The shape `useReportWebVitals` hands its callback, as much as is used. */
export interface WebVitalMetric {
  name: string;
  value: number;
  rating?: string;
}

export function vitalReport(route: string, metric: WebVitalMetric): ClientReport | null {
  return parseReport({
    kind: "vital",
    route,
    name: String(metric?.name ?? ""),
    // The rating the browser gave it -- "good", "needs-improvement", "poor" --
    // which is the part an operator reads first. The metric's own id is the
    // browser's and is not this application's to publish.
    message: String(metric?.rating ?? ""),
    buildId: buildId(),
    value: Number(metric?.value),
  });
}

function describedError(thrown: unknown): { name: string; message: string } {
  if (thrown instanceof Error) {
    // The constructor's name, not the instance's: `error.name` can be
    // assigned anything by the code that threw.
    return { name: thrown.constructor?.name || "Error", message: thrown.message };
  }
  if (typeof thrown === "string") {
    return { name: "Error", message: thrown };
  }
  return { name: "Error", message: "" };
}

export function errorReport(
  route: string,
  event: { message?: string; error?: unknown },
): ClientReport | null {
  const described = describedError(event?.error);
  return parseReport({
    kind: "error",
    route,
    name: described.name,
    message: String(event?.message ?? described.message ?? ""),
    buildId: buildId(),
  });
}

export function rejectionReport(route: string, reason: unknown): ClientReport | null {
  const described = describedError(reason);
  return parseReport({
    kind: "error",
    route,
    name: reason instanceof Error ? described.name : "UnhandledRejection",
    message: described.message,
    buildId: buildId(),
  });
}

export function violationReport(
  route: string,
  event: { effectiveDirective?: string; violatedDirective?: string; blockedURI?: string },
): ClientReport | null {
  return parseReport({
    kind: "csp",
    route,
    name: event?.effectiveDirective || event?.violatedDirective || "csp-violation",
    // Which door was closed, never what was behind it.
    message: String(event?.blockedURI ?? ""),
    buildId: buildId(),
  });
}

/** An injectable transport, so this is testable without a browser. */
export interface ReportTransport {
  beacon?: (url: string, body: Blob) => boolean;
  fetchImpl?: typeof fetch;
}

/**
 * Post one report, or quietly do nothing.
 *
 * `sendBeacon` first: a page being unloaded is exactly when a vital is
 * reported, and a `fetch` at that moment is cancelled by the navigation.
 * `fetch` with `keepalive` is the fallback, and a failure is silent -- a
 * reporter that logs its own failure to the console is noise in the one place
 * a reader might look.
 */
export function sendReport(
  report: ClientReport | null,
  transport: ReportTransport = {},
): boolean {
  if (!report) {
    return false;
  }
  const body = JSON.stringify(report);
  const beacon =
    transport.beacon ??
    (typeof navigator !== "undefined" && typeof navigator.sendBeacon === "function"
      ? navigator.sendBeacon.bind(navigator)
      : undefined);
  try {
    if (beacon && beacon(REPORT_ENDPOINT, new Blob([body], { type: "application/json" }))) {
      return true;
    }
    const send = transport.fetchImpl ?? (typeof fetch === "function" ? fetch : undefined);
    if (!send) {
      return false;
    }
    void send(REPORT_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      keepalive: true,
      cache: "no-store",
    }).catch(() => {});
    return true;
  } catch {
    // A browser that refuses to report is not a browser that should then show
    // the reader an error about reporting.
    return false;
  }
}
