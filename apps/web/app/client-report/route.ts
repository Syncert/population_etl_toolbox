import { NextResponse } from "next/server";

import {
  MAX_BODY_BYTES,
  buildId,
  formatReportLine,
  parseReport,
  reportsFromBrowserPayload,
} from "../../lib/clientReport";

// The sink: where the browser's own account of what went wrong lands.
//
// It is a Next route handler rather than an API route because the CSP says
// `connect-src 'self'` and this application does not widen its CSP to gain a
// log line. It sits outside `/api` and `/tiles`, which `next.config.mjs`
// rewrites away to the API and Martin, so a same-origin POST reaches this
// process and is written to this container's stdout. stdout is the store:
// nothing here samples, batches or persists.
//
// Three rules it does not break:
//
// 1. **It answers 204 to everything, including a report it refuses.** A
//    reporter that is told "400" retries, and a retrying reporter against a
//    broken sink is a loop that runs in every reader's browser at once. The
//    refusal is the operator's business, not the page's.
// 2. **It re-validates.** Anything can POST to a same-origin path; the
//    reporters are not a trust boundary, and `parseReport` is where the
//    closed shape is enforced rather than in the code that builds it.
// 3. **It reads a bounded body.** A report is a few hundred bytes. A body
//    past the bound is refused before it is parsed, not after.
//
// It holds no state, so it needs no runtime beyond the default and answers
// every request the same way twice.

export const dynamic = "force-dynamic";

/** 204 with no body, which is the only answer this route ever gives. */
function accepted(): NextResponse {
  return new NextResponse(null, { status: 204 });
}

export async function POST(request: Request): Promise<NextResponse> {
  const declared = request.headers.get("content-length");
  if (declared !== null && Number(declared) > MAX_BODY_BYTES) {
    return accepted();
  }

  let raw: string;
  try {
    raw = await request.text();
  } catch {
    return accepted();
  }
  // Checked again after reading: `content-length` is a claim, not a fact.
  if (raw.length > MAX_BODY_BYTES) {
    return accepted();
  }

  let payload: unknown;
  try {
    payload = JSON.parse(raw);
  } catch {
    return accepted();
  }

  const contentType = request.headers.get("content-type") || "";
  const build = buildId();

  // A browser's own violation report, or one of this application's. Never
  // both from one request. The content type decides which reader runs, and
  // `application/csp-report` does not contain the word "json" -- which is
  // exactly the kind of thing a shortcut here would get wrong.
  const browserReports = reportsFromBrowserPayload(contentType, payload, build);
  const found = browserReports.length > 0 ? browserReports : [parseReport(payload)];

  for (const report of found) {
    if (report) {
      // One line, on stdout, where `docker compose logs web` reads it.
      console.log(formatReportLine(report));
    }
  }

  return accepted();
}

// Every other method: the same silence. A reporting endpoint that answers
// `405` to a GET tells a scanner it exists and tells an operator nothing.
export async function GET(): Promise<NextResponse> {
  return accepted();
}
