// One navigation against a deployed web container, so a vitals report is
// actually sent by a browser.
//
// The client-report sink is a Next route handler: a report is a POST to
// `/client-report` in the web process, written to that container's stdout
// (`app/client-report/route.ts`). Every other tier proves a piece of that and
// not the path. The unit tier builds payloads with no browser. The browser
// tier drives a real Chromium but against `next dev` on the host, where there
// is no container and therefore no container log. So nothing answered the
// question a deployment actually asks -- does a reader's browser reach the
// sink, and does the sink write the line where an operator reads it
// (DEPLOY-011).
//
// This script is the missing half: it drives one navigation, waits until the
// container has answered a report, and leaves. The assertion about *what* was
// reported is `docker compose logs web`, in the job, because the claim is
// about what the container recorded and not about what this process saw.
//
// Usage: node scripts/report-a-vital.mjs http://127.0.0.1:33100

import { chromium } from "@playwright/test";

const REPORT_PATH = "/client-report";
// `useReportWebVitals` delivers Next's own hydration and render metrics as
// soon as the page settles; the Core Web Vitals that wait for page-hide are
// not needed here and are not waited for.
const TIMEOUT_MS = Number(process.env.VITALS_TIMEOUT_MS || 60_000);

const baseUrl = (process.argv[2] || process.env.WEB_BASE_URL || "").replace(/\/$/, "");
if (!baseUrl) {
  console.error("usage: node scripts/report-a-vital.mjs <web-origin>");
  process.exit(2);
}

const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
const browser = await chromium.launch(executablePath ? { executablePath } : {});
let failure = null;
try {
  const context = await browser.newContext();
  const page = await context.newPage();

  // Watched on the wire as well as in the log, because the two failures need
  // telling apart: a browser that never sent a report and a sink that never
  // wrote one are different bugs, and only this side can see the first.
  //
  // The *response* is what is counted, not the request. `sendReport` prefers
  // `navigator.sendBeacon`, and a beacon is fire-and-forget: seeing the
  // request leave says nothing about whether the container received it. A
  // 204 is the sink's own answer, and the sink logs the line before it
  // answers -- so once one has arrived, the log has already been written and
  // the grep that follows is not racing it.
  //
  // What this cannot see is *which kind* was sent. Playwright reports a
  // beacon's body as null on both `postData()` and `postDataBuffer()`, and
  // making the kind visible here would mean forcing the `fetch` fallback --
  // grading a transport production does not use. So the kind is the log's
  // business, which is where the job asserts it.
  const accepted = [];
  page.on("response", (response) => {
    const request = response.request();
    if (request.url().includes(REPORT_PATH) && request.method() === "POST") {
      accepted.push(response.status());
    }
  });

  const response = await page.goto(`${baseUrl}/`, {
    waitUntil: "domcontentloaded",
    timeout: TIMEOUT_MS,
  });
  if (!response || !response.ok()) {
    throw new Error(
      `the web container answered ${response ? response.status() : "nothing"} for /`,
    );
  }

  const deadline = Date.now() + TIMEOUT_MS;
  while (accepted.length === 0 && Date.now() < deadline) {
    await page.waitForTimeout(250);
  }

  if (accepted.length === 0) {
    throw new Error(
      `no client report reached ${REPORT_PATH} within ${TIMEOUT_MS}ms, so the ` +
        `browser reported nothing and the container has nothing to log`,
    );
  }
  const refused = accepted.filter((status) => status !== 204);
  if (refused.length > 0) {
    throw new Error(
      `the sink answered ${refused.join(", ")}; it answers 204 to everything, ` +
        `including a report it refuses, so anything else is the route breaking`,
    );
  }
  console.log(`${accepted.length} client report(s) accepted at ${REPORT_PATH}`);
} catch (error) {
  failure = error;
} finally {
  await browser.close();
}

if (failure) {
  console.error(String(failure && failure.message ? failure.message : failure));
  process.exit(1);
}
