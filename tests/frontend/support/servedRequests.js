// A Playwright `test` that watches what the client actually sends.
//
// API-093 made the API refuse a query parameter the matched route does not
// declare, because a misspelling used to be answered with a confident, wrong
// page. That turns any undeclared name the web application sends into a 422
// the user sees, and nothing here checked for one: WEB-043 reads the reviewed
// snapshot so each fixture *declares* what its routes accept, but the browser
// tier serves every request from a `page.route` stub, and a stub answers
// whatever arrives. The tier that drives the real client against real URLs
// never looked at the URLs (WEB-052).
//
// `page.on("request")` sees every request the page makes, intercepted or not,
// so the evidence was already flowing past. Specs import `test` and `expect`
// from here instead of from Playwright; the fixture is automatic, so a spec
// added later inherits the check without opting in.

import { expect, test as base } from "../../../apps/web/node_modules/@playwright/test/index.mjs";
import { requestComplaint } from "./servedContract.js";

// WEB-144: a reader sees the API's plain labels (A-2), never a provider's
// label path ("Estimate!!Total:") or an ACS variable code ("B19013_001").
// Every browser test ends by reading what its page shows, so a screen added
// later that prints either fails here without opting in. Codes stay legal in
// attributes (`data-metric-code`, `title`), links and form values; this reads
// only the rendered text, less any `data-provenance` record.
export const RAW_LABEL_PATTERNS = [
  { name: "a provider label path", pattern: /Estimate!!|Margin of Error!!/ },
  { name: "an ACS variable code", pattern: /\b[BCS]\d{5}[A-Z]{0,3}_\d{3}[EM]?\b/ },
];

export function rawLabelComplaints(text) {
  return RAW_LABEL_PATTERNS.flatMap(({ name, pattern }) => {
    const match = String(text || "").match(pattern);
    if (!match) return [];
    const start = Math.max(0, match.index - 50);
    return [`${name}: …${text.slice(start, match.index + match[0].length + 20)}…`];
  });
}

export const test = base.extend({
  plainLabels: [
    async ({ page }, use) => {
      await use(undefined);
      let text = "";
      try {
        // A provenance record (an evidence envelope, a stored request, the
        // API's verdict quoted as given) names the exact identities it holds;
        // it is the one place a code is printed, marked `data-provenance`.
        text = page.isClosed()
          ? ""
          : await page.locator("body").evaluate((body) => {
              const copy = body.cloneNode(true);
              copy.querySelectorAll("[data-provenance], script, style, noscript").forEach((node) => node.remove());
              copy.style.position = "absolute";
              copy.style.left = "-100000px";
              document.documentElement.appendChild(copy);
              const shown = copy.innerText;
              copy.remove();
              return shown;
            });
      } catch {
        // A page that never rendered has no text to read.
        return;
      }
      expect(rawLabelComplaints(text), "the page shows a raw provider label or variable code").toEqual([]);
    },
    { auto: true },
  ],
  servedRequests: [
    async ({ page }, use) => {
      const complaints = [];
      page.on("request", (request) => {
        let complaint = null;
        try {
          complaint = requestComplaint(request.url());
        } catch (error) {
          complaint = `${request.url()}: ${error.message}`;
        }
        if (complaint && !complaints.includes(complaint)) {
          complaints.push(complaint);
        }
      });
      await use(complaints);
      expect(complaints, "the client sent parameters the API does not declare").toEqual(
        [],
      );
    },
    { auto: true },
  ],
});

export { expect };
