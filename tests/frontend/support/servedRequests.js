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

export const test = base.extend({
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
