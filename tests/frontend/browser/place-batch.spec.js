import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-149 — a place page names its measures in one catalog read and
// reads each chapter's newest values as one batch per set of places, the
// same values the single reads drew, and falls back to the single reads when
// the batch cannot be read.

function apiCalls(page) {
  const calls = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith("/api/v1/")) calls.push(url);
  });
  return calls;
}

async function ready(page, path) {
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("place-observation-status")).toContainText("published series read");
}

test("a county page asks one catalog read and one batch per chapter's places", async ({ page }) => {
  await installPlaceFixtures(page);
  const calls = apiCalls(page);
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-population-estimate")).toContainText("persons");

  const named = calls.filter((url) => url.pathname === "/api/v1/catalog/metrics");
  expect(named).toHaveLength(1);
  expect(named[0].searchParams.get("metric_codes").split(",").length).toBeGreaterThan(100);
  // The chapters' candidates are no longer looked up one code at a time.
  const perCode = calls.filter((url) => url.pathname.startsWith("/api/v1/catalog/metrics/"));
  expect(perCode.every((url) => /EIA|BLS%3AAPU|BLS%3ACUUR|BEA%3AMARPP/.test(url.pathname))).toBe(true);

  const batches = calls.filter((url) => url.pathname === "/api/v1/observations/newest/batch");
  expect(batches.length).toBeGreaterThan(0);
  expect(batches.length).toBeLessThan(20);
  const batched = new Set(
    batches.flatMap((url) => {
      const codes = url.searchParams.get("metric_codes").split(",");
      const places = url.searchParams.get("geo_ids").split(",");
      return codes.flatMap((code) => places.map((place) => `${code}|${place}`));
    }),
  );
  expect(batched.has("CENSUS_PEP:POPESTIMATE|state:55|county:025")).toBe(true);
  expect(batched.has("CENSUS_PEP:POPESTIMATE|us:1")).toBe(true);
  // No pair a batch answered is also read on its own.
  const singles = calls.filter(
    (url) => url.pathname === "/api/v1/observations" && url.searchParams.get("newest_per_geography") === "true",
  );
  for (const url of singles) {
    expect(batched.has(`${url.searchParams.get("metric_code")}|${url.searchParams.get("geo_id")}`)).toBe(false);
  }
});

test("when the batch cannot be read the page reads each value on its own", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.route("**/api/v1/observations/newest/batch**", (route) =>
    route.fulfill({ status: 503, json: { detail: "Database service is temporarily unavailable." } }),
  );
  const calls = apiCalls(page);
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-population-estimate")).toContainText("persons");
  const singles = calls.filter(
    (url) => url.pathname === "/api/v1/observations" && url.searchParams.get("newest_per_geography") === "true",
  );
  expect(singles.some((url) => url.searchParams.get("metric_code") === "CENSUS_PEP:POPESTIMATE")).toBe(true);
});

test("a batch item the single read would refuse is stated on its card", async ({ page }) => {
  await installPlaceFixtures(page, {
    onBatchItem: (item) => {
      if (item.metric_code === "CENSUS_PEP:POPESTIMATE" && item.geo_id === "state:55|county:025") {
        Object.assign(item, { status: 503, detail: "required serving relation is not present", observations: null });
      }
    },
  });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-observation-status")).toContainText("1 of");
  await expect(page.getByTestId("place-observation-status")).toContainText("reads failed");
  // The county's card states the refusal; its state and the nation still read.
  await expect(page.getByTestId("card-population-estimate")).toContainText("required serving relation is not present");
  await expect(page.getByTestId("card-population-estimate")).toContainText("11,000 persons");
});
