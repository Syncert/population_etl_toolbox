// Covers: WEB-124, API-163 — the deployed UI reads real repaired FBI mappings.
// This tier starts no services and installs no response fixtures.
import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");
const COUNTY = "state:55|county:025";
const PRODUCT = "summarized_violent_crime";
const REVIEW_PATH = `/use-cases/public-safety-trend?place=${encodeURIComponent(COUNTY)}`;

test("deployed county crime rows retain real values, lineage, and release-pinned paging", async ({ page }, testInfo) => {
  expect(BASE_URL).not.toBe("");
  const requests = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/api/v1/crime/county-rollup") {
      requests.push(new URL(request.url()));
    }
  });
  await page.goto(`${BASE_URL}${REVIEW_PATH}`);
  const rollup = page.getByTestId("county-crime-rollup");
  await expect(rollup).toBeVisible();
  expect(requests).toHaveLength(0);

  const responsePromise = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/crime/county-rollup",
  );
  await rollup.getByRole("button", { name: "Load derived county roll-up" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(200);
  const result = await response.json();
  expect(result.derived).toBe(true);
  expect(result.items.length).toBeGreaterThan(0);
  await expect(rollup.locator("tbody tr")).toHaveCount(result.items.length);
  const first = result.items[0];
  expect(first.geo_id).toBe(COUNTY);
  expect(first.product_id).toBe(PRODUCT);
  expect(first.derived).toBe(true);
  const row = rollup.locator("tbody tr").first();
  await expect(row.locator("td").nth(0)).toHaveText(first.period);
  await expect(row.locator("td").nth(1)).toContainText("derived sum");
  expect((await row.locator("td").nth(2).innerText()).replaceAll(",", "")).toBe(`${first.value} count`);
  await expect(row.locator("td").nth(3)).toHaveText(`${first.reporting_agency_count} / ${first.mapped_agency_count}`);
  await expect(row.locator("td").nth(4)).toHaveText(first.contributing_oris.join(", "));
  await expect(rollup.getByTestId("county-rollup-coverage")).toContainText("never counted as zero");
  await expect(rollup.getByTestId("county-rollup-multi-county")).toContainText("not additive to state totals");
  await expect(rollup.locator("caption")).toContainText(first.release);

  await rollup.scrollIntoViewIfNeeded();
  await rollup.evaluate((element) => window.scrollTo(0, window.scrollY + element.getBoundingClientRect().top - 90));
  const screenshot = testInfo.outputPath("county-crime-rollup-review.png");
  await page.screenshot({ path: screenshot });
  await testInfo.attach("County roll-up on deployed warehouse", { path: screenshot, contentType: "image/png" });

  expect(result.total).toBeGreaterThan(result.limit);
  const nextResponsePromise = page.waitForResponse((next) => {
    const url = new URL(next.url());
    return url.pathname === "/api/v1/crime/county-rollup" && url.searchParams.get("offset") === String(result.limit);
  });
  await rollup.getByRole("button", { name: "Next page" }).click();
  const nextResponse = await nextResponsePromise;
  expect(nextResponse.status()).toBe(200);
  const next = await nextResponse.json();
  expect(next.offset).toBe(result.limit);
  expect(next.items.every((item) => item.release === first.release)).toBe(true);
  const nextRequest = new URL(nextResponse.url());
  expect(nextRequest.searchParams.get("release")).toBe(first.release);
  await expect(rollup.locator("tbody tr")).toHaveCount(next.items.length);
  const reproduction = new URL(await rollup.getByRole("link", { name: "Reproduce this roll-up in the API" }).getAttribute("href"), BASE_URL);
  expect(reproduction.searchParams.get("release")).toBe(first.release);
  expect(reproduction.searchParams.get("offset")).toBe(String(result.limit));
  await rollup.getByRole("button", { name: "Previous page" }).click();
  await expect(rollup.locator("tbody tr").first().locator("td").nth(0)).toHaveText(first.period);
  await expect(rollup.getByRole("button", { name: "Previous page" })).toBeDisabled();
});

test("deployed UI shows the real refusal for a county without an agency mapping", async ({ page }) => {
  expect(BASE_URL).not.toBe("");
  const county = "state:06|county:037";
  await page.goto(`${BASE_URL}/use-cases/public-safety-trend?place=${encodeURIComponent(county)}`);
  const rollup = page.getByTestId("county-crime-rollup");
  const responsePromise = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/crime/county-rollup",
  );
  await rollup.getByRole("button", { name: "Load derived county roll-up" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(404);
  const error = await response.json();
  expect(error.detail).toContain(`No law-enforcement agency is mapped to ${county}`);
  await expect(rollup.getByRole("status")).toHaveText(`status 404: ${error.detail}`);
  await expect(rollup.locator("table")).toHaveCount(0);
});
