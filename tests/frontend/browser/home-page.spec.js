import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-112 — the home page states what the catalog answered and
// nothing else.
//
// It hard-coded one Census variable as its featured metric, so the feature
// was a client-authored choice of provider dressed as the catalog's answer --
// and the explorer link it built named that code even when the catalog had
// never published it. Two cells of the signal strip read "County / national
// map coverage" and "Live / API-backed observations": the first is a claim
// about which grains the warehouse publishes, which varies by source, and the
// second is a claim about the deployment's health that this page never
// checked.

const sources = [
  { source_code: "FRED", source_name: "Federal Reserve Economic Data" },
  { source_code: "BLS", source_name: "Bureau of Labor Statistics" },
];

/** A catalog whose first answer is deliberately not the old hard-coded one. */
const metrics = {
  total: 4210,
  items: [
    {
      metric_code: "FRED:UNRATE",
      metric_display_name: "Unemployment rate",
      source_code: "FRED",
      units: "percent",
    },
    {
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      metric_display_name: "Total population",
      source_code: "CENSUS_ACS",
      units: "people",
    },
  ],
};

async function installRoutes(page, { emptyCatalog = false } = {}) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sources }));
  await page.route("**/api/v1/catalog/metrics?*", (route) =>
    route.fulfill({ json: emptyCatalog ? { total: 0, items: [] } : metrics }),
  );
}

test("the featured link opens the metric the catalog answered first", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/");

  // The catalog's first answer, which is not the code this page used to
  // prefer -- so a link naming that code would be this client's choice.
  const featured = page.getByTestId("home-featured-link");
  await expect(featured).toHaveAttribute("href", /metric=FRED%3AUNRATE/);
  await expect(featured).not.toHaveAttribute("href", /B01003_001/);
});

test("an empty catalog offers the catalog, not a metric nobody published", async ({ page }) => {
  await installRoutes(page, { emptyCatalog: true });
  await page.goto("/");

  await expect(page.getByTestId("home-featured-link")).toHaveAttribute("href", "/catalog");
});

test("the signal strip states only what the catalog answered", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/");

  const strip = page.getByRole("region", { name: "Platform signals" });
  // Two counts, both from the answers this page just read.
  await expect(strip).toContainText("2");
  await expect(strip).toContainText("connected sources");
  await expect(strip).toContainText("4,210");

  // And nothing the API did not say.
  await expect(strip).not.toContainText("national map coverage");
  await expect(strip).not.toContainText("API-backed observations");
});
