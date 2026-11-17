import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-112 — the home page states what the catalog answered and
// nothing else.
//
// It once hard-coded a featured Census variable and two signal cells the API
// never answered. The website UX review (2026-11, E1 to E3) then found the
// page turning from residents to analysts below the search: a count of
// "population matches", a featured analysis linking to an empty page, and a
// raw catalog name as a headline. What remains under the finder is a short
// row of analyst tools and the sources, counted and named once each, as the
// catalog answered them.

const sources = [
  { source_code: "FRED", source_name: "Federal Reserve Economic Data" },
  { source_code: "BLS", source_name: "Bureau of Labor Statistics" },
];

async function installRoutes(page, { sourceItems = sources } = {}) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sourceItems }));
}

test("the sources are counted and named once each, as the catalog answered", async ({ page }) => {
  // Three NOAA programs publish under one agency name.
  await installRoutes(page, {
    sourceItems: [
      ...sources,
      { source_code: "NOAA_NORMALS", source_name: "NOAA National Centers for Environmental Information" },
      { source_code: "NOAA_STORMS", source_name: "NOAA National Centers for Environmental Information" },
    ],
  });
  await page.goto("/");

  await expect(page.getByTestId("home-source-count")).toHaveText("3 public sources, each named beside its numbers");
  await expect(page.getByTestId("home-source-list").getByRole("listitem")).toHaveText([
    "Federal Reserve Economic Data",
    "Bureau of Labor Statistics",
    "NOAA National Centers for Environmental Information",
  ]);
});

test("below the finder the page offers analyst tools and nothing the API did not say", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/");

  const main = page.locator("main");
  await expect(main.getByRole("heading", { level: 1 })).toHaveText("What is going on in your place?");
  await expect(main.getByRole("link", { name: "Open the explorer" })).toHaveAttribute("href", "/explore");
  await expect(main.getByRole("link", { name: "Browse the catalog" })).toHaveAttribute("href", "/catalog");
  await expect(main.getByRole("link", { name: "Where the numbers come from" })).toHaveAttribute("href", "/data");
  // Gone: the metric search count, the featured analysis and the raw-named snapshot.
  await expect(main).not.toContainText("population matches");
  await expect(main).not.toContainText("Featured analysis");
  await expect(main).not.toContainText("National snapshot");
  await expect(main).not.toContainText("national map coverage");
  await expect(main).not.toContainText("API-backed observations");
  await expect(main).not.toContainText("briefing measure");
});

test("a failed source read is said, and no source is named", async ({ page }) => {
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ status: 503, json: { detail: "down" } }));
  await page.goto("/");

  await expect(page.getByTestId("home-status")).toContainText("temporarily unavailable");
  await expect(page.getByTestId("home-source-count")).toHaveText("Public sources, each named beside its numbers");
  await expect(page.getByTestId("home-source-list")).toContainText("unavailable");
});
