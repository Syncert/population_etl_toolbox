import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-024 — the data-quality explorer in the browser. The published
// rollup is presented without a score, four distinct source states stay
// distinct, an unpublished time reads as unknown rather than recent,
// per-metric quality shows the publisher's own fields with unpublished ones
// stated, quality evidence links back to the explorer, and evidence the API
// publishes elsewhere is pointed at rather than fabricated here.

const freshness = {
  total: 4,
  items: [
    {
      source_code: "CENSUS_ACS",
      metric_count: 10,
      current_count: 10,
      stale_count: 0,
      retired_count: 0,
      latest_publication_time: "2026-09-01T00:00:00Z",
      latest_harvested_at: "2026-09-02T00:00:00Z",
    },
    {
      source_code: "BLS",
      metric_count: 8,
      current_count: 5,
      stale_count: 3,
      retired_count: 0,
      latest_publication_time: "2026-08-01T00:00:00Z",
      latest_harvested_at: "2026-08-02T00:00:00Z",
    },
    {
      // Six metrics carry no published freshness state at all.
      source_code: "CDC",
      metric_count: 10,
      current_count: 3,
      stale_count: 0,
      retired_count: 1,
      latest_publication_time: "2026-07-01T00:00:00Z",
      latest_harvested_at: null,
    },
    {
      // Metrics exist but nothing establishes when they were published.
      source_code: "USDA_NASS",
      metric_count: 4,
      current_count: 4,
      stale_count: 0,
      retired_count: 0,
      latest_publication_time: null,
      latest_harvested_at: null,
    },
  ],
};

const blsMetrics = [
  {
    metric_code: "BLS:LAU:UNEMP_RATE",
    metric_display_name: "Unemployment rate",
    source_code: "BLS",
    freshness_state: "fresh",
    publication_time: "2026-08-01T00:00:00Z",
    harvested_at: "2026-08-02T00:00:00Z",
    source_watermark: "2026-07",
    publisher_contract_version: "v3",
  },
  {
    metric_code: "BLS:LAU:LABOR_FORCE",
    metric_display_name: "Labor force",
    source_code: "BLS",
    // The publisher published no freshness, watermark, or contract version.
    publication_time: "2026-08-01T00:00:00Z",
  },
];

async function installRoutes(page) {
  await page.route("**/api/v1/catalog/freshness", (route) => route.fulfill({ json: freshness }));
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const source = new URL(route.request().url()).searchParams.get("source_code");
    const items = source === "BLS" ? blsMetrics : [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
}

test("the published rollup is presented without a score", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/quality");

  const screen = page.getByTestId("quality-explorer");
  await expect(screen).toHaveAttribute("data-source-count", "4");

  // Four distinct facts, each read from the published counts.
  await expect(page.getByTestId("quality-state-CENSUS_ACS")).toContainText(
    "10 current of 10 published metrics",
  );
  await expect(page.getByTestId("quality-state-BLS")).toContainText("3 stale of 8");
  // A metric the rollup left unclassified is not thereby current.
  await expect(page.getByTestId("quality-state-CDC")).toContainText(
    "6 of 10 metrics carry no published freshness state",
  );
  // Metrics exist but nothing establishes when they were published, so this
  // never reads as healthy.
  await expect(page.getByTestId("quality-state-USDA_NASS")).toContainText(
    "no publication time published",
  );
  await expect(page.getByTestId("quality-state-USDA_NASS")).toHaveClass(/pill warn/);
  await expect(page.getByTestId("quality-row-CDC")).toContainText("Not published");

  // Every value in the rollup is a published count or a published time:
  // no percentage, score, or grade stands in for them.
  const rollup = await page.locator('[data-testid^="quality-row-"]').allInnerTexts();
  for (const row of rollup) {
    expect(row).not.toMatch(/%|score|grade/i);
  }
  // The page names the score it does not publish, so absence is not read as
  // a clean bill of health.
  await expect(page.getByTestId("unpublished-evidence")).toContainText("quality score");
  await expect(page.getByTestId("unpublished-evidence")).toContainText(
    "client-authored judgement",
  );
});

test("per-metric quality shows the publisher's own fields and links back", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/quality");

  await page.getByTestId("quality-select-BLS").click();
  await expect(page.getByTestId("quality-explorer")).toHaveAttribute(
    "data-selected-source",
    "BLS",
  );
  await expect(page.getByTestId("quality-metrics-panel")).toBeVisible();

  const published = page.getByTestId("quality-metric-BLS:LAU:UNEMP_RATE");
  await expect(published).toContainText("fresh");
  await expect(published).toContainText("2026-07");
  await expect(published).toContainText("v3");

  // A field the publisher did not publish reads as not published, never as
  // a placeholder that would state something the source did not.
  const partial = page.getByTestId("quality-metric-BLS:LAU:LABOR_FORCE");
  await expect(partial).toContainText("Not published");

  // Quality evidence links back to the context it affects.
  await expect(page.getByTestId("quality-explore-BLS:LAU:UNEMP_RATE")).toHaveAttribute(
    "href",
    /metric=BLS%3ALAU%3AUNEMP_RATE/,
  );
});

test("evidence the rollup does not carry is pointed at, not fabricated", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/quality");

  const locations = page.getByTestId("evidence-locations");
  await expect(locations).toContainText("/observations/releases");
  await expect(locations).toContainText("value_status");
  // Each meaning states what the evidence is not, so a null is never read
  // as a zero.
  await expect(locations).toContainText("never a zero");
  await expect(locations).toContainText("not zero crime");
  await expect(locations).toContainText("publisher_contract_version");
});
