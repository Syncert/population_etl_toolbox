import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-015 — the catalog's search, filters, deterministic limit/offset
// paging over the API's published total, published provenance and freshness
// context, direct explorer links, and shareable catalog URLs, all against
// intercepted deterministic /api/v1 responses.

const PAGE_SIZE = 50;
const TOTAL = 127;

const sources = [
  { source_code: "CENSUS_ACS", source_name: "Census ACS" },
  { source_code: "CDC", source_name: "Centers for Disease Control and Prevention" },
];

// Deterministic metric rows carrying exactly the provenance the glossary
// publishes; the sparse one publishes almost none, and must not be filled in.
function metricAt(index) {
  return {
    metric_code: `ACS:acs5:B0${1000 + index}_001`,
    metric_display_name: `Measure ${index}`,
    source_code: "CENSUS_ACS",
    source_object_type: "acs5",
    units: "people",
    measure_kind: "count",
    aggregation_characteristic: "additive",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
    publisher_contract_version: "v3",
    source_watermark: "2023",
    publication_time: "2024-01-02T00:00:00Z",
    harvested_at: "2024-01-03T00:00:00Z",
    physical_lineage: { schema: "gold_census", relation: "rpt_acs_observations" },
    freshness_state: index === 0 ? "fresh" : "stale",
  };
}

const sparseMetric = {
  metric_code: "CDC:cdc_places_county:OBESITY",
  metric_display_name: "Obesity prevalence",
  source_code: "CDC",
  source_object_type: "cdc_places_county",
  units: "percent",
};

async function installRoutes(page, { requests = [] } = {}) {
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sources }));
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    requests.push(Object.fromEntries(params));

    if (params.get("source_code") === "CDC") {
      return route.fulfill({
        json: { total: 1, limit: PAGE_SIZE, offset: 0, items: [sparseMetric] },
      });
    }
    if (params.get("q")) {
      return route.fulfill({
        json: { total: 1, limit: PAGE_SIZE, offset: 0, items: [metricAt(0)] },
      });
    }

    const offset = Number(params.get("offset") || 0);
    const count = Math.max(0, Math.min(PAGE_SIZE, TOTAL - offset));
    return route.fulfill({
      json: {
        total: TOTAL,
        limit: PAGE_SIZE,
        offset,
        items: Array.from({ length: count }, (_, i) => metricAt(offset + i)),
      },
    });
  });
}

test("catalog paging is deterministic over the API's published total", async ({ page }) => {
  const requests = [];
  await installRoutes(page, { requests });
  await page.goto("/catalog");

  const catalog = page.getByTestId("catalog");
  await expect(catalog).toHaveAttribute("data-total", String(TOTAL));
  await expect(catalog).toHaveAttribute("data-shown", String(PAGE_SIZE));
  await expect(page.getByTestId("catalog-total")).toHaveText("127");
  await expect(page.getByTestId("catalog-range")).toContainText("showing 1-50");
  await expect(page.getByTestId("catalog-range")).toContainText("page 1 of 3");
  await expect(page.getByTestId("catalog-previous")).toBeDisabled();

  await page.getByTestId("catalog-next").click();
  // The range follows the answered offset, so it stays on the loaded rows
  // until the next page arrives rather than relabelling them.
  await expect(page.getByTestId("catalog-results")).toHaveAttribute("data-stale", "true");
  await expect(page.getByTestId("catalog-range")).toContainText("showing 51-100");
  await expect(page.getByTestId("catalog-results")).toHaveAttribute("data-stale", "false");
  expect(requests.at(-1)).toMatchObject({ limit: "50", offset: "50", active_only: "true" });

  // The last page is short, and the published total — not the short page —
  // decides that there is nothing after it.
  await page.getByTestId("catalog-next").click();
  await expect(catalog).toHaveAttribute("data-shown", "27");
  await expect(page.getByTestId("catalog-range")).toContainText("showing 101-127");
  await expect(page.getByTestId("catalog-next")).toBeDisabled();
  await expect(page).toHaveURL(/page=2/);

  // A shared catalog URL reopens the same page.
  await page.goto("/catalog?page=1");
  await expect(page.getByTestId("catalog-range")).toContainText("showing 51-100");
});

test("catalog filters reset paging and reach the resource unmodified", async ({ page }) => {
  const requests = [];
  await installRoutes(page, { requests });
  await page.goto("/catalog?page=2");
  await expect(page.getByTestId("catalog-range")).toContainText("page 3 of 3");

  await page.getByTestId("catalog-search").fill("population");
  await expect(page.getByTestId("catalog-total")).toHaveText("1");
  // Searching returned to the first page instead of an offset past the end.
  expect(requests.at(-1)).toMatchObject({ q: "population", offset: "0" });
  await expect(page).toHaveURL(/q=population/);
  await expect(page).not.toHaveURL(/page=/);

  await page.getByTestId("catalog-search").fill("");
  await page.getByTestId("catalog-include-retired").check();
  // active_only is dropped rather than sent as false, so retired metrics
  // are included by the resource's own default.
  await expect.poll(() => requests.at(-1).active_only).toBeUndefined();
  await expect(page).toHaveURL(/include_retired=1/);
});

test("published provenance, freshness, and explorer links are exact", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/catalog");

  const first = metricAt(0);
  await expect(page.getByTestId(`catalog-freshness-${first.metric_code}`)).toContainText("fresh");
  await expect(page.getByTestId(`catalog-freshness-${metricAt(1).metric_code}`))
    .toContainText("stale");
  await expect(page.getByText("gold_census.rpt_acs_observations").first()).toBeVisible();
  await expect(page.getByText("STATE, COUNTY").first()).toBeVisible();

  // The explorer link carries the metric identity, so the catalog opens a
  // reproducible view rather than a source-specific page.
  await expect(page.getByTestId(`catalog-metric-link-${first.metric_code}`))
    .toHaveAttribute("href", `/explore?metric=${encodeURIComponent(first.metric_code)}`);

  // A metric publishing almost no provenance shows only what it published,
  // and its unpublished freshness reads as unknown, never as healthy.
  await page.getByTestId("catalog-source-CDC").click();
  await expect(page.getByTestId("catalog-total")).toHaveText("1");
  await expect(page.getByTestId(`catalog-freshness-${sparseMetric.metric_code}`))
    .toContainText("freshness not published");
  await expect(page.getByText("Publisher contract")).toHaveCount(0);
  await expect(page.getByText("Harvested")).toHaveCount(0);
  await expect(page.getByText("percent").first()).toBeVisible();
});
