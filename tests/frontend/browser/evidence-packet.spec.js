import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-023 — the evidence packet composer in the browser. The grant
// template arrives with methodology and limits already present, an
// analytical block can only be filled from a saved view that carries an
// envelope, that envelope stays visible on the block and in the preview,
// live and frozen blocks are distinguished, and a block still missing
// context is named rather than rendered as finished evidence.

const savedViews = [
  {
    id: "chart:1",
    title: "Dane County population",
    chartType: "choropleth",
    metricCode: "CENSUS_ACS:acs5:B01003_001",
    metricName: "Total population",
    source: "CENSUS_ACS",
    geoLevel: "COUNTY",
    geoId: "state:55|county:025",
    transformation: "raw",
    apiQuery: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
    period: "2023",
    savedAt: "2026-09-03T00:00:00Z",
  },
  {
    id: "chart:2",
    title: "Population as released 2022",
    chartType: "choropleth",
    metricCode: "CENSUS_ACS:acs5:B01003_001",
    source: "CENSUS_ACS",
    geoLevel: "COUNTY",
    scope: "as_released",
    release: "2022",
    period: "2022",
    apiQuery: "/api/v1/observations?scope=as_released&release=2022",
    savedAt: "2026-09-03T00:00:00Z",
  },
  {
    // A comparison captures no single period -- it carries one per side,
    // which WEB-049 exists to keep visible -- so its saved view records
    // none. Before WEB-069 the builder filled the gap with `savedAt`, and
    // this block would have claimed `2026-09-03T00:00:00Z` as its period
    // and reported itself complete.
    id: "chart:3",
    title: "Population against estimates",
    chartType: "comparison",
    metricCode: "CENSUS_ACS:acs5:B01003_001",
    metricCodeB: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
    source: "CENSUS_ACS",
    sourceB: "CENSUS_PEP",
    geoLevel: "COUNTY",
    transformation: "api-derived",
    apiQuery: "/api/v1/comparison?metric_code_a=CENSUS_ACS%3Aacs5%3AB01003_001",
    savedAt: "2026-09-03T00:00:00Z",
  },
];

async function seedSavedViews(page) {
  await page.addInitScript((views) => {
    window.localStorage.setItem("economic-data-studio:saved-charts:v1", JSON.stringify(views));
    window.localStorage.removeItem("economic-data-studio:builder-draft:v1");
  }, savedViews);
}

test("the grant template ships with its methodology and limits, and reports empty evidence", async ({
  page,
}) => {
  await seedSavedViews(page);
  await page.goto("/builder");

  const packet = page.getByTestId("evidence-packet");
  // Methodology and limits are part of the skeleton, not an appendix.
  await expect(page.getByTestId("block-methodology")).toBeVisible();
  await expect(page.getByTestId("block-limits")).toContainText(
    "do not establish that a program caused",
  );

  // The two analysis slots start empty and are named as such rather than
  // looking finished.
  await expect(packet).toHaveAttribute("data-complete", "false");
  await expect(packet).toHaveAttribute("data-issue-count", "2");
  await expect(page.getByTestId("issue-population-evidence")).toContainText(
    "no reproducibility envelope",
  );
  await expect(page.getByTestId("empty-population-evidence")).toContainText(
    "Attach a saved view",
  );
  await expect(page.getByTestId("packet-status")).toContainText("missing context");
});

test("an analytical block is filled from a saved view and keeps its envelope", async ({ page }) => {
  await seedSavedViews(page);
  await page.goto("/builder");

  // Only a saved view can fill an analytical block, because only a saved
  // view carries the envelope the block needs.
  await page.getByTestId("packet-target").selectOption("population-evidence");
  await page.getByTestId("packet-attach-chart:1").click();

  const block = page.getByTestId("block-population-evidence");
  await expect(block).toHaveAttribute("data-has-envelope", "true");
  const envelope = page.getByTestId("envelope-population-evidence");
  await expect(envelope).toContainText("CENSUS_ACS:acs5:B01003_001");
  await expect(envelope).toContainText("CENSUS_ACS");
  await expect(envelope).toContainText("state:55|county:025");
  await expect(envelope).toContainText("2023");
  await expect(envelope).toContainText("/api/v1/observations");

  // A latest-scope block is live and says what that means for the proposal.
  await expect(page.getByTestId("live-population-evidence")).toContainText("live");
  await expect(envelope).toContainText("change when the source republishes");
  await expect(page.getByTestId("reopen-population-evidence")).toHaveAttribute(
    "href",
    /metric=CENSUS_ACS%3Aacs5%3AB01003_001/,
  );

  // A pinned release is frozen, and is distinguished from the live block.
  await page.getByTestId("packet-target").selectOption("condition-evidence");
  await page.getByTestId("packet-attach-chart:2").click();
  await expect(page.getByTestId("live-condition-evidence")).toContainText(
    "frozen to release 2022",
  );
  await expect(page.getByTestId("envelope-condition-evidence")).toContainText(
    "will not change when the source republishes",
  );

  // With both blocks carrying an envelope the packet reports itself complete.
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-complete", "true");
  await expect(page.getByTestId("packet-issues")).toHaveCount(0);
});

test("a view that captured no period leaves the block incomplete, not complete", async ({
  page,
}) => {
  // Covers: WEB-069 — `envelopeFromSavedChart` filled an absent period with
  // `savedAt`, the moment someone pressed save. No producer wrote `period`
  // at all, so every block attached in the builder claimed a timestamp as
  // the period of its figures and the packet reported itself complete. The
  // module's own rule is that "a field the view never captured stays empty
  // so `packetIssues` can report it rather than a guess filling it in".
  await seedSavedViews(page);
  await page.goto("/builder");

  await page.getByTestId("packet-target").selectOption("population-evidence");
  await page.getByTestId("packet-attach-chart:3").click();

  const packet = page.getByTestId("evidence-packet");
  await expect(packet).toHaveAttribute("data-complete", "false");
  const issue = page.getByTestId("issue-population-evidence");
  await expect(issue).toContainText("period");
  // And the timestamp never reaches the envelope as a period.
  await expect(page.getByTestId("envelope-population-evidence")).not.toContainText(
    "2026-09-03T00:00:00Z",
  );
});

test("the preview keeps every block's evidence, and a new empty block reopens the gap", async ({
  page,
}) => {
  await seedSavedViews(page);
  await page.goto("/builder");
  await page.getByTestId("packet-target").selectOption("population-evidence");
  await page.getByTestId("packet-attach-chart:1").click();

  await page.getByTestId("packet-preview").click();
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-preview", "true");
  // The composer chrome goes; the envelope does not.
  await expect(page.getByTestId("packet-library")).toHaveCount(0);
  await expect(page.getByTestId("envelope-population-evidence")).toContainText("CENSUS_ACS");
  await expect(page.getByTestId("envelope-population-evidence")).toContainText("Period");

  await page.getByTestId("packet-preview").click();
  // A narrative block needs no envelope and raises no issue.
  await page.getByTestId("add-text").click();
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-issue-count", "1");
  await expect(page.getByTestId("issue-condition-evidence")).toBeVisible();
});
