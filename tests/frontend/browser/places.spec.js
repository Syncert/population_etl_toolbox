import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-125 — place-pages: the nation, a state, and a county render the shared chapters
// over the published API, with three-level cards, stated omissions, state
// context for county safety, and no horizontal scroll at 390px.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

async function noHorizontalScroll(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}

test("the nation's page reads every chapter it publishes and lists the states", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("United States");
  await expect(page.getByTestId("chapter-rail").getByRole("link")).toHaveText(["People", "Work and Money", "Housing", "Health", "Safety", "Land and Farms", "Change", "What the record shows", "Starting a family"]);
  // LAUS publishes no national rate; the card says the nation's is another survey's.
  await expect(page.getByTestId("card-unemployment-rate-national")).toContainText("Not published nationally by LAUS");
  await expect(page.getByTestId("card-unemployment-rate-national")).toContainText("Current Population Survey");
  await expect(page.getByTestId("place-children").getByRole("link", { name: "Wisconsin" })).toHaveAttribute("href", "/us/wisconsin");
  await expect(page.getByTestId("chapter-people-trend")).toHaveAttribute("data-scale", "index");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a state page says which headline measures publish no state grain", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Wisconsin");
  // CDC PLACES publishes counties and the nation; the ACS insurance counts in
  // the same chapter publish states, so the chapter stays and the card says so.
  await expect(page.getByTestId("card-obesity-state")).toContainText("Not published at state grain");
  await expect(page.getByTestId("card-obesity-national")).toContainText("%");
  await expect(page.getByTestId("card-population-estimate-state")).toContainText("persons");
  await expect(page.getByTestId("card-population-estimate-national")).toBeVisible();
  await expect(page.getByTestId("place-children").getByRole("link", { name: "Dane County" })).toHaveAttribute("href", "/us/wisconsin/dane-county");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a county page draws three levels, labels state context, and states its gaps", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Dane County, Wisconsin");

  const card = page.getByTestId("card-population-estimate");
  await expect(card.locator("tbody tr")).toHaveCount(3);
  await expect(card).toHaveAttribute("data-period", "2024-01-01 – 2024-12-31");
  // The nation's newest median age is a different year: said, not shown.
  await expect(page.getByTestId("card-median-age-national")).toContainText("Not published for 2024-01-01 – 2024-12-31 (newest published: 2023-01-01 – 2023-12-31)");
  await expect(page.getByTestId("card-median-age-county")).toContainText("margin of error");

  const safety = page.getByTestId("chapter-safety");
  await expect(safety).toHaveAttribute("data-read-grain", "STATE");
  // The FBI publishes no county rate; the county's is computed from its own
  // agencies' reports and says whom it covers (ADR-0012, WEB-150).
  await expect(page.getByTestId("chapter-safety-state-context")).toContainText("The figures for Wisconsin and the nation are the FBI's");
  await expect(page.getByTestId("card-violent-crime-rate-county")).toContainText("computed from agency reports");
  await expect(page.getByTestId("card-violent-crime-rate-state")).toContainText("(state context)");

  // NASS publishes no Dane County cell in the fixture.
  await expect(page.getByTestId("chapter-land-farms")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("Land and Farms: no published values for this place");

  await expect(page.getByTestId("depth-living-alone")).toContainText("Universe: households");
  await expect(page.getByTestId("chapter-people-map")).toHaveAttribute("href", "/map/CENSUS_PEP%3APOPESTIMATE");
  await expect(page.getByTestId("chapter-safety-map")).toHaveCount(0);
  await expect(page.getByTestId("chapter-people-footer")).toContainText("Source: CENSUS_PEP, CENSUS_ACS");
  const explore = page.getByTestId("chapter-people-explore");
  const href = new URL(await explore.getAttribute("href"), "http://localhost");
  expect(href.pathname).toBe("/explore");
  expect(href.searchParams.get("metric")).toBe("CENSUS_PEP:POPESTIMATE");
  expect(href.searchParams.get("geo_level")).toBe("COUNTY");
  expect(href.searchParams.get("state")).toBe("55");

  await noViolations(page);
  await noHorizontalScroll(page);
  await expect(page.getByTestId("chapter-rail")).toBeVisible();
});

// Covers: WEB-125 — a county page shows how it voted beside its state, says
// how a margin is counted and why the nation has no figure, and draws the
// margins on one level scale.
test("a county page's Voting chapter reads the margins beside the state's", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const voting = page.getByTestId("chapter-voting");
  await expect(voting).toBeVisible();
  await expect(page.getByTestId("chapter-rail").getByRole("link", { name: "Voting" })).toBeVisible();
  const card = page.getByTestId("card-president-margin");
  await expect(card).toHaveAttribute("data-period", "2024-11-07");
  await expect(card).toContainText("below zero, the Democrat led");
  await expect(page.getByTestId("card-president-margin-county")).toContainText(/[-−]6\b/);
  await expect(page.getByTestId("card-president-margin-state")).toContainText(/[-−]15\b/);
  await expect(page.getByTestId("card-president-margin-national")).toContainText("The nation's popular vote is not loaded");
  await expect(page.getByTestId("chapter-voting-trend")).toHaveAttribute("data-scale", "level");
  await expect(voting).toContainText("never as a cause");
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-125 — a chapter shows its values as its own reads settle: a
// slow batch for another chapter leaves only that chapter reading (OFFICE_LAB
// 2026-11-08: the Voting cards read "Reading…" for 55 s behind Census and
// BEA batches of 5-10 s).
test("a slow read for one chapter does not hold another chapter's cards", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await installPlaceFixtures(page);
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  await page.route("**/api/v1/observations/newest/batch?**", async (route) => {
    const codes = new URL(route.request().url()).searchParams.get("metric_codes") || "";
    if (codes.includes("B19013_001")) await held;
    await route.fallback();
  });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-president-margin-county")).toContainText(/[-−]6\b/);
  await expect(page.getByTestId("chapter-voting-trend")).toHaveAttribute("data-scale", "level");
  await expect(page.getByTestId("card-median-household-income-county")).toContainText("Reading…");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "false");
  release();
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("card-median-household-income-county")).not.toContainText("Reading…");
});

// Covers: WEB-164 — a headline card draws its own place's history as a
// sparkline, read in one history batch per chapter for that place only, with
// a change line in the record page's verbs.
test("a county's headline cards carry a sparkline of the county's own history", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  const batches = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/observations/history/batch") batches.push(url);
  });
  await ready(page, "/us/wisconsin/dane-county");
  const spark = page.getByTestId("card-bea-per-capita-income-spark");
  await expect(spark).toHaveAttribute("data-points", "6");
  await expect(spark.locator("figcaption")).toContainText("2019");
  await expect(page.getByTestId("card-bea-per-capita-income-spark-change")).toHaveText("Rose from 1,000 in 2019.");
  await expect(page.getByTestId("card-median-household-income-spark-change")).toHaveText(
    "Not distinguishable from 41 in 2019 at the published margins of error.",
  );
  // The sparkline-only reads ask the county alone; a trend's batch asks every level.
  const own = batches.filter((url) => url.searchParams.get("geo_ids") === "state:55|county:025");
  expect(own.length).toBeGreaterThan(0);
  expect(own.some((url) => url.searchParams.get("metric_codes").includes("BEA:CAINC1:3"))).toBe(true);
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-162 — a headline card draws its levels on one scale, margins as
// whiskers, and says how the place compares in the record page's verbs; a
// count draws no strip and keeps its table.
test("a county's headline cards place the county beside its state and the nation on one scale", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const age = page.getByTestId("card-median-age");
  await expect(page.getByTestId("card-median-age-strip").locator(".level-strip-row:not(.level-strip-axis)")).toHaveCount(2);
  await expect(page.getByTestId("card-median-age-strip-sentence")).toHaveText(
    "Dane County was not distinguishable from Wisconsin at the published margins of error.",
  );
  // The table stays for screen readers and copying, out of sight.
  expect((await age.locator("table").boundingBox()).width).toBeLessThanOrEqual(1);
  await expect(page.getByTestId("card-median-age-county")).toContainText("margin of error");
  await expect(page.getByTestId("card-unemployment-rate-strip-sentence")).toHaveText("Dane County was lower than Wisconsin.");
  await expect(page.getByTestId("card-population-estimate-strip")).toHaveCount(0);
  await expect(page.getByTestId("card-population-estimate").locator("table")).toBeVisible();
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-139 — sub-county-geography: a county page paints one ACS
// measure over its tracts, counts the tract with no value instead of
// painting it as zero, and lists every tract in a table.
test("within this county paints tracts, counts the uncoloured one, and tabulates every tract", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.route(/\/tiles\/tracts$/, (route) =>
    route.fulfill({
      json: {
        name: "tracts",
        tiles: ["http://internal-martin:3000/tracts/{z}/{x}/{y}"],
        vector_layers: [{ id: "tracts", fields: { geo_id: "String", geo_level: "String", state_fips: "String", county_fips: "String" } }],
      },
    }),
  );
  await page.route(/\/tiles\/tracts\/\d+\/\d+\/\d+/, (route) =>
    route.fulfill({ status: 200, contentType: "application/vnd.mapbox-vector-tile", body: Buffer.alloc(0) }),
  );
  await ready(page, "/us/wisconsin/dane-county");
  const section = page.getByTestId("within-county");
  await expect(section).toContainText("Within this county");
  await expect(page.getByTestId("within-county-table").locator("tbody tr")).toHaveCount(3);
  await expect(page.getByTestId("within-county-row-state:55|county:025|tract:000202")).toContainText("Not published for this tract");
  await expect(page.getByTestId("within-county-row-state:55|county:025|tract:000100")).toContainText("60,000");
  await expect(page.getByTestId("within-county-uncoloured")).toHaveText(
    "1 of 3 tracts have no published value and are left uncoloured, not shown as zero.",
  );
  await expect(page.getByTestId("within-county-map")).toHaveAttribute("data-colored-values", "2");
  await page.getByTestId("within-county-measure").selectOption("CENSUS_ACS:acs5:B25064_001");
  // The caption names the release's period between the measure and the grain.
  await expect(page.getByTestId("within-county-table").locator("caption")).toHaveText(
    /^Median gross rent, .+ by Census tract$/,
  );
});

// Covers: WEB-137 — bea-regional-accounts: BEA per capita income and real GDP
// sit beside the ACS income, each labelled with how it was counted, and the
// earnings table states a withheld sector.
test("BEA income and GDP sit beside the survey income, labelled apart, with earnings by industry", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-bea-per-capita-income")).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("card-bea-per-capita-income-basis")).toHaveText("BEA personal income account, current dollars");
  await expect(page.getByTestId("card-median-household-income-basis")).toHaveText("Survey estimate (American Community Survey)");
  await expect(page.getByTestId("card-bea-real-gdp-basis")).toHaveText("BEA county GDP, chained 2017 dollars");
  await expect(page.getByTestId("card-median-household-income")).not.toContainText("BEA:");

  const earnings = page.getByTestId("bea-earnings");
  await expect(earnings.locator("tbody tr")).toHaveCount(20);
  await expect(page.getByTestId("bea-earnings-basis")).toContainText("BEA personal income account");
  await expect(page.getByTestId("bea-earnings-bea-earnings-1600")).toContainText("Health care and social assistance");
  // WEB-163: bars sorted largest first; a withheld sector has no bar and is
  // named once in the line under them.
  await expect(page.getByTestId("bea-earnings-bea-earnings-200")).toHaveText("Mining, quarrying, and oil and gas extraction.");
  await expect(page.getByTestId("bea-earnings-bea-earnings-200").locator("xpath=..")).toContainText("Withheld by the publisher to protect confidential employer data:");
  const bea_earningsValues = (await page.getByTestId("bea-earnings").locator(".place-industry-value").allTextContents()).map((text) => Number(text.replace(/,/g, "")));
  expect(bea_earningsValues).toEqual([...bea_earningsValues].sort((left, right) => right - left));
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("never combined with the survey's income");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a state page has no within-this-county section", async ({ page }) => {
  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("within-county")).toHaveCount(0);
});

// Covers: WEB-144 — a reader never sees a provider's label path or a
// variable code as a name, on any level's page; codes stay in attributes.
test("no place page shows an Estimate!! label path or a bare variable code", async ({ page }) => {
  for (const path of ["/us", "/us/wisconsin", "/us/wisconsin/dane-county", "/us/wisconsin/crossing-city"]) {
    await ready(page, path);
    const text = await page.locator("main").innerText();
    expect(text, path).not.toMatch(/Estimate!!/);
    const code = text.match(/.{0,60}\b[BC]\d{5}_\d{3}[EM]?\b.{0,20}/);
    expect(code?.[0] ?? null, path).toBeNull();
  }
});

// Covers: WEB-135 — bls-qcew-county-wages: the county page shows jobs located
// here beside residents who work, each labelled with how it was counted, and
// an industry mix of private jobs that states a withheld sector.
test("jobs located here sit beside residents who work, labelled apart, with an industry mix", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-qcew-jobs")).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("card-qcew-jobs-basis")).toHaveText("Jobs located here, counted by employers (QCEW)");
  await expect(page.getByTestId("card-unemployment-rate-basis")).toHaveText("Residents who work, counted where they live (LAUS)");
  await expect(page.getByTestId("card-qcew-jobs").locator("[data-metric-code]")).toHaveAttribute("data-metric-code", "BLS_QCEW:employment:10:0");
  await expect(page.getByTestId("card-qcew-jobs-county")).toContainText("jobs");
  await expect(page.getByTestId("card-unemployment-rate")).not.toContainText("BLS_QCEW");

  const mix = page.getByTestId("industry-mix");
  await expect(mix.locator("tbody tr")).toHaveCount(20);
  await expect(page.getByTestId("industry-mix-basis")).toContainText("counted by employers (QCEW)");
  await expect(page.getByTestId("industry-mix-qcew-sector-62")).toContainText("Health care and social assistance");
  // WEB-163: bars sorted largest first; a withheld sector has no bar and is
  // named once in the line under them.
  await expect(page.getByTestId("industry-mix-qcew-sector-21")).toHaveText("Mining, quarrying, and oil and gas extraction.");
  await expect(page.getByTestId("industry-mix-qcew-sector-21").locator("xpath=..")).toContainText("Withheld by the publisher to protect confidential employer data:");
  const industry_mixValues = (await page.getByTestId("industry-mix").locator(".place-industry-value").allTextContents()).map((text) => Number(text.replace(/,/g, "")));
  expect(industry_mixValues).toEqual([...industry_mixValues].sort((left, right) => right - left));
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("never added or subtracted");
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-136 — census-building-permits: homes authorized sit beside the
// housing stock, labelled as authorizations, with what was reported directly
// and a monthly trend.
test("homes authorized are labelled as authorizations, with what was reported and a monthly trend", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const card = page.getByTestId("card-bps-single-family-year");
  await expect(card).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("card-bps-single-family-year-basis")).toHaveText("Authorized by building permits, not started or completed");
  await expect(page.getByTestId("card-bps-single-family-year-reported")).toContainText("Reported directly by permit offices");
  await expect(page.getByTestId("card-bps-single-family-year-reported")).toContainText("the Bureau estimates the rest");
  await expect(page.getByTestId("card-median-gross-rent")).not.toContainText("Authorized");
  await expect(page.getByTestId("chapter-housing-trend")).toBeVisible();
  await expect(page.getByTestId("chapter-housing-trend-bps-single-family-month")).toBeVisible();
  await expect(page.getByTestId("chapter-housing-footer")).toContainText("not started or completed");
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-133 — census-saipe-sahie: the county page shows the SAIPE and
// SAHIE cards beside the ACS cards, each labelled with how it was made, with
// its interval, and neither one standing in for the other.
test("model-based SAIPE and SAHIE cards sit beside the survey cards with their own labels", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");

  const survey = page.getByTestId("card-median-household-income");
  const model = page.getByTestId("card-saipe-median-household-income");
  await expect(survey).toHaveAttribute("data-available", "true");
  await expect(model).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("card-median-household-income-basis")).toHaveText("Survey estimate (American Community Survey)");
  await expect(page.getByTestId("card-saipe-median-household-income-basis")).toHaveText("Model-based annual estimate (SAIPE)");
  await expect(model.locator("[data-metric-code]")).toHaveAttribute("data-metric-code", "CENSUS_SAIPE_SAHIE:saipe:SAEMHI");
  await expect(survey).not.toContainText("CENSUS_SAIPE_SAHIE");
  await expect(page.getByTestId("card-saipe-median-household-income-county")).toContainText("margin of error");
  await expect(page.getByTestId("card-saipe-median-household-income-county")).toContainText("confidence lower");
  // The margin's percentage is the served one; a field the source does not
  // publish arrives null and is never printed as 0 (UAT: "±2,947 (0%)").
  await expect(page.getByTestId("card-saipe-median-household-income-county")).toContainText("(3.22%)");
  await expect(page.getByTestId("card-saipe-median-household-income-county")).not.toContainText("(0%)");
  await expect(page.getByTestId("card-obesity-county")).not.toContainText("margin of error");
  await expect(page.getByTestId("card-saipe-poverty-rate-basis")).toHaveText("Model-based annual estimate (SAIPE)");
  await expect(page.getByTestId("depth-saipe-child-poverty-rate-basis")).toContainText("Model-based annual estimate (SAIPE)");
  await expect(page.getByTestId("depth-below-poverty-basis")).toContainText("Survey estimate (American Community Survey)");
  // WEB-163: what the depth rows share is said once above them; a row says
  // its source only where it differs from that line.
  await expect(page.getByTestId("chapter-work-money-depth-shared")).toContainText("Survey estimate (American Community Survey)");
  await expect(page.getByTestId("chapter-work-money-depth-shared")).toContainText("± is the 90% margin of error");
  await expect(page.getByTestId("depth-below-poverty-basis")).toHaveClass("sr-only");
  await expect(page.getByTestId("depth-saipe-child-poverty-rate-basis")).toHaveClass("place-basis");
  await expect(page.getByTestId("depth-below-poverty")).not.toContainText("margin of error ±");

  await expect(page.getByTestId("card-sahie-uninsured-rate-basis")).toHaveText("Model-based annual estimate (SAHIE)");
  await expect(page.getByTestId("card-sahie-uninsured-rate-county")).toContainText("confidence upper");
  await expect(page.getByTestId("depth-uninsured-35-64-basis")).toContainText("Survey estimate (American Community Survey)");
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("CENSUS_SAIPE_SAHIE");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a SAIPE slot the catalog does not serve says it is not loaded yet and borrows no ACS figure", async ({ page }) => {
  await installPlaceFixtures(page, { withoutSource: "CENSUS_SAIPE_SAHIE" });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("card-saipe-median-household-income")).toHaveAttribute("data-available", "false");
  await expect(page.getByTestId("card-saipe-median-household-income")).toContainText("Not loaded into this site yet.");
  await expect(page.getByTestId("card-median-household-income")).toHaveAttribute("data-available", "true");
});

// Covers: WEB-138 — irs-county-migration: the People chapter lists where
// people came from and went, states SOI's withheld category, and the Change
// chapter says beside PEP net migration that the two count different
// populations.
test("migration lists name origins and destinations and say the populations differ", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const inflow = page.getByTestId("migration-inflow");
  await expect(inflow).toContainText("Where people came from");
  await expect(inflow.locator("ol li")).toHaveCount(3);
  await expect(page.getByTestId("migration-inflow-state:27|county:053")).toContainText("Hennepin County");
  await expect(page.getByTestId("migration-outflow")).toContainText("Where people went");
  await expect(page.getByTestId("migration-inflow-withheld")).toContainText("Foreign, other flows is withheld by SOI to protect taxpayers, not zero");
  await expect(page.getByTestId("change-migration-note")).toContainText("The two measure different populations");
  await expect(page.getByTestId("migration-population-note")).toContainText("no net figure is computed");
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-138 — irs-county-migration: the flows are read for the page's
// county only. While a county address is still resolving the chapters hold
// only the state, and the page once read `/migration-flows?geo_id=state:55`,
// which the API refuses with 422, on every Dane County load; a city page
// never reads them at all.
test("migration flows are asked for the county, never its state", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await installPlaceFixtures(page);
  // Hold the state's county list so the chapters render before the county is known.
  await page.route("**/api/v1/catalog/geographies?*", async (route) => {
    if (new URL(route.request().url()).searchParams.get("geo_level") === "COUNTY") {
      await new Promise((resolve) => setTimeout(resolve, 1500));
    }
    return route.fallback();
  });
  const asked = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/migration-flows") asked.push(url.searchParams.get("geo_id"));
  });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("migration-inflow")).toContainText("Where people came from");
  expect([...new Set(asked)]).toEqual(["state:55|county:025"]);
  asked.length = 0;
  await page.goto("/us/wisconsin/madison-city");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  expect(asked).toEqual([]);
});

test("an address by FIPS settles on the named address", async ({ page }) => {
  await ready(page, "/us/55/025");
  await expect(page).toHaveURL(/\/us\/wisconsin\/dane-county$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Dane County, Wisconsin");
});

test("an unknown place is a not-found page with search and no chart", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/atlantis-county");
  await expect(page.getByTestId("place-not-found")).toBeVisible();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("There is no place at this address");
  await expect(page.locator(".place-trend, .place-card")).toHaveCount(0);
  await page.getByRole("searchbox", { name: "Search places" }).fill("rock");
  await expect(page.getByTestId("place-search-results").getByRole("link", { name: "Rock County, Wisconsin", exact: true })).toHaveAttribute("href", "/us/wisconsin/rock-county");
  await noViolations(page);

  const response = await page.goto("/us/Not%20A%20Place");
  expect(response.status()).toBe(404);
  await expect(page.getByTestId("route-not-found")).toBeVisible();
});

test("nearby and related: a place crossing two counties is noted on both pages", async ({ page }) => {
  // Covers: WEB-130 — the Nearby section from the relationship resource.
  await ready(page, "/us/wisconsin/dane-county");
  const nearby = page.getByTestId("place-nearby");
  await expect(nearby.getByTestId("place-nearby-within")).toContainText("Crossing city");
  await expect(nearby.getByTestId("place-nearby-within")).toContainText("60% of it lies in this county; the rest is in another county");
  await expect(nearby.getByTestId("place-nearby-neighbours").getByRole("link", { name: "Rock County, Wisconsin", exact: true })).toHaveAttribute("href", "/us/wisconsin/rock-county");
  // A neighbour in another state is addressed by its FIPS, which settles on its name.
  await expect(nearby.getByTestId("place-nearby-neighbours").getByRole("link", { name: "Hennepin County, Minnesota", exact: true })).toHaveAttribute("href", "/us/minnesota/27053");
  await expect(nearby.getByTestId("place-nearby-part-of").getByRole("link").and(page.locator(':not([data-testid="place-nearby-compare"])'))).toHaveText(["Wisconsin", "Madison, WI", "United States"]);
  // Its metro links to the metro's page by code (WEB-153).
  await expect(nearby.getByTestId("place-nearby-part-of").getByRole("link", { name: "Madison, WI", exact: true })).toHaveAttribute("href", "/metro/31540");
  // Each related place links its comparison (WEB-146).
  await expect(nearby.getByTestId("place-nearby-part-of").getByTestId("place-nearby-compare")).toHaveCount(3);
  await expect(nearby).toContainText("vintage 2025");

  await ready(page, "/us/wisconsin/rock-county");
  await expect(page.getByTestId("place-nearby-within")).toContainText("40% of it lies in this county; the rest is in another county");
});

// Covers: WEB-134 — acs-place-grain: a city or town has its own page in the
// same chapters, read at place grain, with the counties it lies in.
test("a city page reads its own figures, notes the counties it crosses, and omits what no source publishes", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/crossing-city");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-level", "PLACE");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-geo-id", "state:55|place:99999");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Crossing city, Wisconsin");
  await expect(page.locator(".section-kicker").first()).toHaveText("City or town");

  // Four levels: the place, the county holding most of it, its state, the
  // nation.
  const income = page.getByTestId("card-median-household-income");
  await expect(income.locator("tbody tr")).toHaveCount(4);
  await expect(income).toContainText("Dane County");
  await expect(page.getByTestId("card-median-household-income-place")).toContainText("Crossing city");
  await expect(page.getByTestId("card-median-household-income-place")).toContainText("margin of error");
  // BLS publishes no place: said in the card, not borrowed from a county.
  await expect(page.getByTestId("card-unemployment-rate-place")).toContainText("Not published at city or town grain");

  await expect(page.getByTestId("place-cross-county")).toHaveText(
    "Crossing city crosses county lines: it lies in Dane County (60%) and Rock County (40%). A county's figures describe the whole county, not this place.",
  );
  const counties = page.getByTestId("place-nearby-counties");
  await expect(counties.getByRole("link", { name: "Dane County, Wisconsin", exact: true })).toHaveAttribute("href", "/us/wisconsin/dane-county");
  // Its metro, reached through the counties it lies in, with the share inside.
  const metro = page.getByTestId("place-nearby-part-of").locator('li[data-geo-id="cbsa:31540"]');
  await expect(metro).toHaveText("Madison, WI · 60% of this place lies in its counties · compare");
  await expect(metro.getByRole("link", { name: "Madison, WI", exact: true })).toHaveAttribute("href", "/metro/31540");

  await expect(page.getByTestId("chapter-safety")).toHaveCount(0);
  await expect(page.getByTestId("chapter-land-farms")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("Safety: no published city or town values for this place");
  await expect(page.getByTestId("place-omissions")).toContainText("Land and Farms: no published city or town values for this place");
  await expect(page.getByTestId("place-children")).toHaveCount(0);

  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a city addressed by its FIPS settles on its name, and a county links to it", async ({ page }) => {
  // Covers: WEB-134 — the seven-digit address the county page links with.
  await ready(page, "/us/wisconsin/5599999");
  await expect(page).toHaveURL(/\/us\/wisconsin\/crossing-city$/);
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-nearby-within").getByRole("link", { name: "Crossing city, Wisconsin", exact: true })).toHaveAttribute("href", "/us/wisconsin/5599999");
  await ready(page, "/us/wisconsin/madison-city");
  await expect(page.getByTestId("place-cross-county")).toHaveText("Madison city lies in Dane County.");
});

test("a place with no recorded relationships omits the section and says why", async ({ page }) => {
  // Covers: WEB-130 — an empty answer is stated, not an empty section.
  await ready(page, "/us/minnesota/hennepin-county");
  await expect(page.getByTestId("place-nearby")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("Nearby and related: the geography reference records no relationships for this place");
});

test("what stands out lists the highest and lowest measures apart, with withheld siblings named", async ({ page }) => {
  // Covers: WEB-131 — one measure per row; a withheld sibling is counted, not zero.
  // Covers: WEB-165 — each rank drawn as a position; caveats said once.
  await ready(page, "/us/wisconsin/dane-county");
  const section = page.getByTestId("place-standout");
  await expect(section.getByTestId("place-standout-count")).toContainText("7 measures could be ranked among Wisconsin counties, each on its own");
  await expect(section.getByTestId("place-standout-highest").locator("li")).toHaveCount(3);
  await expect(section.getByTestId("place-standout-lowest").locator("li")).toHaveCount(3);
  const income = section.getByTestId("standout-CENSUS_ACS:acs5:B19013_001");
  await expect(income).toContainText("Higher than 68 of 70 Wisconsin counties with a published value (1 withheld a value)");
  // WEB-165: the measure's name opens its map; the dot sits at its rank,
  // lowest to highest; the sources' caveats are said once, not per row.
  await expect(income.getByRole("link")).toHaveAttribute("href", "/map/CENSUS_ACS%3Aacs5%3AB19013_001");
  const dot = await income.locator(".standout-track i").getAttribute("style");
  expect(Number(/left: ([\d.]+)%/.exec(dot)[1])).toBeCloseTo((68 / 70) * 100, 1);
  await expect(section.locator(".standout-track")).toHaveCount(6);
  const caveats = await section.getByTestId("place-standout-caveats").locator("p").allTextContents();
  expect(new Set(caveats).size).toBe(caveats.length);
  // Named by the API's plain label (WEB-144), not the provider's.
  await expect(section.getByTestId("place-standout-lowest")).toContainText("Median age");
  await expect(section.getByTestId("place-standout-lowest")).not.toContainText("Median age (UI fixture)");
  await expect(section).not.toContainText(/score|overall/i);
});

test("what stands out names a bulk-file rate in words and keeps its long unit inside the panel", async ({ page }) => {
  // Covers: WEB-167 — PEP's provider label is the bare variable ("Rdeath");
  // the row reads the registry's plain name, and "per 1,000 people" wraps
  // under the number instead of running past the panel edge.
  await page.setViewportSize({ width: 390, height: 900 });
  await ready(page, "/us/wisconsin/dane-county");
  const death = page.getByTestId("standout-CENSUS_PEP:RDEATH");
  await expect(death.getByRole("link")).toHaveText("Death rate");
  await expect(death).not.toContainText("Rdeath");
  const value = death.locator(".standout-value");
  await expect(value).toContainText("7.1");
  await expect(value).toContainText("per 1,000 people");
  await expect(value).toHaveAttribute("title", "7.1 per 1,000 people");
  const fits = await value.evaluate((element) => {
    const row = element.closest("li").getBoundingClientRect();
    const box = element.getBoundingClientRect();
    return element.scrollWidth <= element.clientWidth + 1 && box.right <= row.right + 1;
  });
  expect(fits).toBe(true);
});

test("a place with no ranked measure omits What stands out and says why", async ({ page }) => {
  // Covers: WEB-131 — an empty answer is stated in the footer.
  await ready(page, "/us/wisconsin/rock-county");
  await expect(page.getByTestId("place-standout")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("What stands out: no measure could be ranked for this place");
});

// Covers: WEB-143 — groceries-and-gas-cards: each figure on the card names
// the larger area and source it describes, falls through to a larger area
// with the reason stated, and reads the CPI only as a change over a year.
test("a county's groceries and gas card names the area behind every figure", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const card = page.getByTestId("place-groceries-gas");
  await expect(card).toContainText("No source publishes grocery or gas prices for a county");
  const gas = page.getByTestId("place-cost-gas");
  await expect(gas).toHaveAttribute("data-area", "region:2");
  await expect(gas).toContainText("$3.05 a gallon");
  await expect(gas).toContainText("Midwest Region figure");
  await expect(gas).toContainText("Bureau of Labor Statistics");
  await expect(gas).toContainText("Wisconsin (EIA): no value published for this area.");
  const food = page.getByTestId("place-cost-food");
  await expect(food).toHaveAttribute("data-area", "region:2");
  await expect(food).toContainText("up 3.2% over the year");
  await expect(food).toContainText("2025-08 is not published");
  await expect(food).not.toContainText("index 1982-84");
  const parity = page.getByTestId("place-cost-parity");
  await expect(parity).toHaveAttribute("data-area", "cbsa:31540");
  await expect(parity).toContainText("97.4 (nation = 100)");
  await expect(parity).toContainText("Madison, WI figure");
  await expect(parity).toContainText("Bureau of Economic Analysis");

  // Covers: WEB-143 — cost-of-living-section: everyday prices in one table,
  // the region named once, an item only the nation publishes named on its
  // row, an item not loaded said so, and rent linked to Housing.
  await expect(card.getByRole("heading", { level: 2 })).toHaveText("Cost of living");
  await expect(page.getByTestId("place-cost-items-area")).toContainText("Midwest Region figure, urban average, except where a row names another area");
  const eggs = page.getByTestId("place-cost-eggs");
  await expect(eggs).toHaveAttribute("data-area", "region:2");
  await expect(eggs).toContainText("$3.18 a dozen");
  await expect(eggs).toContainText("up 6.0% over the year");
  await expect(page.getByTestId("place-cost-eggs-spark")).toBeVisible();
  // Electricity is EIA's Wisconsin price, named on its row; the others BLS's region.
  const electricity = page.getByTestId("place-cost-electricity");
  await expect(electricity).toHaveAttribute("data-area", "state:55");
  await expect(electricity).toContainText("18.0\u00a2 a kilowatt-hour");
  await expect(electricity).toContainText("Wisconsin figure, U.S. Energy Information Administration");
  const chicken = page.getByTestId("place-cost-chicken");
  await expect(chicken).toHaveAttribute("data-area", "us:1");
  await expect(chicken).toContainText("United States figure");
  await expect(page.getByTestId("place-cost-milk")).toContainText("Not loaded into this site yet.");
  // Rent is the Housing chapter's, linked, never read a second time.
  await expect(page.getByTestId("place-cost-rent").getByRole("link")).toHaveAttribute("href", "#housing");

  // Covers: WEB-143 — cost-of-living-section CL-3: minutes of work are read
  // only when chosen, at the county's own wage, stated once with its year.
  const minutesReads = [];
  page.on("request", (request) => { if (request.url().includes("/observations/derived")) minutesReads.push(new URL(request.url())); });
  const view = page.getByTestId("place-cost-view");
  await expect(view.getByRole("button", { name: "Price" })).toHaveAttribute("aria-pressed", "true");
  await view.getByRole("button", { name: "Minutes of work" }).click();
  await expect(page.getByTestId("place-cost-eggs-minutes")).toContainText("6 minutes");
  await expect(page.getByTestId("place-cost-eggs-minutes")).toContainText("for $3.18 a dozen");
  // Minutes of work divide BLS's region price, which has a wage pairing.
  await expect(page.getByTestId("place-cost-electricity-minutes")).toContainText("20 seconds");
  await expect(page.getByTestId("place-cost-electricity-minutes")).toContainText("for 17.0\u00a2 a kilowatt-hour");
  await expect(page.getByTestId("place-cost-chicken-minutes")).toContainText("9 minutes");
  await expect(page.getByTestId("place-cost-chicken-minutes")).toContainText("United States figure");
  await expect(page.getByTestId("place-cost-wage")).toContainText("Dane County's average wage, $1,200 a week in 2025, the newest year published");
  await expect(page.getByTestId("place-cost-items").getByRole("columnheader", { name: "Minutes of work" })).toBeVisible();
  expect(minutesReads.map((url) => url.searchParams.get("wage_geo_id"))).toEqual(Array(3).fill("state:55|county:025"));
  expect(minutesReads.map((url) => url.searchParams.get("ratio_id")).sort()).toEqual([
    "minutes_of_work_chicken:0000", "minutes_of_work_eggs:0200", "minutes_of_work_electricity:0200",
  ]);
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-170 — home-heating-fuel-prices: natural gas is the state's own
// price restated a therm, heating oil falls back to EIA's petroleum region
// with its season said, propane not published says so, and diesel is a
// second figure on the gasoline line.
test("a county's home heating fuels name the area, season and unit behind each price", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const heat = page.getByTestId("place-cost-heat_gas");
  await expect(heat).toHaveAttribute("data-area", "state:55");
  await expect(heat).toContainText("$12.72 per thousand cubic feet ($1.23 a therm)");
  await expect(heat).toContainText("Wisconsin figure");
  await expect(heat).toContainText("up 6.0% over the year");
  await expect(heat).toContainText("10.37 therms");
  const oil = page.getByTestId("place-cost-heating_oil");
  await expect(oil).toHaveAttribute("data-area", "area:eia:R20");
  await expect(oil).toContainText("$3.64 a gallon");
  await expect(oil).toContainText("Midwest (PADD 2) figure");
  await expect(oil).toContainText("up 4.0% over the year");
  await expect(oil).toContainText("October to March");
  await expect(oil).toContainText("Wisconsin (EIA): no value published for this area.");
  await expect(page.getByTestId("place-cost-propane")).toContainText("Not published for this place or any area containing it.");
  const diesel = page.getByTestId("place-cost-diesel");
  await expect(diesel).toHaveAttribute("data-area", "us:1");
  await expect(diesel).toContainText("Diesel $3.71 a gallon");
  await expect(diesel).toContainText("United States figure");
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-171 — county-childcare-prices: a county page shows its own
// eight childcare prices for one study year as published, each with its
// hours of the county's own wage and the arithmetic stated, marks none
// imputed in 2022, says a blank price is not published, and a state page
// says the prices are published for counties only.
test("a county's childcare prices are its own, for one year, with a blank price said to be missing", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const childcare = page.getByTestId("place-childcare");
  await expect(page.getByTestId("place-childcare-headline")).toContainText("A childcare center charges $293 a week for an infant in Dane County, the median full-time price in 2022");
  // Hours of work beside the dollars, never instead of them, the arithmetic
  // stated once: $293 over Dane County's $1,200 week of 40 hours.
  await expect(page.getByTestId("place-childcare-headline-work")).toHaveText("9 hours 46 minutes of work");
  await expect(page.getByTestId("place-childcare-center_infant-work")).toHaveText("9 hours 46 minutes of work");
  await expect(page.getByTestId("place-childcare-center_infant")).toContainText("$293 a week");
  await expect(page.getByTestId("place-childcare-formula")).toContainText(
    "Dane County's average wage in 2022 was $1,200 a week (Bureau of Labor Statistics, QCEW, all jobs located in the county), or $30.00 an hour over a 40-hour week. $293 a week for center-based infant care ÷ $30.00 an hour = 9.8 hours of work.",
  );
  await expect(page.getByTestId("place-childcare-home_school_age-work")).toHaveCount(0);
  await expect(page.getByTestId("place-childcare-work-gap")).toHaveCount(0);
  const table = page.getByTestId("place-childcare-table");
  await expect(table).toContainText("Median weekly full-time price, 2022");
  await expect(table).toContainText("Women's Bureau, National Database of Childcare Prices");
  await expect(page.getByTestId("place-childcare-center_toddler")).toContainText("$258.75 a week");
  await expect(page.getByTestId("place-childcare-center_toddler")).toHaveAttribute("data-imputed", "false");
  await expect(page.getByTestId("place-childcare-home_infant")).toContainText("$220 a week");
  const blank = page.getByTestId("place-childcare-home_school_age");
  await expect(blank).toHaveAttribute("data-missing", "true");
  await expect(blank).toContainText("Not published for 2022.");
  await expect(blank).not.toContainText("$150");
  await expect(page.getByTestId("place-childcare-imputed")).toHaveCount(0);
  await expect(childcare.getByRole("columnheader", { name: "Childcare center" })).toBeVisible();
  await noViolations(page);
  await noHorizontalScroll(page);

  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("place-childcare-counties-only")).toContainText("publishes childcare prices for counties only");
  await expect(page.getByTestId("place-childcare-table")).toHaveCount(0);
});

// Covers: WEB-173 — starting-a-family-collection: a county page gathers
// eight published measures in four groups, each with its own value, period,
// levels and rank; the two prices show hours of the county's own wage with
// the arithmetic stated; no line is called good or bad; a state page says
// where a county-only figure is published instead of showing one.
test("Starting a family gathers its lines, each standing alone, with hours of work for the prices", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const family = page.getByTestId("chapter-family");
  await expect(family.getByRole("heading", { name: "Starting a family" })).toBeVisible();
  await expect(page.getByTestId("chapter-rail").getByRole("link", { name: "Starting a family" })).toBeVisible();
  await expect(page.getByTestId("family-infant-care-value")).toHaveText("$293 a week");
  // $293 over Dane County's $1,200 week of 40 hours, the price's own year.
  await expect(page.getByTestId("family-infant-care-hours")).toHaveText("9 hours 46 minutes of work at Dane County's average wage");
  await expect(page.getByTestId("family-infant-care-rank")).toContainText("Higher than 40 of 70 Wisconsin counties with a published value");
  await expect(page.getByTestId("family-rent-3br-value")).toHaveText("$2,000 a month");
  await expect(page.getByTestId("family-rent-3br-hours")).toHaveText("66 hours 40 minutes of work at Dane County's average wage");
  await expect(page.getByTestId("family-hours-formula")).toContainText(
    "$2,000 a month ÷ (Dane County's average wage in 2025, the newest year QCEW has published, $1,200 a week ÷ 40 hours = $30.00 an hour) = 66.7 hours for a month's three-bedroom rent.",
  );
  await expect(page.getByTestId("family-weekly-wage-value")).toHaveText("$1,200 a week");
  await expect(page.getByTestId("family-students-per-teacher")).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("family-households-with-children-value")).toContainText("%");
  for (const word of ["easier", "harder", "better", "worse", "score"]) await expect(family).not.toContainText(word);
  await noViolations(page);
  await noHorizontalScroll(page);

  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("family-infant-care-gap")).toHaveText("Published by county; the Women's Bureau publishes no state or national figure");
  await expect(page.getByTestId("family-infant-care-hours")).toHaveCount(0);
  await expect(page.getByTestId("family-weekly-wage-value")).toHaveText("$1,100 a week");
});

// Covers: WEB-172 — grocery-spending: a county shows its Census region's
// yearly household grocery spending, named as the region's and as spending,
// not a price, with the nation's figures by household size.
test("a county's grocery spending is its region's, named, with the nation's household sizes", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const spending = page.getByTestId("place-cost-spending");
  await expect(spending).toHaveAttribute("data-area", "region:2");
  await expect(spending).toContainText("Household grocery spending (food at home)");
  await expect(spending).toContainText("$5,846 a year");
  await expect(spending).toContainText("Midwest Region figure, 2024");
  await expect(spending).toContainText("It is spending, not a price");
  await expect(page.getByTestId("place-cost-spending-sizes")).toContainText(
    "Nationally, by household size (2024): one person $3,612, two people $6,120, three $7,310, four $8,905, five or more $10,214 a year.",
  );
  await noViolations(page);
  await noHorizontalScroll(page);
});

// Covers: WEB-143 — CL-3: a county with no wage loaded says so, never zero;
// a metro has no minutes-of-work choice, and says why.
test("minutes of work say when no wage is loaded, and a metro says QCEW has none", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/rock-county");
  await page.getByTestId("place-cost-view").getByRole("button", { name: "Minutes of work" }).click();
  // Only the nation's chicken price reaches Rock County in this fixture.
  await expect(page.getByTestId("place-cost-chicken-minutes")).toContainText("No wage loaded for this place yet.");
  await expect(page.getByTestId("place-cost-chicken-minutes")).not.toContainText("0 minutes");
  await ready(page, "/metro/madison-wi-metro-area");
  await expect(page.getByTestId("place-cost-view")).toHaveCount(0);
  await expect(page.getByTestId("place-cost-no-wage")).toContainText("not metro areas");
});

test("the nation's groceries and gas card is its own, with parity 100 by definition", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us");
  await expect(page.getByTestId("place-cost-no-local")).toHaveCount(0);
  await expect(page.getByTestId("place-cost-gas")).toContainText("$3.18 a gallon · United States,");
  await expect(page.getByTestId("place-cost-gas")).toContainText("U.S. Energy Information Administration");
  await expect(page.getByTestId("place-cost-food")).toContainText("up 2.0% over the year");
  await expect(page.getByTestId("place-cost-parity")).toContainText("100 by definition");
});

// Covers: WEB-153 — web-metro-pages: a metro has its own page in the same
// chapters, beside its principal state and the nation, resolved in one read.
test("a metro page reads its own figures beside its state and the nation and lists its counties", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  const asked = [];
  page.on("request", (request) => { if (request.url().includes("/api/v1/")) asked.push(new URL(request.url()).pathname); });
  await ready(page, "/metro/madison-wi-metro-area");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-level", "METRO");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-geo-id", "cbsa:31540");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Madison, WI Metro Area");
  await expect(page.locator(".section-kicker").first()).toHaveText("Metro area");
  await expect(page.getByRole("navigation", { name: "Place hierarchy" }).getByRole("link", { name: "Wisconsin" })).toHaveAttribute("href", "/us/wisconsin");

  // Three levels: the metro, its principal state, the nation.
  const income = page.getByTestId("card-median-household-income");
  await expect(income.locator("tbody tr")).toHaveCount(3);
  await expect(page.getByTestId("card-median-household-income-metro")).toContainText("margin of error");
  await expect(page.getByTestId("card-unemployment-rate-metro")).toContainText("Percent");
  // BEA's metro income is summed from the counties, and the card says so.
  await expect(page.getByTestId("card-bea-per-capita-income-metro-derivation")).toContainText("Derived: its member counties' summed income");
  await expect(page.getByTestId("card-bea-per-capita-income-state-derivation")).toHaveCount(0);
  // A count published for counties and not for metros is its counties' sum,
  // and says it was derived (WEB-159).
  await expect(page.getByTestId("card-population-estimate-metro")).toContainText("(derived from member counties)");
  await expect(page.getByTestId("chapter-health")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("Health: no published metro area values for this place");

  // Covers: WEB-159 — a measure published for counties and not for metro
  // areas lists the member counties; a count is summed and says so, a
  // median or a chained-dollar figure is not.
  await expect(page.getByTestId("card-qcew-jobs-metro")).toContainText("1,100 jobs (derived from member counties)");
  await expect(page.getByTestId("card-qcew-jobs-members")).toContainText("Dane County");
  await expect(page.getByTestId("card-qcew-jobs-members-note")).toContainText("the sum of its 1 member county's published figures");
  await expect(page.getByTestId("card-saipe-median-household-income-metro")).toContainText("its member counties are listed below");
  await expect(page.getByTestId("card-saipe-median-household-income-members").locator('tr[data-geo-id="state:55|county:025"]')).toContainText("dollars");
  await expect(page.getByTestId("card-saipe-median-household-income-members-note")).toContainText("not added up from its counties");
  await expect(page.getByTestId("card-bea-real-gdp-metro")).toContainText("its member counties are listed below");
  await expect(page.getByTestId("card-bea-real-gdp-members")).toContainText("Dane County");
  // A measure published for metros reads its own row, with no member list.
  await expect(page.getByTestId("card-median-household-income-members")).toHaveCount(0);

  // Prices: BEA's price level is the metro's own; gas and food are larger areas'.
  await expect(page.getByTestId("place-cost-no-local")).toContainText("The price level is BEA's figure for this metro");
  await expect(page.getByTestId("place-cost-parity")).toHaveAttribute("data-area", "cbsa:31540");

  const members = page.getByTestId("place-metro-members");
  await expect(members.getByRole("link", { name: "Dane County, Wisconsin", exact: true })).toHaveAttribute("href", "/us/wisconsin/dane-county");
  await expect(members).toContainText("2023 CBSA delineation");
  await expect(page.getByTestId("place-metro-states")).toHaveCount(0);
  await expect(page.getByTestId("place-compare-with").getByRole("link", { name: "the United States" })).toHaveAttribute("href", "/compare/places?a=cbsa%3A31540&b=us%3A1");

  // One metro read; no ranking among every metro and no relationship read.
  expect(asked.filter((path) => path.startsWith("/api/v1/catalog/geographies/metros/"))).toHaveLength(1);
  expect(asked.filter((path) => path === "/api/v1/place/distinctive" || path.endsWith("/related"))).toEqual([]);
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a metro's CBSA code settles on its named address, and an unknown metro is not found", async ({ page }) => {
  await ready(page, "/metro/31540");
  await expect(page).toHaveURL(/\/metro\/madison-wi-metro-area$/);
  await installPlaceFixtures(page);
  await page.goto("/metro/nowhere-metro-area");
  await expect(page.getByTestId("place-not-found")).toContainText("No metro or micro area is published under that name or code.");
});

test("a state page lists the metros reaching into it", async ({ page }) => {
  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("place-state-metros").getByRole("link", { name: "Madison, WI Metro Area" })).toHaveAttribute("href", "/metro/madison-wi-metro-area");
});
