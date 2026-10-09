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
  await expect(page.getByTestId("chapter-rail").getByRole("link")).toHaveText(["People", "Work and Money", "Housing", "Health", "Safety", "Land and Farms", "Change"]);
  // BLS publishes no national unemployment rate in this catalog.
  await expect(page.getByTestId("card-unemployment-rate-national")).toContainText("Not published at national grain");
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
  await expect(page.getByTestId("chapter-safety-state-context")).toContainText("The figures below are Wisconsin's, not Dane County's");
  await expect(page.getByTestId("card-violent-crime-rate-county")).toContainText("Not published at county grain");
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
  await expect(page.getByTestId("within-county-table")).toContainText("Median gross rent by Census tract");
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
  await expect(earnings.locator("tbody tr")).toHaveCount(21);
  await expect(page.getByTestId("bea-earnings-basis")).toContainText("BEA personal income account");
  await expect(page.getByTestId("bea-earnings-bea-earnings-1600")).toContainText("Health care and social assistance");
  await expect(page.getByTestId("bea-earnings-bea-earnings-200")).toContainText("Published without a value: withheld");
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("never combined with the survey's income");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a state page has no within-this-county section", async ({ page }) => {
  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("within-county")).toHaveCount(0);
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
  await expect(page.getByTestId("card-qcew-jobs")).toContainText("BLS_QCEW:employment:10:0");
  await expect(page.getByTestId("card-qcew-jobs-county")).toContainText("jobs");
  await expect(page.getByTestId("card-unemployment-rate")).not.toContainText("BLS_QCEW");

  const mix = page.getByTestId("industry-mix");
  await expect(mix.locator("tbody tr")).toHaveCount(21);
  await expect(page.getByTestId("industry-mix-basis")).toContainText("counted by employers (QCEW)");
  await expect(page.getByTestId("industry-mix-qcew-sector-62")).toContainText("Health care and social assistance");
  await expect(page.getByTestId("industry-mix-qcew-sector-21")).toContainText("Published without a value: withheld");
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
  await expect(model).toContainText("CENSUS_SAIPE_SAHIE:saipe:SAEMHI");
  await expect(survey).not.toContainText("CENSUS_SAIPE_SAHIE");
  await expect(page.getByTestId("card-saipe-median-household-income-county")).toContainText("margin of error");
  await expect(page.getByTestId("card-saipe-median-household-income-county")).toContainText("confidence lower");
  await expect(page.getByTestId("card-saipe-poverty-rate-basis")).toHaveText("Model-based annual estimate (SAIPE)");
  await expect(page.getByTestId("depth-saipe-child-poverty-rate-basis")).toContainText("Model-based annual estimate (SAIPE)");
  await expect(page.getByTestId("depth-below-poverty-basis")).toContainText("Survey estimate (American Community Survey)");

  await expect(page.getByTestId("card-sahie-uninsured-rate-basis")).toHaveText("Model-based annual estimate (SAHIE)");
  await expect(page.getByTestId("card-sahie-uninsured-rate-county")).toContainText("confidence upper");
  await expect(page.getByTestId("depth-uninsured-35-64-basis")).toContainText("Survey estimate (American Community Survey)");
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("CENSUS_SAIPE_SAHIE");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a SAIPE slot the catalog does not publish says so and borrows no ACS figure", async ({ page }) => {
  await installPlaceFixtures(page, { withoutSource: "CENSUS_SAIPE_SAHIE" });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("card-saipe-median-household-income")).toHaveAttribute("data-available", "false");
  await expect(page.getByTestId("card-saipe-median-household-income")).toContainText("Not published by this warehouse");
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
  await expect(page.getByTestId("place-search-results").getByRole("link", { name: "Rock County, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/rock-county");
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
  await expect(nearby.getByTestId("place-nearby-neighbours").getByRole("link", { name: "Rock County, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/rock-county");
  // A neighbour in another state is addressed by its FIPS, which settles on its name.
  await expect(nearby.getByTestId("place-nearby-neighbours").getByRole("link", { name: "Hennepin County, Minnesota" })).toHaveAttribute("href", "/us/minnesota/27053");
  await expect(nearby.getByTestId("place-nearby-part-of").getByRole("link")).toHaveText(["Wisconsin", "United States"]);
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

  // Three levels: the place, its state, the nation.
  const income = page.getByTestId("card-median-household-income");
  await expect(income.locator("tbody tr")).toHaveCount(3);
  await expect(page.getByTestId("card-median-household-income-place")).toContainText("Crossing city");
  await expect(page.getByTestId("card-median-household-income-place")).toContainText("margin of error");
  // BLS publishes no place: said in the card, not borrowed from a county.
  await expect(page.getByTestId("card-unemployment-rate-place")).toContainText("Not published at city or town grain");

  await expect(page.getByTestId("place-cross-county")).toHaveText(
    "Crossing city crosses county lines: it lies in Dane County (60%) and Rock County (40%). A county's figures describe the whole county, not this place.",
  );
  const counties = page.getByTestId("place-nearby-counties");
  await expect(counties.getByRole("link", { name: "Dane County, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/dane-county");

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
  await expect(page.getByTestId("place-nearby-within").getByRole("link", { name: "Crossing city, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/5599999");
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
  await ready(page, "/us/wisconsin/dane-county");
  const section = page.getByTestId("place-standout");
  await expect(section.getByTestId("place-standout-count")).toContainText("6 measures could be ranked among Wisconsin counties, each on its own");
  await expect(section.getByTestId("place-standout-highest").locator("li")).toHaveCount(3);
  await expect(section.getByTestId("place-standout-lowest").locator("li")).toHaveCount(3);
  const income = section.getByTestId("standout-CENSUS_ACS:acs5:B19013_001");
  await expect(income).toContainText("Higher than 68 of 70 Wisconsin counties with a published value (1 withheld a value)");
  await expect(income.getByRole("link", { name: "See every county on a map" })).toHaveAttribute("href", "/map/CENSUS_ACS%3Aacs5%3AB19013_001");
  await expect(section.getByTestId("place-standout-lowest")).toContainText("Median age (UI fixture)");
  await expect(section).not.toContainText(/score|overall/i);
});

test("a place with no ranked measure omits What stands out and says why", async ({ page }) => {
  // Covers: WEB-131 — an empty answer is stated in the footer.
  await ready(page, "/us/wisconsin/rock-county");
  await expect(page.getByTestId("place-standout")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("What stands out: no measure could be ranked for this place");
});
