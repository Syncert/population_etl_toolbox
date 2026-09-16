import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-108 — every route names itself in the tab, the history entry
// and the link preview, and a shared explorer link names what it shows.
//
// `app/layout.js` held the only `metadata` export, with a `%s | ...` template
// nothing filled in, because every `page.js` was `"use client"` and a client
// component cannot export metadata. So every one of this product's shareable
// links -- the thing `lib/urlState.ts` exists to make -- landed in a tab
// titled "Economic Data Studio".

const sources = [
  { source_code: "CENSUS_ACS", source_name: "Census ACS" },
  { source_code: "FRED", source_name: "Federal Reserve Economic Data" },
];

async function installRoutes(page) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sources }));
  await page.route("**/api/v1/catalog/**", (route) =>
    route.fulfill({ json: { total: 0, items: [] } }),
  );
  await page.route("**/api/v1/**", (route) => route.fulfill({ json: { total: 0, items: [] } }));
}

const ROUTE_TITLES = [
  ["/", "Economic Data Studio"],
  ["/catalog", "Data catalog"],
  ["/explore", "Explore"],
  ["/compare", "Compare"],
  ["/workbench", "Workbench"],
  ["/profiles", "Place profile"],
  ["/quality", "Data quality"],
  ["/saved", "Saved analyses"],
  ["/builder", "Evidence packet builder"],
  ["/articles", "Composed article"],
];

// One test per route rather than one loop over ten. Under `next dev` each
// route compiles the first time a browser asks for it, and ten of those do
// not fit in one test's budget -- and a loop reports "the tenth route" as a
// timeout on the first.
for (const [route, name] of ROUTE_TITLES) {
  test(`${route} names itself`, async ({ page }) => {
    await installRoutes(page);
    await page.goto(route);

    const title = await page.title();
    expect(title, `title of ${route}`).toContain(name);
    // The layout's template appends the site name to every route but the
    // home page, which *is* the site name.
    if (route !== "/") {
      expect(title, `title of ${route}`).toContain("Economic Data Studio");
    }
  });
}

test("an explorer link titles the tab with what it shows", async ({ page }) => {
  await installRoutes(page);
  await page.goto(
    "/explore?source=census&metric=CENSUS_ACS%3Aacs5%3AB01003_001&geo_level=COUNTY",
  );

  const title = await page.title();
  expect(title).toContain("census");
  expect(title).toContain("CENSUS_ACS:acs5:B01003_001");
  expect(title).toContain("COUNTY");
});

test("a query key outside the vocabulary never reaches the tab", async ({ page }) => {
  // The unit tier asserts this against the builders directly. This asserts it
  // where it would actually leak: the rendered document, through Next's own
  // metadata path.
  await installRoutes(page);
  await page.goto("/explore?source=census&value=561504&token=secret-token");

  const title = await page.title();
  expect(title).toContain("census");
  expect(title).not.toContain("561504");
  expect(title).not.toContain("secret-token");
});

test("robots and the sitemap agree on what is published", async ({ page }) => {
  const robots = await page.request.get("/robots.txt");
  expect(robots.status()).toBe(200);
  const rules = await robots.text();
  expect(rules).toContain("Disallow: /saved");
  expect(rules).toContain("Disallow: /builder");
  expect(rules).toContain("Allow: /explore");

  const sitemap = await page.request.get("/sitemap.xml");
  expect(sitemap.status()).toBe(200);
  const urls = await sitemap.text();
  expect(urls).toContain("<loc>");
  expect(urls).toContain("/explore");
  expect(urls).toContain("/catalog");
  // A sitemap naming a route robots.txt disallows is a contradiction a
  // crawler resolves for itself, so neither private route appears.
  expect(urls).not.toContain("/saved");
  expect(urls).not.toContain("/builder");
});
