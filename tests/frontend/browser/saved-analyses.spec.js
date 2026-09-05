import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-022 — saved analyses in the browser. The token reaches the
// API only as an authorization header, no user content enters the address
// bar, a stale configuration is shown unmodified with its reported reason,
// a version conflict is refused rather than merged, another owner's id is
// indistinguishable from one that never existed, and browser-local views
// migrate only where the contract can describe them.

const TOKEN = "operator-provisioned-token";

const configurations = {
  1: {
    configuration_id: 1,
    name: "Dane County population",
    version: 2,
    document: {
      kind: "observations",
      metric_code: "ACS:acs5:B01003_001",
      scope: "latest",
      release: null,
      filters: { geo_level: "COUNTY", state_fips: "55", geo_id: "state:55|county:025" },
      visualization: {},
    },
    validation: { valid: true, reason: null },
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-02T00:00:00Z",
  },
  2: {
    configuration_id: 2,
    name: "Retired measure analysis",
    version: 1,
    document: {
      kind: "observations",
      metric_code: "ACS:acs5:RETIRED_001",
      scope: "latest",
      release: null,
      filters: { geo_level: "COUNTY" },
      visualization: {},
    },
    // Reported, not repaired: the document comes back exactly as saved.
    validation: { valid: false, reason: "metric_code 'ACS:acs5:RETIRED_001' is no longer published" },
    created_at: "2026-08-01T00:00:00Z",
    updated_at: "2026-08-01T00:00:00Z",
  },
};

async function installRoutes(page, { authHeaders = [], requestUrls = [], conflictOnUpdate = false } = {}) {
  await page.route("**/api/v1/analysis-configurations**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    authHeaders.push(request.headers().authorization || "");
    requestUrls.push(`${url.pathname}${url.search}`);

    if ((request.headers().authorization || "") !== `Bearer ${TOKEN}`) {
      // Identical for a missing, malformed, unknown, or revoked token.
      return route.fulfill({ status: 401, json: { detail: "not authenticated" } });
    }

    const idMatch = url.pathname.match(/\/analysis-configurations\/(\d+)$/);
    if (!idMatch) {
      if (request.method() === "POST") {
        const body = JSON.parse(request.postData() || "{}");
        return route.fulfill({
          status: 201,
          json: {
            configuration_id: 99,
            name: body.name,
            version: 1,
            document: body.document,
            validation: { valid: true, reason: null },
            created_at: "2026-09-03T00:00:00Z",
            updated_at: "2026-09-03T00:00:00Z",
          },
          headers: { "cache-control": "private, no-store" },
        });
      }
      return route.fulfill({
        json: {
          total: 2,
          limit: 200,
          offset: 0,
          items: [
            {
              configuration_id: 2,
              name: "Retired measure analysis",
              kind: "observations",
              version: 1,
              created_at: "2026-08-01T00:00:00Z",
              updated_at: "2026-08-01T00:00:00Z",
            },
            {
              configuration_id: 1,
              name: "Dane County population",
              kind: "observations",
              version: 2,
              created_at: "2026-09-01T00:00:00Z",
              updated_at: "2026-09-02T00:00:00Z",
            },
          ],
        },
        headers: { "cache-control": "private, no-store" },
      });
    }

    const id = Number(idMatch[1]);
    if (request.method() === "PUT") {
      if (conflictOnUpdate) {
        return route.fulfill({
          status: 409,
          json: {
            detail: "configuration was modified; expected version 2, current version 3",
          },
        });
      }
      const body = JSON.parse(request.postData() || "{}");
      return route.fulfill({
        json: {
          ...configurations[id],
          name: body.name,
          version: configurations[id].version + 1,
        },
      });
    }
    const configuration = configurations[id];
    // An id another account owns answers exactly like one that never existed.
    return configuration
      ? route.fulfill({ json: configuration, headers: { "cache-control": "private, no-store" } })
      : route.fulfill({ status: 404, json: { detail: "configuration not found" } });
  });
}

test("an account's saved analyses load only with an accepted token", async ({ page }) => {
  const authHeaders = [];
  const requestUrls = [];
  await installRoutes(page, { authHeaders, requestUrls });
  await page.goto("/saved");

  const screen = page.getByTestId("saved-analyses");
  await expect(screen).toHaveAttribute("data-signed-in", "false");
  await expect(page.getByTestId("saved-list-status")).toContainText("not signed in");

  // A token the API does not accept is reported without guessing why.
  await page.getByTestId("token-input").fill("wrong-token");
  await page.getByTestId("token-submit").click();
  await expect(page.getByTestId("saved-list-status")).toContainText("was not accepted");
  await expect(screen).toHaveAttribute("data-count", "0");

  await page.getByTestId("token-input").fill(TOKEN);
  await page.getByTestId("token-submit").click();
  await expect(screen).toHaveAttribute("data-count", "2");

  // The token reached the API only as an authorization header, and never
  // appeared in a request URL.
  expect(authHeaders.some((header) => header === `Bearer ${TOKEN}`)).toBe(true);
  expect(requestUrls.every((url) => !url.includes(TOKEN))).toBe(true);

  // No user content is written to the address bar.
  expect(new URL(page.url()).search).toBe("");
  await expect(page.getByTestId("privacy-note")).toContainText("private, no-store");
});

test("a stale configuration is shown unmodified with the reported reason", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/saved");
  await page.getByTestId("token-input").fill(TOKEN);
  await page.getByTestId("token-submit").click();

  await page.getByTestId("saved-open-2").click();
  await expect(page.getByTestId("saved-detail")).toBeVisible();
  // Never healthy: replaying it would not produce the analysis it describes.
  await expect(page.getByTestId("saved-validation")).toContainText("no longer published");
  await expect(page.getByTestId("stale-note")).toContainText("rather than rewriting your content");
  // The document itself is intact, exactly as saved.
  await expect(page.getByTestId("saved-document")).toContainText("ACS:acs5:RETIRED_001");

  // Opening a valid one reopens into the explorer carrying the selection
  // and nothing identifying.
  await page.getByTestId("saved-open-1").click();
  await expect(page.getByTestId("saved-validation")).toContainText("matches live capabilities");
  const reopen = page.getByTestId("reopen");
  await expect(reopen).toHaveAttribute("href", /metric=ACS%3Aacs5%3AB01003_001/);
  await expect(reopen).toHaveAttribute("href", /geo=state%3A55%7Ccounty%3A025/);
  const href = await reopen.getAttribute("href");
  expect(href).not.toContain("configuration");
  expect(href).not.toContain(TOKEN);
});

test("a version conflict is refused rather than merged", async ({ page }) => {
  await installRoutes(page, { conflictOnUpdate: true });
  await page.goto("/saved");
  await page.getByTestId("token-input").fill(TOKEN);
  await page.getByTestId("token-submit").click();
  await page.getByTestId("saved-open-1").click();

  await page.getByTestId("rename-input").fill("Renamed while someone else edited");
  await page.getByTestId("rename-save").click();

  // The API's own explanation is shown, and nothing was overwritten.
  await expect(page.getByTestId("conflict-note")).toContainText("current version 3");
  await expect(page.getByTestId("saved-detail-status")).toContainText("version conflict");
});

test("browser-local views migrate only where the contract describes them", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/saved");
  // Seed the pre-accounts local store this browser would already hold.
  await page.evaluate(() => {
    window.localStorage.setItem(
      "economic-data-studio:saved-charts:v1",
      JSON.stringify([
        {
          id: "chart:1",
          title: "County population",
          chartType: "choropleth",
          metricCode: "ACS:acs5:B01003_001",
          geoLevel: "COUNTY",
        },
        { id: "profile:1", title: "Community profile", chartType: "profile" },
      ]),
    );
  });
  await page.reload();
  await page.getByTestId("token-input").fill(TOKEN);
  await page.getByTestId("token-submit").click();

  await page.getByTestId("migration-plan").click();
  await expect(page.getByTestId("migration-summary")).toContainText("1 can be imported");
  // A shape the contract cannot describe is named with its reason, never
  // coerced into a document the API would refuse.
  await expect(page.getByTestId("migration-skipped")).toContainText("reading order");

  await page.getByTestId("migration-run").click();
  await expect(page.getByTestId("migration-result")).toContainText("1 imported");
  await expect(page.getByTestId("migration-result")).toContainText("local copies were kept");
  // The local store is untouched, so nothing is lost by importing.
  const stored = await page.evaluate(() =>
    JSON.parse(window.localStorage.getItem("economic-data-studio:saved-charts:v1") || "[]"),
  );
  expect(stored).toHaveLength(2);
});
