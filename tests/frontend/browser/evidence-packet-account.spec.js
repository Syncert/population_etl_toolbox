import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-031 — evidence packets on the account in the browser. The
// composer saves to the account whenever a token is held and to the browser
// otherwise, stating the destination before the click and on the outcome;
// the token reaches the API only as a header and no packet id, name, or
// token reaches the address bar; an update sends the version it read and a
// 409 is surfaced rather than merged; an account packet opened in the
// composer or the reader carries the API's per-block verdict so a retired
// measure is named on its block; and a refused account save is reported,
// never rewritten to the browser store.

const TOKEN = "operator-provisioned-token";

const envelope = {
  metric_codes: ["CENSUS_ACS:acs5:B01003_001"],
  source_codes: ["CENSUS_ACS"],
  geo_id: "state:55|county:025",
  geo_level: "COUNTY",
  scope: "latest",
  release: "",
  period: "2023",
  units: "people",
  transformation: "none",
  api_query: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
  caveats: ["ACS estimates carry a margin of error"],
};

const storedPacket = {
  packet_id: 12,
  name: "Housing needs",
  version: 3,
  document: {
    schema_version: 1,
    title: "Housing needs",
    purpose: "Why housing.",
    blocks: [
      { block_id: "summary", type: "text", title: "Summary", content: "The need." },
      {
        block_id: "population-evidence",
        type: "analysis",
        title: "Population context",
        content: "Dane County population",
        envelope,
        document: {
          kind: "observations",
          metric_code: "CENSUS_ACS:acs5:B01003_001",
          scope: "latest",
          release: null,
          filters: { geo_level: "COUNTY", geo_id: "state:55|county:025" },
        },
      },
      { block_id: "limits", type: "caveat", title: "Limits", content: "Associations." },
    ],
  },
  // The API can see what the client cannot: the measure was retired.
  validation: {
    valid: false,
    reason: "1 of 1 analytical blocks cannot be read as evidence",
    blocks: [
      {
        block_id: "population-evidence",
        valid: false,
        reason: "metric_code 'CENSUS_ACS:acs5:B01003_001' is not a published metric",
        missing: [],
      },
    ],
  },
  created_at: "2026-09-10T00:00:00Z",
  updated_at: "2026-09-11T00:00:00Z",
};

const savedViews = [
  {
    id: "chart:1",
    title: "Dane County population",
    chartType: "choropleth",
    metricCode: "CENSUS_ACS:acs5:B01003_001",
    source: "CENSUS_ACS",
    geoLevel: "COUNTY",
    geoId: "state:55|county:025",
    apiQuery: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
    period: "2023",
    savedAt: "2026-09-03T00:00:00Z",
  },
];

async function installPacketRoutes(page, { writes = [], conflictOnUpdate = false, refuseCreate = null } = {}) {
  await page.route("**/api/v1/evidence-packets**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const authorization = request.headers().authorization || "";
    if (authorization !== `Bearer ${TOKEN}`) {
      return route.fulfill({ status: 401, json: { detail: "a valid bearer token is required" } });
    }
    const idMatch = url.pathname.match(/\/evidence-packets\/(\d+)$/);
    const headers = { "cache-control": "private, no-store" };

    if (!idMatch && request.method() === "GET") {
      return route.fulfill({
        headers,
        json: {
          total: 1,
          limit: 200,
          offset: 0,
          items: [
            {
              packet_id: 12,
              name: "Housing needs",
              version: 3,
              block_count: 3,
              analytical_block_count: 1,
              created_at: storedPacket.created_at,
              updated_at: storedPacket.updated_at,
            },
          ],
        },
      });
    }
    if (!idMatch && request.method() === "POST") {
      const body = JSON.parse(request.postData() || "{}");
      writes.push({ method: "POST", url: request.url(), authorization, body });
      if (refuseCreate) {
        return route.fulfill({ status: refuseCreate.status, headers, json: { detail: refuseCreate.detail } });
      }
      return route.fulfill({
        status: 201,
        headers,
        json: {
          packet_id: 99,
          name: body.name,
          version: 1,
          document: body.document,
          validation: { valid: true, reason: null, blocks: [] },
          created_at: "2026-09-12T00:00:00Z",
          updated_at: "2026-09-12T00:00:00Z",
        },
      });
    }
    if (idMatch && request.method() === "GET") {
      return Number(idMatch[1]) === 12
        ? route.fulfill({ headers, json: storedPacket })
        : route.fulfill({ status: 404, headers, json: { detail: "packet not found" } });
    }
    if (idMatch && request.method() === "PUT") {
      const body = JSON.parse(request.postData() || "{}");
      writes.push({ method: "PUT", url: request.url(), authorization, body });
      if (conflictOnUpdate) {
        return route.fulfill({
          status: 409,
          headers,
          json: { detail: "packet was modified; expected version 3, current version 4" },
        });
      }
      return route.fulfill({
        headers,
        json: { ...storedPacket, name: body.name, version: 4, document: body.document, validation: { valid: true, reason: null, blocks: [] } },
      });
    }
    return route.fulfill({ status: 405, json: { detail: "unexpected" } });
  });
}

async function signIn(page, token = TOKEN) {
  await page.addInitScript(
    ({ value, views }) => {
      window.sessionStorage.setItem("economic-data-studio:api-token", value);
      window.localStorage.setItem("economic-data-studio:saved-charts:v1", JSON.stringify(views));
      window.localStorage.removeItem("economic-data-studio:builder-draft:v1");
    },
    { value: token, views: savedViews },
  );
}

test("signed in, the composer saves to the account and says so before and after", async ({ page }) => {
  const writes = [];
  await installPacketRoutes(page, { writes });
  await signIn(page);
  await page.goto("/builder");

  const save = page.getByTestId("packet-save");
  await expect(save).toHaveAttribute("data-destination", "account");
  await expect(save).toContainText("Save to account");
  await expect(page.getByTestId("packet-account-status")).toContainText("1 packet in your account");

  await page.getByTestId("packet-target").selectOption("population-evidence");
  await page.getByTestId("packet-attach-chart:1").click();
  await save.click();

  const toast = page.getByTestId("packet-save-toast");
  await expect(toast).toContainText("your account");
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-account-packet", "99");

  expect(writes).toHaveLength(1);
  const [write] = writes;
  expect(write.method).toBe("POST");
  // The token travels only as a header, and never into the URL or the
  // address bar; neither does anything about the packet.
  expect(write.authorization).toBe(`Bearer ${TOKEN}`);
  expect(write.url).not.toContain(TOKEN);
  expect(page.url()).not.toContain(TOKEN);
  expect(page.url()).not.toContain("99");
  expect(page.url()).not.toContain("Needs");

  // The stored document is the composition: the filled block's query and
  // its recorded envelope, in snake_case, and never an observation value.
  const filled = write.body.document.blocks.find((block) => block.block_id === "population-evidence");
  expect(filled.envelope.metric_codes).toEqual(["CENSUS_ACS:acs5:B01003_001"]);
  expect(filled.envelope.api_query).toContain("/api/v1/observations");
  expect(filled.document.kind).toBe("observations");
  expect(filled.document.scope).toBe(filled.envelope.scope);
  expect(JSON.stringify(write.body)).not.toContain("561504");
  // The local draft was not written: the account was the chosen destination.
  const draft = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:builder-draft:v1"),
  );
  expect(draft).toBeNull();
});

test("an opened account packet carries the API's verdict, and an update sends the version it read", async ({
  page,
}) => {
  const writes = [];
  await installPacketRoutes(page, { writes, conflictOnUpdate: true });
  await signIn(page);
  await page.goto("/builder");

  await page.getByTestId("packet-open-12").click();
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-account-packet", "12");
  await expect(page.getByTestId("packet-title")).toHaveValue("Housing needs");
  await expect(page.getByTestId("envelope-population-evidence")).toContainText("state:55|county:025");

  // The client's own report finds nothing wrong with the filled block; only
  // the API can see the measure was retired, and it is named on the block.
  await expect(page.getByTestId("packet-issues")).toHaveCount(0);
  await expect(page.getByTestId("stale-population-evidence")).toContainText("not a published metric");
  await expect(page.getByTestId("packet-stale")).toContainText("reports the mismatch rather than rewriting");

  const save = page.getByTestId("packet-save");
  await expect(save).toContainText("update");
  await save.click();

  expect(writes).toHaveLength(1);
  expect(writes[0].method).toBe("PUT");
  expect(writes[0].url).toMatch(/\/evidence-packets\/12$/);
  expect(writes[0].body.expected_version).toBe(3);
  // The conflict is surfaced with the API's own explanation, never merged.
  const toast = page.getByTestId("packet-save-toast");
  await expect(toast).toContainText("current version 4");
  await expect(toast).toContainText("not saved");
});

test("a refused account save is reported and never rewritten to the browser", async ({ page }) => {
  await installPacketRoutes(page, { refuseCreate: { status: 422, detail: "block 'population-evidence' names measure(s) X in its envelope that its query does not ask for" } });
  await signIn(page);
  await page.goto("/builder");
  await page.getByTestId("packet-save").click();

  const toast = page.getByTestId("packet-save-toast");
  await expect(toast).toContainText("not saved");
  await expect(toast).toContainText("does not ask for");
  const draft = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:builder-draft:v1"),
  );
  expect(draft).toBeNull();
  await expect(page.getByTestId("evidence-packet")).toHaveAttribute("data-account-packet", "");
});

test("signed out, the composer saves in the browser and names that limit", async ({ page }) => {
  let called = false;
  await page.route("**/api/v1/evidence-packets**", (route) => {
    called = true;
    return route.fulfill({ status: 401, json: { detail: "a valid bearer token is required" } });
  });
  await page.addInitScript(() => {
    window.sessionStorage.removeItem("economic-data-studio:api-token");
    window.localStorage.removeItem("economic-data-studio:builder-draft:v1");
  });
  await page.goto("/builder");

  const save = page.getByTestId("packet-save");
  await expect(save).toHaveAttribute("data-destination", "browser");
  await expect(save).toContainText("browser");
  await expect(page.getByTestId("packet-account-library")).toHaveCount(0);
  await save.click();
  await expect(page.getByTestId("packet-save-toast")).toContainText("this browser only");
  const draft = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:builder-draft:v1"),
  );
  expect(draft).not.toBeNull();
  expect(called).toBe(false);
});

test("the reader shows an account packet with the API's verdict, and writes nothing to the URL", async ({
  page,
}) => {
  await installPacketRoutes(page);
  await signIn(page);
  await page.goto("/articles");

  await expect(page.getByTestId("article-account-status")).toContainText("1 packet in your account");
  // Signed in with no selection, the browser draft is still what is shown.
  await expect(page.getByTestId("composed-article")).toHaveAttribute("data-state", "empty");

  await page.getByTestId("article-packet-select").selectOption("12");
  const article = page.getByTestId("composed-article");
  await expect(article).toHaveAttribute("data-state", "ready");
  await expect(article).toHaveAttribute("data-source", "account");
  await expect(page.getByRole("heading", { level: 1, name: "Housing needs" })).toBeVisible();
  await expect(page.getByTestId("envelope-population-evidence")).toContainText("ACS estimates carry a margin of error");
  await expect(page.getByTestId("article-stale-population-evidence")).toContainText("not a published metric");
  await expect(page.getByTestId("article-block-limits")).toContainText("Associations.");

  // Nothing about the packet reached the address bar.
  expect(page.url()).not.toContain("12");
  expect(page.url()).not.toContain("Housing");
  expect(page.url()).not.toContain(TOKEN);

  // Back to the browser draft: the reader states which source it is showing.
  await page.getByTestId("article-packet-select").selectOption("");
  await expect(page.getByTestId("composed-article")).toHaveAttribute("data-state", "empty");
});
