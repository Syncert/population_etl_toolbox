import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-132 — the studio needs a credential, renders a frame with its
// footer, exports PNGs of the declared size, and saves and reopens a frame
// record with the token only in the Authorization header.

const TOKEN_KEY = "economic-data-studio:api-token";

async function installAccount(page, saved) {
  await page.route("**/api/v1/analysis-configurations**", async (route) => {
    const request = route.request();
    expect(request.url()).not.toContain("operator-token");
    expect(request.headers().authorization).toBe("Bearer operator-token");
    const url = new URL(request.url());
    if (request.method() === "POST") {
      const body = request.postDataJSON();
      const stored = { configuration_id: saved.length + 1, name: body.name, version: 1, document: body.document, validation: { valid: true }, created_at: "2026-10-06T00:00:00Z", updated_at: "2026-10-06T00:00:00Z" };
      saved.push(stored);
      return route.fulfill({ status: 201, headers: { "cache-control": "private, no-store" }, json: stored });
    }
    const id = url.pathname.split("/").pop();
    if (/^\d+$/.test(id)) {
      const found = saved.find((item) => String(item.configuration_id) === id);
      return found ? route.fulfill({ headers: { "cache-control": "private, no-store" }, json: found }) : route.fulfill({ status: 404, json: { detail: "configuration not found" } });
    }
    return route.fulfill({ headers: { "cache-control": "private, no-store" }, json: { total: saved.length, limit: 50, offset: 0, items: saved.map(({ document, validation, ...summary }) => ({ ...summary, kind: document.kind })) } });
  });
}

async function pngSize(download) {
  const chunks = [];
  for await (const chunk of await download.createReadStream()) chunks.push(chunk);
  const bytes = Buffer.concat(chunks);
  expect(bytes.subarray(1, 4).toString("ascii")).toBe("PNG");
  return [bytes.readUInt32BE(16), bytes.readUInt32BE(20)];
}

test("without a credential the studio shows sign-in and no frame", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/studio");
  await expect(page.getByTestId("studio-signed-out")).toBeVisible();
  await expect(page.getByTestId("studio-frame")).toHaveCount(0);
});

test("an operator renders, exports and reopens a frame", async ({ page }) => {
  const saved = [];
  await installPlaceFixtures(page);
  await installAccount(page, saved);
  await page.addInitScript((key) => window.sessionStorage.setItem(key, "operator-token"), TOKEN_KEY);
  await page.goto("/studio");
  const frame = page.getByTestId("studio-frame");
  await expect(frame).toBeVisible();
  await expect(frame).toHaveAttribute("alt", /Dane County: 1,100 persons. Wisconsin: 11,000 persons/);
  await expect(page.getByTestId("studio-notes")).toContainText("Definition (not reviewed)");

  for (const [format, size] of [["16:9", [1920, 1080]], ["9:16", [1080, 1920]], ["1:1", [1080, 1080]]]) {
    await page.getByTestId("studio-format").selectOption(format);
    await expect(frame).toHaveAttribute("data-format", format);
    const [download] = await Promise.all([page.waitForEvent("download"), page.getByTestId("studio-export-png").click()]);
    expect(await pngSize(download)).toEqual(size);
  }

  await page.getByTestId("studio-save").click();
  await expect(page.getByTestId("studio-notice")).toHaveText("Frame record saved to your account.");
  expect(saved).toHaveLength(1);
  const studio = saved[0].document.visualization.studio;
  expect(studio.requests.map((request) => request.level)).toEqual(["COUNTY", "STATE", "NATIONAL"]);
  expect(studio.requests[0].url).toContain("geo_id=state%3A55%7Ccounty%3A025");
  expect(studio.releases["state:55|county:025"]).toBe("fixture-release-2025");

  const replayed = [];
  page.on("request", (request) => { if (request.url().includes("/api/v1/observations")) replayed.push(new URL(request.url()).pathname + new URL(request.url()).search); });
  await page.getByTestId("studio-history").getByRole("link").first().click();
  await expect(page.getByTestId("studio-notice")).toContainText("Reopened Frame");
  await expect(page.getByTestId("studio-frame")).toHaveAttribute("data-format", "1:1");
  await expect.poll(() => studio.requests.every((request) => replayed.includes(request.url))).toBe(true);
});
