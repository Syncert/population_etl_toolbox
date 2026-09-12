import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-030 — the articles route in the browser. It presents composed
// evidence blocks and nothing else: no hand-written example, no metric or
// geography the page chose for itself, and no value this client computed. A
// composition that was never made, one this build cannot read, and one whose
// analytical blocks lack context are three distinct states, each stated.

const envelope = {
  metricCodes: ["CENSUS_ACS:acs5:B01003_001"],
  sourceCodes: ["CENSUS_ACS"],
  geoId: "state:55|county:025",
  geoLevel: "COUNTY",
  scope: "latest",
  release: "",
  period: "2023",
  units: "people",
  transformation: "none",
  apiQuery: "/api/v1/observations?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001",
  caveats: ["ACS estimates carry a margin of error"],
};

const composed = {
  version: 1,
  title: "Needs assessment",
  purpose: "Describe the need this proposal addresses.",
  blocks: [
    { id: "summary", type: "text", title: "Summary of need", content: "The need is stated here." },
    {
      id: "population-evidence",
      type: "analysis",
      title: "Population context",
      content: "Dane County population",
      envelope,
      document: {
        kind: "observations",
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        filters: { geo_level: "COUNTY", geo_id: "state:55|county:025" },
      },
    },
    {
      id: "condition-evidence",
      type: "analysis",
      title: "Condition being addressed",
      content: "Add a saved view from the explorer to fill this block.",
    },
    {
      id: "limits",
      type: "caveat",
      title: "What these measures do not establish",
      content: "State associations as associations.",
    },
  ],
  updatedAt: "2026-09-12T00:00:00Z",
};

async function seedComposition(page, value) {
  await page.addInitScript((raw) => {
    if (raw === null) {
      window.localStorage.removeItem("economic-data-studio:builder-draft:v1");
      return;
    }
    window.localStorage.setItem("economic-data-studio:builder-draft:v1", raw);
  }, value);
}

test("with nothing composed the route says so and points at the composer", async ({ page }) => {
  await seedComposition(page, null);
  await page.goto("/articles");

  const article = page.getByTestId("composed-article");
  await expect(article).toHaveAttribute("data-state", "empty");
  await expect(page.getByTestId("article-empty")).toContainText("nothing has been composed");
  await expect(page.getByRole("link", { name: "Compose one in the builder" })).toBeVisible();

  // The retired hand-written example is gone: no measure, geography, or
  // headline percentage the page picked for itself survives anywhere on it.
  await expect(article).not.toContainText("Dane County");
  await expect(article).not.toContainText("B01003_001");
  await expect(article).not.toContainText("%");
});

test("a stored composition this build cannot read is not reported as nothing composed", async ({
  page,
}) => {
  await seedComposition(page, "{not json");
  await page.goto("/articles");

  // Telling a reader whose composition is still in this browser that nothing
  // has been composed would read as though their work were gone.
  const article = page.getByTestId("composed-article");
  await expect(article).toHaveAttribute("data-state", "unreadable");
  await expect(page.getByTestId("article-status")).toContainText("not readable as a packet");
  await expect(page.getByTestId("article-empty")).not.toContainText("nothing has been composed");
});

test("a composed packet is presented with every block's envelope intact", async ({ page }) => {
  await seedComposition(page, JSON.stringify(composed));
  await page.goto("/articles");

  const article = page.getByTestId("composed-article");
  await expect(article).toHaveAttribute("data-state", "ready");
  await expect(page.getByRole("heading", { level: 1, name: "Needs assessment" })).toBeVisible();

  // The filled analytical block carries its whole envelope onto the page, so
  // the value can be read and the query re-derived from what is shown.
  const filled = page.getByTestId("article-block-population-evidence");
  await expect(filled).toHaveAttribute("data-has-envelope", "true");
  const shown = page.getByTestId("envelope-population-evidence");
  await expect(shown).toContainText("CENSUS_ACS:acs5:B01003_001");
  await expect(shown).toContainText("state:55|county:025");
  await expect(shown).toContainText("2023");
  await expect(shown).toContainText("/api/v1/observations");
  await expect(shown).toContainText("ACS estimates carry a margin of error");

  // A latest-scope block is live and says what that means, so a reader never
  // takes a value that can still change for a settled one.
  await expect(page.getByTestId("live-population-evidence")).toContainText("live");
  await expect(shown).toContainText("change when the source republishes");
  await expect(page.getByTestId("reopen-population-evidence")).toHaveAttribute(
    "href",
    /metric=CENSUS_ACS%3Aacs5%3AB01003_001/,
  );

  // The block with no envelope is named and is not presented as evidence.
  await expect(article).toHaveAttribute("data-complete", "false");
  await expect(page.getByTestId("article-issue-condition-evidence")).toContainText(
    "no reproducibility envelope",
  );
  await expect(page.getByTestId("article-empty-condition-evidence")).toContainText(
    "not presented as evidence",
  );
  await expect(page.getByTestId("envelope-condition-evidence")).toHaveCount(0);

  // The caveat block travels with the composition rather than being dropped
  // on the way to the reading surface.
  await expect(page.getByTestId("article-block-limits")).toContainText(
    "associations as associations",
  );
});

test("a block type this build cannot present is named rather than silently dropped", async ({
  page,
}) => {
  await seedComposition(
    page,
    JSON.stringify({
      ...composed,
      blocks: [composed.blocks[0], { id: "future", type: "forecast", title: "Projected demand" }],
    }),
  );
  await page.goto("/articles");

  await expect(page.getByTestId("article-unsupported")).toContainText("Projected demand");
  await expect(page.getByTestId("article-block-future")).toHaveCount(0);
});

test("a block with a partial envelope shows what it recorded and what it lacks", async ({
  page,
}) => {
  await seedComposition(
    page,
    JSON.stringify({
      ...composed,
      blocks: [
        {
          id: "population-evidence",
          type: "analysis",
          title: "Population context",
          envelope: { ...envelope, period: "", apiQuery: "" },
        },
      ],
    }),
  );
  await page.goto("/articles");

  // A reader deciding whether to trust the block needs to see how far short
  // it falls, not only that it falls short — so the recorded fields stay and
  // the gaps read "Not recorded".
  const shown = page.getByTestId("envelope-population-evidence");
  await expect(shown).toContainText("CENSUS_ACS:acs5:B01003_001");
  await expect(shown).toContainText("Not recorded");
  await expect(page.getByTestId("article-empty-population-evidence")).toContainText(
    "missing period, apiQuery",
  );
  await expect(page.getByTestId("article-empty-population-evidence")).toContainText(
    "not presented as evidence",
  );
  await expect(page.getByTestId("composed-article")).toHaveAttribute("data-complete", "false");
});
