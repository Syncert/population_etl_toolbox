// Covers: WEB-118 — each source's explorer map paints its values, in a real
// browser with WebGL, against a running deployment.
//
// The data-path sweep (`smoke/map-display.smoke.test.js`) proves the values
// reach the choropleth model. It cannot see what happens after: the tile
// worker failing to load (the map drew nothing until bb39f46), a join key the
// tiles do not carry, a paint expression MapLibre refuses, a grain whose
// polygons the layer filter hides. The browser tier cannot see them either:
// it runs against stubbed data, and headless Chromium there has no GL
// context, so MapLibre never requests a tile (see map-wiring.smoke.test.js).
//
// So this tier launches Chromium with a software GL implementation, opens the
// explorer for one source and grain at a time, and grades three things:
//
//   1. the page's own answer: `data-colored-values` equals what the rows the
//      page itself received say it should be (the shared oracle, fed from the
//      page's intercepted responses -- not from a second request that might
//      be answered differently);
//   2. the pixels: the map canvas carries the legend's value-bin colours over
//      a real share of its area, so "coloured" means painted, not modelled;
//   3. a map with nothing to colour says why, rather than drawing grey.
//
// One subject per source and drawable grain: of the first metrics in catalog
// order, the one whose latest answer at that grain has one series per
// geography and colours the most geographies. Run it with the stack up:
//
//   SMOKE_BASE_URL=http://localhost:3001 npm --prefix apps/web run test:maps

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";
import sharp from "../../../apps/web/node_modules/sharp/dist/index.cjs";

import { oracle } from "../support/mapOracle.js";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");
// The subjects are the reviewed matrix's own (source, grain) pairs, read at
// definition time: every source the checkout says advertises a drawable
// grain gets a test, so a deployment that stops serving one fails by name
// rather than quietly grading fewer maps.
const REVIEWED = JSON.parse(
  readFileSync(
    // Playwright runs from the config's directory, apps/web.
    join(process.cwd(), "../../tests/fixtures/api/viz_coverage.json"),
    "utf8",
  ),
);
const DRAWABLE = REVIEWED.drawable_tile_grains.map((entry) =>
  typeof entry === "string" ? entry : entry.grain,
);
/** Metrics tried per source and grain before the source is reported. */
const CANDIDATES = Number(process.env.MAP_PAINT_CANDIDATES || 12);
/**
 * Share of the canvas a painted map must cover in value-bin colours.
 *
 * A floor, not a coverage expectation: a broken map paints essentially no
 * value colour at all (the grey FBI map painted none), while a correct map of
 * a sparse measure can colour a few dozen counties -- USDA NASS traditional
 * corn colours ~26 counties and 0.7% of the canvas. 0.05% is a few small
 * polygons at the test viewport.
 */
const MIN_PAINTED_SHARE = 0.0005;
/** Per-channel distance a 0.95-opacity fill may sit from its swatch. */
const CHANNEL_TOLERANCE = 20;
const FALLBACK_COLORS = new Set(["#9fb0ba", "#c8b7a6"]);

test.skip(!BASE_URL, "SMOKE_BASE_URL is unset; the live map tier needs a running stack");

async function getJson(path, params = {}) {
  const url = new URL(`${BASE_URL}/api/v1${path}`);
  for (const [name, value] of Object.entries(params)) {
    url.searchParams.set(name, String(value));
  }
  const response = await fetch(url);
  expect(response.ok, `${url} answered ${response.status}`).toBe(true);
  return response.json();
}

async function metricsOf(sourceCode) {
  const items = [];
  for (let offset = 0; ; offset += 1000) {
    const page = await getJson("/catalog/metrics", {
      source_code: sourceCode,
      active_only: "true",
      limit: 1000,
      offset,
    });
    items.push(...page.items);
    if (items.length >= page.total || page.items.length === 0) {
      return items.sort((a, b) => String(a.metric_code).localeCompare(String(b.metric_code)));
    }
  }
}

function hexToRgb(color) {
  const match = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(color.trim());
  if (match) {
    return match.slice(1).map((part) => Number.parseInt(part, 16));
  }
  const rgb = /rgba?\((\d+),\s*(\d+),\s*(\d+)/.exec(color);
  return rgb ? rgb.slice(1, 4).map(Number) : null;
}

function rgbToHex([r, g, b]) {
  return `#${[r, g, b].map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

/** Share of the canvas painted in any of `colors`, within tolerance. */
async function paintedShare(png, colors) {
  const { data, info } = await sharp(png).raw().toBuffer({ resolveWithObject: true });
  const pixels = info.width * info.height;
  let painted = 0;
  for (let offset = 0; offset < data.length; offset += info.channels) {
    for (const [r, g, b] of colors) {
      if (
        Math.abs(data[offset] - r) <= CHANNEL_TOLERANCE &&
        Math.abs(data[offset + 1] - g) <= CHANNEL_TOLERANCE &&
        Math.abs(data[offset + 2] - b) <= CHANNEL_TOLERANCE
      ) {
        painted += 1;
        break;
      }
    }
  }
  return painted / pixels;
}

for (const [sourceCode, grains] of Object.entries(REVIEWED.advertised_geo_grains)) {
  for (const grain of grains.filter((value) => DRAWABLE.includes(value))) {
    const capability = { source_code: sourceCode };
    test(`${sourceCode} ${grain}: the map paints its values`, async ({ page }) => {
      test.setTimeout(240_000);
      const metrics = (await metricsOf(capability.source_code)).filter((metric) =>
        (metric.valid_geo_grains || []).map((value) => String(value).toUpperCase()).includes(grain),
      );
      expect(
        metrics.length,
        `${capability.source_code} advertises ${grain} and the catalog lists no ${grain} metric`,
      ).toBeGreaterThan(0);

      // Of the first CANDIDATES metrics, the one colouring the most
      // geographies: a broad map makes the paint check a strong one.
      let subject = null;
      let best = 0;
      for (const metric of metrics.slice(0, CANDIDATES)) {
        const answer = await getJson("/observations", {
          metric_code: metric.metric_code,
          scope: "latest",
          geo_level: grain,
          limit: 5000,
        });
        const expected = oracle(answer.items || []);
        if (expected.numeric && !expected.stratified && expected.colourable > best) {
          subject = metric;
          best = expected.colourable;
        }
      }
      expect(
        subject,
        `none of the first ${CANDIDATES} ${capability.source_code} ${grain} metrics has one numeric series per geography`,
      ).not.toBeNull();

      // The rows the page itself received, read off the wire.
      const received = [];
      page.on("response", async (response) => {
        const url = new URL(response.url());
        if (
          /\/observations(\/latest)?$/.test(url.pathname) &&
          url.searchParams.get("metric_code") === subject.metric_code &&
          !url.searchParams.get("geo_id") &&
          response.ok()
        ) {
          try {
            const body = await response.json();
            received.push(...(body.items || []));
          } catch {
            // A body the page could not read either; the grade below says so.
          }
        }
      });

      const errors = [];
      page.on("pageerror", (error) => errors.push(String(error)));

      const url =
        `${BASE_URL}/explore?source=${encodeURIComponent(capability.source_code)}` +
        `&metric=${encodeURIComponent(subject.metric_code)}&geo_level=${grain}`;
      await page.goto(url, { waitUntil: "networkidle", timeout: 120_000 });
      const canvas = page.getByTestId("map-canvas");
      await canvas.scrollIntoViewIfNeeded();
      await expect(canvas).toHaveAttribute("data-map-ready", "true", { timeout: 60_000 });
      await expect
        .poll(async () => Number(await canvas.getAttribute("data-colored-values")), {
          timeout: 60_000,
        })
        .toBeGreaterThan(0);
      // Let MapLibre fetch tiles and paint the settled expression.
      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(3_000);

      const expected = oracle(received);
      const colored = Number(await canvas.getAttribute("data-colored-values"));
      expect(colored, `${url}: the page coloured ${colored} of ${expected.colourable}`).toBe(
        expected.colourable,
      );

      const swatches = await page
        .locator(".map-legend .legend-swatch")
        .evaluateAll((nodes) => nodes.map((node) => getComputedStyle(node).backgroundColor));
      const binColors = swatches
        .map(hexToRgb)
        .filter((rgb) => rgb && !FALLBACK_COLORS.has(rgbToHex(rgb)));
      expect(binColors.length, `${url}: the legend shows no value bin`).toBeGreaterThan(0);

      const share = await paintedShare(await canvas.locator("canvas").first().screenshot(), binColors);
      expect(
        share,
        `${url}: ${(share * 100).toFixed(2)}% of the map is painted in value colours`,
      ).toBeGreaterThanOrEqual(MIN_PAINTED_SHARE);
      expect(errors, `${url}: the page threw`).toEqual([]);
    });
  }
}
