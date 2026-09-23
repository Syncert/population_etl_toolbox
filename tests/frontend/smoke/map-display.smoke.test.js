import { beforeAll, describe, expect, test } from "vitest";

// Covers: WEB-118 — every explorer map that has values to show colours them,
// graded against the rows themselves, live.
//
// The visualization sweep (WEB-104) asks whether the API *answers* each
// screen. It passed while the FBI UCR state map drew every state grey: the API
// answered 20,902 rows, and the explorer's own reduction then declared the
// answer stratified and handed the choropleth nothing (WEB-117). A check that
// asks the API cannot see a client that throws the answer away.
//
// So this sweep runs the explorer's data path end to end -- the same request
// builder, the same pager, the same row normalization, the same `mapRows`
// reduction, the same choropleth model -- and grades the result against an
// oracle computed from the raw rows by this file, not by the application:
//
//   - rows at the grain with no numeric value: the map is legitimately empty;
//   - two rows for one geography and one period: the answer really is
//     stratified, and the page must decline *and* name what varies;
//   - otherwise one series per geography: the page must not decline, and the
//     model must colour exactly one value per geography whose newest row
//     carries a number.
//
// Anything else is a map that shows the reader less than the warehouse holds,
// and fails here by name. Painting is graded separately, in a real browser
// (`tests/frontend/live/map-paint.live.spec.js`): this tier proves the values
// reach the model, that one proves the model reaches the pixels.
//
// Scope: every source, every drawable grain a metric declares. Sources with
// at most MAP_SWEEP_METRICS metrics are read whole; larger ones (BLS, ACS) are
// read at an even deterministic spread of that many. MAP_SWEEP_ALL=1 reads
// every metric; MAP_SWEEP_SOURCES=FBI_UCR,CDC narrows it. Opt-in like the rest of the smoke tier: without SMOKE_BASE_URL
// it skips, and SMOKE_REQUIRED=1 forbids that.

import { apiFetch, fetchCollectionPages } from "../../../apps/web/lib/api/client";
import { buildExplorerSources } from "../../../apps/web/lib/explorerSources";
import {
  buildChoroplethModel,
  metricSupportedGeoLevels,
} from "../../../apps/web/lib/explorerViewModel";
import {
  buildLatestObservationRequest,
  mapRows,
  normalizeObservationRows,
  SCOPE_LATEST,
} from "../../../apps/web/lib/observationAccess";
import { DRAWABLE_TILE_GRAINS } from "../../../apps/web/lib/tileGrains";
import { grade, oracle } from "../support/mapOracle";
import { reportUnhandledErrors } from "./unhandledErrors";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");
const SWEEP_ALL = process.env.MAP_SWEEP_ALL === "1";
const METRIC_BUDGET = Number(process.env.MAP_SWEEP_METRICS || 40);
/** Optional comma-separated source codes, to re-run one source's maps. */
const ONLY_SOURCES = (process.env.MAP_SWEEP_SOURCES || "")
  .split(",")
  .map((code) => code.trim())
  .filter(Boolean);
/** The explorer's own page size and page bound for the latest read. */
const PAGE_SIZE = 5000;
const PAGE_LIMIT = 40;
const DRAWABLE = DRAWABLE_TILE_GRAINS.map((entry) => entry.grain);

test("the map-display sweep is configured wherever it is required", () => {
  if (process.env.SMOKE_REQUIRED === "1") {
    expect(BASE_URL, "SMOKE_REQUIRED=1 but SMOKE_BASE_URL is unset").toBeTruthy();
  }
});

function installOriginResolvingFetch() {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (input, init) => {
    const target = typeof input === "string" ? input : input.url;
    return realFetch(target.startsWith("/") ? `${BASE_URL}${target}` : target, init);
  };
}

/** An even, deterministic spread of `budget` items: first, last, and between. */
export function spread(items, budget) {
  if (SWEEP_ALL || items.length <= budget) {
    return items;
  }
  const picked = [];
  for (let index = 0; index < budget; index += 1) {
    picked.push(items[Math.round((index * (items.length - 1)) / (budget - 1))]);
  }
  return [...new Set(picked)];
}

describe.skipIf(!BASE_URL)("every explorer map colours what the rows hold, live", () => {
  /** @type {Array<{source: string, metric: string, grain: string, verdict: string, rows: number, coloured: number, problem: string|null}>} */
  const results = [];

  beforeAll(async () => {
    installOriginResolvingFetch();
    const capabilities = await apiFetch("/catalog/capabilities");
    const sources = buildExplorerSources(capabilities.items || []);
    for (const source of sources) {
      if (ONLY_SOURCES.length > 0 && !ONLY_SOURCES.includes(source.sourceCode)) {
        continue;
      }
      const metrics = await fetchCollectionPages("/catalog/metrics", {
        params: { source_code: source.sourceCode, active_only: "true" },
        pageSize: 1000,
        maxPages: 100,
      });
      const ordered = [...metrics.items].sort((a, b) =>
        String(a.metric_code).localeCompare(String(b.metric_code)),
      );
      for (const metric of spread(ordered, METRIC_BUDGET)) {
        const grains = metricSupportedGeoLevels(metric).filter((grain) => DRAWABLE.includes(grain));
        for (const grain of grains) {
          const { resource, params } = buildLatestObservationRequest(source, {
            metricCode: metric.metric_code,
            geoLevel: grain,
            stateFips: "",
            limit: String(PAGE_SIZE),
            newestPerGeography: true,
            scope: SCOPE_LATEST,
            dimensions: {},
          });
          const pages = await fetchCollectionPages(resource, {
            params,
            pageSize: PAGE_SIZE,
            maxPages: PAGE_LIMIT,
          });
          const rows = normalizeObservationRows(source, pages.items);
          const view = mapRows(source, rows, SCOPE_LATEST);
          const model = buildChoroplethModel(view.mappable, "geo_id");
          const outcome = pages.complete
            ? grade(oracle(rows), view, model)
            : { verdict: "fail", problem: `read stopped at ${rows.length} of ${pages.total} rows` };
          results.push({
            source: source.sourceCode,
            metric: metric.metric_code,
            grain,
            verdict: outcome.verdict,
            rows: rows.length,
            coloured: model.valueCount,
            problem: outcome.problem,
          });
        }
      }
    }

    const tally = new Map();
    for (const result of results) {
      const key = `${result.source} ${result.grain}`;
      const counts = tally.get(key) || { coloured: 0, declined: 0, empty: 0, fail: 0 };
      counts[result.verdict] += 1;
      tally.set(key, counts);
    }
    console.info(
      "map-display sweep\n" +
        [...tally.entries()]
          .map(([key, c]) =>
            `  ${key.padEnd(24)} coloured ${c.coloured}  declined ${c.declined}  empty ${c.empty}  FAIL ${c.fail}`,
          )
          .join("\n"),
    );
  }, 1_800_000);

  test("the sweep read maps from every source that publishes a drawable grain", () => {
    expect(results.length).toBeGreaterThan(0);
    const coloured = new Set(
      results.filter((result) => result.verdict === "coloured").map((result) => result.source),
    );
    const withValues = new Set(
      results.filter((result) => result.verdict !== "empty").map((result) => result.source),
    );
    // A source whose every map is declined or empty cannot show a reader
    // anything by default; name it rather than letting the tally hide it.
    expect([...withValues].filter((source) => !coloured.has(source))).toEqual([]);
  });

  test("no map shows the reader less than its rows hold", () => {
    const failures = results
      .filter((result) => result.verdict === "fail")
      .map(
        (result) =>
          `${result.source} ${result.grain} ${result.metric} (${result.rows} rows, ${result.coloured} coloured): ${result.problem}`,
      );
    expect(failures).toEqual([]);
  });

  reportUnhandledErrors();
});
