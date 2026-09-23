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
//     stratified, and the page must decline *and* name what varies -- and
//     narrowing it with the declared filters, as a reader would, must yield a
//     map that colours ("narrowed"); a dimension no filter can narrow fails;
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
  observationDimensionValue,
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

/** The most common published value of one dimension across the rows. */
function mostCommon(rows, name) {
  const counts = new Map();
  for (const row of rows) {
    const value = observationDimensionValue(row, name);
    if (value !== "") {
      counts.set(value, (counts.get(value) || 0) + 1);
    }
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0] || "";
}

/** One explorer map read end to end, exactly as the page reads it. */
async function readMap(source, metricCode, grain, dimensions) {
  const { resource, params } = buildLatestObservationRequest(source, {
    metricCode,
    geoLevel: grain,
    stateFips: "",
    limit: String(PAGE_SIZE),
    newestPerGeography: true,
    scope: SCOPE_LATEST,
    dimensions,
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
  return { rows, view, model, outcome };
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
          let dimensions = {};
          let read = await readMap(source, metric.metric_code, grain, dimensions);
          let outcome = read.outcome;
          // A declined map is only half an answer: the reader must be able to
          // narrow it to one series with the filters the source declares, and
          // the narrowed map must colour. Narrow as a reader would -- each
          // declared filter among the separating dimensions, set to its most
          // common published value -- until it colours or cannot be narrowed.
          for (let attempt = 0; outcome.verdict === "declined" && attempt < 3; attempt += 1) {
            const varying = read.view.stratification.varyingDimensions.filter(
              (name) => !(name in dimensions),
            );
            // Only the declared filters can be set. A dimension that merely
            // describes one of them (CDC's `strata` and footnotes move with
            // `stratum_id`) narrows with it; if it does not, the re-read
            // below is still declined and fails by name.
            const filterable = varying.filter((name) => source.dimensionFilters.includes(name));
            if (filterable.length === 0) {
              outcome = {
                verdict: "fail",
                problem: `declined by ${varying.join(", ") || "nothing named"}, which no declared filter narrows${
                  Object.keys(dimensions).length ? ` (after narrowing ${JSON.stringify(dimensions)})` : ""
                }`,
              };
              break;
            }
            dimensions = { ...dimensions };
            for (const name of filterable) {
              dimensions[name] = mostCommon(read.rows, name);
            }
            read = await readMap(source, metric.metric_code, grain, dimensions);
            outcome =
              read.outcome.verdict === "coloured"
                ? { verdict: "narrowed", problem: null }
                : read.outcome.verdict === "declined"
                  ? read.outcome
                  : {
                      verdict: "fail",
                      problem: `narrowed by ${JSON.stringify(dimensions)}: ${read.outcome.problem || read.outcome.verdict}`,
                    };
          }
          if (outcome.verdict === "declined") {
            outcome = {
              verdict: "fail",
              problem: `still declined after narrowing ${JSON.stringify(dimensions)}`,
            };
          }
          const { rows, model } = read;
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
      const counts = tally.get(key) || { coloured: 0, narrowed: 0, empty: 0, fail: 0 };
      counts[result.verdict] += 1;
      tally.set(key, counts);
    }
    console.info(
      "map-display sweep\n" +
        [...tally.entries()]
          .map(([key, c]) =>
            `  ${key.padEnd(24)} coloured ${c.coloured}  narrowed ${c.narrowed}  empty ${c.empty}  FAIL ${c.fail}`,
          )
          .join("\n"),
    );
  }, 1_800_000);

  test("the sweep read maps from every source that publishes a drawable grain", () => {
    expect(results.length).toBeGreaterThan(0);
    const coloured = new Set(
      results
        .filter((result) => ["coloured", "narrowed"].includes(result.verdict))
        .map((result) => result.source),
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
