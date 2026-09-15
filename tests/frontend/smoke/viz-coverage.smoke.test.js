import { beforeAll, describe, expect, test } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, parse } from "node:path";

// Covers: WEB-104 — every visualization the web offers is asked for, against a
// real deployment, for every source the reviewed matrix says it serves.
//
// The two offline tiers establish that the API *declares* what each screen
// needs and that the client offers exactly what is declared. Neither can see
// whether the deployment behind those declarations has anything to put on the
// screen. A source can declare `/observations`, accept every parameter the
// heatmap sends, and answer `total: 0` — and the reader gets an empty grid
// with no error, which is the failure `/health/content` was built to catch one
// level down and this tier catches at the presentation.
//
// It is deliberately a *coverage report* first and a bound second. Every run
// prints which source can be drawn in which presentation, so the answer to
// "is the deployment serving everything the web can show" is a table an
// operator reads rather than a pass/fail they infer. `SMOKE_REQUIRE_ALL_VIZ`
// then turns the table into a failure, and is set where the content is seeded
// and therefore known (`frontend-smoke`), and opt-in for a deployment whose
// warehouse may legitimately be part-way through a load.
//
// Two facts are always asserted, bound or not, because neither depends on how
// much the warehouse holds:
//
//   - the deployed capability payload declares what this checkout's reviewed
//     matrix says it declares, so a deployment running an older API is named
//     rather than quietly grading fewer screens;
//   - the sweep was not vacuous.

import { apiFetch } from "../../../apps/web/lib/api/client";
import { buildExplorerSources } from "../../../apps/web/lib/explorerSources";
import {
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
  buildSettledHistoryRequest,
  buildSettledSurfaceRequest,
} from "../../../apps/web/lib/observationAccess";
import { DRAWABLE_TILE_GRAINS } from "../../../apps/web/lib/tileGrains";
import { reportUnhandledErrors } from "./unhandledErrors";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");
const REQUIRE_ALL_VIZ = process.env.SMOKE_REQUIRE_ALL_VIZ === "1";
// A stricter bound: every served cell must also be reachable to probe.
const REQUIRE_FULL_VIZ_SAMPLE = process.env.SMOKE_REQUIRE_FULL_VIZ_SAMPLE === "1";

function findSnapshot() {
  let directory = process.cwd();
  for (;;) {
    const candidate = join(directory, "tests", "fixtures", "api", "viz_coverage.json");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(directory);
    if (parent === directory || directory === parse(directory).root) {
      throw new Error("the reviewed visualization coverage matrix was not found");
    }
    directory = parent;
  }
}

const REVIEWED = JSON.parse(readFileSync(findSnapshot(), "utf8"));

// A skipped suite is a passing suite, so the one environment that must never
// skip says so for itself — the same guard the live-stack tier carries.
test("the visualization sweep is configured wherever it is required", () => {
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

/** How many current metrics of a source the sweep reads. */
const METRIC_SAMPLE = 2;
/** Rows a probe asks for: enough to tell "published" from "nothing". */
const PROBE_LIMIT = 5;

const DRAWABLE = DRAWABLE_TILE_GRAINS.map((entry) => entry.grain);

/**
 * The live probe for each surface that reads observations.
 *
 * Every request is built by the module the application calls, so a probe that
 * answers is evidence about the screen and not about this file. A surface
 * absent here is one whose reachability is a declaration rather than a row
 * (`explorer.quality`, `explorer.export`); those are graded by the offline
 * tiers, and asking the deployment for them again would prove nothing new.
 */
const PROBES = {
  "explorer.map": (source, { metricCode, drawableGrain }) =>
    drawableGrain
      ? buildLatestObservationRequest(source, {
          metricCode,
          geoLevel: drawableGrain,
          limit: PROBE_LIMIT,
        })
      : null,
  "explorer.table": (source, { metricCode, grain }) =>
    buildLatestObservationRequest(source, {
      metricCode,
      geoLevel: grain,
      limit: PROBE_LIMIT,
    }),
  "explorer.trend": (source, { metricCode, geoId }) =>
    geoId
      ? buildHistoryObservationRequest(source, {
          metricCode,
          geoId,
          limit: PROBE_LIMIT,
        })
      : null,
  "explorer.as_released": (source, { metricCode, grain }) =>
    buildLatestObservationRequest(source, {
      metricCode,
      geoLevel: grain,
      limit: PROBE_LIMIT,
      scope: "as_released",
    }),
  "explorer.settled_history": (source, { metricCode, geoId }) =>
    geoId
      ? buildSettledHistoryRequest(source, {
          metricCode,
          geoId,
          limit: PROBE_LIMIT,
        })
      : null,
  "workbench.heatmap": (source, { metricCode, grain }) =>
    buildSettledSurfaceRequest(source, {
      metricCode,
      geoLevel: grain,
      limit: PROBE_LIMIT,
    }),
  "workbench.cross_section": (source, { metricCode, grain }) =>
    source.supportsNewestPerGeography
      ? buildLatestObservationRequest(source, {
          metricCode,
          geoLevel: grain,
          limit: PROBE_LIMIT,
          newestPerGeography: true,
        })
      : null,
  "explorer.distribution": (source, { metricCode, grain }) => ({
    resource: "/distribution/bins",
    params: { metric_code: metricCode, geo_level: grain, bin_count: 5 },
  }),
  // The preflight is the cheapest live proof that the aligned analysis
  // surface resolves this source's metric at all: it answers 200 for any
  // known pair, and a pair of one metric with itself asks nothing of the
  // warehouse's breadth.
  "comparison.workspace": (source, { metricCode }) => ({
    resource: "/comparison/preflight",
    params: { metric_code_a: metricCode, metric_code_b: metricCode },
  }),
};

/** Whether one probe's answer is something the screen could draw. */
function answered(surfaceId, page) {
  if (surfaceId === "comparison.workspace") {
    // The preflight is a verdict, not a page: it answers 200 for any known
    // pair, and what it proves here is that the analysis surface resolves
    // this source's metric at all.
    return Boolean(page && page.metric_code_a);
  }
  if (surfaceId === "explorer.distribution") {
    // The bins resource answers a collection like every other one: `items`
    // holds the bins and `total` counts the geographies binned.
    return Array.isArray(page?.items) && page.items.length > 0;
  }
  return Number(page?.total) > 0;
}

describe.skipIf(!BASE_URL)("every visualization is asked for, live", () => {
  /** @type {ReturnType<typeof buildExplorerSources>} */
  let sources;
  /**
   * One ready-made probe subject per source: a current measure, a grain it
   * publishes, a grain the boundary can draw where it has one, and a geography
   * the deployment actually served for it.
   *
   * The geography is read rather than composed. A `geo_id` this file spelled
   * would be this file's idea of the deployment's coverage, and a trend that
   * answered nothing would then mean "the seed does not hold that county"
   * rather than "this screen is empty" — two facts a coverage report must not
   * confuse.
   *
   * @type {Map<string, {metricCode: string, grain: string, drawableGrain: string|null, geoId: string|null}>}
   */
  const subjects = new Map();

  beforeAll(async () => {
    installOriginResolvingFetch();
    const health = await apiFetch("/health");
    expect(health, "the deployment did not answer /health").toBeTruthy();
    const discovered = await apiFetch("/catalog/capabilities");
    sources = buildExplorerSources(discovered.items || []);

    for (const source of sources) {
      const catalog = await apiFetch("/catalog/metrics", {
        params: {
          source_code: source.sourceCode,
          active_only: "true",
          limit: METRIC_SAMPLE,
        },
      });
      for (const metric of catalog.items || []) {
        const grains = metric.valid_geo_grains || [];
        if (grains.length === 0) {
          continue;
        }
        const grain = grains[0];
        const drawableGrain = grains.find((entry) => DRAWABLE.includes(entry)) || null;
        const latest = buildLatestObservationRequest(source, {
          metricCode: metric.metric_code,
          geoLevel: grain,
          limit: PROBE_LIMIT,
        });
        const page = await apiFetch(latest.resource, { params: latest.params });
        const geoId = (page.items || []).map((row) => row.geo_id).find(Boolean) || null;
        subjects.set(source.sourceCode, {
          metricCode: metric.metric_code,
          grain,
          drawableGrain,
          geoId,
        });
        if (geoId) {
          break;
        }
      }
    }
  }, 180_000);

  test("the deployed contract declares what the reviewed matrix says it does", () => {
    // A deployment running an older API declares fewer routes, so fewer
    // screens are gradable -- and a sweep over the smaller set reports the
    // same green as one over the whole surface. That is the silent skip
    // DB-043 closed in the catalog sweeps, and it reaches here through the
    // deployment rather than through a fixture.
    const drift = [];
    const declaredNow = new Map(
      buildExplorerSources(REVIEWED.capabilities).map((source) => [
        source.sourceCode,
        source,
      ]),
    );
    for (const [sourceCode, expected] of declaredNow) {
      const live = sources.find((source) => source.sourceCode === sourceCode);
      if (!live) {
        drift.push(`${sourceCode}: declared in this checkout, absent from the deployment`);
        continue;
      }
      for (const flag of [
        "servesDistribution",
        "servesComparison",
        "servesCorrelation",
        "servesMatrix",
        "servesReleases",
        "supportsAsReleased",
        "supportsReleasePin",
        "supportsNewestPerGeography",
        "supportsSettledHistory",
        "publishesAlignedReduction",
      ]) {
        if (live[flag] !== expected[flag]) {
          drift.push(
            `${sourceCode}.${flag}: deployment says ${live[flag]}, this ` +
              `checkout's reviewed matrix says ${expected[flag]}`,
          );
        }
      }
    }
    expect(drift, drift.join("\n")).toEqual([]);
  });

  test("every source the matrix serves can be drawn in every surface", async () => {
    const report = [];
    const empty = [];
    const unexercised = [];
    let probed = 0;

    for (const surface of REVIEWED.surfaces) {
      const probe = PROBES[surface.surface_id];
      if (!probe) {
        continue;
      }
      for (const sourceCode of surface.served_sources) {
        const source = sources.find((entry) => entry.sourceCode === sourceCode);
        if (!source) {
          // Named by the drift test above; not re-reported per surface.
          continue;
        }
        const subject = subjects.get(sourceCode);
        if (!subject) {
          unexercised.push(
            `${surface.surface_id}/${sourceCode}: the deployment publishes no ` +
              "current measure at any grain",
          );
          report.push(`  ${surface.surface_id} ${sourceCode}: NO CURRENT MEASURE`);
          continue;
        }

        const request = probe(source, subject);
        if (!request) {
          // The screen would not make the request either -- the source has no
          // drawable grain here, or the deployment served no geography to ask
          // a history of. That is a fact about this warehouse's content, not
          // about the presentation, so it is reported apart from an empty
          // answer and bound separately.
          unexercised.push(
            `${surface.surface_id}/${sourceCode}: no request could be built ` +
              `from ${JSON.stringify(subject)}`,
          );
          report.push(`  ${surface.surface_id} ${sourceCode}: NOT EXERCISED`);
          continue;
        }

        probed += 1;
        let page = null;
        let failure = "";
        try {
          page = await apiFetch(request.resource, { params: request.params });
        } catch (error) {
          failure = ` -> ${error.message}`;
        }
        const drew = !failure && answered(surface.surface_id, page);
        report.push(
          `  ${surface.surface_id} ${sourceCode}: ${drew ? "draws" : "EMPTY"}`,
        );
        if (!drew) {
          empty.push(
            `${surface.surface_id}/${sourceCode}: ${surface.title} answered ` +
              `nothing a reader could see; ${request.resource} ` +
              `${JSON.stringify(request.params)}${failure}`,
          );
        }
      }
    }

    // Printed on every run, bound or not. The question this tier exists to
    // answer is "what can the deployment actually show", and that is a table.
    // eslint-disable-next-line no-console
    console.log(
      `\nvisualization coverage against ${BASE_URL}\n${report.join("\n")}\n` +
        `${probed} probes, ${empty.length} empty, ` +
        `${unexercised.length} not exercised\n`,
    );

    expect(
      probed,
      "no visualization was probed: the deployment publishes no current " +
        "measure at any grain, so this sweep graded nothing",
    ).toBeGreaterThan(0);

    if (REQUIRE_ALL_VIZ) {
      // A request that was made and answered nothing is always a defect: the
      // API declared the screen servable for this source and it is not.
      expect(empty, empty.join("\n")).toEqual([]);
    }
    if (REQUIRE_FULL_VIZ_SAMPLE) {
      // A stricter bound, for a stack whose content is seeded and therefore
      // known: every cell the matrix serves must also have been reachable to
      // ask. Separate because a violation means the seed is narrow, not that
      // the deployment is broken.
      expect(unexercised, unexercised.join("\n")).toEqual([]);
    }
  }, 300_000);

  test("a source the matrix declines is not silently offered by the deployment", () => {
    const offered = [];
    for (const surface of REVIEWED.surfaces) {
      for (const sourceCode of Object.keys(surface.declined_sources)) {
        const source = sources.find((entry) => entry.sourceCode === sourceCode);
        if (!source) {
          continue;
        }
        const live = {
          "explorer.distribution": source.servesDistribution,
          "comparison.workspace": source.servesComparison,
          "workbench.correlation": source.servesCorrelation,
          "workbench.matrix": source.servesMatrix,
        }[surface.surface_id];
        if (live === true) {
          offered.push(
            `${surface.surface_id}/${sourceCode}: the deployment declares it, ` +
              `the reviewed matrix declines it — ` +
              `${surface.declined_sources[sourceCode]}`,
          );
        }
      }
    }
    expect(offered, offered.join("\n")).toEqual([]);
  });
});

reportUnhandledErrors();
