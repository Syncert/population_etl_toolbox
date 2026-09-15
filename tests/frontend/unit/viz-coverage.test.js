import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join, parse } from "node:path";
import { existsSync } from "node:fs";

// Covers: WEB-103 — the web offers exactly the visualizations the API
// declares, for exactly the sources it declares them for.
//
// `tests/unit/api/test_viz_coverage.py` establishes one half of this: which
// source can be drawn in which presentation, according to the capability
// payload `/catalog/capabilities` serves. That is a statement about the API.
// This is the other half, and the two must agree, because a screen goes blank
// in either direction:
//
//   - The API declares a route for a source and the client does not offer the
//     presentation. The reader is told the data does not exist when it does.
//   - The client offers a presentation the API declares no route for. The
//     request 422s, or answers an empty page, and the reader sees an empty
//     chart with no error — the failure mode this whole matrix exists for.
//
// Nothing is mocked. `buildExplorerSources` runs over the real capability
// payload the API serves, and every request below is built by the module the
// application itself calls. The snapshot is the only shared artifact: two
// languages cannot import one declaration, so they read one reviewed file.

import {
  buildExplorerSources,
  NEUTRAL_OBSERVATIONS_PATH,
} from "../../../apps/web/lib/explorerSources";
import {
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
  buildSettledHistoryRequest,
  buildSettledSurfaceRequest,
  servesAsReleased,
} from "../../../apps/web/lib/observationAccess";
import { servesHistory, spatialGrains } from "../../../apps/web/lib/viewModes";
import { DRAWABLE_TILE_GRAINS } from "../../../apps/web/lib/tileGrains";
import { correlationEligibility } from "../../../apps/web/lib/workbench";
import { getFreshness, getMetric } from "../../../apps/web/lib/api/client";

/** The reviewed matrix, found by walking up from the working directory. */
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

const snapshot = JSON.parse(readFileSync(findSnapshot(), "utf8"));
const sources = buildExplorerSources(snapshot.capabilities);
const sourceByCode = new Map(sources.map((source) => [source.sourceCode, source]));

const METRIC = "TEST:metric";
const GEO_ID = "state:55";

/** A grain this source actually publishes, preferring one the map can draw. */
function grainFor(sourceCode) {
  const published = snapshot.advertised_geo_grains[sourceCode] || [];
  const drawable = published.filter((grain) =>
    DRAWABLE_TILE_GRAINS.some((entry) => entry.grain === grain),
  );
  return drawable[0] || published[0] || "NATIONAL";
}

/** The tile fields a boundary publishing every drawable grain would carry. */
const ALL_TILE_FIELDS = ["geo_id", ...DRAWABLE_TILE_GRAINS.map((e) => e.attributionField)];

/**
 * Whether the web offers one surface for one source, decided by the modules
 * the application itself calls.
 *
 * Each entry answers the same question its screen asks, in the same terms: a
 * request the client would actually build, or the offer predicate the page
 * reads. Nothing here consults the matrix — that is what it is graded against.
 */
const WEB_OFFERS = {
  "explorer.map": (source) =>
    spatialGrains(ALL_TILE_FIELDS).includes(grainFor(source.sourceCode)),
  "explorer.trend": (source) => servesHistory(source),
  "explorer.table": (source) =>
    Boolean(
      buildLatestObservationRequest(source, {
        metricCode: METRIC,
        geoLevel: grainFor(source.sourceCode),
        limit: 50,
      }).resource,
    ),
  "explorer.export": (source) => source.publishedDimensions.length > 0,
  // Read from the catalog for every source alike. The offer is that the
  // client has a way to ask at all, which is the same fact the API side
  // asserts about the two catalog routes.
  "explorer.quality": () =>
    typeof getFreshness === "function" && typeof getMetric === "function",
  "explorer.distribution": (source) => source.servesDistribution,
  "explorer.as_released": (source) =>
    servesAsReleased(source) && source.supportsReleasePin,
  "explorer.settled_history": (source) =>
    buildSettledHistoryRequest(source, { metricCode: METRIC, geoId: GEO_ID }) !== null,
  "comparison.workspace": (source) => source.servesComparison,
  "workbench.series": (source) => servesHistory(source),
  "workbench.cross_section": (source) => source.supportsNewestPerGeography,
  "workbench.heatmap": (source) =>
    buildSettledSurfaceRequest(source, {
      metricCode: METRIC,
      geoLevel: grainFor(source.sourceCode),
    }) !== null,
  "workbench.correlation": (source) => source.servesCorrelation,
  "workbench.matrix": (source) => source.servesMatrix,
  "profiles.product": (source) => servesHistory(source),
};

describe("the web offers what the API declares", () => {
  test("discovery yields every source the capability payload publishes", () => {
    expect(sources.length).toBe(snapshot.capabilities.length);
    expect(sources.length).toBeGreaterThanOrEqual(7);
    for (const capability of snapshot.capabilities) {
      expect(sourceByCode.has(capability.source_code)).toBe(true);
    }
  });

  test("every surface in the reviewed matrix has a web predicate", () => {
    // Without this, a surface added to the matrix would simply not be graded
    // here, and the web half of the contract would silently stop covering it.
    const declared = snapshot.surfaces.map((surface) => surface.surface_id).sort();
    expect(Object.keys(WEB_OFFERS).sort()).toEqual(declared);
  });

  test.each(snapshot.surfaces.map((surface) => [surface.surface_id, surface]))(
    "%s is offered for exactly the sources the API serves it for",
    (surfaceId, surface) => {
      const offers = WEB_OFFERS[surfaceId];
      const served = new Set(surface.served_sources);
      const mismatched = [];
      for (const capability of snapshot.capabilities) {
        const source = sourceByCode.get(capability.source_code);
        const offered = Boolean(source && offers(source));
        if (offered !== served.has(capability.source_code)) {
          mismatched.push(
            offered
              ? `${capability.source_code}: the web offers ${surface.title}, and the ` +
                `API declares no route for it — ` +
                `${surface.declined_sources[capability.source_code] || "not served"}`
              : `${capability.source_code}: the API serves ${surface.title} and the ` +
                "web does not offer it, so a reader is told data is missing " +
                "that is published",
          );
        }
      }
      expect(mismatched, mismatched.join("\n")).toEqual([]);
    },
  );
});

describe("a request the web builds carries what the surface needs", () => {
  /** The builders whose requests are checked against the declared parameters. */
  const REQUESTS = {
    "explorer.map": (source) =>
      buildLatestObservationRequest(source, {
        metricCode: METRIC,
        geoLevel: grainFor(source.sourceCode),
        limit: 50,
      }),
    "explorer.trend": (source) =>
      buildHistoryObservationRequest(source, {
        metricCode: METRIC,
        geoId: GEO_ID,
        limit: 500,
      }),
    "explorer.settled_history": (source) =>
      buildSettledHistoryRequest(source, {
        metricCode: METRIC,
        geoId: GEO_ID,
        limit: 500,
      }),
    "workbench.heatmap": (source) =>
      buildSettledSurfaceRequest(source, {
        metricCode: METRIC,
        geoLevel: grainFor(source.sourceCode),
        limit: 500,
      }),
  };

  test.each(Object.keys(REQUESTS))(
    "%s sends every parameter the matrix says the route requires",
    (surfaceId) => {
      const surface = snapshot.surfaces.find((entry) => entry.surface_id === surfaceId);
      expect(surface, `${surfaceId} is not in the reviewed matrix`).toBeTruthy();
      const required = surface.required_parameters[NEUTRAL_OBSERVATIONS_PATH] || [];
      expect(required.length, `${surfaceId} requires no parameter`).toBeGreaterThan(0);

      const dropped = [];
      for (const sourceCode of surface.served_sources) {
        const source = sourceByCode.get(sourceCode);
        const request = REQUESTS[surfaceId](source);
        if (!request || request.resource !== NEUTRAL_OBSERVATIONS_PATH) {
          // A source-scoped shape is a different route with different
          // parameters; the matrix's neutral requirement does not describe it.
          continue;
        }
        for (const name of required) {
          if (
            request.params[name] === undefined ||
            request.params[name] === null ||
            request.params[name] === ""
          ) {
            dropped.push(
              `${sourceCode}: ${surfaceId} builds ${request.resource} without ` +
                `${name}, which the surface needs; the answer would be a ` +
                "wider or emptier page than the screen asked for",
            );
          }
        }
      }
      expect(dropped, dropped.join("\n")).toEqual([]);
    },
  );
});

describe("the correlation panel asks only on routes that are declared", () => {
  const declaredRoutes = Object.fromEntries(
    sources.map((source) => [
      source.sourceCode,
      { correlation: source.servesCorrelation, matrix: source.servesMatrix },
    ]),
  );

  function seriesFor(sourceCode, metricCode) {
    return {
      sourceKey: sourceCode.toLowerCase(),
      sourceCode,
      metricCode,
      scope: "latest",
      geoLevel: "STATE",
      geoId: GEO_ID,
      filters: {},
    };
  }

  test("a pair of analysis-ready sources is eligible", () => {
    const eligible = snapshot.surfaces.find(
      (surface) => surface.surface_id === "workbench.correlation",
    ).served_sources;
    expect(eligible.length).toBeGreaterThanOrEqual(2);
    const verdict = correlationEligibility({
      series: [seriesFor(eligible[0], "A"), seriesFor(eligible[1], "B")],
      declaredRoutes,
    });
    expect(verdict.eligible, verdict.reason).toBe(true);
    expect(verdict.route).toBe("correlation");
  });

  test("a source the matrix declines is refused before any request", () => {
    const surface = snapshot.surfaces.find(
      (entry) => entry.surface_id === "workbench.correlation",
    );
    const declined = Object.keys(surface.declined_sources);
    expect(declined.length).toBeGreaterThan(0);
    for (const sourceCode of declined) {
      const verdict = correlationEligibility({
        series: [
          seriesFor(surface.served_sources[0], "A"),
          seriesFor(sourceCode, "B"),
        ],
        declaredRoutes,
      });
      expect(verdict.eligible, `${sourceCode} was offered a correlation`).toBe(false);
      expect(verdict.route).toBeNull();
    }
  });
});

describe("the matrix itself is not vacuous", () => {
  test("it grades every surface against every source", () => {
    const sourceCount = snapshot.capabilities.length;
    for (const surface of snapshot.surfaces) {
      const graded =
        surface.served_sources.length + Object.keys(surface.declined_sources).length;
      expect(graded, `${surface.surface_id} grades ${graded} of ${sourceCount}`).toBe(
        sourceCount,
      );
    }
  });

  test("the drawable grains it recorded are the ones the web declares", () => {
    expect(snapshot.drawable_tile_grains).toEqual(
      DRAWABLE_TILE_GRAINS.map((entry) => entry.grain),
    );
  });
});
