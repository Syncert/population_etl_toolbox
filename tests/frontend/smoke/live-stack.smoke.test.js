import { beforeAll, describe, expect, test } from "vitest";

// Covers: WEB-027 — the live-stack smoke tier. The explorer's own discovery
// and request-building modules run unmocked against a deployed API, Martin,
// and proxy, and are required to reach a state a reader could actually use:
// a vector layer that publishes the join key, a tile that decodes to real
// features, and a served observation page for every metric the catalog
// publishes, through whichever access shape the client itself selects.
//
// Why this tier exists. Two defects reached a running deployment with every
// other tier green, and both were invisible for the same reason: the fixtures
// encoded a shape the real services do not serve.
//
//   - Tile discovery read Martin's catalog as a flat map of layer ids. Martin
//     groups sources under section keys (`tiles`, `sprites`, `fonts`,
//     `styles`), so discovery probed the section names, got a 404 for each,
//     and reported that the deployment publishes no spatial layer. The browser
//     fixture served the flat shape, so it passed.
//   - Observations were read through the legacy source-scoped pair, which
//     keyed rows on the pre-glossary metric identity (`ACS:acs5:B01003_001`)
//     while the catalog publishes the glossary identity
//     (`CENSUS_ACS:acs5:B01003_001`). The real API answered an empty page; the
//     fixtures spelled both identities the same way, so they answered a full
//     one. ARC-005 later ended that disagreement in the warehouse, but the
//     lesson this tier encodes is unchanged: only a real response can catch a
//     client that is reading a correct server wrongly.
//
// A fixture cannot catch either class of defect, because in both cases the
// server was right and the client's reading of it was wrong. Only a real
// response can. So nothing here is stubbed: the only injected seam is a fetch
// that resolves the app's own same-origin paths against the deployed origin.
//
// The tier is opt-in. Without SMOKE_BASE_URL there is no stack to be right
// about, and these tests skip rather than inventing one.

import { apiFetch } from "../../../apps/web/lib/api/client";
import { buildExplorerSources } from "../../../apps/web/lib/explorerSources";
import { buildLatestObservationRequest } from "../../../apps/web/lib/observationAccess";
import { discoverTileMetadata, loadPreviewTileFeatures } from "../../../apps/web/lib/tiles";

const BASE_URL = (process.env.SMOKE_BASE_URL || "").replace(/\/+$/, "");

// A skipped suite is a passing suite, so the one environment that must never
// skip says so for itself. Without this, dropping SMOKE_BASE_URL from the CI
// job would turn this whole tier green and silent — the same failure mode it
// was built to end.
test("the smoke tier is configured wherever it is required", () => {
  if (process.env.SMOKE_REQUIRED === "1") {
    expect(BASE_URL, "SMOKE_REQUIRED=1 but SMOKE_BASE_URL is unset").toBeTruthy();
  }
});

// The app addresses its API and its tiles as same-origin paths, because in a
// deployment they are: nginx rewrites /api/ and /tiles/ onto the API and
// Martin. Node has no origin to resolve those against, so this is the one
// seam the tier installs — a base URL, not a stubbed response.
function installOriginResolvingFetch() {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (input, init) => {
    const target = typeof input === "string" ? input : input.url;
    return realFetch(target.startsWith("/") ? `${BASE_URL}${target}` : target, init);
  };
}

describe.skipIf(!BASE_URL)("live stack smoke", () => {
  /** @type {Awaited<ReturnType<typeof discoverTileMetadata>>} */
  let tiles;
  /** @type {ReturnType<typeof buildExplorerSources>} */
  let sources;

  beforeAll(async () => {
    installOriginResolvingFetch();
    // Fail the whole tier loudly if the stack is not actually there, rather
    // than letting every test report its own confusing symptom.
    const health = await apiFetch("/health");
    expect(health).toBeTruthy();
    tiles = await discoverTileMetadata();
    const capabilities = await apiFetch("/catalog/capabilities");
    sources = buildExplorerSources(capabilities.items);
  }, 60_000);

  test("the deployed tile catalog resolves to a layer that publishes its join key", () => {
    expect(tiles.layerId).toBeTruthy();
    expect(tiles.tileTemplate).toMatch(/\{z\}\/\{x\}\/\{y\}/);

    // pickJoinKey falls back to "geo_id" when it recognises nothing, so a
    // layer that publishes no usable key still yields a plausible-looking
    // join key. Asserting the key against the layer's own published fields is
    // what separates "discovered a joinable layer" from "guessed".
    expect(tiles.fields).toContain(tiles.joinKey);
  });

  test("a real tile decodes to features carrying the join key", async () => {
    const all = await loadPreviewTileFeatures(tiles.tileTemplate, tiles.sourceLayer, "NATIONAL");
    expect(all.features.length).toBeGreaterThan(0);
    for (const feature of all.features) {
      expect(feature.properties[tiles.joinKey]).toBeTruthy();
    }

    // The explorer filters the decoded features by grain. That filter reads
    // the published properties, so it is only correct against real ones.
    const counties = await loadPreviewTileFeatures(tiles.tileTemplate, tiles.sourceLayer, "COUNTY");
    expect(counties.features.length).toBeGreaterThan(0);
  }, 30_000);

  test("capability discovery yields at least one explorable source", () => {
    expect(sources.length).toBeGreaterThan(0);
    for (const source of sources) {
      expect(source.sourceCode).toBeTruthy();
      // A source-scoped source is a source the API declared no neutral route
      // for. That is legitimate, but it is now the exception, and a silent
      // drift back to it is the defect this tier exists to catch.
      expect(["neutral", "source-scoped"]).toContain(source.accessShape);
    }
  });

  test("every catalog metric answers through the access shape the explorer picks", async () => {
    const empty = [];
    let checked = 0;

    for (const source of sources) {
      const catalog = await apiFetch("/catalog/metrics", {
        params: { source_code: source.sourceCode, limit: 50 },
      });

      for (const metric of catalog.items || []) {
        const request = buildLatestObservationRequest(source, {
          metricCode: metric.metric_code,
          geoLevel: (metric.valid_geo_grains || [])[0] || "COUNTY",
          limit: 5,
        });
        const page = await apiFetch(request.resource, { params: request.params });
        checked += 1;

        // An empty page is the exact symptom both shipped defects produced,
        // and the one a reader cannot tell from "this geography publishes
        // nothing". The request that produced it is named so the failure says
        // which shape was chosen, not just that nothing came back.
        if (!(Number(page.total) > 0)) {
          empty.push(
            `${source.key}: ${metric.metric_code} returned ${page.total} from ${request.resource}`,
          );
        }
      }
    }

    // A catalog that publishes nothing would make the loop above vacuous, and
    // a vacuous pass here is precisely the reassurance this tier must not give.
    expect(checked, "no catalog metric was exercised: the stack has no seeded metrics").toBeGreaterThan(0);
    expect(empty).toEqual([]);
  }, 120_000);

  test("observed geographies are present in the discovered tile layer", async () => {
    const decoded = await loadPreviewTileFeatures(tiles.tileTemplate, tiles.sourceLayer, "NATIONAL");
    const tileGeoIds = new Set(
      decoded.features.map((feature) => feature.properties[tiles.joinKey]).filter(Boolean),
    );

    // The capability resource declares every completed source, including ones
    // the deployment has published no metric for yet. Joining needs a source
    // that actually publishes one, not whichever is declared first.
    let source = null;
    let metric = null;
    for (const candidate of sources) {
      const catalog = await apiFetch("/catalog/metrics", {
        params: { source_code: candidate.sourceCode, limit: 1 },
      });
      const published = (catalog.items || [])[0];
      if (published) {
        source = candidate;
        metric = published;
        break;
      }
    }
    expect(metric, "the stack publishes no metric to join against").toBeTruthy();

    const request = buildLatestObservationRequest(source, {
      metricCode: metric.metric_code,
      geoLevel: (metric.valid_geo_grains || [])[0] || "COUNTY",
      limit: 50,
    });
    const page = await apiFetch(request.resource, { params: request.params });
    const observed = (page.items || []).map((row) => row.geo_id).filter(Boolean);
    expect(observed.length).toBeGreaterThan(0);

    // The map draws a value only where an observation's geo_id matches a
    // feature's. Both halves can be individually healthy and still never
    // intersect, which renders as an empty map with no error anywhere.
    const joined = observed.filter((geoId) => tileGeoIds.has(geoId));
    expect(
      joined.length,
      `no observed geography is present in tile layer '${tiles.layerId}': ` +
        `observed ${observed.slice(0, 3).join(", ")}; tile publishes ${[...tileGeoIds].slice(0, 3).join(", ")}`,
    ).toBeGreaterThan(0);
  }, 60_000);
});
