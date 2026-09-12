import { afterEach, describe, expect, test, vi } from "vitest";

// Covers: WEB-026 — tile-layer discovery against the catalog shapes Martin
// actually serves. Martin groups its sources into catalog sections
// (`tiles`, `sprites`, `fonts`, `styles`) alongside a `settings` object, so
// the source ids live one level down. Treating every top-level key as a
// layer id makes discovery probe the section names, find nothing, and
// report that the deployment publishes no spatial layer at all — which is a
// very different statement from "this build could not read the catalog".

import {
  buildSampleUrlFromTemplate,
  collectTileCandidates,
  discoverTileMetadata,
  normalizeTileTemplateFromTileJson,
  prioritizeTileCandidates,
} from "../../../apps/web/lib/tiles";

// Captured verbatim from Martin 1.11.0 at /catalog, the version pinned in
// infra/docker/docker-compose.yml.
const MARTIN_CATALOG = {
  tiles: {
    counties: {
      content_type: "application/x-protobuf",
      description: "gold.dim_geo_latest.geo_geom",
    },
  },
  sprites: {},
  fonts: {},
  styles: {},
  settings: { rendering: false },
};

describe("tile candidates come from the catalog's source sections", () => {
  test("Martin's section-keyed catalog yields its source ids, not its sections", () => {
    const candidates = collectTileCandidates(MARTIN_CATALOG);
    expect(candidates).toContain("counties");
    // The section names are the catalog's own structure. Probing them as
    // layers is what made a healthy deployment look like it published none.
    for (const section of ["tiles", "sprites", "fonts", "styles", "settings"]) {
      expect(candidates).not.toContain(section);
    }
  });

  test("a source is still found when other sections carry their own entries", () => {
    const candidates = collectTileCandidates({
      ...MARTIN_CATALOG,
      fonts: { "Open Sans Regular": { family: "Open Sans" } },
      sprites: { basic: { images: [] } },
    });
    // Only tile sources may be offered as vector layers; a font or sprite
    // id is not something the map can join observations to.
    expect(candidates).toEqual(["counties"]);
  });

  test("a flat catalog of bare layer ids still works", () => {
    // Older/simpler deployments answer with the ids at the top level.
    expect(collectTileCandidates({ counties: {}, dim_geo_latest: {} })).toEqual([
      "counties",
      "dim_geo_latest",
    ]);
  });

  test("array and collection catalog shapes are unchanged", () => {
    expect(collectTileCandidates(["counties", "states"])).toEqual(["counties", "states"]);
    expect(collectTileCandidates([{ id: "counties" }])).toEqual(["counties"]);
    expect(
      collectTileCandidates({ collections: [{ id: "dim_geo_latest" }] }),
    ).toContain("dim_geo_latest");
  });

  test("an unreadable catalog yields nothing rather than a guess", () => {
    expect(collectTileCandidates(null)).toEqual([]);
    expect(collectTileCandidates(undefined)).toEqual([]);
    expect(collectTileCandidates({})).toEqual([]);
    // A catalog whose only content is metadata publishes no tile source.
    expect(collectTileCandidates({ settings: { rendering: false } })).toEqual([]);
  });

  test("the reviewed geography layers keep their probe priority", () => {
    expect(prioritizeTileCandidates(["counties", "dim_geo_latest"])).toEqual([
      "dim_geo_latest",
      "counties",
    ]);
    expect(prioritizeTileCandidates(collectTileCandidates(MARTIN_CATALOG))).toEqual([
      "counties",
    ]);
  });
});

// Covers: WEB-026 — a TileJSON `tiles` entry is a URL *template*, and the
// `{z}/{x}/{y}` placeholders in it are the whole point. Martin answers with an
// absolute template built from the request Host, so this is the ordinary case
// on every deployment, not an edge one.
describe("a TileJSON template survives normalization with its placeholders intact", () => {
  // Captured verbatim from Martin 1.11.0 at /tiles/counties.
  const MARTIN_TEMPLATE = "http://127.0.0.1:3200/tiles/counties/{z}/{x}/{y}";

  test("the absolute template keeps its literal braces", () => {
    const normalized = normalizeTileTemplateFromTileJson(MARTIN_TEMPLATE);
    expect(normalized).toBe("/tiles/counties/{z}/{x}/{y}");
    // Percent-encoded braces are the specific corruption to guard against:
    // `new URL(...).pathname` produces them, and they read as a plausible
    // path while being one no substitution can ever fill.
    expect(normalized).not.toContain("%7B");
  });

  test("the normalized template still substitutes into a real tile URL", () => {
    // This is the assertion that matters. A template whose placeholders were
    // encoded still looks like a template, and still yields a URL — one that
    // asks the tile server for a tile at row "{y}", which it answers with a
    // 404. Discovery then rejects a healthy layer and falls through to a
    // guessed path, so the map is empty on exactly the deployments whose
    // TileJSON was telling the truth.
    const sample = buildSampleUrlFromTemplate(normalizeTileTemplateFromTileJson(MARTIN_TEMPLATE));
    expect(sample).toBe("/tiles/counties/0/0/0");
    expect(sample).not.toContain("{");
    expect(sample).not.toContain("%7B");
  });

  test("a query string is carried through unencoded", () => {
    // A deployment that authenticates its tile server puts the key here, and
    // an encoded template would drop it into a 404 as well.
    const normalized = normalizeTileTemplateFromTileJson(
      "https://tiles.example.org/counties/{z}/{x}/{y}.pbf?key=abc123",
    );
    expect(normalized).toBe("/tiles/counties/{z}/{x}/{y}.pbf?key=abc123");
    expect(buildSampleUrlFromTemplate(normalized)).toBe("/tiles/counties/0/0/0.pbf?key=abc123");
  });

  test("a relative template is unchanged and a non-tiles path is mounted under /tiles", () => {
    expect(normalizeTileTemplateFromTileJson("/tiles/counties/{z}/{x}/{y}")).toBe(
      "/tiles/counties/{z}/{x}/{y}",
    );
    expect(normalizeTileTemplateFromTileJson("/counties/{z}/{x}/{y}")).toBe(
      "/tiles/counties/{z}/{x}/{y}",
    );
    expect(normalizeTileTemplateFromTileJson("counties/{z}/{x}/{y}")).toBe(
      "/tiles/counties/{z}/{x}/{y}",
    );
    expect(normalizeTileTemplateFromTileJson("")).toBe("");
    expect(normalizeTileTemplateFromTileJson(null)).toBe("");
  });
});

// Covers: WEB-026 — discovery reads at most one body per probe and releases
// the rest. Deciding a probe from its status or its content type leaves a
// response whose body was never read, and an unread body holds its
// connection open until the response is collected. The tile samples are the
// case that matters: each probe is a whole world tile, and discovery probes
// up to six templates per candidate layer before it draws anything.
//
// The live-stack tier is where this first showed as more than a tidiness
// point — Node's HTTP client asserts on a socket whose parser is still
// paused by an unread body, so the leak surfaced as an uncaught exception
// after every otherwise-passing run.
describe("tile discovery releases the response bodies it does not read", () => {
  const realFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = realFetch;
  });

  /** A response that records whether its body was cancelled. */
  function stubResponse({ ok = true, status = 200, contentType = "", json = {} }) {
    const cancel = vi.fn().mockResolvedValue(undefined);
    return {
      ok,
      status,
      headers: { get: (name) => (name.toLowerCase() === "content-type" ? contentType : null) },
      json: async () => json,
      body: { cancel },
      cancel,
    };
  }

  const TILE_JSON = {
    name: "counties",
    tiles: ["http://127.0.0.1:3200/tiles/counties/{z}/{x}/{y}"],
    vector_layers: [{ id: "counties", fields: { geo_id: "", state_fips: "", county_fips: "" } }],
  };

  test("a rejected discovery endpoint and every tile-sample probe are released", async () => {
    const served = new Map();
    // The catalog endpoint answers 404, so discovery falls through to
    // `/tiles/` — the rejected response is decided from its status alone.
    served.set("/tiles/catalog", stubResponse({ ok: false, status: 404 }));
    served.set("/tiles/", stubResponse({ json: MARTIN_CATALOG }));
    served.set("/tiles/counties", stubResponse({ json: TILE_JSON }));
    // The first template probed is the one TileJSON publishes, and it is
    // accepted on its content type — without its body ever being read.
    served.set(
      "/tiles/counties/0/0/0",
      stubResponse({ contentType: "application/x-protobuf" }),
    );

    globalThis.fetch = vi.fn(async (path) => {
      const response = served.get(path);
      if (!response) {
        throw new Error(`unexpected discovery request: ${path}`);
      }
      return response;
    });

    const discovered = await discoverTileMetadata();
    expect(discovered.layerId).toBe("counties");
    expect(discovered.joinKey).toBe("geo_id");

    // The two bodies discovery decided without reading.
    expect(served.get("/tiles/catalog").cancel).toHaveBeenCalled();
    expect(served.get("/tiles/counties/0/0/0").cancel).toHaveBeenCalled();
    // The two it did read are consumed, not cancelled; cancelling a body
    // mid-read is what would truncate the catalog it is parsing.
    expect(served.get("/tiles/").cancel).not.toHaveBeenCalled();
    expect(served.get("/tiles/counties").cancel).not.toHaveBeenCalled();
  });

  test("a response with no body to cancel is not an error", async () => {
    // A 304 or a HEAD carries no body, and a discovery probe must not fail
    // over how its own discarded response was disposed of.
    const bodiless = { ok: false, status: 304, headers: { get: () => null }, json: async () => ({}) };
    const served = new Map([
      ["/tiles/catalog", bodiless],
      ["/tiles/", stubResponse({ json: MARTIN_CATALOG })],
      ["/tiles/counties", stubResponse({ json: TILE_JSON })],
      ["/tiles/counties/0/0/0", stubResponse({ contentType: "application/x-protobuf" })],
    ]);

    globalThis.fetch = vi.fn(async (path) => served.get(path));

    await expect(discoverTileMetadata()).resolves.toMatchObject({ layerId: "counties" });
  });
});
