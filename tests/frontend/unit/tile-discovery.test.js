import { describe, expect, test } from "vitest";

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
