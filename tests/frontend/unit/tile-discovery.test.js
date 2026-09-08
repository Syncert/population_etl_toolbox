import { describe, expect, test } from "vitest";

// Covers: WEB-026 — tile-layer discovery against the catalog shapes Martin
// actually serves. Martin groups its sources into catalog sections
// (`tiles`, `sprites`, `fonts`, `styles`) alongside a `settings` object, so
// the source ids live one level down. Treating every top-level key as a
// layer id makes discovery probe the section names, find nothing, and
// report that the deployment publishes no spatial layer at all — which is a
// very different statement from "this build could not read the catalog".

import {
  collectTileCandidates,
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
