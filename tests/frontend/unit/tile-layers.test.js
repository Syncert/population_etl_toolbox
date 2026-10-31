// Covers: WEB-147 — tiles-places-and-metros: the web picks a tile layer from
// /catalog/tile-layers, draws exactly the boundary vintage it asks for (never
// another), and states the uncoloured-is-not-zero rule the same way for
// every grain.

import { describe, expect, it } from "vitest";

import { tileLayerForGrain, versionedTileTemplate } from "../../../apps/web/lib/tiles.ts";
import { GEOGRAPHY_NOUNS, uncolouredSentence } from "../../../apps/web/lib/uncoloured.ts";
import { uncolouredSentence as tractSentence } from "../../../apps/web/components/WithinCounty.tsx";

const PLACES = {
  layer_id: "places",
  source_layer: "places",
  grains: ["PLACE"],
  versioned: true,
  minzoom: 6,
  maxzoom: 14,
  join_key: "geo_id",
  name_field: "place_name",
  tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024",
  vintages: [
    { boundary_vintage: 2024, feature_count: 32350, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024" },
    { boundary_vintage: 2010, feature_count: 29514, is_default: false, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2010" },
  ],
  default_vintage: 2024,
  available: true,
};
const METROS_EMPTY = {
  ...PLACES,
  layer_id: "metros",
  source_layer: "metros",
  grains: ["METRO"],
  tile_template: "/tiles/metros/{z}/{x}/{y}",
  vintages: [],
  default_vintage: null,
  available: false,
};
const COUNTIES = {
  ...PLACES,
  layer_id: "counties",
  source_layer: "counties",
  grains: ["STATE", "COUNTY"],
  versioned: false,
  tile_template: "/tiles/counties/{z}/{x}/{y}",
  vintages: [],
  default_vintage: null,
};
const CATALOG = { layers: [COUNTIES, PLACES, METROS_EMPTY], uncoloured_rule: "…" };

describe("choosing the layer a grain is drawn from", () => {
  it("finds the layer that draws the grain, whatever its case", () => {
    expect(tileLayerForGrain(CATALOG, "place")?.layer_id).toBe("places");
    expect(tileLayerForGrain(CATALOG, "STATE")?.layer_id).toBe("counties");
  });

  it("offers no layer for a grain nothing draws, or a layer with no boundary loaded", () => {
    expect(tileLayerForGrain(CATALOG, "METRO")).toBeNull();
    expect(tileLayerForGrain(CATALOG, "ZCTA")).toBeNull();
    expect(tileLayerForGrain(null, "PLACE")).toBeNull();
  });
});

describe("drawing one boundary vintage", () => {
  it("answers the template of a held vintage, and the default when none is asked for", () => {
    expect(versionedTileTemplate(PLACES, 2010)).toBe("/tiles/places/{z}/{x}/{y}?vintage=2010");
    expect(versionedTileTemplate(PLACES)).toBe("/tiles/places/{z}/{x}/{y}?vintage=2024");
  });

  it("never falls back to another vintage", () => {
    expect(versionedTileTemplate(PLACES, 2012)).toBeNull();
    expect(versionedTileTemplate(COUNTIES, 2012)).toBeNull();
    expect(versionedTileTemplate(METROS_EMPTY)).toBeNull();
  });

  it("draws current boundaries for an unversioned layer asked for none", () => {
    expect(versionedTileTemplate(COUNTIES)).toBe("/tiles/counties/{z}/{x}/{y}");
  });
});

describe("the uncoloured-is-not-zero sentence", () => {
  it("says the same thing for every grain", () => {
    expect(uncolouredSentence(GEOGRAPHY_NOUNS.PLACE, 120, 117)).toBe(
      "3 of 120 cities and towns have no published value and are left uncoloured, not shown as zero.",
    );
    expect(uncolouredSentence(GEOGRAPHY_NOUNS.METRO, 925, 925)).toBe("All 925 metro areas have a published value.");
    expect(tractSentence(290, 287)).toBe(uncolouredSentence(GEOGRAPHY_NOUNS.TRACT, 290, 287));
  });
});
