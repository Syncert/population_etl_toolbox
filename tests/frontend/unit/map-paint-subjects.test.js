import { expect, test } from "vitest";

import { deploymentPaintGrains, reviewedPaintSources } from "../support/mapPaintSubjects.js";

test("the pixel tier tests only drawable grains actually advertised by this deployment", () => {
  // Covers: WEB-118 — the smoke seed publishes county data without
  // pretending it publishes a STATE metric named in the reviewed matrix.
  const metrics = [
    { valid_geo_grains: ["COUNTY"] },
    { valid_geo_grains: [] },
    { valid_geo_grains: null },
  ];
  expect(deploymentPaintGrains(metrics, ["STATE", "COUNTY", "PLACE"])).toEqual(["COUNTY"]);
  expect(deploymentPaintGrains([], ["STATE", "COUNTY"])).toEqual([]);
});

test("the pixel tier excludes reviewed national-only sources that cannot draw a map", () => {
  expect(reviewedPaintSources({ BLS: ["COUNTY", "NATIONAL"], FRED: ["NATIONAL"] }, ["COUNTY", "STATE"]))
    .toEqual(["BLS"]);
});
