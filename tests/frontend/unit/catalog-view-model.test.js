import { describe, expect, test } from "vitest";

// Covers: WEB-011 — the catalog source filter derives from API discovery
// instead of a closed client-side source enumeration.

import { sourceFilterOptions } from "../../../apps/web/lib/catalog";

describe("catalog source filter", () => {
  test("builds options from the published source list", () => {
    expect(
      sourceFilterOptions([
        { source_code: "CENSUS_ACS", source_name: "Census ACS" },
        { source_code: "USDA_NASS" },
      ]),
    ).toEqual([
      { value: "", label: "All sources" },
      { value: "CENSUS_ACS", label: "Census ACS" },
      { value: "USDA_NASS", label: "USDA_NASS" },
    ]);
  });

  test("degrades to the all-sources option when discovery is unavailable", () => {
    expect(sourceFilterOptions(undefined)).toEqual([
      { value: "", label: "All sources" },
    ]);
  });
});
