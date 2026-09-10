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

// Covers: WEB-015 — catalog search state, the exact request parameters it
// produces, deterministic limit/offset paging over the API's published
// total, published provenance without placeholders, and freshness mapped
// onto the shared request-state vocabulary.

import {
  CATALOG_PAGE_SIZE,
  DEFAULT_CATALOG_STATE,
  catalogPageModel,
  catalogRequestParams,
  metricProvenance,
  metricQualityState,
  parseCatalogState,
  serializeCatalogState,
} from "../../../apps/web/lib/catalog";

describe("catalog search state", () => {
  test("parses every supported link parameter and drops invalid ones", () => {
    expect(parseCatalogState("?q=population&source=CENSUS_ACS&page=3&include_retired=1")).toEqual({
      query: "population",
      sourceCode: "CENSUS_ACS",
      activeOnly: false,
      page: 3,
    });
    expect(parseCatalogState("?source=Not%2FValid&page=-2")).toEqual(DEFAULT_CATALOG_STATE);
    expect(parseCatalogState("")).toEqual(DEFAULT_CATALOG_STATE);
  });

  test("serialization omits defaults and round-trips", () => {
    expect(serializeCatalogState(DEFAULT_CATALOG_STATE)).toBe("");
    const state = { query: "corn", sourceCode: "USDA_NASS", activeOnly: false, page: 2 };
    expect(serializeCatalogState(state)).toBe(
      "q=corn&source=USDA_NASS&include_retired=1&page=2",
    );
    expect(parseCatalogState(`?${serializeCatalogState(state)}`)).toEqual(state);
  });

  test("request parameters are exactly the ones the resource declares", () => {
    expect(catalogRequestParams(DEFAULT_CATALOG_STATE)).toEqual({
      active_only: "true",
      limit: String(CATALOG_PAGE_SIZE),
      offset: "0",
    });
    expect(
      catalogRequestParams({
        query: "  median income  ",
        sourceCode: "CENSUS_ACS",
        activeOnly: false,
        page: 2,
      }),
    ).toEqual({
      q: "median income",
      source_code: "CENSUS_ACS",
      limit: String(CATALOG_PAGE_SIZE),
      offset: String(CATALOG_PAGE_SIZE * 2),
    });
  });
});

describe("deterministic catalog paging", () => {
  const state = (page) => ({ ...DEFAULT_CATALOG_STATE, page });
  const page = (count, offset) => ({
    items: Array.from({ length: count }, (_, i) => i),
    ...(offset === undefined ? {} : { offset }),
  });

  test("the display range and page count come from the published total", () => {
    expect(catalogPageModel({ ...page(50, 50), total: 127 }, state(1), 50)).toEqual({
      total: 127,
      shown: 50,
      pageIndex: 1,
      pageCount: 3,
      firstRow: 51,
      lastRow: 100,
      hasPrevious: true,
      hasNext: true,
    });
    expect(catalogPageModel({ ...page(27, 100), total: 127 }, state(2), 50)).toMatchObject({
      firstRow: 101,
      lastRow: 127,
      hasNext: false,
      hasPrevious: true,
    });
  });

  test("the range follows the answered offset, not the requested page", () => {
    // Between a page click and its answer the requested page and the loaded
    // rows disagree; labelling the previous page's rows with the new range
    // would present stale values as current.
    const answered = catalogPageModel({ ...page(50, 0), total: 127 }, state(1), 50);
    expect(answered).toMatchObject({ pageIndex: 0, firstRow: 1, lastRow: 50, hasPrevious: false });
    // Without a published offset the requested page is the only anchor.
    expect(catalogPageModel({ ...page(50), total: 127 }, state(1), 50)).toMatchObject({
      pageIndex: 1,
      firstRow: 51,
    });
  });

  test("an empty page reports no range rather than an invented one", () => {
    expect(catalogPageModel({ items: [], total: 0 }, state(0), 50)).toMatchObject({
      total: 0,
      shown: 0,
      firstRow: null,
      lastRow: null,
      hasNext: false,
      hasPrevious: false,
      pageCount: 1,
    });
  });

  test("an absent total is not replaced by a client-side count", () => {
    // Without a published total the client must not claim one; a full page
    // is the only evidence that another page may exist.
    const full = catalogPageModel(page(50), state(0), 50);
    expect(full.total).toBeNull();
    expect(full.pageCount).toBe(0);
    expect(full.hasNext).toBe(true);
    expect(catalogPageModel(page(12), state(0), 50).hasNext).toBe(false);
    expect(catalogPageModel(null, state(0), 50)).toMatchObject({ total: null, shown: 0 });
  });
});

describe("published provenance and quality context", () => {
  const metric = {
    metric_code: "ACS:acs5:B01003_001",
    units: "people",
    measure_kind: "count",
    aggregation_characteristic: "additive",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
    source_object_type: "acs5",
    publisher_contract_version: "v3",
    source_watermark: "2023",
    publication_time: "2024-01-02T00:00:00Z",
    harvested_at: "2024-01-03T00:00:00Z",
    physical_lineage: { schema: "gold_census", relation: "rpt_acs_observations" },
    freshness_state: "fresh",
  };

  test("every published provenance field appears, in a fixed order", () => {
    expect(metricProvenance(metric).map((entry) => entry.label)).toEqual([
      "Units",
      "Measure kind",
      "Aggregation",
      "Geographies",
      "Time grain",
      "Source object",
      "Publisher contract",
      "Source watermark",
      "Published",
      "Harvested",
      "Serving relation",
    ]);
    expect(metricProvenance(metric)).toContainEqual({
      label: "Serving relation",
      value: "gold_census.rpt_acs_observations",
    });
    expect(metricProvenance(metric)).toContainEqual({
      label: "Geographies",
      value: "STATE, COUNTY",
    });
  });

  test("an unpublished field is omitted rather than shown as a placeholder", () => {
    const sparse = metricProvenance({ metric_code: "X:Y:Z", units: "percent" });
    expect(sparse).toEqual([{ label: "Units", value: "percent" }]);
    expect(metricProvenance({ metric_code: "X:Y:Z", valid_geo_grains: [] })).toEqual([]);
    expect(metricProvenance(null)).toEqual([]);
  });

  test("freshness maps onto the shared state vocabulary, unknown when absent", () => {
    expect(metricQualityState(metric)).toEqual({ state: "ok", label: "fresh" });
    expect(metricQualityState({ freshness_state: "stale" })).toEqual({
      state: "warn",
      label: "stale",
    });
    // Absent freshness is never presented as healthy.
    expect(metricQualityState({ metric_code: "X:Y:Z" })).toEqual({
      state: "idle",
      label: "freshness not published",
    });
    expect(metricQualityState(null).state).toBe("idle");
  });
});
