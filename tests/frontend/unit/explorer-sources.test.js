import { describe, expect, test } from "vitest";

// Covers: WEB-013 — explorer sources derive from /catalog/capabilities
// observation routes; no closed client-side source enumeration decides
// which sources the explorer can drive. WEB-014 owns the neutral access
// shape those declarations select; this file owns membership, identity,
// parameter derivation, and the labeled offline fallback.

import {
  FALLBACK_EXPLORER_SOURCES,
  buildExplorerSources,
  findExplorerSource,
  sourceSupportsParameter,
} from "../../../apps/web/lib/explorerSources";
import {
  datasetFacetOptions,
  preferredDatasetFacet,
} from "../../../apps/web/lib/explorerViewModel";

// Shaped exactly like the served CapabilityListResponse items (see
// docs/reference/API_CONSUMER_GUIDE.md and the OpenAPI snapshot): every
// completed source, with the exact versioned routes that answer for it.
// The registry declares both neutral routes for every completed source, so
// every capability entry carries them alongside whatever source-scoped
// routes it also publishes. Parameter lists are the served ones (see
// tests/fixtures/api/openapi_contract.json).
const neutralRoutes = [
  {
    path: "/api/v1/observations",
    parameters: [
      "adjustment_status",
      "county_fips",
      "domain_desc",
      "domaincat_desc",
      "geo_id",
      "geo_level",
      "limit",
      "metric_code",
      "offset",
      "release",
      "scope",
      "state_fips",
      "stratum_id",
      "subject_code",
      "subject_type",
      "year_from",
      "year_to",
    ],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const sourceRoutes = (segment) => [
  {
    path: `/api/v1/${segment}/observations/latest`,
    parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
  },
  {
    path: `/api/v1/${segment}/observations/timeseries`,
    parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
  ...neutralRoutes,
];

const capabilities = [
  {
    source_code: "BLS",
    display_name: "Bureau of Labor Statistics",
    route_segment: "bls",
    served_by_neutral_routes: true,
    datasets: [],
    observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
    observation_routes: sourceRoutes("bls"),
  },
  {
    source_code: "CDC",
    display_name: "Centers for Disease Control and Prevention",
    route_segment: "cdc",
    served_by_neutral_routes: true,
    datasets: ["cdc_places_county"],
    observation_filters: ["adjustment_status", "geo_id", "stratum_id"],
    observation_routes: [
      ...neutralRoutes,
      { path: "/api/v1/cdc/observations", parameters: ["geo_id", "limit"] },
    ],
  },
  {
    source_code: "CENSUS_ACS",
    display_name: "Census American Community Survey",
    route_segment: "census",
    served_by_neutral_routes: true,
    datasets: [],
    observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
    observation_routes: sourceRoutes("census"),
  },
  {
    source_code: "CENSUS_PEP",
    display_name: "Census Population Estimates Program",
    route_segment: "pep",
    served_by_neutral_routes: true,
    datasets: [],
    observation_filters: ["geo_id", "geo_level"],
    observation_routes: sourceRoutes("pep"),
  },
  {
    source_code: "FBI_UCR",
    display_name: "Federal Bureau of Investigation Uniform Crime Reporting Program",
    route_segment: null,
    served_by_neutral_routes: true,
    datasets: ["fbi_ucr_srs_estimates"],
    observation_filters: ["geo_id", "subject_code", "subject_type"],
    observation_routes: [...neutralRoutes],
  },
  {
    source_code: "FRED",
    display_name: "Federal Reserve Economic Data",
    route_segment: "fred",
    served_by_neutral_routes: true,
    datasets: [],
    observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
    observation_routes: sourceRoutes("fred"),
  },
  {
    source_code: "USDA_NASS",
    display_name: "USDA National Agricultural Statistics Service",
    route_segment: "usda-nass",
    served_by_neutral_routes: true,
    datasets: ["nass_crops_county"],
    observation_filters: ["domain_desc", "geo_id"],
    observation_routes: [
      ...neutralRoutes,
      { path: "/api/v1/usda-nass/observations", parameters: ["geo_id", "limit"] },
    ],
  },
];

describe("capability-derived explorer sources", () => {
  test("membership comes from the declared routes, in the served order", () => {
    const sources = buildExplorerSources(capabilities);
    // A source is explorable when its declarations carry either the
    // source-scoped latest + timeseries pair or the neutral /observations
    // resource. Every completed source declares one of the two, so all
    // seven appear — and none of them appears because of a source-code list.
    expect(sources.map((source) => source.key)).toEqual([
      "bls",
      "cdc",
      "census",
      "pep",
      "FBI_UCR",
      "fred",
      "usda-nass",
    ]);
    expect(findExplorerSource(sources, "pep")).toMatchObject({
      key: "pep",
      segment: "pep",
      sourceCode: "CENSUS_PEP",
      title: "Census Population Estimates Program",
      tabLabel: "PEP",
      accessShape: "source-scoped",
    });
  });

  test("a source declaring neither access shape is left out", () => {
    const undeclared = {
      source_code: "FUTURE_SOURCE",
      display_name: "A source whose serving contract is not declared yet",
      route_segment: "future",
      served_by_neutral_routes: false,
      datasets: [],
      observation_filters: ["geo_id"],
      observation_routes: [{ path: "/api/v1/future/measures", parameters: [] }],
    };
    const sources = buildExplorerSources([...capabilities, undeclared]);
    expect(findExplorerSource(sources, "future")).toBeNull();
    expect(sources.some((source) => source.sourceCode === "FUTURE_SOURCE")).toBe(false);
  });

  test("parameter support comes from the declared route parameters", () => {
    const sources = buildExplorerSources(capabilities);
    const census = findExplorerSource(sources, "census");
    expect(sourceSupportsParameter(census, "state_fips")).toBe(true);
    expect(sourceSupportsParameter(census, "stratum_id")).toBe(false);

    const trimmed = buildExplorerSources([
      {
        ...capabilities[2],
        observation_routes: [
          { path: "/api/v1/census/observations/latest", parameters: ["metric_code"] },
          { path: "/api/v1/census/observations/timeseries", parameters: ["geo_id", "metric_code"] },
        ],
      },
    ]);
    expect(sourceSupportsParameter(trimmed[0], "state_fips")).toBe(false);
  });

  test("source keys resolve case-insensitively so shared links stay valid", () => {
    const sources = buildExplorerSources(capabilities);
    expect(findExplorerSource(sources, "fbi_ucr")?.sourceCode).toBe("FBI_UCR");
    expect(findExplorerSource(sources, "CENSUS")?.sourceCode).toBe("CENSUS_ACS");
    expect(findExplorerSource(sources, "missing")).toBeNull();
  });

  test("the as-released surface is read from the declared neutral routes", () => {
    const sources = buildExplorerSources(capabilities);
    // Every completed source declares /observations/releases and the neutral
    // `scope`/`release` parameters, so the as-released surface is reachable
    // for the source-scoped sources too — it lives only on that resource.
    for (const source of sources) {
      expect(source.servesReleases).toBe(true);
      expect(source.supportsAsReleased).toBe(true);
      expect(source.supportsReleasePin).toBe(true);
    }

    // A source-scoped source keeps its own route parameters for a latest
    // read, and carries its declared neutral filters for an as-released one.
    const census = findExplorerSource(sources, "census");
    expect(census.accessShape).toBe("source-scoped");
    expect(census.requestFilters).toEqual([
      "geo_level",
      "limit",
      "metric_code",
      "offset",
      "state_fips",
    ]);
    expect(census.neutralFilters).toEqual([
      "county_fips",
      "geo_id",
      "geo_level",
      "limit",
      "metric_code",
      "offset",
      "release",
      "scope",
      "state_fips",
    ]);
    expect(census.neutralDimensionFilters).toEqual([]);

    // The neutral shape's dimension filters are the same list it already
    // uses, so a stratified source keeps its controls under either scope.
    const cdc = findExplorerSource(sources, "cdc");
    expect(cdc.neutralDimensionFilters).toEqual(["adjustment_status", "stratum_id"]);
    expect(cdc.neutralFilters).toEqual(cdc.requestFilters);
  });

  test("a source whose neutral route declares no scope gets no as-released surface", () => {
    // The declaration decides, not the source's identity: a neutral route
    // published without `scope` cannot answer an as-released question, and a
    // route published without `release` cannot have one pinned.
    const [scopeless] = buildExplorerSources([
      {
        ...capabilities[1],
        observation_routes: [
          { path: "/api/v1/observations", parameters: ["metric_code", "limit"] },
        ],
      },
    ]);
    expect(scopeless.servesReleases).toBe(false);
    expect(scopeless.supportsAsReleased).toBe(false);
    expect(scopeless.supportsReleasePin).toBe(false);

    const [unpinnable] = buildExplorerSources([
      {
        ...capabilities[1],
        observation_routes: [
          { path: "/api/v1/observations", parameters: ["metric_code", "scope"] },
          { path: "/api/v1/observations/releases", parameters: ["metric_code"] },
        ],
      },
    ]);
    expect(unpinnable.servesReleases).toBe(true);
    expect(unpinnable.supportsAsReleased).toBe(true);
    expect(unpinnable.supportsReleasePin).toBe(false);
  });

  test("comparison support is read from the declared analysis routes", () => {
    // Declaring the analysis routes does not make a source comparable with
    // any other — /comparison/preflight decides each pair — but a source
    // declaring none is one the analysis routes have already declined.
    const analysisRoutes = [
      { path: "/api/v1/comparison/preflight", parameters: ["metric_code_a", "metric_code_b"] },
      { path: "/api/v1/comparison", parameters: ["metric_code_a", "metric_code_b"] },
    ];
    const [ready] = buildExplorerSources([
      { ...capabilities[2], observation_routes: [...capabilities[2].observation_routes, ...analysisRoutes] },
    ]);
    expect(ready.servesComparison).toBe(true);

    // CDC declares no analysis routes in these capabilities.
    const sources = buildExplorerSources(capabilities);
    expect(findExplorerSource(sources, "cdc").servesComparison).toBe(false);
    expect(FALLBACK_EXPLORER_SOURCES[0].servesComparison).toBe(false);
  });

  test("degrades to the labeled offline fallback when discovery is unavailable", () => {
    expect(buildExplorerSources([])).toEqual([]);
    expect(buildExplorerSources(null)).toEqual([]);
    expect(FALLBACK_EXPLORER_SOURCES.map((source) => source.key)).toEqual(["census"]);
    expect(findExplorerSource(FALLBACK_EXPLORER_SOURCES, "census")).toMatchObject({
      sourceCode: "CENSUS_ACS",
      accessShape: "source-scoped",
      // Discovery is unavailable, so nothing has declared an as-released
      // surface; offering one anyway would be an invented contract.
      servesReleases: false,
      supportsAsReleased: false,
    });
    expect(findExplorerSource(FALLBACK_EXPLORER_SOURCES, "missing")).toBeNull();
  });
});

describe("dataset facets derived from published metric identity", () => {
  const acsMetrics = [
    { metric_code: "ACS:acs5:B01003_001", metric_display_name: "Population" },
    { metric_code: "ACS:acs1:B01003_001", metric_display_name: "Population" },
    { metric_code: "ACS:acs5:B19013_001", metric_display_name: "Income" },
  ];

  test("facet options come from metric codes with published coverage labels", () => {
    expect(datasetFacetOptions(acsMetrics)).toEqual([
      { value: "acs1", label: "ACS 1-year — partial county coverage" },
      { value: "acs5", label: "ACS 5-year — complete county coverage" },
    ]);
    expect(preferredDatasetFacet(acsMetrics)).toBe("acs5");
  });

  test("sources without a metric-embedded dataset facet get no selector", () => {
    expect(datasetFacetOptions([{ metric_code: "FRED:UNRATE" }])).toEqual([]);
    expect(preferredDatasetFacet([{ metric_code: "FRED:UNRATE" }])).toBe("");
  });

  test("unlabeled facets fall back to their published spelling", () => {
    const pepMetrics = [
      { metric_code: "CENSUS_PEP:pep_nst_alldata:POPESTIMATE" },
      { metric_code: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE" },
    ];
    expect(datasetFacetOptions(pepMetrics)).toEqual([
      { value: "pep_cty_alldata", label: "PEP_CTY_ALLDATA" },
      { value: "pep_nst_alldata", label: "PEP_NST_ALLDATA" },
    ]);
    expect(preferredDatasetFacet(pepMetrics)).toBe("pep_cty_alldata");
  });
});
