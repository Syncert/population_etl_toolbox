import { describe, expect, test } from "vitest";

// Covers: WEB-014 — dispatch-shaped sources are reached through the neutral
// /observations resource, every request filter comes from the capability's
// declared observation_filters, the neutral envelope maps onto the explorer
// row shape without invention, and a stratified answer is reported rather
// than collapsed to one value per geography.

import { buildExplorerSources, findExplorerSource } from "../../../apps/web/lib/explorerSources";
import {
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
  describeStratification,
  normalizeObservationRows,
  observationDimensionOptions,
  observationDimensionValue,
  observationPeriodLabel,
} from "../../../apps/web/lib/observationAccess";

// Shaped exactly like the served CapabilityListResponse items (see
// docs/reference/API_CONSUMER_GUIDE.md and the OpenAPI snapshot).
const sourceScopedRoutes = (segment) => [
  {
    path: `/api/v1/${segment}/observations/latest`,
    parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
  },
  {
    path: `/api/v1/${segment}/observations/timeseries`,
    parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
  { path: "/api/v1/observations", parameters: ["metric_code", "scope"] },
  { path: "/api/v1/observations/releases", parameters: ["metric_code"] },
  { path: "/api/v1/distribution/bins", parameters: ["metric_code"] },
];

const neutralRoutes = [
  { path: "/api/v1/observations", parameters: ["metric_code", "scope"] },
  { path: "/api/v1/observations/releases", parameters: ["metric_code"] },
];

const capabilities = [
  {
    source_code: "CDC",
    display_name: "Centers for Disease Control and Prevention",
    route_segment: "cdc",
    served_by_neutral_routes: true,
    datasets: ["cdc_places_county"],
    observation_filters: [
      "adjustment_status",
      "geo_id",
      "geo_level",
      "stratum_id",
      "year_from",
      "year_to",
    ],
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
    observation_routes: sourceScopedRoutes("census"),
  },
  {
    source_code: "FBI_UCR",
    display_name: "Federal Bureau of Investigation Uniform Crime Reporting Program",
    route_segment: null,
    served_by_neutral_routes: true,
    datasets: ["fbi_ucr_srs_estimates"],
    observation_filters: ["geo_id", "subject_code", "subject_type", "year_from", "year_to"],
    observation_routes: neutralRoutes,
  },
  {
    source_code: "USDA_NASS",
    display_name: "USDA National Agricultural Statistics Service",
    route_segment: "usda-nass",
    served_by_neutral_routes: true,
    datasets: ["nass_crops_county"],
    observation_filters: [
      "county_fips",
      "domain_desc",
      "domaincat_desc",
      "geo_id",
      "geo_level",
      "state_fips",
      "year_from",
      "year_to",
    ],
    observation_routes: [
      ...neutralRoutes,
      { path: "/api/v1/usda-nass/observations", parameters: ["geo_id", "limit"] },
    ],
  },
];

const sources = buildExplorerSources(capabilities);
const cdc = findExplorerSource(sources, "cdc");
const census = findExplorerSource(sources, "census");
const fbi = findExplorerSource(sources, "FBI_UCR");
const nass = findExplorerSource(sources, "usda-nass");

describe("declared access shapes", () => {
  test("dispatch-shaped sources join the explorer through the neutral resource", () => {
    expect(sources.map((source) => source.key)).toEqual([
      "cdc",
      "census",
      "FBI_UCR",
      "usda-nass",
    ]);
    expect(cdc.accessShape).toBe("neutral");
    expect(nass.accessShape).toBe("neutral");
    expect(census.accessShape).toBe("source-scoped");
  });

  test("a segment-less source keeps its published identity as its key", () => {
    expect(fbi).toMatchObject({
      key: "FBI_UCR",
      segment: null,
      sourceCode: "FBI_UCR",
      tabLabel: "FBI_UCR",
      accessShape: "neutral",
    });
  });

  test("dimension filters are the declared filters outside the shared vocabulary", () => {
    expect(cdc.dimensionFilters).toEqual(["adjustment_status", "stratum_id"]);
    expect(fbi.dimensionFilters).toEqual(["subject_code", "subject_type"]);
    expect(nass.dimensionFilters).toEqual(["domain_desc", "domaincat_desc"]);
    // A source-scoped source's filters are its own route parameters, all of
    // which are the shared vocabulary, so it generates no dimension control.
    expect(census.dimensionFilters).toEqual([]);
  });

  test("analysis support is read from the declared routes, never assumed", () => {
    expect(census.servesDistribution).toBe(true);
    expect(cdc.servesDistribution).toBe(false);
    expect(fbi.servesDistribution).toBe(false);
  });
});

describe("requests carry only declared filters", () => {
  test("the neutral latest request asks /observations with scope=latest", () => {
    expect(
      buildLatestObservationRequest(cdc, {
        metricCode: "CDC:cdc_places_county:OBESITY",
        geoLevel: "COUNTY",
        stateFips: "55",
        limit: "4000",
        dimensions: { stratum_id: "overall", adjustment_status: "age-adjusted" },
      }),
    ).toEqual({
      resource: "/observations",
      params: {
        metric_code: "CDC:cdc_places_county:OBESITY",
        scope: "latest",
        limit: "4000",
        geo_level: "COUNTY",
        stratum_id: "overall",
        adjustment_status: "age-adjusted",
      },
    });
  });

  test("an undeclared filter never reaches the request", () => {
    // CDC declares no state_fips, so the selected state must not be sent:
    // the resource would answer 422, and dropping it here would silently
    // widen the answer to every state instead.
    const request = buildLatestObservationRequest(cdc, {
      metricCode: "CDC:cdc_places_county:OBESITY",
      geoLevel: "COUNTY",
      stateFips: "55",
    });
    expect(request.params.state_fips).toBeUndefined();

    // FBI UCR declares neither geo_level nor state_fips.
    const fbiRequest = buildLatestObservationRequest(fbi, {
      metricCode: "FBI_UCR:fbi_ucr_srs_estimates:VIOLENT",
      geoLevel: "COUNTY",
      stateFips: "55",
    });
    expect(fbiRequest.params).toEqual({
      metric_code: "FBI_UCR:fbi_ucr_srs_estimates:VIOLENT",
      scope: "latest",
    });

    // A dimension the source did not declare is not sent either.
    const nassRequest = buildLatestObservationRequest(nass, {
      metricCode: "USDA_NASS:nass_crops_county:CORN",
      dimensions: { domain_desc: "TOTAL", stratum_id: "overall" },
    });
    expect(nassRequest.params).toEqual({
      metric_code: "USDA_NASS:nass_crops_county:CORN",
      scope: "latest",
      domain_desc: "TOTAL",
    });
  });

  test("source-scoped sources keep their own routes and parameter discipline", () => {
    expect(
      buildLatestObservationRequest(census, {
        metricCode: "ACS:acs5:B01003_001",
        geoLevel: "COUNTY",
        stateFips: "55",
        limit: "4000",
      }),
    ).toEqual({
      resource: "/census/observations/latest",
      params: {
        metric_code: "ACS:acs5:B01003_001",
        limit: "4000",
        geo_level: "COUNTY",
        state_fips: "55",
      },
    });
    expect(
      buildHistoryObservationRequest(census, {
        metricCode: "ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }),
    ).toEqual({
      resource: "/census/observations/timeseries",
      params: {
        metric_code: "ACS:acs5:B01003_001",
        geo_id: "state:55|county:025",
        limit: "1000",
      },
    });
  });

  test("the neutral history request scopes the published series to one geography", () => {
    expect(
      buildHistoryObservationRequest(cdc, {
        metricCode: "CDC:cdc_places_county:OBESITY",
        geoId: "state:55|county:025",
        limit: "1000",
        dimensions: { stratum_id: "overall" },
      }),
    ).toEqual({
      resource: "/observations",
      params: {
        metric_code: "CDC:cdc_places_county:OBESITY",
        scope: "latest",
        limit: "1000",
        geo_id: "state:55|county:025",
        stratum_id: "overall",
      },
    });
  });
});

describe("the neutral envelope maps onto the explorer row shape", () => {
  const neutralRow = {
    metric_code: "CDC:cdc_places_county:OBESITY",
    source_code: "CDC",
    geo_id: "state:55|county:025",
    geo_level: "COUNTY",
    value: null,
    value_status: "suppressed",
    unit: "percent",
    period_start: "2021-01-01",
    period_end: "2022-12-31",
    dimensions: { stratum_id: "overall", adjustment_status: "age-adjusted" },
    uncertainty: { confidence_lower: "12.1", confidence_upper: "14.9" },
  };

  test("published period bounds are preserved exactly, range included", () => {
    expect(observationPeriodLabel(neutralRow)).toBe("2021-01-01 – 2022-12-31");
    expect(observationPeriodLabel({ period_start: "2023-01-01", period_end: "2023-01-01" }))
      .toBe("2023-01-01");
    expect(observationPeriodLabel({ period_end: "2020-12-31" })).toBe("2020-12-31");
    expect(observationPeriodLabel({})).toBe("");
  });

  test("normalization adds display fields without touching the published value", () => {
    const [row] = normalizeObservationRows(cdc, [neutralRow]);
    expect(row.value).toBeNull();
    expect(row.value_status).toBe("suppressed");
    expect(row.period).toBe("2021-01-01 – 2022-12-31");
    expect(row.observation_date).toBe("2022-12-31");
    expect(row.units).toBe("percent");
    expect(row.source).toBe("CDC");
    // Nothing invents a margin of error the source did not publish.
    expect(row.margin_of_error).toBeUndefined();
  });

  test("source-scoped rows pass through untouched", () => {
    const acsRow = { geo_id: "state:55|county:025", value: "12.4", units: "people" };
    expect(normalizeObservationRows(census, [acsRow])).toEqual([acsRow]);
    expect(normalizeObservationRows(null, null)).toEqual([]);
  });

  test("dimension options come from the values the source published", () => {
    const rows = [
      { dimensions: { stratum_id: "overall" } },
      { dimensions: { stratum_id: "age_18_44" } },
      { dimensions: { stratum_id: "overall" } },
      { dimensions: {} },
    ];
    expect(observationDimensionOptions(rows, "stratum_id")).toEqual(["age_18_44", "overall"]);
    expect(observationDimensionOptions(rows, "not_published")).toEqual([]);
    expect(observationDimensionValue(rows[0], "stratum_id")).toBe("overall");
    expect(observationDimensionValue({ subject_type: "person" }, "subject_type")).toBe("person");
  });
});

describe("stratified answers are reported, never collapsed", () => {
  const stratified = [
    { geo_id: "a", dimensions: { stratum_id: "overall", adjustment_status: "crude" } },
    { geo_id: "a", dimensions: { stratum_id: "age_18_44", adjustment_status: "crude" } },
    { geo_id: "b", dimensions: { stratum_id: "overall", adjustment_status: "crude" } },
  ];

  test("several declared-dimension series per geography are detected and named", () => {
    const summary = describeStratification(stratified, cdc.dimensionFilters);
    expect(summary).toEqual({
      seriesCount: 2,
      stratified: true,
      varyingDimensions: ["stratum_id"],
    });
  });

  test("a single narrowed series is not reported as stratified", () => {
    const narrowed = stratified.filter(
      (row) => row.dimensions.stratum_id === "overall",
    );
    expect(describeStratification(narrowed, cdc.dimensionFilters)).toEqual({
      seriesCount: 1,
      stratified: false,
      varyingDimensions: [],
    });
  });

  test("a source with no declared dimensions is never reported as stratified", () => {
    expect(describeStratification([{ geo_id: "a" }], census.dimensionFilters)).toEqual({
      seriesCount: 1,
      stratified: false,
      varyingDimensions: [],
    });
    expect(describeStratification([], cdc.dimensionFilters)).toEqual({
      seriesCount: 0,
      stratified: false,
      varyingDimensions: [],
    });
  });
});
