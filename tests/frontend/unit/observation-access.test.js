import { describe, expect, test } from "vitest";

// Covers: WEB-014 — dispatch-shaped sources are reached through the neutral
// /observations resource, every request filter comes from the capability's
// declared observation_filters, the neutral envelope maps onto the explorer
// row shape without invention, and a stratified answer is reported rather
// than collapsed to one value per geography.
//
// Covers: WEB-016 — the as-released surface. `scope=as_released` and a
// pinned `release` are sent only where the capability declares them, they
// always answer on the neutral resource, and an unpinned as-released read is
// reported as one series per release rather than collapsed.

import { buildExplorerSources, findExplorerSource } from "../../../apps/web/lib/explorerSources";
import {
  servedParameters,
  servedParametersWithout,
  servedSchemaFields,
} from "../support/servedContract.js";
import {
  RELEASE_DIMENSION,
  SCOPE_AS_RELEASED,
  SCOPE_LATEST,
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
  buildNewestValueRequest,
  buildSettledHistoryRequest,
  buildSettledSurfaceRequest,
  buildReleaseListRequest,
  collapseToNewestRelease,
  describeHistoryLoad,
  countObservationPeriods,
  ALL_DIMENSION_VALUES,
  describeStratification,
  dimensionSelectionsForLink,
  dimensionSelectionsFromLink,
  effectiveDimensionSelections,
  dimensionsCarriedBy,
  newestPerGeography,
  normalizeObservationRows,
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_BEYOND_MARGIN,
  OBSERVATION_UNCERTAINTY_FIELDS,
  observationCoverageValue,
  observationUncertaintyLabel,
  observationUncertaintyValue,
  publishesCoverage,
  publishesUncertainty,
  observationDimensionOptions,
  observationDimensionLabel,
  observationDimensionValue,
  observationPeriodLabel,
  sharedObservationPeriod,
  scopedDimensionFilters,
  stateScopeNote,
  servesAsReleased,
  stratificationDimensions,
} from "../../../apps/web/lib/observationAccess";

// Shaped exactly like the served CapabilityListResponse items (see
// docs/reference/API_CONSUMER_GUIDE.md and the OpenAPI snapshot).
// The served neutral parameter list, read from the reviewed snapshot
// rather than copied: a copy that claims to be the served list and is
// not models a weaker API than the one that ships (WEB-043).
const NEUTRAL_PARAMETERS = servedParameters("/api/v1/observations");

const neutralRoutes = [
  { path: "/api/v1/observations", parameters: NEUTRAL_PARAMETERS },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const sourceScopedRoutes = (segment) => [
  {
    path: `/api/v1/${segment}/observations/latest`,
    parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
  },
  {
    path: `/api/v1/${segment}/observations/timeseries`,
    parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
  ...neutralRoutes,
  { path: "/api/v1/distribution/bins", parameters: ["metric_code"] },
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

// A source publishing only its own route pair — no neutral resource. This is
// the fallback shape, and the only one that still reaches the source-scoped
// routes now that the neutral resource is preferred wherever it is declared.
const [scopedOnly] = buildExplorerSources([
  {
    source_code: "CENSUS_ACS",
    display_name: "Census American Community Survey",
    route_segment: "census",
    served_by_neutral_routes: false,
    datasets: [],
    observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
    observation_routes: [
      {
        path: "/api/v1/census/observations/latest",
        parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
      },
      {
        path: "/api/v1/census/observations/timeseries",
        parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
      },
    ],
  },
]);

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
    // Census declares its own route pair as well, but the neutral resource
    // is preferred: the pair reads the legacy union views, which key
    // observations on that era's metric identity rather than the glossary
    // identity the catalog publishes.
    expect(census.accessShape).toBe("neutral");
    // Only a source with no neutral route at all falls back to the pair.
    expect(scopedOnly.accessShape).toBe("source-scoped");
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

  test("a source-scoped-only source keeps its own routes and parameter discipline", () => {
    expect(
      buildLatestObservationRequest(scopedOnly, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoLevel: "COUNTY",
        stateFips: "55",
        limit: "4000",
      }),
    ).toEqual({
      resource: "/census/observations/latest",
      params: {
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        limit: "4000",
        geo_level: "COUNTY",
        state_fips: "55",
      },
    });
    expect(
      buildHistoryObservationRequest(scopedOnly, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }),
    ).toEqual({
      resource: "/census/observations/timeseries",
      params: {
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        geo_id: "state:55|county:025",
        limit: "1000",
      },
    });
  });

  test("a source declaring both shapes reads through the neutral resource", () => {
    // This is the case that mattered in practice: Census declares both, and
    // routing to its own pair with a glossary metric code returned an empty
    // page while the same code answered 3,234 rows on /observations.
    const latest = buildLatestObservationRequest(census, {
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      geoLevel: "COUNTY",
      stateFips: "55",
      limit: "4000",
    });
    expect(latest.resource).toBe("/observations");
    expect(latest.params).toMatchObject({
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      scope: "latest",
      geo_level: "COUNTY",
      state_fips: "55",
    });

    const history = buildHistoryObservationRequest(census, {
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      geoId: "state:55|county:025",
      limit: "1000",
    });
    expect(history.resource).toBe("/observations");
    expect(history.params).toMatchObject({
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      geo_id: "state:55|county:025",
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
    expect(normalizeObservationRows(scopedOnly, [acsRow])).toEqual([acsRow]);
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

  test("a dimension that only names the geography is not a second series", () => {
    // FBI UCR declares subject_code, and at the state grain a state's
    // subject_code is that state: 52 states carry 52 codes, each one series.
    // Counting signatures across geographies read that as 52 series and
    // emptied the map (WEB-117).
    const states = ["PA", "WI", "VI"].flatMap((code, index) =>
      ["2023-05-01", "2023-06-01"].map((period_start) => ({
        geo_id: `state:${index}`,
        period_start,
        dimensions: { subject_type: "state", subject_code: code },
      })),
    );
    expect(describeStratification(states, ["subject_type", "subject_code"])).toEqual({
      seriesCount: 1,
      stratified: false,
      varyingDimensions: [],
    });
    expect(newestPerGeography(states)).toHaveLength(3);
  });

  test("only a dimension that varies inside one geography is named", () => {
    const rows = [
      { geo_id: "a", dimensions: { stratum_id: "overall", subject_code: "A" } },
      { geo_id: "a", dimensions: { stratum_id: "age_18_44", subject_code: "A" } },
      { geo_id: "b", dimensions: { stratum_id: "overall", subject_code: "B" } },
    ];
    expect(describeStratification(rows, ["stratum_id", "subject_code"])).toEqual({
      seriesCount: 2,
      stratified: true,
      varyingDimensions: ["stratum_id"],
    });
  });

  test("a final value beside its forecasts is stratified by a published dimension (WEB-118)", () => {
    // USDA NASS publishes a year's final value and its August and October
    // forecasts for one state; only reference_period_desc separates them, and
    // no filter declares it. The map must not colour whichever arrived last.
    const rows = ["YEAR", "YEAR - AUG FORECAST", "YEAR - OCT FORECAST"].map((reference) => ({
      geo_id: "state:01",
      period_start: "1999",
      period_end: "1999",
      value: "2.3",
      dimensions: { reference_period_desc: reference, domain_desc: "TOTAL" },
    }));
    expect(
      describeStratification(rows, ["domain_desc", "reference_period_desc"]),
    ).toEqual({ seriesCount: 3, stratified: true, varyingDimensions: ["reference_period_desc"] });
  });

  test("a dimension that moves with the period is one series, not many", () => {
    const rows = ["01-2023", "02-2023"].map((period, index) => ({
      geo_id: "state:55",
      period_start: `2023-0${index + 1}-01`,
      period_end: `2023-0${index + 1}-28`,
      dimensions: { period },
    }));
    expect(describeStratification(rows, ["period"])).toEqual({
      seriesCount: 1,
      stratified: false,
      varyingDimensions: [],
    });
  });

  test("rows no published dimension tells apart are still not collapsed", () => {
    const rows = ["20", "22"].map((value) => ({
      geo_id: "state:05|county:998",
      period_start: "1990",
      period_end: "1990",
      value,
      dimensions: { domain_desc: "TOTAL" },
    }));
    expect(describeStratification(rows, ["domain_desc"])).toEqual({
      seriesCount: 2,
      stratified: true,
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

describe("as-released reads", () => {
  test("the release listing is requested only where the route is declared", () => {
    expect(buildReleaseListRequest(cdc, { metricCode: "CDC:x", limit: "200" })).toEqual({
      resource: "/observations/releases",
      params: { metric_code: "CDC:x", limit: "200" },
    });
    // A source whose capability entry omits the release listing gets null —
    // the honest "this source publishes none here". Nothing may guess an
    // identity that /observations/releases never published.
    const [undeclared] = buildExplorerSources([
      {
        ...capabilities[0],
        observation_routes: [{ path: "/api/v1/observations", parameters: ["metric_code"] }],
      },
    ]);
    expect(servesAsReleased(undeclared)).toBe(false);
    expect(buildReleaseListRequest(undeclared, { metricCode: "CDC:x" })).toBeNull();
    expect(buildReleaseListRequest(null, { metricCode: "CDC:x" })).toBeNull();
  });

  test("an as-released read answers on the neutral resource with its declared filters", () => {
    // `scope=as_released` lives only on /observations, and the request
    // carries the neutral filters the capability declares rather than the
    // parameters of any source-scoped route the same source also publishes.
    expect(
      buildLatestObservationRequest(census, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoLevel: "COUNTY",
        stateFips: "55",
        limit: "4000",
        scope: SCOPE_AS_RELEASED,
        release: "2022",
      }),
    ).toEqual({
      resource: "/observations",
      params: {
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        scope: "as_released",
        release: "2022",
        limit: "4000",
        geo_level: "COUNTY",
        state_fips: "55",
      },
    });
  });

  test("a pinned release travels only with scope=as_released", () => {
    // `release` without `scope=as_released` is a 422 by contract, so a
    // release carried alone is never sent.
    const latest = buildLatestObservationRequest(cdc, {
      metricCode: "CDC:cdc_places_county:OBESITY",
      geoLevel: "COUNTY",
      limit: "4000",
      release: "20240115",
    });
    expect(latest.params.scope).toBe(SCOPE_LATEST);
    expect(latest.params.release).toBeUndefined();

    // A source whose neutral route declares no `release` can read as
    // released but cannot pin one; sending it would be a 422.
    const [unpinnable] = buildExplorerSources([
      {
        ...capabilities[0],
        observation_routes: [
          { path: "/api/v1/observations", parameters: ["geo_id", "metric_code", "scope"] },
          { path: "/api/v1/observations/releases", parameters: ["metric_code"] },
        ],
      },
    ]);
    const pinned = buildLatestObservationRequest(unpinnable, {
      metricCode: "CDC:cdc_places_county:OBESITY",
      limit: "4000",
      scope: SCOPE_AS_RELEASED,
      release: "20240115",
    });
    expect(pinned.params.scope).toBe(SCOPE_AS_RELEASED);
    expect(pinned.params.release).toBeUndefined();
  });

  test("a source with no as-released surface falls back to its latest scope", () => {
    const [undeclared] = buildExplorerSources([
      {
        ...capabilities[0],
        observation_routes: [
          { path: "/api/v1/observations", parameters: ["geo_id", "metric_code"] },
        ],
      },
    ]);
    const request = buildLatestObservationRequest(undeclared, {
      metricCode: "CDC:cdc_places_county:OBESITY",
      limit: "4000",
      scope: SCOPE_AS_RELEASED,
      release: "20240115",
    });
    expect(request.params.scope).toBe(SCOPE_LATEST);
    expect(request.params.release).toBeUndefined();
  });

  test("history reads carry the same scope so a pinned release reproduces", () => {
    expect(
      buildHistoryObservationRequest(census, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
        scope: SCOPE_AS_RELEASED,
        release: "2022",
      }),
    ).toEqual({
      resource: "/observations",
      params: {
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        scope: "as_released",
        release: "2022",
        limit: "1000",
        geo_id: "state:55|county:025",
      },
    });

    // A latest-scope history for the same source stays on the neutral
    // resource too, so both scopes resolve the same metric identity.
    expect(
      buildHistoryObservationRequest(census, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }).resource,
    ).toBe("/observations");

    // Only a source publishing no neutral route uses its own timeseries.
    expect(
      buildHistoryObservationRequest(scopedOnly, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }).resource,
    ).toBe("/census/observations/timeseries");
  });

  test("dimension controls under as-released are the neutral declared ones", () => {
    // A source-scoped source declares none of its own; the neutral filters
    // it carries into an as-released read are all shared vocabulary.
    expect(scopedDimensionFilters(census, SCOPE_LATEST)).toEqual([]);
    expect(scopedDimensionFilters(census, SCOPE_AS_RELEASED)).toEqual([]);
    expect(scopedDimensionFilters(cdc, SCOPE_AS_RELEASED)).toEqual([
      "adjustment_status",
      "stratum_id",
    ]);
    expect(scopedDimensionFilters(null, SCOPE_AS_RELEASED)).toEqual([]);
  });

  test("a saved view records the stratum its request carried, not its selection", () => {
    // Covers: WEB-081 — what a saved view records has to be what it asked
    // for. A document carrying a filter the request never sent replays a
    // narrower set than the view showed; one missing a filter the request
    // did send replays a wider one, and for a stratified measure that is a
    // different population. Read from the request, so a selection that
    // outlived the scope it was made under cannot be recorded as a narrowing
    // the view never applied.
    const stratum = { stratum_id: "female-45-54" };
    const carried = buildLatestObservationRequest(cdc, {
      metricCode: "CDC:nvss:INFANT_MORTALITY",
      geoLevel: "STATE",
      limit: "500",
      dimensions: stratum,
    });
    expect(carried.params.stratum_id).toBe("female-45-54");
    expect(dimensionsCarriedBy(carried, cdc.dimensionFilters)).toEqual(stratum);

    // The same selection against a source that declares no such filter: the
    // builder drops it from the request, so the view records no narrowing.
    const dropped = buildLatestObservationRequest(fbi, {
      metricCode: "FBI_UCR:summary:VIOLENT_CRIME",
      geoLevel: "STATE",
      limit: "500",
      dimensions: stratum,
    });
    expect(dropped.params.stratum_id).toBeUndefined();
    expect(dimensionsCarriedBy(dropped, fbi.dimensionFilters)).toEqual({});
    expect(dimensionsCarriedBy(null, cdc.dimensionFilters)).toEqual({});
  });

  test("an unpinned as-released answer is one series per release, not one value", () => {
    const rows = [
      { geo_id: "state:55|county:025", release: "2022", value: "555000" },
      { geo_id: "state:55|county:025", release: "2023", value: "561504" },
    ];
    expect(stratificationDimensions(census.dimensionFilters, SCOPE_LATEST)).toEqual([]);
    expect(stratificationDimensions(census.dimensionFilters, SCOPE_AS_RELEASED)).toEqual([
      RELEASE_DIMENSION,
    ]);

    // Two releases for one geography: the caller declines to colour or chart
    // rather than keeping whichever release sorted last.
    expect(
      describeStratification(
        rows,
        stratificationDimensions(census.dimensionFilters, SCOPE_AS_RELEASED),
      ),
    ).toEqual({
      seriesCount: 2,
      stratified: true,
      varyingDimensions: [RELEASE_DIMENSION],
    });

    // Pinning one resolves it to a single series.
    expect(
      describeStratification(
        rows.filter((row) => row.release === "2023"),
        stratificationDimensions(census.dimensionFilters, SCOPE_AS_RELEASED),
      ),
    ).toEqual({
      seriesCount: 1,
      stratified: false,
      varyingDimensions: [],
    });

    // The release axis composes with a source's own declared dimensions.
    expect(stratificationDimensions(cdc.dimensionFilters, SCOPE_AS_RELEASED)).toEqual([
      "adjustment_status",
      "stratum_id",
      RELEASE_DIMENSION,
    ]);
  });
});

describe("a history read across published releases", () => {
  // ACS's latest relation keeps one row per geography, so a geography's
  // history exists only across its releases; each period is shown as last
  // published, and a later release revising a period replaces it.
  test("keeps the newest release of each period, in period order", () => {
    const rows = [
      { geo_id: "g", period_end: "2023-12-31", release: "2023", value: "first" },
      { geo_id: "g", period_end: "2024-12-31", release: "2024", value: "newest" },
      { geo_id: "g", period_end: "2023-12-31", release: "2024", value: "revised" },
    ];
    expect(collapseToNewestRelease(rows).map((row) => row.value)).toEqual(["revised", "newest"]);
  });

  test("compares numeric release identities as numbers, not text", () => {
    const rows = [
      { period_end: "2024-12-31", release: "9", value: "old" },
      { period_end: "2024-12-31", release: "10", value: "new" },
    ];
    expect(collapseToNewestRelease(rows)[0].value).toBe("new");
  });

  test("drops a row with no period to order by", () => {
    expect(collapseToNewestRelease([{ release: "1", value: "x" }])).toEqual([]);
  });
});

// A latest publication that is a series: Census PEP answers every estimated
// year of the current vintage per county under scope=latest, so a map that
// keeps whichever row arrived last, or only the first page, colours the
// wrong rows or too few of them.
describe("newestPerGeography", () => {
  const rows = [
    { geo_id: "state:01|county:001", period_start: "2020-07-01", period_end: "2020-07-01", value: "165" },
    { geo_id: "state:01|county:001", period_start: "2025-07-01", period_end: "2025-07-01", value: "180" },
    { geo_id: "state:01|county:001", period_start: "2023-07-01", period_end: "2023-07-01", value: "172" },
    { geo_id: "state:01|county:003", period_start: "2025-07-01", period_end: "2025-07-01", value: "900" },
    { geo_id: "state:01|county:003", period_start: "2024-07-01", period_end: "2024-07-01", value: "880" },
    { geo_id: "state:01|county:005", observation_date: "2021-01-01", value: "12" },
    { geo_id: null, period_start: "2025-07-01", value: "1" },
    { geo_id: "state:01|county:007", value: "no period" },
  ];

  test("keeps each geography's newest period, in first-seen geography order", () => {
    expect(newestPerGeography(rows).map((row) => [row.geo_id, row.value])).toEqual([
      ["state:01|county:001", "180"],
      ["state:01|county:003", "900"],
      ["state:01|county:005", "12"],
    ]);
  });

  test("keeps the first published row when one geography repeats a period", () => {
    const repeated = [
      { geo_id: "state:06", period_end: "2025-07-01", value: "first" },
      { geo_id: "state:06", period_end: "2025-07-01", value: "second" },
    ];
    expect(newestPerGeography(repeated).map((row) => row.value)).toEqual(["first"]);
  });

  test("passes a one-row-per-geography publication through unchanged", () => {
    const single = [
      { geo_id: "state:01|county:001", period_end: "2020-01-01", value: "1" },
      { geo_id: "state:01|county:003", period_end: "2015-01-01", value: "2" },
    ];
    expect(newestPerGeography(single)).toEqual(single);
    expect(newestPerGeography([])).toEqual([]);
    expect(newestPerGeography(null)).toEqual([]);
  });

  test("counts the distinct periods a publication spans", () => {
    expect(countObservationPeriods(rows)).toBe(5);
    expect(countObservationPeriods([])).toBe(0);
  });
});


// Covers: WEB-028 — the map asks the resource for one row per geography
// rather than paging a source's whole latest publication and reducing it in
// the browser.
describe("newest per geography", () => {
  const declaring = buildExplorerSources([
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level", "year_from", "year_to"],
      // Census PEP is analysis-ready, so the resource answers the reduction
      // it declares. Both halves are the contract: the route's parameter and
      // the source's own `publishes_aligned_reduction` (API-139).
      publishes_aligned_reduction: true,
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: [
            "geo_id",
            "geo_level",
            "limit",
            "metric_code",
            "newest_per_geography",
            "offset",
            "release",
            "scope",
            "year_from",
            "year_to",
          ],
        },
        { path: "/api/v1/observations/releases", parameters: ["metric_code"] },
      ],
    },
  ])[0];

  const silent = buildExplorerSources([
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: ["geo_id", "geo_level", "limit", "metric_code", "scope"],
        },
      ],
    },
  ])[0];

  test("the capability entry decides whether the parameter exists", () => {
    expect(declaring.supportsNewestPerGeography).toBe(true);
    expect(silent.supportsNewestPerGeography).toBe(false);
  });

  test("the map read asks for one row per geography where it is declared", () => {
    const request = buildLatestObservationRequest(declaring, {
      metricCode: "CENSUS_PEP:POPESTIMATE",
      geoLevel: "COUNTY",
      limit: "5000",
      newestPerGeography: true,
    });
    expect(request.resource).toBe("/observations");
    expect(request.params.newest_per_geography).toBe("true");
    expect(request.params.geo_level).toBe("COUNTY");
  });

  test("a source that does not declare it is never sent it", () => {
    const request = buildLatestObservationRequest(silent, {
      metricCode: "CENSUS_PEP:POPESTIMATE",
      geoLevel: "COUNTY",
      newestPerGeography: true,
    });
    expect(request.params.newest_per_geography).toBeUndefined();
  });

  test("an as-released read never carries it", () => {
    // The resource refuses the combination, because one series per release
    // reduced per geography would show whichever release sorted last.
    const request = buildLatestObservationRequest(declaring, {
      metricCode: "CENSUS_PEP:POPESTIMATE",
      geoLevel: "COUNTY",
      newestPerGeography: true,
      scope: SCOPE_AS_RELEASED,
    });
    expect(request.params.scope).toBe(SCOPE_AS_RELEASED);
    expect(request.params.newest_per_geography).toBeUndefined();
  });

  test("the history request reads the whole series, never the reduction", () => {
    // A geography's trend is the series; reducing it would leave one point.
    const request = buildHistoryObservationRequest(declaring, {
      metricCode: "CENSUS_PEP:POPESTIMATE",
      geoId: "state:01|county:001",
      limit: "1000",
    });
    expect(request.params.newest_per_geography).toBeUndefined();
    expect(request.params.geo_id).toBe("state:01|county:001");
  });

  test("it is not sent unless the caller asks for it", () => {
    const request = buildLatestObservationRequest(declaring, {
      metricCode: "CENSUS_PEP:POPESTIMATE",
      geoLevel: "COUNTY",
    });
    expect(request.params.newest_per_geography).toBeUndefined();
  });

  // Covers: WEB-103 — a route parameter is not a source capability. The
  // route declares `newest_per_geography` for all seven sources, because a
  // route declares one parameter set; the resource refuses it for the three
  // whose rows carry strata, naming what a reduction would collapse. Reading
  // only the parameter is how the workbench's cross-section and the
  // explorer's settled trend each sent a request the API had already said it
  // would not answer, and drew nothing.
  test("a stratified source declares the parameter and is still not sent it", () => {
    const stratified = buildExplorerSources([
      {
        source_code: "CDC",
        display_name: "Centers for Disease Control and Prevention",
        route_segment: "cdc",
        served_by_neutral_routes: true,
        datasets: [],
        observation_filters: ["geo_id", "geo_level", "stratum_id"],
        // The whole served parameter set, exactly as the route declares it
        // for every source — and no aligned reduction, exactly as CDC's
        // dispatch entry declares it.
        publishes_aligned_reduction: false,
        observation_routes: [
          {
            path: "/api/v1/observations",
            parameters: servedParameters("/api/v1/observations"),
          },
        ],
      },
    ])[0];

    expect(stratified.publishesAlignedReduction).toBe(false);
    expect(stratified.supportsNewestPerGeography).toBe(false);
    expect(stratified.supportsSettledHistory).toBe(false);

    const request = buildLatestObservationRequest(stratified, {
      metricCode: "CDC:cdi:ALC1_1",
      geoLevel: "COUNTY",
      newestPerGeography: true,
    });
    expect(request.params.newest_per_geography).toBeUndefined();
    expect(
      buildSettledHistoryRequest(stratified, {
        metricCode: "CDC:cdi:ALC1_1",
        geoId: "state:55|county:025",
      }),
    ).toBeNull();
    expect(
      buildSettledSurfaceRequest(stratified, {
        metricCode: "CDC:cdi:ALC1_1",
        geoLevel: "COUNTY",
      }),
    ).toBeNull();
  });
});

// Covers: WEB-036 — a bounded read is never presented as a whole answer. A
// profile card wants one number: the geography's newest published value.
// Taking the last row of a bounded ascending page is that number only when
// the whole publication fitted in the page, which for Census PEP -- whose
// latest publication is every estimated year of the current vintage -- it
// does not.
describe("the newest published value for one geography", () => {
  const pep = buildExplorerSources([
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level", "year_from", "year_to"],
      publishes_aligned_reduction: true,
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: [
            "geo_id",
            "geo_level",
            "limit",
            "metric_code",
            "newest_per_geography",
            "offset",
            "scope",
          ],
        },
      ],
    },
  ])[0];

  // The same capability, except that it does not declare the reduction. The
  // narrowing is expressed as a subtraction from the served list, so the
  // client's refusal is caused by that one absence rather than by a fixture
  // that happens to be narrow in some other way (WEB-043).
  const withoutReduction = buildExplorerSources([
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id"],
      publishes_aligned_reduction: true,
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: servedParametersWithout("/api/v1/observations", [
            "newest_per_geography",
          ]),
        },
      ],
    },
  ])[0];

  test("it asks the resource to reduce, and takes one row", () => {
    const request = buildNewestValueRequest(pep, {
      metricCode: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      geoId: "state:01|county:001",
    });
    expect(request.resource).toBe("/observations");
    expect(request.params.newest_per_geography).toBe("true");
    expect(request.params.geo_id).toBe("state:01|county:001");
    expect(request.params.scope).toBe(SCOPE_LATEST);
    expect(String(request.params.limit)).toBe("1");
    expect(request.reducedByResource).toBe(true);
  });

  test("a source that cannot reduce says so, and reads a bounded page", () => {
    const request = buildNewestValueRequest(withoutReduction, {
      metricCode: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      geoId: "state:01|county:001",
    });
    expect(request.params.newest_per_geography).toBeUndefined();
    expect(request.reducedByResource).toBe(false);
    // Still one geography's own publication, and still bounded.
    expect(request.params.geo_id).toBe("state:01|county:001");
    expect(Number(request.params.limit)).toBeGreaterThan(1);
  });

  test("no parameter the capability did not declare is ever sent", () => {
    const request = buildNewestValueRequest(withoutReduction, {
      metricCode: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      geoId: "state:01|county:001",
    });
    for (const name of Object.keys(request.params)) {
      expect(
        withoutReduction.neutralFilters.includes(name),
        `${name} is not declared`,
      ).toBe(true);
    }
  });
});

// Covers: WEB-036 — the trend panel says when the page bound cut the series
// short, in the same words the map panel already uses. A prefix labelled
// "N historical observations" reads as the history.
describe("the history panel's status line", () => {
  test("a complete history is reported as what it is", () => {
    expect(describeHistoryLoad(48, 48, true, false)).toBe("48 historical observations");
    expect(describeHistoryLoad(1, 1, true, false)).toBe("1 historical observation");
  });

  test("a bounded read names the shortfall and calls the trend incomplete", () => {
    expect(describeHistoryLoad(5000, 18864, false, false)).toBe(
      "loaded 5000 of 18864 historical observations; the page bound cut the " +
        "answer short, so the trend is incomplete",
    );
  });

  test("a resource that published no total is not reported as short", () => {
    // Without a total there is no shortfall to state, and inventing one
    // would be this client asserting a count the API did not publish.
    expect(describeHistoryLoad(120, null, false, false)).toBe(
      "120 historical observations",
    );
  });

  test("the release context travels with either shape", () => {
    expect(describeHistoryLoad(9, 9, true, true)).toBe(
      "9 historical observations across published releases",
    );
    expect(describeHistoryLoad(5000, 9000, false, true)).toContain(
      "across published releases; the page bound cut the answer short",
    );
  });
});

// Covers: WEB-046 — the settled history is asked for, not computed. Deciding
// which release is newer is a rule the warehouse publishes and every dispatch
// entry declares; the client's own comparison could disagree with it, because
// `2023.10` and `2023.9` order one way as numbers and the other as text.
describe("a settled history is the resource's answer", () => {
  const declaring = buildExplorerSources([
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level"],
      publishes_aligned_reduction: true,
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: servedParameters("/api/v1/observations"),
        },
      ],
    },
  ])[0];

  const olderApi = buildExplorerSources([
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level"],
      publishes_aligned_reduction: true,
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: servedParametersWithout("/api/v1/observations", [
            "newest_release_per_period",
          ]),
        },
      ],
    },
  ])[0];

  test("it asks the resource to reduce across releases", () => {
    const request = buildSettledHistoryRequest(declaring, {
      metricCode: "CENSUS_ACS:acs5:B01003_001",
      geoId: "state:55|county:025",
      limit: "1000",
    });
    expect(request).not.toBeNull();
    expect(request.resource).toBe("/observations");
    expect(request.params.scope).toBe(SCOPE_AS_RELEASED);
    expect(request.params.newest_release_per_period).toBe("true");
    expect(request.params.geo_id).toBe("state:55|county:025");
    // A pinned release contradicts the reduction, and the resource refuses
    // the pair; this client does not send it.
    expect(request.params.release).toBeUndefined();
  });

  test("a deployment whose API does not declare it is not sent it", () => {
    // The trend must not be lost against an older API: the caller falls back
    // to reading the releases and reducing them, which is why this answers
    // null rather than a request without the parameter.
    expect(
      buildSettledHistoryRequest(olderApi, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }),
    ).toBeNull();
  });

  test("a source with no as-released surface is not asked at all", () => {
    const scopedOnly = buildExplorerSources([
      {
        source_code: "CENSUS_ACS",
        display_name: "Census American Community Survey",
        route_segment: "census",
        served_by_neutral_routes: false,
        datasets: [],
        observation_filters: [],
        observation_routes: [
          {
            path: "/api/v1/census/observations/latest",
            parameters: ["geo_level", "limit", "metric_code"],
          },
          {
            path: "/api/v1/census/observations/timeseries",
            parameters: ["geo_id", "limit", "metric_code"],
          },
        ],
      },
    ])[0];
    expect(
      buildSettledHistoryRequest(scopedOnly, {
        metricCode: "CENSUS_ACS:acs5:B01003_001",
        geoId: "state:55|county:025",
        limit: "1000",
      }),
    ).toBeNull();
  });
});

describe("a published coverage qualifier travels with its value", () => {
  // Covers: WEB-051 — FBI UCR is the one source the API refuses to serve
  // through the per-source row shape, because it publishes agency-level facts
  // with a participation basis that shape cannot represent honestly. The
  // neutral envelope carries that basis under `coverage`; normalization
  // mapped `uncertainty` onto the row and left `coverage` behind, so an
  // agency's offence count was rendered with no indication of the
  // participation it rests on.

  const neutralSource = {
    key: "fbi",
    accessShape: "neutral",
    neutralFilters: ["geo_id", "geo_level"],
    requestFilters: ["geo_id", "geo_level"],
    dimensionFilters: [],
    neutralDimensionFilters: [],
  };

  const reportingRow = {
    metric_code: "FBI_UCR:summarized:VIOLENT",
    source_code: "FBI_UCR",
    geo_id: "agency:WI0130000",
    geo_level: "AGENCY",
    value: "412",
    value_status: "valid",
    period_start: "2023-01-01",
    period_end: "2023-12-31",
    coverage: {
      population: "269840",
      participated_population: "167000",
      coverage_percent: "61.9",
      coverage_basis: "reported months",
      participation_status: "partial",
      population_denominator: "agency service population",
    },
  };

  test("every published coverage field survives normalization", () => {
    // Normalization carries the envelope through by construction -- it
    // spreads the row it was given -- so this pins that rather than a change
    // it needed. What was missing was any surface that read the field.
    const [row] = normalizeObservationRows(neutralSource, [reportingRow]);
    for (const [field, value] of Object.entries(reportingRow.coverage)) {
      expect(observationCoverageValue(row, field)).toBe(value);
    }
  });

  test("a source that publishes no coverage publishes none", () => {
    // Absent stays absent: an unpublished qualifier is not an empty one, and
    // inventing a dash in the data would make the two indistinguishable.
    const [row] = normalizeObservationRows(neutralSource, [
      { ...reportingRow, coverage: undefined },
    ]);
    expect(observationCoverageValue(row, "participation_status")).toBe("");
    expect(publishesCoverage([row])).toBe(false);
  });

  test("the answer says whether any row published a participation", () => {
    // Read from the loaded rows rather than from a list of sources, so a
    // source that starts publishing coverage is shown it without an edit
    // here, and one that does not grows no empty column.
    const [row] = normalizeObservationRows(neutralSource, [reportingRow]);
    expect(publishesCoverage([row])).toBe(true);
    expect(publishesCoverage([])).toBe(false);
    expect(publishesCoverage(null)).toBe(false);
  });

  test("a not-reported agency keeps its explanation", () => {
    // The schema's own reason for the field: a not-reported subject keeps
    // null values, and the coverage context explains the gap instead of the
    // API inventing a zero. The gap is only explained if it is shown.
    const [row] = normalizeObservationRows(neutralSource, [
      {
        ...reportingRow,
        value: null,
        value_status: "not_reported",
        coverage: { participation_status: "did not report", coverage_percent: "0" },
      },
    ]);
    expect(row.value).toBeNull();
    expect(observationCoverageValue(row, "participation_status")).toBe("did not report");
    // `0` is a published number here, not a missing one.
    expect(observationCoverageValue(row, "coverage_percent")).toBe("0");
  });
});

describe("a published uncertainty travels with its value", () => {
  // Covers: WEB-053 — the envelope's other qualifier object. Normalization
  // lifts `margin_of_error` and its percentage out of `uncertainty` for the
  // chart and leaves the other five inside it, and nothing read them: CDC's
  // published confidence bounds and USDA NASS's coefficient of variation --
  // the figure NASS publishes a symbol for precisely to say an estimate is
  // unreliable -- reached neither the table nor the export.

  const neutralSource = {
    key: "cdc",
    accessShape: "neutral",
    neutralFilters: ["geo_id", "geo_level"],
    requestFilters: ["geo_id", "geo_level"],
    dimensionFilters: [],
    neutralDimensionFilters: [],
  };

  const intervalRow = {
    metric_code: "CDC:cdc_places_county:OBESITY",
    source_code: "CDC",
    geo_id: "state:55|county:025",
    geo_level: "COUNTY",
    value: "32.4",
    value_status: "valid",
    uncertainty: { confidence_lower: "30.9", confidence_upper: "33.9" },
  };

  const coefficientRow = {
    metric_code: "USDA_NASS:CORN:YIELD",
    source_code: "USDA_NASS",
    geo_id: "state:55",
    geo_level: "STATE",
    value: "181.2",
    value_status: "valid",
    uncertainty: { cv_value: "14.7", cv_status: "unreliable", cv_symbol: "(D)" },
  };

  test("the exported field list is the one the contract declares", () => {
    // Read from the reviewed snapshot, so a field added to the envelope fails
    // this rather than being silently dropped from every export.
    expect([...OBSERVATION_UNCERTAINTY_FIELDS].sort()).toEqual(
      servedSchemaFields("ObservationUncertainty"),
    );
    expect([...OBSERVATION_COVERAGE_FIELDS].sort()).toEqual(
      servedSchemaFields("ObservationCoverage"),
    );
  });

  test("every published uncertainty field survives normalization", () => {
    for (const source of [intervalRow, coefficientRow]) {
      const [row] = normalizeObservationRows(neutralSource, [source]);
      for (const [field, value] of Object.entries(source.uncertainty)) {
        expect(observationUncertaintyValue(row, field)).toBe(value);
      }
    }
  });

  test("a source-scoped row's top-level margin is read as published", () => {
    // The per-source shapes carry `margin_of_error` at the top level and no
    // `uncertainty` object; the accessor reads both rather than only the one
    // the neutral envelope nests.
    expect(
      observationUncertaintyValue({ margin_of_error: "1.5" }, "margin_of_error"),
    ).toBe("1.5");
  });

  test("a source that publishes no uncertainty publishes none", () => {
    // Absent stays absent: an unpublished bound is not an empty one, and a
    // dash in the exported data would make the two indistinguishable.
    const [row] = normalizeObservationRows(neutralSource, [
      { ...intervalRow, uncertainty: undefined },
    ]);
    expect(observationUncertaintyValue(row, "confidence_lower")).toBe("");
    expect(publishesUncertainty([row])).toBe(false);
    expect(observationUncertaintyLabel(row)).toBe("");
  });

  test("the answer says whether any row published an uncertainty", () => {
    const [row] = normalizeObservationRows(neutralSource, [intervalRow]);
    expect(publishesUncertainty([row])).toBe(true);
    expect(publishesUncertainty([])).toBe(false);
  });

  test("the label can be asked for the fields beyond the margin", () => {
    // The profile product decodes the margin itself -- `marginOfErrorText`
    // knows the Census sentinel margins, which a field-value join cannot --
    // and needs the rest of what the row published beside it, without
    // repeating the margin (WEB-060).
    const [row] = normalizeObservationRows(neutralSource, [
      { ...intervalRow, uncertainty: { ...intervalRow.uncertainty, margin_of_error: "1200" } },
    ]);
    expect(observationUncertaintyLabel(row)).toContain("margin of error 1200");
    const beyond = observationUncertaintyLabel(row, OBSERVATION_UNCERTAINTY_BEYOND_MARGIN);
    expect(beyond).not.toContain("margin of error");
    expect(beyond).toContain("confidence lower");
  });

  test("the fields beyond the margin are derived from the one list", () => {
    // Not a second list: a field added to OBSERVATION_UNCERTAINTY_FIELDS
    // reaches every surface that reads it without an edit.
    expect(OBSERVATION_UNCERTAINTY_BEYOND_MARGIN.every((field) =>
      OBSERVATION_UNCERTAINTY_FIELDS.includes(field),
    )).toBe(true);
    expect([...OBSERVATION_UNCERTAINTY_BEYOND_MARGIN].sort()).toEqual(
      OBSERVATION_UNCERTAINTY_FIELDS.filter(
        (field) => !field.startsWith("margin_of_error"),
      ).slice().sort(),
    );
  });

  test("the label names each published field rather than composing a notation", () => {
    // A margin, an interval and a coefficient of variation are not
    // interchangeable; rendering them into one notation would be this client
    // deciding what three sources' numbers mean.
    const [row] = normalizeObservationRows(neutralSource, [coefficientRow]);
    expect(observationUncertaintyLabel(row)).toBe(
      "cv value 14.7 · cv status unreliable · cv symbol (D)",
    );
  });
});

// Covers: WEB-061 — the declared dimensions a table has no column for.
describe("the declared dimensions ride in one cell", () => {
  const row = {
    geo_id: "state:94",
    dimensions: {
      footnote_code: "1",
      footnote_text: "Estimates are model-based",
      estimate_method: "model-based",
    },
  };

  test("each published field is named, under the source's own name", () => {
    expect(
      observationDimensionLabel(row, [
        "footnote_code",
        "footnote_text",
        "estimate_method",
      ]),
    ).toBe(
      "footnote code 1 · footnote text Estimates are model-based · "
        + "estimate method model-based",
    );
  });

  test("a field the row did not publish is left out, not shown empty", () => {
    expect(observationDimensionLabel(row, ["footnote_code", "population_basis"])).toBe(
      "footnote code 1",
    );
  });

  test("a row that published none, or no names asked for, is empty", () => {
    expect(observationDimensionLabel(row, [])).toBe("");
    expect(observationDimensionLabel({}, ["footnote_code"])).toBe("");
    expect(observationDimensionLabel(null, ["footnote_code"])).toBe("");
  });
});

describe("the one period a view can honestly name", () => {
  test("rows that share a period name it; rows that differ name none", () => {
    // Covers: WEB-069 — what a saved view records as its period, and what
    // the packet builder then puts in a block's envelope. Census PEP's
    // latest publication carries every estimated year of the vintage, so
    // naming one of them would make the other years read as that period's
    // values.
    const row = (period) => ({ geo_id: "state:55", period });
    expect(sharedObservationPeriod([row("2023"), row("2023")])).toBe("2023");
    expect(sharedObservationPeriod([row("2023"), row("2022")])).toBe("");
    // Nothing loaded is nothing to name, not a guess.
    expect(sharedObservationPeriod([])).toBe("");
    expect(sharedObservationPeriod(null)).toBe("");
    // A row publishing no period does not veto one that does.
    expect(sharedObservationPeriod([row("2023"), row("")])).toBe("2023");
  });

  test("the period is the one the row labels, start and end included", () => {
    // `observationPeriodLabel`'s own rule: a bounded period reads as its
    // range, so two rows covering different ranges do not share a period.
    const ranged = (start, end) => ({ period_start: start, period_end: end });
    expect(sharedObservationPeriod([ranged("2021", "2023"), ranged("2021", "2023")])).toBe(
      "2021 – 2023",
    );
    expect(sharedObservationPeriod([ranged("2021", "2023"), ranged("2020", "2022")])).toBe(
      "",
    );
  });
});

describe("what a selected state narrowed", () => {
  // Covers: WEB-075 — a state narrows the map and the geography picker on
  // every source, and the rows only where the source declares the filter.
  // The control was gated on the filter, so Census PEP's county and place
  // pickers said "select a state first" and could never be given one: a PEP
  // county's history was reachable only by clicking the map, and places —
  // the grain PEP alone publishes — not at all.
  test("says the rows are national when the state did not reach them", () => {
    const note = stateScopeNote({
      stateSelected: true,
      narrowsRows: false,
      sourceTitle: "Census Population Estimates Program",
    });
    expect(note).toContain("Census Population Estimates Program");
    expect(note).toContain("declares no state filter");
    expect(note).toContain("these rows are national");
    expect(note).toContain("narrows the map and the geography list only");
  });

  test("says nothing when there is nothing to qualify", () => {
    expect(stateScopeNote({ stateSelected: false, narrowsRows: false })).toBe("");
    expect(stateScopeNote({ stateSelected: true, narrowsRows: true })).toBe("");
    // No title is still a sentence, not a blank subject.
    expect(
      stateScopeNote({ stateSelected: true, narrowsRows: false, sourceTitle: " " }),
    ).toContain("This source declares no state filter");
  });
});


describe("declared filter defaults (API-157)", () => {
  const nass = {
    key: "usda-nass",
    dimensionFilters: ["domain_desc", "reference_period_desc"],
    filterDefaults: { reference_period_desc: "YEAR" },
  };

  test("an untouched filter reads with the source's declared default", () => {
    expect(effectiveDimensionSelections(nass, {})).toEqual({ reference_period_desc: "YEAR" });
    expect(effectiveDimensionSelections(nass, { domain_desc: "TOTAL" })).toEqual({
      reference_period_desc: "YEAR",
      domain_desc: "TOTAL",
    });
  });

  test("a reader's own choice wins, including a choice of every value", () => {
    expect(
      effectiveDimensionSelections(nass, { reference_period_desc: "YEAR - AUG FORECAST" }),
    ).toEqual({ reference_period_desc: "YEAR - AUG FORECAST" });
    expect(effectiveDimensionSelections(nass, { reference_period_desc: "" })).toEqual({
      reference_period_desc: "",
    });
  });

  test("a source declaring no default is read exactly as selected", () => {
    const cdc = { dimensionFilters: ["stratum_id"], filterDefaults: {} };
    expect(effectiveDimensionSelections(cdc, {})).toEqual({});
    expect(effectiveDimensionSelections(null, { stratum_id: "x" })).toEqual({ stratum_id: "x" });
  });

  test("a link carries an explicit 'all' on a defaulted filter and reads it back", () => {
    const recorded = dimensionSelectionsForLink(nass, {
      reference_period_desc: "",
      domain_desc: "",
    });
    // "all" on the defaulted filter survives; on an ordinary filter it is
    // simply absent, as before.
    expect(recorded).toEqual({ reference_period_desc: ALL_DIMENSION_VALUES });
    expect(dimensionSelectionsFromLink(recorded)).toEqual({ reference_period_desc: "" });
    expect(effectiveDimensionSelections(nass, dimensionSelectionsFromLink(recorded))).toEqual({
      reference_period_desc: "",
    });
    expect(dimensionSelectionsForLink(nass, {})).toEqual({});
  });
});
