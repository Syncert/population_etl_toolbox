import { expect, test } from "../support/servedRequests.js";
import { servedParameters } from "../support/servedContract.js";

// Covers: WEB-004, WEB-005, WEB-006, WEB-010, WEB-013, WEB-014, WEB-016,
// WEB-017, WEB-018, WEB-029 —
// browser catalog/tile/selection/failure flows, URL reproduction of the
// selected exploration state, capability-driven source discovery and
// switching, dispatch-shaped sources reached through the neutral
// /observations resource with capability-declared filters, as-released
// exploration over the published release listing, presentation modes
// offered only where the selection can answer them, and the retired
// demonstration dashboards redirecting into the live explorer.

const MVT = Buffer.from(
  "GvEBCghjb3VudGllcxImEhAAAAEBAgIDAwQEBQUGBgcHGAMiEAm+FMQFGgDDBtQNAADEBg8aC2NvdW50eV9maXBzGgtjb3VudHlfbmFtZRoGZ2VvX2lkGglnZW9fbGV2ZWwaCGxhdGl0dWRlGglsb25naXR1ZGUaCnN0YXRlX2ZpcHMaCnN0YXRlX25hbWUiBQoDMDI1Ig0KC0RhbmUgQ291bnR5IhUKE3N0YXRlOjU1fGNvdW50eTowMjUiCAoGQ09VTlRZIgkZVFInoImIRUAiCRmamZmZmVlWwCIECgI1NSILCglXaXNjb25zaW4ogCB4Ag==",
  "base64",
);

const metrics = [
  {
    metric_code: "CENSUS_ACS:acs5:B01003_001",
    metric_display_name: "Total population",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
  },
  {
    metric_code: "CENSUS_ACS:acs1:B01003_001",
    metric_display_name: "Total population ACS1",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
  },
  // A measure published only at the national grain. The vector boundary
  // publishes no national geometry, so this series has no map at all.
  {
    metric_code: "CENSUS_ACS:acs5:B01003_001_US",
    metric_display_name: "Total population, United States",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["NATIONAL"],
    valid_time_grains: ["ANNUAL"],
    units: "people",
    freshness_state: "fresh",
  },
];

const pepMetric = {
  metric_code: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
  metric_display_name: "Resident population estimate",
  source_code: "CENSUS_PEP",
  // Census PEP publishes places as well as states and counties, which is why
  // the served geography rows carry `place_fips`/`place_name` at all, and why
  // the explorer's grain selector needed all five words (WEB-038).
  valid_geo_grains: ["STATE", "COUNTY", "PLACE"],
  valid_time_grains: ["ANNUAL"],
};

// BLS publishes two identity shapes. CES is one national series per metric
// and sorts ahead of every LAU code in the catalog; LAUS publishes per
// measure, so one metric spans every published state and county.
const blsNationalMetric = {
  metric_code: "BLS:CES0000000001",
  metric_display_name: "Total Nonfarm Payroll Employment",
  source_code: "BLS",
  units: "Thousands of Persons",
  valid_geo_grains: ["NATIONAL"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

const blsMeasureMetric = {
  metric_code: "BLS:LAU:UNEMP_RATE",
  metric_display_name: "Unemployment rate",
  source_code: "BLS",
  units: "Percent",
  valid_geo_grains: ["COUNTY", "STATE"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

const cdcMetric = {
  metric_code: "CDC:cdc_places_county:OBESITY",
  metric_display_name: "Obesity prevalence",
  source_code: "CDC",
  valid_geo_grains: ["COUNTY"],
  valid_time_grains: ["ANNUAL"],
};

// FBI UCR publishes agency-level facts and no route segment of its own, so
// it is reachable only through the neutral resource and its measures declare
// the AGENCY grain. It is the case the explorer's three-word grain
// vocabulary could not express (WEB-038).
const nassMetric = {
  metric_code: "USDA_NASS:hay_survey_annual:3bf8ec4d",
  metric_display_name: "HAY - YIELD, MEASURED IN TONS / ACRE",
  source_code: "USDA_NASS",
  units: "TONS / ACRE",
  valid_geo_grains: ["STATE"],
  valid_time_grains: ["ANNUAL"],
  freshness_state: "current",
};

const fbiMetric = {
  metric_code: "FBI_UCR:summarized:VIOLENT_CRIME",
  metric_display_name: "Violent crime offences",
  source_code: "FBI_UCR",
  units: "offences",
  valid_geo_grains: ["AGENCY"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

// Shaped like the served CapabilityListResponse. The explorer derives both
// its source tabs and how it reaches each source from these declarations:
// a source-scoped latest/timeseries pair, or the neutral /observations
// resource for a dispatch-shaped source.
const neutralRoutes = [
  // Read from the reviewed snapshot rather than copied: a list that claims
  // to be the served one and is not models a weaker API than the one that
  // ships, and the client then goes untested for the parameter it is
  // missing (WEB-043).
  { path: "/api/v1/observations", parameters: servedParameters("/api/v1/observations") },
  {
    path: "/api/v1/observations/releases",
    parameters: servedParameters("/api/v1/observations/releases"),
  },
];

const capabilityRoutes = (segment) => [
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

const capabilities = {
  total: 5,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      datasets: [],
      observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
      observation_routes: capabilityRoutes("census"),
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: capabilityRoutes("pep"),
    },
    {
      source_code: "USDA_NASS",
      display_name: "USDA National Agricultural Statistics Service",
      route_segment: "usda-nass",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: false,
      datasets: ["nass_crops_county"],
      observation_filters: ["domain_desc", "geo_id", "geo_level", "reference_period_desc"],
      // The final value, not one of that year's forecasts (API-157).
      observation_filter_defaults: { reference_period_desc: "YEAR" },
      observation_routes: [
        ...neutralRoutes,
        { path: "/api/v1/usda-nass/observations", parameters: ["geo_id", "limit"] },
      ],
    },
    {
      source_code: "BLS",
      display_name: "Bureau of Labor Statistics",
      route_segment: "bls",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      datasets: [],
      observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
      observation_routes: capabilityRoutes("bls"),
    },
    {
      source_code: "CDC",
      display_name: "Centers for Disease Control and Prevention",
      route_segment: "cdc",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: false,
      datasets: ["cdc_places_county"],
      // No state_fips and no distribution route: the explorer must send
      // neither, and must not present a failed bins request as a fallback.
      observation_filters: [
        "adjustment_status",
        "geo_id",
        "geo_level",
        "stratum_id",
        "year_from",
        "year_to",
      ],
      // The declared set `/catalog/capabilities` answers: a different and
      // much longer list than the filterable one, which is the whole of
      // WEB-061. `footnote_text` is how CDC qualifies an estimate.
      observation_dimensions: [
        "adjustment_status",
        "estimate_method",
        "footnote_code",
        "footnote_text",
        "population_basis",
        "stratum_id",
      ],
      observation_routes: neutralRoutes,
    },
    {
      source_code: "FBI_UCR",
      display_name: "FBI Uniform Crime Reporting",
      // No route segment: its observation surface is the neutral resource.
      route_segment: null,
      served_by_neutral_routes: true,
      publishes_aligned_reduction: false,
      datasets: ["summarized"],
      observation_filters: ["geo_id", "geo_level", "subject_code", "subject_type"],
      observation_routes: neutralRoutes,
    },
  ],
};

// Provider-neutral envelope rows (NeutralObservation): period bounds rather
// than one date, `unit`, nested `dimensions`, and a suppressed value that
// must never render as a number.
const cdcRow = (stratumId, value, extra = {}) => ({
  metric_code: cdcMetric.metric_code,
  source_code: "CDC",
  geo_id: "state:55|county:025",
  geo_level: "COUNTY",
  value,
  value_status: value === null ? "suppressed" : "valid",
  unit: "percent",
  period_start: "2021-01-01",
  period_end: "2022-12-31",
  // The confidence bounds CDC's dispatch entry declares. A fixture without
  // them models a weaker contract than the one that ships, and the client
  // then goes untested for the fields it is missing (WEB-043, WEB-053).
  uncertainty:
    value === null
      ? { confidence_lower: null, confidence_upper: null }
      : { confidence_lower: "30.9", confidence_upper: "33.9" },
  // The dimensions CDC's dispatch entry declares, not only the two the
  // route happens to filter on. A fixture carrying the filterable subset
  // models a weaker contract than the one that ships, and the client then
  // goes untested for the fields it is missing (WEB-043, WEB-061).
  dimensions: {
    stratum_id: stratumId,
    adjustment_status: "age-adjusted",
    estimate_method: "model-based",
    footnote_code: "1",
    footnote_text: "Estimates are model-based",
    population_basis: "adults",
  },
  ...extra,
});

const county = {
  source_code: "CENSUS_ACS",
  source: "CENSUS_ACS",
  observation_date: "2023-01-01",
  period: "2023",
  duration_start: "2023-01-01",
  duration_end: "2023-12-31",
  as_of_date: "2024-01-01",
  release_date: "2024-01-01",
  updated_at: "2024-01-02T00:00:00Z",
  geo_id: "state:55|county:025",
  geo_level: "COUNTY",
  geo_name: "Dane County",
  state_fips: "55",
  county_fips: "025",
  state_name: "Wisconsin",
  county_name: "Dane County",
  geo_latitude: 43.0667,
  geo_longitude: -89.4,
  metric_code: "CENSUS_ACS:acs5:B01003_001",
  metric_display_name: "Total population",
  value: "561504",
  units: "people",
  unit: "people",
  dataset_code: "acs5",
  dataset: "acs5",
  vintage_year: 2023,
  margin_of_error: "1200",
  margin_of_error_pct: "0.21",
};

// Census ACS published releases, as /observations/releases lists them:
// newest first, each with the identity `release=` accepts.
const acsReleases = [
  { release: "2023", as_of: "2024-01-01", observation_count: 3143 },
  { release: "2022", as_of: "2023-01-01", observation_count: 3142 },
];

// The same county as each release published it. Under scope=as_released
// both rows answer, so the geography carries one row per release.
const acsReleasedRow = (release, value) => ({
  ...county,
  release,
  as_of: release === "2023" ? "2024-01-01" : "2023-01-01",
  period_start: `${release}-01-01`,
  period_end: `${release}-12-31`,
  value,
});

async function installRoutes(
  page,
  {
    failLatest = false,
    neutralRequests = [],
    releaseRequests = [],
    truncateReleases = false,
    settledHistory = false,
  } = {},
) {
  let tileRequests = 0;
  await page.route("**/api/v1/observations/releases?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    releaseRequests.push(Object.fromEntries(params));
    if (truncateReleases) {
      // More published releases than the client's page bound can read: one
      // per page against a total no number of pages will meet.
      const offset = Number(params.get("offset") || 0);
      return route.fulfill({
        json: {
          metric_code: params.get("metric_code"),
          source_code: "CENSUS_ACS",
          total: 99999,
          limit: Number(params.get("limit") || 100),
          offset,
          items: [{ release: `r${offset}`, as_of: "2024-01-01", observation_count: 1 }],
        },
      });
    }
    const items = params.get("metric_code")?.startsWith("CENSUS_ACS:") ? acsReleases : [];
    return route.fulfill({
      json: {
        metric_code: params.get("metric_code"),
        source_code: "CENSUS_ACS",
        total: items.length,
        limit: Number(params.get("limit") || 100),
        offset: 0,
        items,
      },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/observations?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    neutralRequests.push(Object.fromEntries(params));
    const metric = params.get("metric_code") || "";

    if (params.get("scope") === "as_released") {
      const pinned = params.get("release");
      const rows = [acsReleasedRow("2023", "561504"), acsReleasedRow("2022", "555000")];
      const items = metric.startsWith("CENSUS_ACS:")
        ? rows.filter((row) => !pinned || row.release === pinned)
        : [];
      return route.fulfill({
        json: {
          total: items.length,
          limit: Number(params.get("limit") || 100),
          offset: 0,
          scope: "as_released",
          release: pinned,
          metric_code: metric,
          source_code: "CENSUS_ACS",
          items,
        },
        headers: { "x-cache": "MISS" },
      });
    }

    // Latest scope. Every source now reads through this resource, so the
    // fixture dispatches on the metric's own published source prefix the way
    // the registry does. A single-geography request carries `geo_id`; a
    // cross-geography one carries `geo_level`.
    const geoId = params.get("geo_id");
    const answer = (items, sourceCode) =>
      route.fulfill({
        json: {
          total: items.length,
          limit: Number(params.get("limit") || 100),
          offset: 0,
          scope: params.get("scope") || "latest",
          metric_code: metric,
          source_code: sourceCode,
          items,
        },
        headers: { "x-cache": "MISS" },
      });

    if (metric.startsWith("USDA_NASS:")) {
      // A year's final value beside its August forecast, one state: the
      // answer is two series per geography unless the reference period is
      // narrowed, exactly as Quick Stats publishes it.
      const reference = params.get("reference_period_desc");
      const nassRow = (referencePeriod, value) => ({
        metric_code: metric,
        source_code: "USDA_NASS",
        geo_id: "state:55",
        geo_level: "STATE",
        value,
        value_status: "valid",
        unit: "TONS / ACRE",
        period_start: "2023",
        period_end: "2023",
        dimensions: { reference_period_desc: referencePeriod, domain_desc: "TOTAL" },
      });
      const rows = [nassRow("YEAR", "2.4"), nassRow("YEAR - AUG FORECAST", "2.6")];
      return answer(
        rows.filter((row) => !reference || row.dimensions.reference_period_desc === reference),
        "USDA_NASS",
      );
    }

    if (metric.startsWith("FBI_UCR:")) {
      // Agency rows: a grain the tile boundary publishes no geometry for, so
      // the map declines and the table answers.
      const agencyRow = {
        metric_code: metric,
        source_code: "FBI_UCR",
        source: "FBI_UCR",
        geo_id: "agency:WI0130000",
        geo_level: "AGENCY",
        value: "412",
        value_status: "valid",
        unit: "offences",
        period_start: "2023-01-01",
        period_end: "2023-12-31",
        // The participation basis the neutral envelope publishes for this
        // source, and the reason it is served through that envelope at all
        // (WEB-051). A fixture without it models a weaker contract than the
        // one that ships, and the client then goes untested for the field it
        // is missing (WEB-043).
        coverage: {
          population: "269840",
          participated_population: "167000",
          coverage_percent: "61.9",
          coverage_basis: "reported months",
          participation_status: "partial",
          population_denominator: "agency service population",
        },
      };
      return answer([agencyRow], "FBI_UCR");
    }

    if (metric.startsWith("CENSUS_PEP:")) {
      const pepRow = {
        ...county,
        metric_code: metric,
        source_code: "CENSUS_PEP",
        source: "CENSUS_PEP",
        dataset_code: "pep_cty_alldata",
        dataset: "pep_cty_alldata",
        value: "561800",
      };
      // PEP publishes no history for this geography in this fixture.
      return answer(geoId ? [] : [pepRow], "CENSUS_PEP");
    }

    if (metric.startsWith("BLS:")) {
      const blsRow = {
        ...county,
        metric_code: metric,
        source_code: "BLS",
        source: "BLS",
        metric_display_name: "Unemployment rate",
        value: "3.1",
        unit: "Percent",
        units: "Percent",
        period_start: "2025-07-01",
        period_end: "2025-07-31",
        dimensions: { series_id: "LAUCN550250000000003" },
      };
      // The national series has no county rows; the measure spans them.
      return answer(metric === "BLS:CES0000000001" ? [] : [blsRow], "BLS");
    }

    if (metric.startsWith("CENSUS_ACS:")) {
      if (failLatest) {
        return route.fulfill({ status: 503, json: { detail: "fallback unavailable" } });
      }
      if (settledHistory && geoId && params.get("scope") === "as_released") {
        // The resource's own reduction: one row per period, already ranked
        // by the source's declared release order (API-081). The client must
        // not reduce it again.
        return answer(
          [
            { ...county, metric_code: metric, observation_date: "2022-01-01", period: "2022", value: "555000", release: "2024" },
            { ...county, metric_code: metric, observation_date: "2023-01-01", period: "2023", value: "561504", release: "2024" },
          ],
          "CENSUS_ACS",
        );
      }
      if (settledHistory && geoId) {
        // A latest read over one geography: ACS serves only its newest
        // vintage, so this is the single point that sends the client to the
        // as-released surface.
        return answer([{ ...county, metric_code: metric }], "CENSUS_ACS");
      }
      if (geoId) {
        return answer(
          [
            { ...county, metric_code: metric, observation_date: "2022-01-01", period: "2022", value: "555000" },
            { ...county, metric_code: metric },
          ],
          "CENSUS_ACS",
        );
      }
      // ACS1 publishes only counties above its population threshold, so this
      // selection legitimately answers with nothing.
      return answer(metric.includes(":acs1:") ? [] : [{ ...county, metric_code: metric }], "CENSUS_ACS");
    }

    const stratum = params.get("stratum_id");
    const rows = [cdcRow("overall", "32.4"), cdcRow("age_18_44", null)];
    const items = stratum ? rows.filter((row) => row.dimensions.stratum_id === stratum) : rows;
    return answer(items, "CDC");
  });
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/capabilities", (route) => route.fulfill({
    json: capabilities,
    headers: { "x-cache": "MISS" },
  }));
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const sourceCode = new URL(route.request().url()).searchParams.get("source_code");
    const bySource = {
      CENSUS_PEP: [pepMetric],
      CDC: [cdcMetric],
      BLS: [blsNationalMetric, blsMeasureMetric],
      FBI_UCR: [fbiMetric],
      USDA_NASS: [nassMetric],
    };
    const items = bySource[sourceCode] || metrics;
    return route.fulfill({
      json: { total: items.length, limit: 1000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/catalog/geographies?*", (route) => {
    // Answers per grain, as the projection does. Every grain used to fall to
    // the county row, which is what let the picker offer one grain's
    // geographies for another (WEB-064). `gold_glossary.dim_geo_latest`
    // carries us/state/county/place and no agency identity, so AGENCY is
    // empty here for the same reason it is empty there.
    const level = new URL(route.request().url()).searchParams.get("geo_level");
    const byGrain = {
      STATE: [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin", latitude: 44.5, longitude: -89.5 }],
      COUNTY: [{ geo_id: county.geo_id, geo_level: "COUNTY", state_fips: "55", county_fips: "025", state_name: "Wisconsin", county_name: "Dane County", latitude: 43.0667, longitude: -89.4 }],
      PLACE: [{ geo_id: "state:55|place:48000", geo_level: "PLACE", state_fips: "55", state_name: "Wisconsin", place_fips: "48000", place_name: "Madison city", latitude: 43.07, longitude: -89.4 }],
      AGENCY: [],
    };
    const items = byGrain[level] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/census/observations/latest?*", (route) => {
    if (failLatest) return route.fulfill({ status: 503, json: { detail: "fallback unavailable" } });
    const metric = new URL(route.request().url()).searchParams.get("metric_code");
    const items = metric?.includes(":acs1:") ? [] : [{ ...county, metric_code: metric }];
    return route.fulfill({
      json: { total: items.length, limit: 4000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/pep/observations/latest?*", (route) => {
    const metric = new URL(route.request().url()).searchParams.get("metric_code");
    const items = [{
      ...county,
      metric_code: metric,
      source_code: "CENSUS_PEP",
      source: "CENSUS_PEP",
      dataset_code: "pep_cty_alldata",
      dataset: "pep_cty_alldata",
      value: "561800",
    }];
    return route.fulfill({
      json: { total: items.length, limit: 4000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/pep/observations/timeseries?*", (route) => route.fulfill({
    json: { total: 0, limit: 1000, offset: 0, items: [] },
  }));
  await page.route("**/api/v1/census/observations/timeseries?*", (route) => route.fulfill({
    json: {
      total: 2,
      limit: 1000,
      offset: 0,
      items: [
        { ...county, observation_date: "2022-01-01", period: "2022", value: "555000" },
        county,
      ],
    },
  }));
  await page.route("**/api/v1/distribution/bins?*", (route) => route.fulfill({
    json: {
      total: 1,
      bin_count: 1,
      min_value: 561504,
      max_value: 561504,
      // The period the served answer publishes for its bins. A fixture
      // without it models an API that does not say which period painted the
      // map (WEB-043, WEB-054).
      period: "2023-01-01",
      periods_differ: false,
      // What the served answer says its bins could not carry (WEB-055). The
      // ACS fixture's source publishes a margin of error, so the API names
      // it; a fixture without the field models an analysis that never says.
      caveats: [
        "source 'CENSUS_ACS' publishes margin_of_error, margin_of_error_pct; "
        + "an aligned analysis carries none of it, so read the published "
        + "uncertainty on /observations before treating a derived value as "
        + "exact",
      ],
      // Each bin carries its own bounds, as the API publishes them
      // (WEB-057).
      items: [{ bin_index: 1, lower_bound: 561504, upper_bound: 561504, count: 1 }],
    },
  }));
  await page.route("**/tiles/catalog", (route) =>
    route.fulfill({
      // Martin's real catalog shape: sources sit under section keys rather
      // than at the top level (1.11.0, pinned in docker-compose.yml). The
      // flat `{counties:{}}` this previously mocked is a shape Martin does
      // not serve, which is how a broken discovery path stayed green.
      json: {
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
      },
    }),
  );
  await page.route(/\/tiles\/counties$/, (route) => route.fulfill({
    json: {
      name: "counties",
      tiles: ["http://internal-martin:3000/counties/{z}/{x}/{y}"],
      vector_layers: [{
        id: "counties",
        fields: { geo_id: "String", state_fips: "String", county_fips: "String", county_name: "String" },
      }],
    },
  }));
  await page.route(/\/tiles\/counties\/\d+\/\d+\/\d+(?:\.pbf)?$/, (route) => {
    tileRequests += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    });
  });
  await page.route("**/tiles/counties/**", (route) => {
    tileRequests += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    });
  });
  return () => tileRequests;
}

test("catalog, observation coloring, Martin tile, selection, history, and keyboard flow", async ({ page }) => {
  const tileRequests = await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-metric-count", "3");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "1");
  await expect(page.getByLabel("Choropleth value legend")).toContainText("API distribution");
  await expect(page.getByTestId("tiles-status")).toContainText("healthy_tile=true");
  expect(tileRequests()).toBeGreaterThan(0);

  await page.getByTestId("state-select").selectOption("55");
  await page.getByTestId("county-select").selectOption(county.geo_id);
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
  await expect(page.getByText("2 historical observations")).toBeVisible();
  // The selected geography is named in its own heading; the observation
  // table names it again in its Geo cell, so the assertion targets the heading.
  await expect(page.getByRole("heading", { name: "Dane County, Wisconsin" })).toBeVisible();

  // The URL reproduces the selected exploration state without navigation.
  await expect(page).toHaveURL(/metric=CENSUS_ACS%3Aacs5%3AB01003_001/);
  await expect(page).toHaveURL(/state=55/);
  await expect(page).toHaveURL(/geo=state%3A55%7Ccounty%3A025/);

  const map = page.getByTestId("map-canvas");
  await map.focus();
  await page.keyboard.press("Escape");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", "");
  await page.keyboard.press("Enter");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
});

test("the map's worker loads from this origin and stays alive", async ({ page }) => {
  // `data-colored-values` counts what the page asked MapLibre to colour, not
  // what MapLibre drew. With MapLibre 6 bundled, its worker resolved to an
  // empty URL, loaded the page itself as a module, and died -- every map was
  // blank on the running stack while the test above stayed green. Every
  // source is tiled in that worker, so the worker living is the map drawing.
  const workers = [];
  page.on("worker", (worker) => {
    const record = { url: worker.url(), closed: false };
    worker.on("close", () => {
      record.closed = true;
    });
    workers.push(record);
  });
  const workerScript = page.waitForResponse((response) =>
    response.url().endsWith("/vendor/maplibre-gl/maplibre-gl-worker.mjs"),
  );
  await installRoutes(page);
  await page.goto("/explore");

  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "1");
  const response = await workerScript;
  expect(response.status()).toBe(200);
  expect(response.headers()["content-type"]).toMatch(/javascript/);
  // A worker script under the page's nonce policy could not import its
  // shared module, so the vendored files are served without one.
  expect(response.headers()["content-security-policy"]).toBeUndefined();

  await expect.poll(() => workers.map((worker) => worker.url)).toContainEqual(
    expect.stringMatching(/\/vendor\/maplibre-gl\/maplibre-gl-worker\.mjs$/),
  );
  // The failure mode is a worker that starts and closes at once; give it the
  // time it took to die, then require every MapLibre worker still running.
  await page.waitForTimeout(1_000);
  expect(workers.filter((worker) => worker.closed)).toEqual([]);
});

test("source tabs derive from capability discovery and switch the explored source", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  // Every source whose declarations carry an access shape becomes a tab —
  // the source-scoped pair or the neutral /observations resource — and the
  // tab records which shape reaches it.
  await expect(dashboard).toHaveAttribute("data-source-count", "6");
  await expect(page.getByTestId("source-tab-census")).toHaveAttribute("aria-selected", "true");
  // Census declares its own route pair as well, and is still reached through
  // the neutral resource: the pair reads the legacy union views, which key
  // observations on that era's metric identity rather than the glossary
  // identity the catalog publishes, so it answers an empty page.
  await expect(page.getByTestId("source-tab-census"))
    .toHaveAttribute("data-access-shape", "neutral");
  await expect(page.getByTestId("source-tab-pep")).toBeVisible();
  await expect(page.getByTestId("source-tab-usda-nass"))
    .toHaveAttribute("data-access-shape", "neutral");
  await expect(page.getByTestId("source-tab-cdc"))
    .toHaveAttribute("data-access-shape", "neutral");

  await page.getByTestId("source-tab-pep").click();
  await expect(dashboard).toHaveAttribute("data-source-key", "pep");
  await expect(dashboard).toHaveAttribute("data-selected-metric", pepMetric.metric_code);
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(page).toHaveURL(/source=pep/);

  await page.getByTestId("source-tab-census").click();
  await expect(dashboard).toHaveAttribute("data-selected-metric", "CENSUS_ACS:acs5:B01003_001");
  await expect(page).not.toHaveURL(/source=/);

  // A shared URL reproduces the non-default source directly.
  await page.goto("/explore?source=pep");
  await expect(dashboard).toHaveAttribute("data-source-key", "pep");
  await expect(dashboard).toHaveAttribute("data-selected-metric", pepMetric.metric_code);
});

test("a dataset with no published rows says so, without explaining the provider's rule", async ({
  page,
}) => {
  await installRoutes(page);
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-metric-count", "3");
  await page.getByTestId("dataset-select").selectOption("acs1");

  // This asserted a sentence naming a Census population threshold. The API
  // publishes no field for that rule, so the client could not know it was
  // still true and said it anyway (WEB-112). What stays is what the answer
  // itself supports: no rows, said plainly.
  await expect(page.getByText(/county coverage is partial/)).toHaveCount(0);
  await expect(page.getByTestId("observations-status")).toContainText("0 county records published");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "0");
  await expect(page.getByText("No observations available for selected metric.")).toHaveCount(1);

  const failing = await page.context().newPage();
  await installRoutes(failing, { failLatest: true });
  await failing.goto("/explore");
  await expect(failing.getByTestId("observations-status")).toContainText("status 503");
  await expect(failing.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "0");
  await expect(failing.getByText("No observations available for selected metric.")).toHaveCount(1);
  await failing.close();
});

test("a dispatch-shaped source is explored through the neutral resource", async ({ page }) => {
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-count", "6");

  await page.getByTestId("source-tab-cdc").click();
  await expect(dashboard).toHaveAttribute("data-access-shape", "neutral");
  await expect(dashboard).toHaveAttribute("data-selected-metric", cdcMetric.metric_code);
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");

  // The request went to /observations with scope=latest, and carried only the
  // filters CDC declares — no state_fips, which would be a 422.
  const request = neutralRequests.at(-1);
  expect(request.metric_code).toBe(cdcMetric.metric_code);
  expect(request.scope).toBe("latest");
  expect(request.geo_level).toBe("COUNTY");
  expect(request.state_fips).toBeUndefined();

  // /distribution/bins is not declared for CDC, so it is not requested and
  // not reported as a failure.
  await expect(page.getByTestId("distribution-status")).toContainText("not declared for this source");

  // Two declared-dimension series per geography: the map declines to color
  // rather than keeping whichever row arrived last.
  await expect(dashboard).toHaveAttribute("data-stratified", "true");
  await expect(dashboard).toHaveAttribute("data-series-count", "2");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "0");
  await expect(page.getByTestId("stratification-note")).toContainText("stratum_id");

  // The published period range and the suppressed value stay exact.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "2021-01-01 – 2022-12-31" }).first()).toBeVisible();
  await expect(page.getByRole("cell", { name: "suppressed" })).toBeVisible();

  // Narrowing the declared dimension filter resolves it to one series and
  // the map colors again.
  await page.getByTestId("dimension-select-stratum_id").selectOption("overall");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  expect(neutralRequests.at(-1).stratum_id).toBe("overall");
});

test("a saved stratified view records the stratum it was read for", async ({ page }) => {
  // Covers: WEB-081 — the explorer's account save passed its dimension
  // selection into the document; the browser save, which is the store the
  // packet builder attaches from, recorded no dimensions at all. So a CDC
  // measure read for one stratum attached to a packet as a query asking for
  // every stratum the source publishes -- a different population -- while
  // the envelope's `api_query` beside it still named the one the block was
  // composed from. Both records now carry what the request carried, read
  // back from the request itself.
  const created = [];
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    created.push(body);
    return route.fulfill({
      json: {
        configuration_id: 11,
        name: body.name,
        version: 1,
        document: body.document,
        validation: { valid: true, reasons: [] },
      },
    });
  });
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });
  await page.goto("/explore?source=cdc");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", cdcMetric.metric_code);
  await page.getByTestId("dimension-select-stratum_id").selectOption("overall");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  await expect.poll(() => neutralRequests.at(-1)?.stratum_id).toBe("overall");

  await page.getByTestId("save-view").click();
  await expect.poll(() => created.length).toBe(1);
  expect(created[0].document.filters.stratum_id).toBe("overall");

  // Signed out, the same view is saved in the browser, and that is the store
  // the packet builder composes a block's query from.
  const browserSave = await page.context().newPage();
  await installRoutes(browserSave);
  await browserSave.goto("/explore?source=cdc");
  await browserSave.getByTestId("dimension-select-stratum_id").selectOption("overall");
  await expect(browserSave.getByTestId("dashboard")).toHaveAttribute(
    "data-stratified",
    "false",
  );
  const save = browserSave.getByTestId("save-view");
  await expect(save).toHaveAttribute("data-destination", "browser");
  await save.click();
  await expect(browserSave.getByTestId("save-toast")).toHaveAttribute(
    "data-destination",
    "browser",
  );
  const stored = await browserSave.evaluate(() =>
    JSON.parse(
      window.localStorage.getItem("economic-data-studio:saved-charts:v1") || "[]",
    ),
  );
  expect(stored).toHaveLength(1);
  expect(stored[0].dimensions).toEqual({ stratum_id: "overall" });
  // And the request it recorded names the same stratum, which is the pair
  // the API cross-checks (API-129).
  expect(stored[0].apiQuery).toContain("stratum_id=overall");
  await browserSave.close();
});

test("as-released exploration pins a published release and reproduces it", async ({ page }) => {
  const neutralRequests = [];
  const releaseRequests = [];
  await installRoutes(page, { neutralRequests, releaseRequests });
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "CENSUS_ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-scope", "latest");

  // The release identities come from /observations/releases for the selected
  // metric; nothing infers them from a period or a vintage.
  await expect(page.getByTestId("releases-status")).toContainText("2 published releases");
  await expect(dashboard).toHaveAttribute("data-release-count", "2");
  expect(releaseRequests.at(-1).metric_code).toBe("CENSUS_ACS:acs5:B01003_001");

  // Reading every published release: the request moves to the neutral
  // resource with scope=as_released, and the geography now carries one row
  // per release, so the map declines to colour rather than showing whichever
  // release sorted last.
  await page.getByTestId("publication-select").selectOption("as_released");
  await expect(dashboard).toHaveAttribute("data-scope", "as_released");
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");
  await expect(dashboard).toHaveAttribute("data-stratified", "true");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "0");
  await expect(page.getByTestId("stratification-note")).toContainText("release");
  await expect(page.getByTestId("as-released-note")).toContainText("every published release");

  let request = neutralRequests.at(-1);
  expect(request.scope).toBe("as_released");
  expect(request.release).toBeUndefined();
  expect(request.metric_code).toBe("CENSUS_ACS:acs5:B01003_001");

  // /distribution/bins declares no scope: its bins describe the latest
  // publication, so they are not requested for an as-released read and the
  // legend is not labelled with them.
  await expect(page.getByTestId("distribution-status"))
    .toContainText("API bins describe the latest publication only");

  // Pinning one release resolves it to a single series and reproduces the
  // analysis as that release published it.
  await page.getByTestId("publication-select").selectOption("release:2022");
  await expect(dashboard).toHaveAttribute("data-release", "2022");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  request = neutralRequests.at(-1);
  expect(request.scope).toBe("as_released");
  expect(request.release).toBe("2022");

  // The pinned row is the value that release published, with its own release
  // identity visible in the table.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "555000" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "2022", exact: true }).first()).toBeVisible();

  // The link carries the scope and the pin, so it reproduces the same
  // as-released analysis.
  await expect(page).toHaveURL(/scope=as_released/);
  await expect(page).toHaveURL(/release=2022/);

  const shared = await page.context().newPage();
  const sharedNeutral = [];
  await installRoutes(shared, { neutralRequests: sharedNeutral });
  await shared.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001&scope=as_released&release=2022");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-scope", "as_released");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-release", "2022");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  expect(sharedNeutral.at(-1).release).toBe("2022");
  await shared.close();

  // Returning to the latest publication drops both from the request and the
  // link: `release` without `scope=as_released` is a 422 by contract.
  await page.getByTestId("publication-select").selectOption("latest");
  await expect(dashboard).toHaveAttribute("data-scope", "latest");
  await expect(dashboard).toHaveAttribute("data-release", "");
  await expect(page).not.toHaveURL(/release=/);
});

test("a national series gets the explicit non-spatial experience, not an empty map", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-metric-count", "3");
  // The default county selection is mappable and offers every mode.
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByRole("tab", { name: "map" })).toBeVisible();
  await expect(page.getByTestId("map-canvas")).toBeVisible();

  // A measure published only at the national grain resolves the geography
  // level to NATIONAL, and the tile boundary publishes no national geometry.
  await page.getByTestId("metric-select").selectOption("CENSUS_ACS:acs5:B01003_001_US");
  await expect(dashboard).toHaveAttribute("data-map-supported", "false");

  // The map is not rendered at all: an uncoloured map reads as "no data",
  // which is a different fact from "this series is not spatial".
  await expect(page.getByTestId("map-canvas")).toHaveCount(0);
  await expect(page.getByRole("tab", { name: "map" })).toHaveCount(0);
  await expect(dashboard).not.toHaveAttribute("data-view-modes", /(^|,)map(,|$)/);

  // The reason is stated, and the alternative paths to the same values are named.
  const note = page.getByTestId("non-spatial-note");
  await expect(note).toContainText("no national geometry");
  await expect(note).toContainText("observation table");
  await expect(page.getByTestId("unsupported-modes")).toContainText("map —");

  // Returning to a mappable measure brings the map back as the rendered
  // view without the user re-choosing it: the tab the user asked for is
  // remembered, so a mode that is briefly unavailable is not a lost one.
  await page.getByTestId("metric-select").selectOption("CENSUS_ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByTestId("map-canvas")).toBeVisible();

  // Every non-spatial mode still answers, and the value is still retrievable.
  await page.getByTestId("metric-select").selectOption("CENSUS_ACS:acs5:B01003_001_US");
  await expect(page.getByRole("tab", { name: "table" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "quality" })).toBeVisible();
  await expect(page.getByTestId("export-csv")).toBeEnabled();
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "561504" })).toBeVisible();

  // Quality is the measure's own published freshness and provenance.
  await page.getByRole("tab", { name: "quality" }).click();
  await expect(page.getByTestId("explorer-freshness")).toContainText("fresh");
  await expect(page.getByTestId("explorer-provenance")).toContainText("people");
});

test("a measure-identified source draws its map through the shared paths", async ({ page }) => {
  // Covers: WEB-029 — BLS LAUS publishes per measure, so a BLS metric spans
  // geographies and the map, bins, and state filter answer for it through the
  // same capability-driven paths every other source uses. The client carries
  // no BLS special case; if this needed one, that would be a warehouse or API
  // defect rather than a reason to add one here.
  const neutralRequests = [];
  const tileRequests = await installRoutes(page, { neutralRequests });
  await page.goto("/explore");

  await page.getByTestId("source-tab-bls").click();
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-key", "bls");

  // BLS lists 56 national series ahead of every LAUS measure, and the default
  // must not open the source on a selection its map can never draw.
  await expect(dashboard).toHaveAttribute("data-selected-metric", "BLS:LAU:UNEMP_RATE");
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");

  // Only three-part codes carry a dataset facet, so BLS publishes one facet
  // and the selector stays hidden with the whole list offered.
  await expect(page.getByTestId("dataset-select")).toHaveCount(0);
  await expect(dashboard).toHaveAttribute("data-metric-count", "2");

  // The choropleth colours the measure's county rows and the legend reports
  // the API's own distribution rather than a client-computed one.
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "1");
  await expect(page.getByLabel("Choropleth value legend")).toContainText("API distribution");
  expect(tileRequests()).toBeGreaterThan(0);

  // The read went through the neutral resource at the county grain, with the
  // geography filters BLS declares.
  const blsRequest = neutralRequests.findLast(
    (request) => request.metric_code === "BLS:LAU:UNEMP_RATE",
  );
  expect(blsRequest.geo_level).toBe("COUNTY");

  // The state filter BLS declares narrows the same read, and a clicked county
  // answers its own history.
  await page.getByTestId("state-select").selectOption("55");
  await page.getByTestId("county-select").selectOption(county.geo_id);
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);

  // The series id rides along as a published dimension, so lineage back to
  // the BLS series survives the measure-level identity.
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  // A national BLS series is still correctly non-spatial.
  await page.getByTestId("metric-select").selectOption("BLS:CES0000000001");
  await expect(dashboard).toHaveAttribute("data-map-supported", "false");
  await expect(page.getByTestId("non-spatial-note")).toContainText("no national geometry");
});

test("the retired source dashboards land on the live explorer for their source", async ({ page }) => {
  await installRoutes(page);

  // These routes rendered demonstration dashboards whose charts, secondary
  // KPIs, ranked lists, and stylized maps were illustrative examples. They
  // are retired: the link stays valid and reaches the capability-driven
  // explorer for the same source, which answers from published data only.
  await page.goto("/census");
  await expect(page).toHaveURL(/\/explore/);
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-key", "census");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "CENSUS_ACS:acs5:B01003_001");
  // Nothing illustrative survives the retirement.
  await expect(page.getByTestId("demo-banner")).toHaveCount(0);
  // The site navigation, which the dashboards suppressed, is back.
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();

  // Each retired route keeps its own source identity through the redirect.
  await page.goto("/fred");
  await expect(page).toHaveURL(/source=fred/);
  await page.goto("/bls");
  await expect(page).toHaveURL(/source=bls/);
});

// Covers: WEB-022 — a saved view is a configuration on the account when the
// user is signed in, and a browser-local chart when they are not. The two are
// different facts with different consequences for whether the work still
// exists tomorrow, so the destination is stated before and after the save.
test("an explorer view saves to the account when signed in, and says so", async ({ page }) => {
  await installRoutes(page);

  const created = [];
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const request = route.request();
    created.push({
      authorization: request.headers()["authorization"] || "",
      body: JSON.parse(request.postData() || "{}"),
      url: request.url(),
    });
    return route.fulfill({
      json: {
        configuration_id: 7,
        name: JSON.parse(request.postData() || "{}").name,
        version: 1,
        document: JSON.parse(request.postData() || "{}").document,
        validation: { valid: true, reasons: [] },
      },
    });
  });

  // The token the saved-analysis screen remembers for the tab.
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  // The destination is stated before the click, not discovered after it.
  const save = page.getByTestId("save-view");
  await expect(save).toHaveAttribute("data-destination", "account");
  await save.click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-destination", "account");
  await expect(toast).toContainText("your account");

  expect(created).toHaveLength(1);
  // The token reaches the API only as a header, and never appears in the URL
  // it was sent to or in the address bar.
  expect(created[0].authorization).toBe("Bearer operator-token");
  expect(created[0].url).not.toContain("operator-token");
  expect(page.url()).not.toContain("operator-token");

  // The document is intent, not data: it names the measure and the filters
  // and carries no observation value.
  const document = created[0].body.document;
  expect(document.kind).toBe("observations");
  expect(document.metric_code).toBeTruthy();
  expect(JSON.stringify(document)).not.toContain("561504");
});

test("an explorer view saves in the browser when signed out, and names that limit", async ({
  page,
}) => {
  await installRoutes(page);
  let configurationWrites = 0;
  await page.route("**/api/v1/analysis-configurations", (route) => {
    configurationWrites += 1;
    return route.fulfill({ status: 401, json: { detail: "missing token" } });
  });

  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  const save = page.getByTestId("save-view");
  await expect(save).toHaveAttribute("data-destination", "browser");
  await save.click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-destination", "browser");
  // Not merely "Saved": a reader who is told only that would believe the work
  // survives the tab.
  await expect(toast).toContainText("this browser only");

  // Signed out, the account is never reached at all — an unauthenticated
  // write attempt would be a request the client knows the API must refuse.
  expect(configurationWrites).toBe(0);
  const stored = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:saved-charts:v1"),
  );
  expect(JSON.parse(stored || "[]")).toHaveLength(1);
});

test("a refused account save is reported and never falls back to the browser", async ({
  page,
}) => {
  await installRoutes(page);
  await page.route("**/api/v1/analysis-configurations", (route) =>
    route.fulfill({ status: 401, json: { detail: "token revoked" } }),
  );
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "stale-token");
  });
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  await page.getByTestId("save-view").click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-state", "unauthorized");
  await expect(toast).toContainText("not accepted");
  // The refusal is reported, not worked around: writing to the browser store
  // instead would tell the user their work is safe in a place they did not
  // choose and cannot see from their account.
  await expect(toast).not.toHaveAttribute("data-destination", "browser");
  const stored = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:saved-charts:v1"),
  );
  expect(JSON.parse(stored || "[]")).toHaveLength(0);
});

// Covers: WEB-002 — the view-level control offers only the grains the
// publisher declares for the selected measure.
//
// Offering all three unconditionally meant choosing one the measure does not
// publish sent a request the API answered with zero rows, after which a
// corrective effect snapped the selection back. The choice was offered,
// accepted, and silently discarded, which reads as the screen losing the
// click rather than as the measure not being published at that grain.
test("the view level offers only the grains the measure declares, and says why", async ({
  page,
}) => {
  const observationRequests = [];
  await installRoutes(page, { neutralRequests: observationRequests });
  await page.goto("/explore");
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");

  const level = page.getByTestId("geo-level-select");

  // ACS county population declares STATE and COUNTY, so NATIONAL is absent
  // rather than offered and then revoked.
  await page.getByTestId("metric-select").selectOption("CENSUS_ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "CENSUS_ACS:acs5:B01003_001");
  await expect(level.locator("option")).toHaveCount(2);
  await expect(level.locator('option[value="STATE"]')).toHaveCount(1);
  await expect(level.locator('option[value="COUNTY"]')).toHaveCount(1);
  await expect(level.locator('option[value="NATIONAL"]')).toHaveCount(0);

  // The narrowing is attributed to the publisher, not left to be inferred
  // from a shorter list.
  await expect(page.getByTestId("geo-grain-note")).toContainText("published at");
  await expect(page.getByTestId("geo-grain-note")).toContainText("publisher");

  // No request is ever sent for a grain the measure does not declare.
  const requestedLevels = observationRequests.map((entry) => entry.geo_level).filter(Boolean);
  expect(requestedLevels).not.toContain("NATIONAL");
});

test("a shared link asks for a state, and the view saves only what it sent", async ({
  page,
}) => {
  // Covers: WEB-066, WEB-075 — WEB-066 dropped a link's state on a source
  // that declares no `state_fips`, for three reasons. Two were the disabled
  // control itself: a state the reader could not see or clear, and a map
  // narrowed while the rows stayed national with nothing saying so. WEB-075
  // fixed both — the control is usable because a state narrows the map and
  // the geography picker on every source — so the link is honoured again.
  // The third reason stands, and this asserts it: the *saved document*
  // records the state only where the request carried it, so a save is never
  // a 422 over a filter the source does not declare.
  const created = [];
  await installRoutes(page);
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    created.push(body);
    return route.fulfill({
      json: {
        configuration_id: 7,
        name: body.name,
        version: 1,
        document: body.document,
        validation: { valid: true, reasons: [] },
      },
    });
  });
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });
  await page.goto("/explore?source=pep&state=55");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-key", "pep");
  await expect(dashboard).toHaveAttribute("data-selected-state", "55");
  await expect(page.getByTestId("state-select")).toBeEnabled();
  await expect(page).toHaveURL(/state=55/);
  // And the rows are national, which the observations line says.
  await expect(page.getByTestId("observations-status")).toContainText(
    "these rows are national",
  );

  await page.getByTestId("save-view").click();
  await expect(page.getByTestId("save-toast")).toHaveAttribute(
    "data-destination",
    "account",
  );
  expect(created).toHaveLength(1);
  // Census PEP's serving relation carries no fips columns, so the document
  // carries no `state_fips` either: a stored filter its own route would
  // refuse is a view that cannot be reopened (API-117).
  expect(created[0].document.filters.state_fips).toBeUndefined();
  expect(created[0].document.filters.geo_level).toBe("COUNTY");

  // A source that does declare the filter stores it.
  await page.goto("/explore?source=census&state=55");
  await expect(page.getByTestId("dashboard")).toHaveAttribute(
    "data-selected-state",
    "55",
  );
  await page.getByTestId("save-view").click();
  await expect
    .poll(() => created.length)
    .toBe(2);
  expect(created[1].document.filters.state_fips).toBe("55");
});

test("the geography picker offers the grain that was asked for", async ({ page }) => {
  // Covers: WEB-064 — the grain selector publishes all five declared words
  // (WEB-038); the picker answered two and fell through to a third. At PLACE
  // — Census PEP's own grain, and the only grain the API publishes
  // `place_fips`/`place_name` for — it offered *states*, labelled by their
  // state names, as the places to choose from. Picking one sent a geo_id
  // that cannot exist at that grain, and the control could not even hold the
  // choice: its value was checked against a list that is empty for every
  // grain but STATE and COUNTY.
  await installRoutes(page);
  await page.goto("/explore?source=pep");

  const picker = page.getByTestId("county-select");
  const level = page.getByTestId("geo-level-select");
  await expect(level.locator('option[value="PLACE"]')).toHaveCount(1);

  // Counties are chosen after a state, and places are chosen the same way:
  // the projection carries some 32k of them and a picker is not the place to
  // load them all.
  await level.selectOption("COUNTY");
  await expect(picker).toBeDisabled();
  await expect(picker.locator("option")).toHaveText(["Select a state first"]);

  await level.selectOption("PLACE");
  await expect(picker).toBeDisabled();
  await expect(picker.locator("option")).toHaveText(["Select a state first"]);
  // The defect: none of these is a place.
  await expect(picker.locator("option", { hasText: "Wisconsin" })).toHaveCount(0);
  await expect(picker.locator("option", { hasText: "Dane County" })).toHaveCount(0);
});

test("a state narrows what it can narrow, and says what it did not", async ({
  page,
}) => {
  // Covers: WEB-075 — the state control was disabled unless the source
  // declared `state_fips` as an observation filter. Census PEP declares none:
  // `gold_pep.population_estimate_latest`, the relation the neutral route
  // reads, carries `geo_id` and `geo_type` and no fips columns. So on the one
  // source that publishes places, the county and place pickers said "select a
  // state first" and could never be given one — a PEP county's history was
  // reachable only by clicking the map, and a place not at all. A state
  // narrows the map and the picker on every source; only the rows depend on
  // the filter, and now the observations line says which is which.
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto("/explore?source=pep");

  const state = page.getByTestId("state-select");
  await expect(state).toBeEnabled();
  await state.selectOption("55");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-state", "55");
  // The rows are national, and the screen says so rather than describing
  // them as this selection's.
  await expect(page.getByTestId("observations-status")).toContainText(
    "declares no state filter",
  );
  await expect(page.getByTestId("observations-status")).toContainText(
    "these rows are national",
  );
  const stateFilters = neutralRequests
    .filter((entry) => (entry.metric_code || "").startsWith("CENSUS_PEP:"))
    .map((entry) => entry.state_fips)
    .filter(Boolean);
  expect(stateFilters).toEqual([]);

  // And the picker has a state now, so a county's history is reachable
  // without the map.
  const picker = page.getByTestId("county-select");
  await expect(picker).toBeEnabled();
  await picker.selectOption("state:55|county:025");
  await expect(dashboard).toHaveAttribute(
    "data-selected-geo-id",
    "state:55|county:025",
  );
  await expect
    .poll(() =>
      neutralRequests.filter(
        (entry) => entry.geo_id === "state:55|county:025",
      ).length,
    )
    .toBeGreaterThan(0);
});

test("a grain the projection publishes nothing for says so", async ({ page }) => {
  // Covers: WEB-064 — `gold_glossary.dim_geo_latest` takes its grains from
  // `dim_geo_current.geo_level` (us/state/county/place), so it carries no
  // agency identity. Offering states there is not a smaller version of the
  // right answer; it is a different grain's list under this grain's label.
  await installRoutes(page);
  await page.goto("/explore?source=FBI_UCR&metric=FBI_UCR%3Asummarized%3AVIOLENT_CRIME");

  const picker = page.getByTestId("county-select");
  await expect(page.getByTestId("geo-level-select")).toHaveValue("AGENCY");
  await expect(picker).toBeDisabled();
  await expect(picker.locator("option")).toHaveText([
    "No agencies are published to choose from",
  ]);
  await expect(picker.locator("option", { hasText: "Wisconsin" })).toHaveCount(0);
});

test("a measure published at an agency grain is offered that grain, and asked for it", async ({
  page,
}) => {
  // Covers: WEB-038 — the explorer knew three of the five published grain
  // words. FBI UCR publishes agency-level facts, so every one of its
  // measures fell past each branch, took the COUNTY fallback it does not
  // publish, offered no levels at all, and reported "0 COUNTY records
  // published for this selection" — the measure reading as unpublished
  // because the client could not name its grain.
  const observationRequests = [];
  await installRoutes(page, { neutralRequests: observationRequests });
  await page.goto("/explore?source=FBI_UCR&metric=FBI_UCR%3Asummarized%3AVIOLENT_CRIME");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "FBI_UCR:summarized:VIOLENT_CRIME");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");

  // The declared grain is the one offered, and the only one.
  const level = page.getByTestId("geo-level-select");
  await expect(level.locator("option")).toHaveCount(1);
  await expect(level.locator('option[value="AGENCY"]')).toHaveCount(1);

  // And the one asked for. Nothing was ever requested at COUNTY.
  const requested = observationRequests
    .filter((entry) => (entry.metric_code || "").startsWith("FBI_UCR:"))
    .map((entry) => entry.geo_level)
    .filter(Boolean);
  expect(requested.length).toBeGreaterThan(0);
  expect(new Set(requested)).toEqual(new Set(["AGENCY"]));

  // The map still declines, with the published reason it already gives: this
  // plan did not make agencies mappable.
  await expect(page.getByRole("tab", { name: "map" })).toHaveCount(0);
});

test("a link naming a measure and its source opens on that measure", async ({ page }) => {
  // Covers: WEB-072 — a link into the explorer names the source that
  // publishes its measure, as the metric row publishes it (`source_code`).
  // The catalog, quality, profile and home links carried the metric alone;
  // `/explore` mounts Census ACS, the requested code was absent from that
  // catalog, and the page silently selected `pickPreferredMetric` instead.
  // `BLS` is the glossary source code and `bls` the route segment the tabs
  // use: both are published identities, so both resolve.
  await installRoutes(page);
  await page.goto("/explore?source=BLS&metric=BLS%3ALAU%3AUNEMP_RATE");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "BLS:LAU:UNEMP_RATE");
  await expect(page.getByTestId("requested-metric-note")).toHaveCount(0);
});

test("a link naming a measure this source does not publish says so", async ({ page }) => {
  // Covers: WEB-072 — the substitution, said out loud. A link that reaches
  // the explorer without a source still mounts Census ACS, and the measure it
  // names is published by BLS. Selecting Census ACS total population instead
  // would answer a question the reader did not ask, with no statement that
  // anything had changed.
  await installRoutes(page);
  await page.goto("/explore?metric=BLS%3ALAU%3AUNEMP_RATE");

  const note = page.getByTestId("requested-metric-note");
  await expect(note).toContainText("BLS:LAU:UNEMP_RATE");
  await expect(note).toContainText("Nothing was substituted");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-selected-metric", "");
});

test("a metric with more releases than the page bound says so, and pages toward them", async ({
  page,
}) => {
  // Covers: WEB-045 — the release control is a picker: selecting a release is
  // the only way this screen sends `scope=as_released&release=…` or builds
  // the link that reproduces it. Asking once for two hundred left every
  // release past the two hundredth unreachable and unshareable, and reported
  // that in green.
  const releaseRequests = [];
  await installRoutes(page, { releaseRequests, truncateReleases: true });
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  const status = page.getByTestId("releases-status");
  await expect(status).toContainText("the page bound cut the answer short");
  await expect(status).toContainText("of 99999 published releases");
  // A partial listing is never green.
  await expect(status).toHaveClass(/pill bad/);

  // It paged rather than asking once, and each page asked for the next rows.
  const offsets = releaseRequests.map((entry) => Number(entry.offset));
  expect(offsets.length).toBeGreaterThan(1);
  expect(offsets[0]).toBe(0);
  expect(offsets[1]).toBe(1);
});

test("a geography's history is the settled one the resource answers", async ({ page }) => {
  // Covers: WEB-046 — the client used to read every release and decide which
  // was newer from the identity's spelling, a rule the warehouse publishes
  // and every dispatch entry declares. It now asks for the reduction.
  const observationRequests = [];
  await installRoutes(page, { neutralRequests: observationRequests, settledHistory: true });
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001&geo=state%3A55%7Ccounty%3A025");

  await expect(page.getByTestId("history-status")).toContainText("historical observation");

  const settled = observationRequests.filter(
    (entry) => entry.newest_release_per_period === "true",
  );
  expect(settled.length).toBeGreaterThan(0);
  for (const request of settled) {
    expect(request.scope).toBe("as_released");
    expect(request.geo_id).toBe("state:55|county:025");
    // A pinned release contradicts the reduction; the resource refuses the
    // pair and this client never sends it.
    expect(request.release).toBeUndefined();
  }
});

test("a copied link reopens at the grain it names", async ({ page }) => {
  // Covers: WEB-073 — WEB-038 widened the grain vocabulary, the control and
  // the serializer to all five published words, and left the apply step at
  // two: `NATIONAL`, `PLACE` and `AGENCY` were parsed, validated, and then
  // discarded, so the selection fell to COUNTY. Census PEP publishes places,
  // which is why the projection carries `place_fips` at all, and a shared
  // view of one reopened as a county map of the same measure.
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto(
    "/explore?source=CENSUS_PEP&metric=CENSUS_PEP%3Apep_cty_alldata%3APOPESTIMATE&geo_level=PLACE",
  );

  await expect(page.getByTestId("geo-level-select")).toHaveValue("PLACE");
  // The request the explorer issues, not just the control it draws.
  await expect
    .poll(() =>
      neutralRequests.filter(
        (entry) =>
          (entry.metric_code || "").startsWith("CENSUS_PEP:") &&
          entry.geo_level === "PLACE",
      ).length,
    )
    .toBeGreaterThan(0);
  const grains = neutralRequests
    .filter((entry) => (entry.metric_code || "").startsWith("CENSUS_PEP:"))
    .map((entry) => entry.geo_level)
    .filter(Boolean);
  expect(new Set(grains)).toEqual(new Set(["PLACE"]));
});

test("a copied link reopens the dimension narrowing it names", async ({ page }) => {
  // Covers: WEB-073 — `ExplorerState` had no dimension field and the
  // serializer omitted `dimensionSelections`, so a CDC view narrowed to one
  // stratum — the narrowing this screen demands before it will colour a map —
  // reopened stratified with a blank map. The saved-view document carried the
  // narrowing all along, so the two records of one view disagreed.
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto(
    "/explore?source=CDC&metric=CDC%3Acdc_places_county%3AOBESITY&stratum_id=overall",
  );

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  // Narrowed, so the map can colour: the state the link was copied from.
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  await expect(page.getByTestId("dimension-select-stratum_id")).toHaveValue("overall");

  const cdcRequests = neutralRequests.filter((entry) =>
    (entry.metric_code || "").startsWith("CDC:"),
  );
  expect(cdcRequests.length).toBeGreaterThan(0);
  expect(cdcRequests.every((entry) => entry.stratum_id === "overall")).toBe(true);

  // And the link the screen keeps is the one that was opened, so copying it
  // again reproduces the same view.
  await expect
    .poll(() => new URL(page.url()).searchParams.get("stratum_id"))
    .toBe("overall");
});

test("a saved map view records the reduction the map asked for", async ({ page }) => {
  // Covers: WEB-047 — the document a saved view stores is the request the
  // view issued. It recorded no reduction, so a map of a source whose latest
  // publication is a series reopened as the whole publication: every
  // estimated year of the vintage, joined to one polygon, coloured by
  // whichever row arrived last. The claim is read back from the issued
  // request rather than asserted, so a source that does not declare
  // `newest_per_geography` cannot be saved as though it had been reduced.
  const observationRequests = [];
  await installRoutes(page, { neutralRequests: observationRequests });

  const created = [];
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    created.push(body);
    return route.fulfill({
      json: {
        configuration_id: 11,
        name: body.name,
        version: 1,
        document: body.document,
        validation: { valid: true, reasons: [] },
      },
    });
  });
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });

  // The link names the source that publishes the measure, as every link the
  // app builds now does. Without it this navigation landed on the mounted
  // Census ACS catalog, which does not list the PEP code, and the page
  // substituted an ACS metric -- so this spec graded the saved document of a
  // view the reader never asked for (WEB-072).
  await page.goto(
    "/explore?source=CENSUS_PEP&metric=CENSUS_PEP%3Apep_cty_alldata%3APOPESTIMATE",
  );
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute(
    "data-selected-metric",
    "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
  );
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await page.getByTestId("save-view").click();
  await expect(page.getByTestId("save-toast")).toHaveAttribute("data-destination", "account");

  expect(created).toHaveLength(1);
  const document = created[0].document;
  // The map's own cross-geography request, which is what the document claims
  // to reproduce.
  const mapRequests = observationRequests.filter((entry) => entry.geo_level && !entry.geo_id);
  expect(mapRequests.length).toBeGreaterThan(0);
  const askedForTheReduction = mapRequests.every(
    (entry) => entry.newest_per_geography === "true",
  );
  expect(document.newest_per_geography).toBe(askedForTheReduction);
  expect(document.newest_per_geography).toBe(true);
  // Each reduction belongs to one scope; the document stores a pairing the
  // live route would serve, never one it refuses.
  expect(document.scope).toBe("latest");
  expect(document.newest_release_per_period).toBe(false);
  expect(document.release).toBeNull();
});

test("a published participation basis is shown beside the value it qualifies", async ({
  page,
}) => {
  // Covers: WEB-051 — FBI UCR is served through the neutral envelope because
  // its agency-level facts cannot be read without their participation basis.
  // The client mapped `uncertainty` onto the row and left `coverage` behind,
  // so an agency covering 61.9% of its population showed an offence count
  // that read as the whole jurisdiction.
  await installRoutes(page);
  await page.goto(
    "/explore?source=FBI_UCR&metric=FBI_UCR%3Asummarized%3AVIOLENT_CRIME",
  );

  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  const coverage = page.getByTestId("coverage-agency:WI0130000");
  await expect(coverage).toContainText("partial");
  await expect(coverage).toContainText("61.9% covered");
});

test("a source that publishes no participation grows no column for it", async ({ page }) => {
  // Covers: WEB-051 — read from the loaded rows, not from a list of sources.
  await installRoutes(page);
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  // As above: the zero means nothing until the table panel is on screen.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("columnheader", { name: "Value" })).toHaveCount(1);
  await expect(page.getByRole("columnheader", { name: "Participation" })).toHaveCount(0);
});

test("a published uncertainty is shown beside the value it qualifies", async ({ page }) => {
  // Covers: WEB-053 — the neutral envelope's other qualifier object. The
  // client lifted `margin_of_error` out of `uncertainty` and left the rest
  // inside it, so CDC's published confidence bounds reached neither the table
  // nor the export and a prevalence estimate read as a point estimate.
  await installRoutes(page);
  await page.goto("/explore?source=CDC&metric=CDC%3Acdc_places_county%3AOBESITY");

  const uncertainty = page.getByTestId("uncertainty-state:55|county:025").first();
  await expect(uncertainty).toContainText("confidence lower 30.9");
  await expect(uncertainty).toContainText("confidence upper 33.9");
});

test("every dimension the source declares is shown, not just the filterable ones", async ({
  page,
}) => {
  // Covers: WEB-061 — the table's dimension columns came from the source's
  // *filterable* names, so four of seven sources showed no dimension at all
  // and CDC showed two of fourteen. `footnote_text` is how CDC qualifies an
  // estimate, and it reached neither the table nor the export.
  await installRoutes(page);
  await page.goto("/explore?source=CDC&metric=CDC%3Acdc_places_county%3AOBESITY");
  // The table panel is `display: none` until its tab is selected, so a
  // column assertion against the default view sees nothing at all.
  await page.getByRole("tab", { name: "table" }).click();

  // A control assertion first: a column that certainly exists. Without it a
  // `toHaveCount(0)` below would pass by seeing nothing rather than by the
  // column being absent.
  await expect(page.getByRole("columnheader", { name: "Value" })).toHaveCount(1);
  // The filterable ones keep their own columns: they are what the reader is
  // filtering on.
  await expect(page.getByRole("columnheader", { name: "stratum id" })).toHaveCount(1);
  // The rest of the declared set rides in one cell, as the seven uncertainty
  // fields do rather than seven columns.
  const dimensions = page.getByTestId("dimensions-state:55|county:025").first();
  await expect(dimensions).toContainText("footnote text Estimates are model-based");
  await expect(dimensions).toContainText("estimate method model-based");
  await expect(dimensions).toContainText("population basis adults");
});

test("a source declaring no dimensions grows no cell for them", async ({ page }) => {
  // Covers: WEB-061 — read from the declaration, so a source that declares
  // none grows nothing, exactly as an absent uncertainty grows no column.
  await installRoutes(page);
  await page.goto(
    "/explore?source=FBI_UCR&metric=FBI_UCR%3Asummarized%3AVIOLENT_CRIME",
  );

  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("columnheader", { name: "Value" })).toHaveCount(1);
  await expect(page.getByRole("columnheader", { name: "Dimensions" })).toHaveCount(0);
});

test("a source that publishes no uncertainty grows no column for it", async ({ page }) => {
  // Covers: WEB-053 — read from the loaded rows, not from a list of sources.
  await installRoutes(page);
  await page.goto(
    "/explore?source=FBI_UCR&metric=FBI_UCR%3Asummarized%3AVIOLENT_CRIME",
  );

  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  // The table panel is `display: none` until its tab is selected: this
  // assertion used to run against the default view and pass by seeing no
  // columns at all, so it could not have failed (WEB-043's lesson, applied
  // to an assertion rather than a fixture). The control assertion is what
  // makes the zero mean something.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("columnheader", { name: "Value" })).toHaveCount(1);
  await expect(page.getByRole("columnheader", { name: "Uncertainty" })).toHaveCount(0);
});

test("the legend says which period the API's bins describe", async ({ page }) => {
  // Covers: WEB-054 — the bins build the colour scale the choropleth is
  // painted with, and `/distribution/bins` reduces each geography to its own
  // newest period, so the answer's own statement of which period that is
  // travels with the count.
  await installRoutes(page);
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  await expect(page.getByTestId("distribution-status")).toContainText("for 2023-01-01");
});

test("a legend built from mixed periods says so", async ({ page }) => {
  // Covers: WEB-054 — a scale over a mix of periods is a legitimate map of
  // each geography's newest value and a misleading one to read as a
  // snapshot. The client cannot work it out: the bins are computed over every
  // geography the metric publishes while it holds one page of rows.
  await installRoutes(page);
  await page.route("**/api/v1/distribution/bins?*", (route) =>
    route.fulfill({
      json: {
        total: 2,
        bin_count: 1,
        min_value: 1,
        max_value: 561504,
        period: null,
        periods_differ: true,
        items: [{ bin_index: 1, lower_bound: 1, upper_bound: 561504, count: 2 }],
      },
    }),
  );
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  const status = page.getByTestId("distribution-status");
  await expect(status).toContainText("bins mix periods");
  // The pill's caution treatment, which is what "not proven good" looks like
  // in this UI: a stale or partial value can never present as current.
  await expect(status).toHaveClass(/warn/);
});

test("the map says what its bins could not carry", async ({ page }) => {
  // Covers: WEB-055 — the bins draw boundaries to the value, and each ACS
  // value carries a margin of error that can straddle them. The API names
  // what it dropped; this is the surface that shows it beside the map those
  // bins paint.
  await installRoutes(page);
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  await expect(page.getByTestId("distribution-caveats")).toContainText(
    "margin_of_error",
  );
});

test("a distribution with nothing to caveat shows no note", async ({ page }) => {
  // Covers: WEB-055 — published strings rendered as published, so an answer
  // carrying none grows no empty paragraph.
  await installRoutes(page);
  await page.route("**/api/v1/distribution/bins?*", (route) =>
    route.fulfill({
      json: {
        total: 1,
        bin_count: 1,
        min_value: 1,
        max_value: 1,
        period: "2023-01-01",
        periods_differ: false,
        caveats: [],
        items: [{ bin_index: 1, lower_bound: 1, upper_bound: 1, count: 1 }],
      },
    }),
  );
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");

  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  await expect(page.getByTestId("distribution-caveats")).toHaveCount(0);
});


test("USDA NASS starts on the final value and a reader's 'all' survives the link", async ({ page }) => {
  // Covers: WEB-119 — the explorer applies the source's declared default
  // (API-157) and records an explicit "all" so reopening does not reapply it.
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto("/explore?source=usda-nass&geo_level=STATE");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", nassMetric.metric_code);
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  const nassReads = () =>
    neutralRequests.filter(
      (request) => request.metric_code === nassMetric.metric_code && !request.geo_id,
    );
  expect(nassReads().at(-1).reference_period_desc).toBe("YEAR");
  await expect(page.getByTestId("dimension-select-reference_period_desc")).toHaveValue("YEAR");
  // The default reproduces itself, so it is not written into the link.
  await expect(page).not.toHaveURL(/reference_period_desc/);

  // Every reference period: the final value and its forecast compete for the
  // state, so the map declines and says which filter narrows it.
  await page.getByTestId("dimension-select-reference_period_desc").selectOption("");
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");
  await expect(dashboard).toHaveAttribute("data-stratified", "true");
  expect(nassReads().at(-1).reference_period_desc).toBeUndefined();
  await expect(page).toHaveURL(/reference_period_desc=\*/);

  // Reopening that link keeps the reader's choice rather than the default.
  await page.goto(page.url());
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");
  expect(nassReads().at(-1).reference_period_desc).toBeUndefined();
  await expect(page.getByTestId("dimension-select-reference_period_desc")).toHaveValue("");
});
