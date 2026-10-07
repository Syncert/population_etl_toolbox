// Deterministic place-page fixtures (place-pages). Values are synthetic UI
// fixtures, not provider-published statistics; every metric identity is a
// candidate the chapter contract names, verified against a deployed catalog.
import { placeChapterMetricCodes } from "../../../apps/web/lib/placeChapters.ts";
import { servedParameters } from "./servedContract.js";

const sources = ["CENSUS_ACS", "CENSUS_PEP", "BLS", "CDC", "FBI_UCR", "USDA_NASS"];

export const NATION = { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "us:1" };
export const STATES = [
  { geo_id: "state:55", geo_level: "STATE", geo_name: "Wisconsin", state_fips: "55", state_name: "Wisconsin" },
  { geo_id: "state:27", geo_level: "STATE", geo_name: "Minnesota", state_fips: "27", state_name: "Minnesota" },
];
export const COUNTIES = [
  { geo_id: "state:55|county:025", geo_level: "COUNTY", geo_name: "Dane County", state_fips: "55", county_fips: "025", county_name: "Dane County", state_name: "Wisconsin" },
  { geo_id: "state:55|county:105", geo_level: "COUNTY", geo_name: "Rock County", state_fips: "55", county_fips: "105", county_name: "Rock County", state_name: "Wisconsin" },
  { geo_id: "state:27|county:053", geo_level: "COUNTY", geo_name: "Hennepin County", state_fips: "27", county_fips: "053", county_name: "Hennepin County", state_name: "Minnesota" },
];

function grainsFor(code) {
  if (code.startsWith("BLS:")) return ["COUNTY", "STATE"];
  if (code.startsWith("CDC:")) return ["COUNTY", "NATIONAL"];
  if (code.startsWith("FBI_UCR:")) return ["AGENCY", "STATE", "NATIONAL"];
  return ["COUNTY", "STATE", "NATIONAL"];
}

function unitFor(code) {
  if (code.startsWith("CENSUS_PEP:R")) return "per_1000_population";
  if (code.startsWith("CENSUS_PEP:")) return "persons";
  if (code.startsWith("BLS:")) return "Percent";
  if (code.startsWith("CDC:")) return "%";
  if (code.startsWith("FBI_UCR:")) return "per_100000_population";
  if (code.startsWith("USDA_NASS:")) return "ACRES";
  if (/B19013|B19301|B25064|B25077/.test(code)) return "dollars";
  return null;
}

// Only the acs5 identities are published, so each slot's first candidate wins.
const codes = placeChapterMetricCodes().filter((code) => !code.includes(":acs1:"));
const metrics = Object.fromEntries(codes.map((code) => [code, {
  metric_code: code, metric_display_name: `${code.split(":").at(-1)} (UI fixture)`, source_code: code.split(":")[0],
  units: unitFor(code), valid_geo_grains: grainsFor(code), freshness_state: "fresh",
  publication_time: "2026-09-01T00:00:00Z", harvested_at: "2026-09-02T00:00:00Z", source_watermark: "fixture-2026-09", publisher_contract_version: "v1",
}]));

const scale = { "us:1": 600, "state:55": 10, "state:27": 9.5, "state:55|county:025": 1, "state:55|county:105": 0.3, "state:27|county:053": 2.2 };
const rates = /B01002|B19013|B19301|B19083|B25064|B25077|BLS:|CDC:|FBI_UCR:|PEP:R/;

export async function installPlaceFixtures(page, { nationLagsMedianAge = true } = {}) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: sources.length, items: sources.map((source_code) => ({ source_code,
      display_name: source_code, route_segment: null, served_by_neutral_routes: true,
      publishes_value_status: source_code !== "CENSUS_PEP", publishes_aligned_reduction: !["CDC", "FBI_UCR", "USDA_NASS"].includes(source_code),
      observation_filters: ["geo_id", "geo_level", "state_fips"], observation_dimensions: [],
      observation_routes: [{ path: "/api/v1/observations", parameters: servedParameters("/api/v1/observations") }],
    })) } });
    if (path === "/api/v1/catalog/geographies") {
      const grain = params.get("geo_level");
      const items = grain === "NATIONAL" ? [NATION] : grain === "STATE" ? STATES
        : grain === "COUNTY" ? COUNTIES.filter((county) => !params.get("state_fips") || county.state_fips === params.get("state_fips")) : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      return metrics[code] ? route.fulfill({ json: metrics[code] }) : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
    }
    if (path === "/api/v1/observations") {
      const code = params.get("metric_code");
      const metric = metrics[code];
      const geo = params.get("geo_id");
      const place = [NATION, ...STATES, ...COUNTIES].find((item) => item.geo_id === geo);
      if (!metric || !place) return route.fulfill({ status: 404, json: { detail: "No reviewed fixture" } });
      // NASS publishes no cell for Dane County in this fixture: the Land and
      // Farms chapter must be omitted there and named in the footer.
      const empty = !metric.valid_geo_grains.includes(place.geo_level) || (code.startsWith("USDA_NASS") && geo === "state:55|county:025");
      const newest = params.get("limit") === "1" || params.get("newest_per_geography") === "true";
      const lastYear = nationLagsMedianAge && geo === "us:1" && code.endsWith("B01002_001") ? 2023 : 2024;
      let items = empty ? [] : Array.from({ length: 6 }, (_, index) => {
        const year = lastYear - 5 + index;
        const base = rates.test(code) ? 40 + (scale[geo] || 1) : 1000 * (scale[geo] || 1);
        return { metric_code: code, source_code: metric.source_code, geo_id: geo, geo_level: place.geo_level, geo_name: place.geo_name,
          value: String(Math.round(base * (1 + index * 0.02) * 100) / 100), value_status: "valid", unit: metric.units,
          period_start: `${year}-01-01`, period_end: `${year}-12-31`, release: "fixture-release-2025", as_of: "2025-09-01", dimensions: {},
          uncertainty: code.startsWith("CENSUS_ACS") ? { margin_of_error: "12" } : code.startsWith("CDC") ? { confidence_lower: "30.1", confidence_upper: "33.4" } : null,
          coverage: null };
      });
      if (newest) items = items.slice(-1);
      return route.fulfill({ json: { metric_code: code, source_code: metric.source_code, scope: "latest", total: items.length, offset: 0, limit: Number(params.get("limit") || 500), items } });
    }
    if (path === "/api/v1/migration-flows") {
      // IRS SOI flows exist for Dane County only; every other county is the
      // API's 404, which the page answers by showing nothing.
      if (params.get("geo_id") !== "state:55|county:025") return route.fulfill({ status: 404, json: { detail: "No published SOI file covers this county." } });
      const inflow = params.get("direction") === "inflow";
      const counties = inflow
        ? [["state:27|county:053", "Hennepin County", 412], ["state:55|county:105", "Rock County", 388], ["state:17|county:031", "Cook County", 301]]
        : [["state:55|county:105", "Rock County", 455], ["state:27|county:053", "Hennepin County", 290]];
      return route.fulfill({ json: {
        source_code: "IRS_MIGRATION", derived: false, geo_id: params.get("geo_id"), direction: params.get("direction"),
        year_pair: "2022-2023", period_start: "2022-01-01", period_end: "2023-12-31", measure: "returns", unit: "returns",
        release: "2026-10-06T00:00:00Z", caveats: [],
        totals: [{ category: "total_us_and_foreign", category_label: "Total migration, US and foreign", returns: inflow ? 9210 : 8740, value_status: "valid", value_source: "" }],
        categories: [
          { category: "other_flows_same_state", category_label: "Other flows, same state", returns: 1200, value_status: "valid", value_source: "" },
          { category: "foreign_other_flows", category_label: "Foreign, other flows", returns: null, value_status: "withheld", value_source: "-1,-1,-1" },
        ],
        total: counties.length, limit: Number(params.get("limit") || 25),
        items: counties.map(([geo, name, returns]) => ({ category: "county", category_label: "County-to-county flow", counterpart_geo_id: geo, counterpart_name: name, returns, value_status: "valid", value_source: "" })),
      } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}
