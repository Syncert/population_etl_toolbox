// Deterministic place-page fixtures (place-pages). Values are synthetic UI
// fixtures, not provider-published statistics; every metric identity is a
// candidate the chapter contract names, verified against a deployed catalog.
import { placeChapterMetricCodes } from "../../../apps/web/lib/placeChapters.ts";
import { servedParameters } from "./servedContract.js";

const sources = ["BEA", "CENSUS_ACS", "CENSUS_PEP", "BLS", "BLS_QCEW", "CDC", "FBI_UCR", "USDA_NASS"];

export const NATION = { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "us:1" };
export const STATES = [
  { geo_id: "state:55", geo_level: "STATE", geo_name: "Wisconsin", state_fips: "55", state_name: "Wisconsin" },
  { geo_id: "state:27", geo_level: "STATE", geo_name: "Minnesota", state_fips: "27", state_name: "Minnesota" },
];
export const DANE_TRACTS = ["000100", "000201", "000202"].map((tract, index) => ({
  geo_id: `state:55|county:025|tract:${tract}`, geo_level: "TRACT", state_fips: "55", county_fips: "025",
  county_name: "Dane County", area_name: ["Census Tract 1", "Census Tract 2.01", "Census Tract 2.02"][index],
  geo_name: ["Census Tract 1", "Census Tract 2.01", "Census Tract 2.02"][index],
}));

export const COUNTIES = [
  { geo_id: "state:55|county:025", geo_level: "COUNTY", geo_name: "Dane County", state_fips: "55", county_fips: "025", county_name: "Dane County", state_name: "Wisconsin" },
  { geo_id: "state:55|county:105", geo_level: "COUNTY", geo_name: "Rock County", state_fips: "55", county_fips: "105", county_name: "Rock County", state_name: "Wisconsin" },
  { geo_id: "state:27|county:053", geo_level: "COUNTY", geo_name: "Hennepin County", state_fips: "27", county_fips: "053", county_name: "Hennepin County", state_name: "Minnesota" },
];

// Cities and towns (acs-place-grain). "Crossing city" spans Dane and Rock
// counties; the ACS and PEP publish places, nothing else here does.
export const PLACES = [
  { geo_id: "state:55|place:99999", geo_level: "PLACE", geo_name: "Crossing city", state_fips: "55", place_fips: "99999", place_name: "Crossing city", state_name: "Wisconsin" },
  { geo_id: "state:55|place:48000", geo_level: "PLACE", geo_name: "Madison city", state_fips: "55", place_fips: "48000", place_name: "Madison city", state_name: "Wisconsin" },
];

function grainsFor(code) {
  if (code.startsWith("BLS:")) return ["COUNTY", "STATE"];
  if (code.startsWith("CDC:")) return ["COUNTY", "NATIONAL"];
  if (code.startsWith("FBI_UCR:")) return ["AGENCY", "STATE", "NATIONAL"];
  if (code.startsWith("CENSUS_ACS:") || code.startsWith("CENSUS_PEP:POPESTIMATE")) return ["PLACE", "COUNTY", "STATE", "NATIONAL"];
  return ["COUNTY", "STATE", "NATIONAL"];
}

function unitFor(code) {
  if (code.startsWith("CENSUS_PEP:R")) return "per_1000_population";
  if (code.startsWith("CENSUS_PEP:")) return "persons";
  if (code === "BEA:CAINC1:3") return "Dollars";
  if (code === "BEA:CAGDP1:1") return "Thousands of chained 2017 dollars";
  if (code.startsWith("BEA:")) return "Thousands of dollars";
  if (code.startsWith("BLS_QCEW:avg_weekly_wage")) return "dollars per week";
  if (code.startsWith("BLS_QCEW:")) return "jobs";
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

const scale = { "us:1": 600, "state:55": 10, "state:27": 9.5, "state:55|county:025": 1, "state:55|county:105": 0.3, "state:27|county:053": 2.2, "state:55|place:99999": 0.05, "state:55|place:48000": 0.5 };
const rates = /B01002|B19013|B19301|B19083|B25064|B25077|BLS:|CDC:|FBI_UCR:|PEP:R/;

// Relationships as the reference would record them. "Crossing city" spans
// Dane and Rock counties: 60% of its area in Dane, 40% in Rock.
const CROSSING = PLACES[0];
const link = (relationship, place, extra = {}) => ({ relationship, geo_id: place.geo_id, geo_level: place.geo_level, geo_name: place.place_name || place.county_name || place.state_name || place.geo_name, state_fips: place.state_fips || null,
  geography_vintage: 2025, evidence_source: relationship === "adjacent" ? "census_boundary_adjacency" : relationship === "intersects" ? "census_boundary_intersection" : "exact_census_code_hierarchy",
  overlap_area_m2: null, overlap_weight: null, ...extra });
export const RELATED = {
  "us:1": [], // the nation's states are listed by the page itself
  "state:55": [link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" })],
  "state:27": [link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" })],
  "state:55|county:025": [
    link("adjacent", COUNTIES[1]), link("adjacent", COUNTIES[2]),
    link("intersects", CROSSING, { overlap_weight: 0.6, overlap_area_m2: 6e6 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
  "state:55|county:105": [
    link("adjacent", COUNTIES[0]),
    link("intersects", CROSSING, { overlap_weight: 0.4, overlap_area_m2: 4e6 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
  "state:27|county:053": [],
  "state:55|place:99999": [
    link("intersects", COUNTIES[0], { overlap_weight: 0.6, overlap_area_m2: 6e6 }),
    link("intersects", COUNTIES[1], { overlap_weight: 0.4, overlap_area_m2: 4e6 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
  "state:55|place:48000": [
    link("intersects", COUNTIES[0], { overlap_weight: 1, overlap_area_m2: 2e8 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
};

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
    if (path.startsWith("/api/v1/catalog/geographies/") && path.endsWith("/related")) {
      const geoId = decodeURIComponent(path.slice("/api/v1/catalog/geographies/".length, -"/related".length));
      const place = [NATION, ...STATES, ...COUNTIES, ...PLACES].find((item) => item.geo_id === geoId);
      if (!place) return route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
      const items = RELATED[geoId] || [];
      return route.fulfill({ json: { geo_id: geoId, geo_level: place.geo_level, total: items.length, items } });
    }
    if (path === "/api/v1/place/distinctive") {
      const geoId = params.get("geo_id");
      if (![...STATES, ...COUNTIES].some((item) => item.geo_id === geoId)) return route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
      const rank = (metric_code, name, value, below, extra = {}) => ({ metric_code, metric_display_name: `${name} (UI fixture)`, source_code: metric_code.split(":")[0], units: null,
        period_start: "2020-01-01", value, siblings_with_value: 70, siblings_withheld: 0, siblings_missing: 0, siblings_below: below, siblings_tied: 0, percentile_rank: below / 70,
        caveats: [], request: `/api/v1/observations?metric_code=${metric_code}&scope=latest&geo_level=COUNTY&state_fips=55&newest_per_geography=true`, ...extra });
      const ranked = geoId === "state:55|county:025" ? [
        rank("CENSUS_ACS:acs5:B19013_001", "Median household income", 92000, 68, { siblings_withheld: 1, siblings_with_value: 70 }),
        rank("CENSUS_ACS:acs5:B25077_001", "Median home value", 385000, 66),
        rank("CENSUS_ACS:acs5:B19301_001", "Per capita income", 51000, 64),
        rank("CENSUS_ACS:acs5:B01002_001", "Median age", 35.7, 2),
        rank("BLS:LAU:UNEMP_RATE", "Unemployment rate", 2.4, 4),
        rank("CENSUS_PEP:RDEATH", "Death rate", 7.1, 5),
      ] : [];
      return route.fulfill({ json: { derived: true, geo_id: geoId, geo_level: geoId.includes("county") ? "COUNTY" : "STATE", parent_scope: "state:55", minimum_siblings: 10,
        method: "UI fixture", ranked, not_ranked: [{ metric_code: "CDC:places_county:OBESITY:AgeAdjPrv", reason: "not published at COUNTY (UI fixture)" }] } });
    }
    if (path === "/api/v1/catalog/geographies") {
      const grain = params.get("geo_level");
      if (grain === "TRACT") {
        const tracts = DANE_TRACTS.filter((tract) => params.get("county_fips") === "025" && params.get("state_fips") === "55");
        return route.fulfill({ json: { total: tracts.length, limit: 1000, offset: 0, items: tracts } });
      }
      const items = grain === "NATIONAL" ? [NATION] : grain === "STATE" ? STATES
        : grain === "COUNTY" ? COUNTIES.filter((county) => !params.get("state_fips") || county.state_fips === params.get("state_fips"))
        : grain === "PLACE" ? PLACES.filter((place) => !params.get("state_fips") || place.state_fips === params.get("state_fips")) : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/comparison/preflight") {
      const code = params.get("metric_code_a");
      const refused = code?.startsWith("FBI_UCR");
      return route.fulfill({ json: { metric_code_a: code, metric_code_b: params.get("metric_code_b"), comparable: !refused, derivations: [], caveats: [],
        rules: refused ? [{ rule: "source_analysis_ready", status: "fail", reason: "FBI UCR subjects are not canonical geographies (UI fixture)" }] : [{ rule: "units", status: "pass", reason: "same measure" }] } });
    }
    if (path === "/api/v1/catalog/sources") return route.fulfill({ json: sources.map((source_code) => ({ source_code, source_name: source_code === "BLS" ? "Bureau of Labor Statistics" : source_code, source_type: "PRIMARY", reference_url: `https://example.org/${source_code.toLowerCase()}` })) });
    // USDA NASS is deliberately absent from the rollup: the public data page
    // must call it "not reported", never fresh.
    if (path === "/api/v1/catalog/freshness") return route.fulfill({ json: { total: sources.length - 1, items: sources.filter((code) => code !== "USDA_NASS").map((source_code) => ({ source_code, metric_count: 12, current_count: 11, stale_count: 1, retired_count: 0,
      latest_publication_time: "2026-09-01T00:00:00Z", latest_harvested_at: `2026-09-0${sources.indexOf(source_code) + 2}T00:00:00Z`, geo_grains: grainsFor(`${source_code}:x`).filter((grain) => grain !== "AGENCY") })) } });
    if (path === "/api/v1/catalog/metrics") return route.fulfill({ json: { total: 0, limit: 6, offset: 0, items: [] } });
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      return metrics[code] ? route.fulfill({ json: metrics[code] }) : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
    }
    if (path === "/api/v1/observations" && params.get("geo_level") === "TRACT") {
      // Two of Dane County's three fixture tracts publish a value; the third
      // publishes none and must be counted, not painted as zero.
      const code = params.get("metric_code");
      const items = params.get("county_fips") === "025"
        ? DANE_TRACTS.slice(0, 2).map((tract, index) => ({
          metric_code: code, source_code: "CENSUS_ACS", geo_id: tract.geo_id, geo_level: "TRACT",
          value: String(60000 + index * 5000), value_status: "valid", unit: "dollars",
          period_start: "2019-01-01", period_end: "2023-12-31", release: "2023", as_of: "2025-09-01",
          dimensions: {}, uncertainty: { margin_of_error: "4100" }, coverage: null,
        }))
        : [];
      return route.fulfill({ json: { metric_code: code, source_code: "CENSUS_ACS", scope: "latest", total: items.length, offset: 0, limit: 1000, items } });
    }
    if (path === "/api/v1/observations") {
      const code = params.get("metric_code");
      const metric = metrics[code];
      const geo = params.get("geo_id");
      const place = [NATION, ...STATES, ...COUNTIES, ...PLACES].find((item) => item.geo_id === geo);
      if (!metric || !place) return route.fulfill({ status: 404, json: { detail: "No reviewed fixture" } });
      // NASS publishes no cell for Dane County in this fixture: the Land and
      // Farms chapter must be omitted there and named in the footer.
      const empty = !metric.valid_geo_grains.includes(place.geo_level) || (code.startsWith("USDA_NASS") && geo === "state:55|county:025");
      const newest = params.get("limit") === "1" || params.get("newest_per_geography") === "true";
      const lastYear = nationLagsMedianAge && geo === "us:1" && code.endsWith("B01002_001") ? 2023 : 2024;
      // BEA withholds mining earnings for Dane County in this fixture, as it
      // does for a sector that would disclose one employer; QCEW withholds a
      // sector it cannot disclose, and Mining is withheld here too.
      const withheld = (code === "BEA:CAINC5N:200" || code === "BLS_QCEW:employment:21:5") && geo === "state:55|county:025";
      let items = empty ? [] : Array.from({ length: 6 }, (_, index) => {
        const year = lastYear - 5 + index;
        const base = rates.test(code) ? 40 + (scale[geo] || 1) : 1000 * (scale[geo] || 1);
        return { metric_code: code, source_code: metric.source_code, geo_id: geo, geo_level: place.geo_level, geo_name: place.geo_name,
          value: withheld ? null : String(Math.round(base * (1 + index * 0.02) * 100) / 100), value_status: withheld ? "withheld" : "valid", unit: metric.units,
          period_start: `${year}-01-01`, period_end: `${year}-12-31`, release: "fixture-release-2025", as_of: "2025-09-01",
          dimensions: code.startsWith("BEA:") ? { dollar_basis: code === "BEA:CAGDP1:1" ? "chained_dollars" : "current_dollars", value_source: withheld ? "(D)" : "" } : {},
          uncertainty: code.startsWith("CENSUS_ACS") ? { margin_of_error: "12" } : code.startsWith("CDC") ? { confidence_lower: "30.1", confidence_upper: "33.4" } : null,
          coverage: null };
      });
      if (newest) items = items.slice(-1);
      return route.fulfill({ json: { metric_code: code, source_code: metric.source_code, scope: "latest", total: items.length, offset: 0, limit: Number(params.get("limit") || 500), items } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}
