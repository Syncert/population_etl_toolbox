// Deterministic UI examples, not provider-published statistics. Every metric
// identity comes from the existing verified template candidates. Values are
// intentionally synthetic, and every delivered screenshot states that fact.
import { useCasePages } from "../../../apps/web/lib/useCasePages.ts";
import { servedParameters } from "./servedContract.js";

export const scenarios = [
  ["community-conditions", "Dane County: read county PLACES health, explicit Wisconsin safety context, and a 1% growth scenario", "COUNTY", "CENSUS_ACS:acs5:B01003_001"],
  ["population-growth", "Dane County: review published population and a 10-year scenario at 1% assumed annual growth", "COUNTY", "CENSUS_PEP:POPESTIMATE"],
  ["workforce", "Dane County: inspect labor-market depth for an employer briefing", "COUNTY", "BLS:LAU:UNEMP_RATE"],
  ["housing-affordability", "Dane County: inspect rent pressure for a housing needs discussion", "COUNTY", "CENSUS_ACS:acs5:B25064_001"],
  ["cost-of-living", "United States: read price movement alongside the limits of local interpretation", "NATIONAL", "FRED:CPIAUCSL"],
  ["disease-illness-burden", "Wisconsin: inspect a published chronic-condition indicator and its interval", "STATE", "CDC:cdi:ALC06:AGEADJPREV"],
  ["disease-capacity-watch", "Wisconsin: monitor the illustrative health series and reporting qualifications", "STATE", "CDC:cdi:ALC06:AGEADJPREV"],
  ["public-safety-trend", "Wisconsin: inspect the program-published violent-crime rate and participation", "STATE", "FBI_UCR:summarized_violent_crime:V:offense:rate"],
  ["crime-economic-context", "Wisconsin: read reported crime beside economic context without causal claims", "STATE", "FBI_UCR:summarized_violent_crime:V:offense:absolute_total"],
  ["rural-agricultural-economy", "Dane County: inspect agricultural production and rural household context", "COUNTY", "USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11"],
  ["agricultural-production-prices", "Dane County: inspect crop history before adding separate national price context", "COUNTY", "USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11"],
  ["agricultural-workforce", "Dane County: read agricultural labor-force context without equating industries", "COUNTY", "CENSUS_ACS:acs5:B23025_002"],
  ["aging-population", "Dane County: review residents aged 65+ for an aging-services discussion", "COUNTY", "CENSUS_ACS:acs5:B09020_001"],
  ["economic-shock-recovery", "Dane County: inspect unemployment during an illustrative disruption and recovery", "COUNTY", "BLS:LAU:UNEMP_RATE"],
  ["grant-needs-assessment", "Dane County: assemble income evidence for a grant needs narrative", "COUNTY", "CENSUS_ACS:acs5:B19013_001"],
  ["business-location", "Dane County: inspect the population base for a candidate-location briefing", "COUNTY", "CENSUS_PEP:POPESTIMATE"],
  ["peer-benchmarking", "Wisconsin and Minnesota: compare population with explicit neighboring-state peer criteria", "STATE", "CENSUS_ACS:acs5:B01003_001"],
  ["data-journalism", "Dane County: prepare a reproducible population chart for a local story", "COUNTY", "CENSUS_ACS:acs5:B01003_001"],
  ["program-evidence-library", "Dane County: save an income indicator for a recurring public-program plan", "COUNTY", "CENSUS_ACS:acs5:B19013_001"],
  ["source-data-quality", "All sources: review freshness and select BLS publication evidence", "COUNTY", null],
].map(([id, scenario, grain, metric], index) => ({ id, scenario, grain, metric, rank: index + 1,
  geoId: grain === "STATE" ? "state:55" : grain === "NATIONAL" ? "us" : "state:55|county:025",
}));

const sources = ["CENSUS_ACS", "CENSUS_PEP", "BLS", "FRED", "CDC", "FBI_UCR", "USDA_NASS"];
const slots = useCasePages.flatMap((entry) => entry.sections.flatMap((section) => section.measures));
const metrics = Object.fromEntries(slots.flatMap((slot) => slot.candidates.map((code) => {
  const source = code.split(":")[0];
  return [code, { metric_code: code, metric_display_name: `${slot.label} (UI fixture)`, source_code: source,
    units: unitFor(code), freshness_state: source === "BLS" ? "stale" : "fresh",
    valid_geo_grains: source === "FRED" ? ["NATIONAL"] : code.includes("places_county") ? ["COUNTY", "NATIONAL"] : source === "CDC" || source === "FBI_UCR" ? ["STATE", "NATIONAL"] : ["COUNTY", "STATE", "NATIONAL"],
    publication_time: "2026-09-01T00:00:00Z", harvested_at: "2026-09-02T00:00:00Z", source_watermark: "fixture-2026-09", publisher_contract_version: "v1",
  }];
})));

function unitFor(code) {
  if (code.includes("CPI")) return "index";
  if (code.startsWith("FBI_UCR:")) return code.endsWith(":rate") ? "offenses per 100,000 residents" : "offenses";
  if (code.includes("RATE") || code.startsWith("CDC:") || code.includes("MORTGAGE") || code.includes("CIVPART")) return "percent";
  if (/B25064|B19013|B25077|B25105|B19301|MSPUS/.test(code)) return "dollars";
  if (code.includes("B01002")) return "years";
  if (/B25001|B25002|B25003|B25034|B25091/.test(code)) return "housing units";
  if (/B11001|B25070|B09020_002/.test(code)) return "households";
  if (code.startsWith("USDA_NASS")) return "bushels";
  return "people";
}

// No catch-all population value: every tested identity owns a distinct,
// explicitly named example. Equal table universes are allowed intentionally.
const acsBases = {
  B01003_001: 550000, B01001_001: 550000, B01002_001: 38.4,
  B09020_001: 79000, B09020_002: 61000, B11001_001: 235000,
  B15003_001: 370000, B17001_001: 530000, B19013_001: 78000,
  B19301_001: 43500, B23025_002: 325000, B23025_005: 13700,
  B25001_001: 250000, B25002_001: 250000, B25003_001: 235000,
  B25034_001: 250000, B25064_001: 1300, B25070_001: 87000,
  B25077_001: 335000, B25091_001: 148000, B25105_001: 1650,
  B27010_001: 538000, C18108_001: 538000, C24050_001: 311000,
};
function exampleBase(code) {
  if (code.startsWith("CENSUS_ACS:")) return acsBases[code.split(":").at(-1)];
  if (code === "CENSUS_PEP:POPESTIMATE") return 560000;
  if (code === "BLS:LAU:UNEMP_RATE") return 4.2;
  if (code === "FRED:CPIAUCSL") return 290;
  if (code === "FRED:CIVPART") return 62.5;
  if (code === "FRED:MORTGAGE30US") return 6.4;
  if (code === "FRED:MSPUS") return 415000;
  if (code.includes("places_county")) return 26.3;
  if (code.startsWith("CDC:cdi:")) return 4.2;
  if (code.startsWith("FBI_UCR:")) return code.includes("clearance") ? 5100 : code.endsWith(":rate") ? 290 : 17100;
  if (code.startsWith("USDA_NASS:")) return 4200000;
  return undefined;
}

export async function installUseCaseFixtures(page, { failHistory = false, stratified = false, withheld = false } = {}) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: sources.length, items: sources.map((source_code) => ({ source_code,
      display_name: source_code, route_segment: ({ CENSUS_ACS: "census", CENSUS_PEP: "pep", FRED: "fred", BLS: "bls", CDC: "cdc", USDA_NASS: "usda-nass" })[source_code] || null,
      served_by_neutral_routes: true, publishes_value_status: !["FRED", "CENSUS_PEP"].includes(source_code), publishes_aligned_reduction: !["CDC", "FBI_UCR", "USDA_NASS"].includes(source_code),
      observation_filters: ["geo_id", "geo_level", "state_fips"], observation_dimensions: source_code === "CDC" ? ["stratum_id", "adjustment_status", "footnote_text"] : source_code === "USDA_NASS" ? ["domain_desc", "short_desc"] : [],
      observation_routes: [{ path: "/api/v1/observations", parameters: servedParameters("/api/v1/observations") }, { path: "/api/v1/observations/releases", parameters: servedParameters("/api/v1/observations/releases") }],
    })) } });
    if (path === "/api/v1/catalog/freshness") return route.fulfill({ json: { total: 7, items: sources.map((source_code) => ({ source_code, metric_count: 12,
      current_count: source_code === "BLS" ? 9 : 12, stale_count: source_code === "BLS" ? 3 : 0, retired_count: 0,
      latest_publication_time: "2026-09-01T00:00:00Z", latest_harvested_at: "2026-09-02T00:00:00Z",
    })) } });
    if (path === "/api/v1/catalog/geographies") {
      const grain = params.get("geo_level");
      const items = grain === "STATE" ? [{ geo_id: "state:55", geo_level: "STATE", state_name: "Wisconsin", state_fips: "55" }, { geo_id: "state:27", geo_level: "STATE", state_name: "Minnesota", state_fips: "27" }]
        : grain === "NATIONAL" ? [{ geo_id: "us", geo_level: "NATIONAL", geo_name: "United States" }]
        : grain === "COUNTY" ? [{ geo_id: "state:55|county:025", geo_level: "COUNTY", state_fips: "55", county_fips: "025", county_name: "Dane County", state_name: "Wisconsin" }, { geo_id: "state:55|county:105", geo_level: "COUNTY", state_fips: "55", county_fips: "105", county_name: "Rock County", state_name: "Wisconsin" }]
        : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      return metrics[code] ? route.fulfill({ json: metrics[code] }) : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
    }
    if (path === "/api/v1/catalog/metrics") {
      const items = Object.values(metrics).filter((metric) => !params.get("source_code") || metric.source_code === params.get("source_code"));
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/population/scenario") {
      const metric_code = params.get("metric_code");
      const annual_change_percent = Number(params.get("annual_change_percent"));
      const horizon_years = Number(params.get("horizon_years"));
      const value = Math.round(exampleBase(metric_code) * 1.12 * 100) / 100;
      return route.fulfill({ json: { derived: true, model: "compound-growth-v1", annual_change_percent, horizon_years,
        base: { metric_code, source_code: metric_code.split(":")[0], geo_id: params.get("geo_id"), period_start: "2024-01-01", period_end: "2024-12-31", value: String(value), unit: "people", release: "fixture-release-2025", uncertainty: null },
        formula: "base_population * (1 + annual_change_percent / 100) ** years_after_base",
        caveats: ["Illustrative fixture; assumption-based planning scenario, not an official forecast.", "No prediction interval is calculated."],
        items: Array.from({ length: horizon_years }, (_, index) => ({ year: 2025 + index, value: Math.round(value * (1 + annual_change_percent / 100) ** (index + 1) * 100) / 100 })),
      } });
    }
    if (path === "/api/v1/observations") {
      const code = params.get("metric_code");
      const metric = metrics[code];
      const history = params.get("limit") !== "1" && params.get("newest_per_geography") !== "true";
      if (failHistory && history) return route.fulfill({ status: 503, json: { detail: "example source unavailable" } });
      const geo = params.get("geo_id");
      const geo_level = geo === "us" ? "NATIONAL" : geo?.includes("county:") ? "COUNTY" : "STATE";
      const example = exampleBase(code);
      if (example === undefined) return route.fulfill({ status: 404, json: { detail: "No reviewed fixture for this metric" } });
      if (!metric.valid_geo_grains.includes(geo_level)) return route.fulfill({ json: { total: 0, items: [] } });
      const base = example * (geo === "state:27" ? .92 : geo?.endsWith("105") ? .27 : 1);
      let items = Array.from({ length: 6 }, (_, index) => ({ metric_code: code, source_code: metric?.source_code,
        geo_id: geo, geo_level, geo_name: geo === "us" ? "United States" : geo === "state:27" ? "Minnesota" : geo_level === "STATE" ? "Wisconsin" : geo?.endsWith("105") ? "Rock County" : "Dane County",
        value: withheld && index === 2 ? null : String(Math.round((base * (1 + index * .024) + (index === 2 && code.startsWith("BLS") ? base * .7 : 0)) * 100) / 100),
        value_status: withheld && index === 2 ? "suppressed" : "valid", unit: metric?.units,
        period_start: `${2019 + index}-01-01`, period_end: `${2019 + index}-12-31`, release: "fixture-release-2025", as_of: "2025-09-01",
        dimensions: code.startsWith("CDC") ? { stratum_id: "all", adjustment_status: "age_adjusted", footnote_text: "Illustrative fixture; provisional reporting context" } : code.startsWith("FBI_UCR") ? { subject_type: "state", subject_code: "55", program_code: "summarized_violent_crime" } : code.startsWith("USDA_NASS") ? { domain_desc: "TOTAL", short_desc: "Illustrative crop measure" } : {},
        uncertainty: code.startsWith("CDC") ? { confidence_lower: "3.5", confidence_upper: "5.4" } : code.startsWith("CENSUS_ACS") ? { margin_of_error: "1200" } : null,
        coverage: code.startsWith("FBI_UCR") ? { coverage_percent: "87", participation_status: "partial", population_denominator: "5900000", coverage_basis: "UI fixture reporting basis" } : null,
      }));
      if (!history) items = items.slice(-1);
      if (stratified && code.startsWith("CDC")) items = items.flatMap((row) => [row, { ...row, dimensions: { ...row.dimensions, stratum_id: "female" } }]);
      return route.fulfill({ json: { metric_code: code, source_code: metric?.source_code, scope: params.get("scope") || "latest", total: items.length, offset: 0, limit: Number(params.get("limit") || 500), items } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}
