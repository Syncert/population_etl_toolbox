// Deterministic place-page fixtures (place-pages). Values are synthetic UI
// fixtures, not provider-published statistics; every metric identity is a
// candidate the chapter contract names, verified against a deployed catalog.
import { placeChapterMetricCodes } from "../../../apps/web/lib/placeChapters.ts";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { servedParameters } from "./servedContract.js";

// The plain labels the API serves beside each provider label (A-2), read from
// the same registry the API reads, so the fixture answers as the API does.
// Both runners start in apps/web or at the repository root.
const DEFINITIONS_PATH = ["../../docs/semantics/metric_definitions.json", "docs/semantics/metric_definitions.json"]
  .map((path) => resolve(process.cwd(), path))
  .find((path) => existsSync(path));
const DEFINED = new Map(
  JSON.parse(readFileSync(DEFINITIONS_PATH, "utf8"))
    .entries.flatMap((entry) => entry.metric_codes.map((code) => [code, [entry.name, entry.status]])),
);

const sources = ["BEA", "CENSUS_ACS", "CENSUS_BPS", "CENSUS_PEP", "CENSUS_SAIPE_SAHIE", "BLS", "BLS_QCEW", "CDC", "FBI_UCR", "USDA_NASS"];

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

// The measures this fixture publishes for metros (web-metro-pages): ACS
// income, rent and median age, LAUS unemployment, and BEA's per capita
// income, which the warehouse sums from member counties.
const METRO_CODES = new Set(["CENSUS_ACS:acs5:B19013_001", "CENSUS_ACS:acs5:B25064_001", "CENSUS_ACS:acs5:B01002_001", "BLS:LAU:UNEMP_RATE", "BEA:CAINC1:3"]);

function grainsFor(code) {
  const own = baseGrainsFor(code);
  return METRO_CODES.has(code) ? [...own, "METRO"] : own;
}

function baseGrainsFor(code) {
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
  if (code.startsWith("CENSUS_BPS:")) return "housing units";
  if (code.startsWith("BLS:")) return "Percent";
  if (code.startsWith("CDC:")) return "%";
  if (/SAEPOVRT|PCTUI/.test(code)) return "percent";
  if (code.endsWith(":SAEMHI")) return "dollars";
  if (code.startsWith("CENSUS_SAIPE_SAHIE:")) return "people";
  if (code.startsWith("FBI_UCR:")) return "per_100000_population";
  if (code.startsWith("USDA_NASS:")) return "ACRES";
  if (/B19013|B19301|B25064|B25077/.test(code)) return "dollars";
  return null;
}

// Only the acs5 identities are published, so each slot's first candidate wins.
const codes = placeChapterMetricCodes().filter((code) => !code.includes(":acs1:"));
export const metrics = Object.fromEntries(codes.map((code) => [code, {
  metric_code: code, metric_display_name: `${code.split(":").at(-1)} (UI fixture)`, source_code: code.split(":")[0],
  metric_label: DEFINED.get(code)?.[0] ?? null, metric_label_status: DEFINED.get(code)?.[1] ?? null,
  units: unitFor(code), valid_geo_grains: grainsFor(code), freshness_state: "fresh",
  publication_time: "2026-09-01T00:00:00Z", harvested_at: "2026-09-02T00:00:00Z", source_watermark: "fixture-2026-09", publisher_contract_version: "v1",
}]));

// Groceries and gas (groceries-and-gas-cards). EIA publishes nine states and
// Wisconsin is not one, so a Wisconsin page falls through to BLS's Midwest
// figure; Dane County is in the Madison metro, Rock County in none here.
export const AREAS = {
  metro: { geo_id: "cbsa:31540", geo_level: "METRO", geo_name: "Madison, WI" },
  division: { geo_id: "division:3", geo_level: "CENSUS_DIVISION", geo_name: "East North Central" },
  region: { geo_id: "region:2", geo_level: "CENSUS_REGION", geo_name: "Midwest Region" },
};
const costMetric = (metric_code, units, valid_geo_grains) => ({ metric_code, metric_display_name: `${metric_code} (UI fixture)`, source_code: metric_code.split(":")[0],
  units, valid_geo_grains, freshness_state: "fresh", publication_time: "2026-09-01T00:00:00Z", harvested_at: "2026-09-02T00:00:00Z", source_watermark: "fixture-2026-09", publisher_contract_version: "v1" });
const COST_METRICS = Object.fromEntries([
  costMetric("EIA:EPMR", "U.S. dollars per gallon", ["NATIONAL", "STATE", "PROVIDER_AREA"]),
  costMetric("BLS:APU020074714", "U.S. dollars per gallon", ["CENSUS_REGION"]),
  costMetric("BLS:CUUR0230SAF11", "index 1982-84=100", ["CENSUS_DIVISION"]),
  costMetric("BLS:CUUR0200SAF11", "index 1982-84=100", ["CENSUS_REGION"]),
  costMetric("BLS:CUUR0000SAF11", "index 1982-84=100", ["NATIONAL"]),
  costMetric("BEA:MARPP:1", "price_level_us_100", ["METRO"]),
  costMetric("BEA:SARPP:1", "price_level_us_100", ["STATE"]),
].map((metric) => [metric.metric_code, metric]));
// Which area publishes which cost measure, and its value. The division's
// food index skips August 2025, so its change over the year cannot be read
// and the region answers instead.
const COST_VALUES = {
  "EIA:EPMR": { "us:1": "3.18" },
  "BLS:APU020074714": { "region:2": "3.05" },
  "BLS:CUUR0230SAF11": { "division:3": "monthly-gap" },
  "BLS:CUUR0200SAF11": { "region:2": "monthly" },
  "BLS:CUUR0000SAF11": { "us:1": "monthly" },
  "BEA:MARPP:1": { "cbsa:31540": "97.4" },
  "BEA:SARPP:1": { "state:55": "93.2" },
};
function costRows(code, geo) {
  const value = COST_VALUES[code]?.[geo];
  const area = [NATION, ...STATES, ...Object.values(AREAS)].find((item) => item.geo_id === geo);
  if (!value || !area) return [];
  const base = { metric_code: code, source_code: code.split(":")[0], geo_id: geo, geo_level: area.geo_level, geo_name: area.geo_name,
    value_status: "valid", unit: COST_METRICS[code].units, release: "fixture", as_of: "2026-09-01", dimensions: {}, uncertainty: null, coverage: null };
  if (value.startsWith("monthly")) {
    return Array.from({ length: 13 }, (_, index) => {
      const month = new Date(Date.UTC(2025, 7 + index, 1)).toISOString().slice(0, 10);
      return { ...base, value: String(300 + index * (geo === "us:1" ? 0.5 : 0.8)), period_start: month, period_end: month };
    }).filter((item) => !(value === "monthly-gap" && item.period_start === "2025-08-01"));
  }
  const weekly = code === "EIA:EPMR" || code.startsWith("BLS:APU");
  return [{ ...base, value, period_start: weekly ? "2026-09-28" : "2024-01-01", period_end: weekly ? "2026-10-04" : "2024-12-31" }];
}

// The Madison metro's page (web-metro-pages), as one
// `/catalog/geographies/metros/{segment}` read answers it: Dane County its
// only member here, Wisconsin its principal state.
export const METRO_PAGE = {
  requested_segment: "madison-wi-metro-area",
  metro: { geo_id: "cbsa:31540", geo_name: "Madison, WI Metro Area", short_name: "Madison", cbsa_code: "31540", segment: "madison-wi-metro-area", page_path: "/metro/madison-wi-metro-area",
    principal_state_fips: "55", principal_state_name: "Wisconsin", population: 680796, population_year: 2023, population_metric_code: "CENSUS_ACS:acs5:B01003_001" },
  delineation_vintage: 2023,
  evidence_source: "census_cbsa_delineation",
  member_counties: [{ geo_id: "state:55|county:025", geo_name: "Dane County", state_fips: "55", state_name: "Wisconsin", page_path: "/us/wisconsin/dane-county", population: 561504, population_year: 2023 }],
  states: [{ geo_id: "state:55", state_fips: "55", state_name: "Wisconsin", page_path: "/us/wisconsin", member_counties: 1, principal: true }],
  principal_state_areas: [
    { geo_id: "division:3", geo_level: "CENSUS_DIVISION", geo_name: "East North Central" },
    { geo_id: "region:2", geo_level: "CENSUS_REGION", geo_name: "Midwest Region" },
    { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" },
  ],
};

const scale = { "us:1": 600, "cbsa:31540": 1.2, "state:55": 10, "state:27": 9.5, "state:55|county:025": 1, "state:55|county:105": 0.3, "state:27|county:053": 2.2, "state:55|place:99999": 0.05, "state:55|place:48000": 0.5 };
const rates = /B01002|B19013|B19301|B19083|B25064|B25077|BLS:|CDC:|FBI_UCR:|PEP:R|SAEPOVRT|SAEMHI|PCTUI/;

// Relationships as the reference would record them. "Crossing city" spans
// Dane and Rock counties: 60% of its area in Dane, 40% in Rock.
const CROSSING = PLACES[0];
const link = (relationship, place, extra = {}) => ({ relationship, geo_id: place.geo_id, geo_level: place.geo_level, geo_name: place.place_name || place.county_name || place.state_name || place.geo_name, state_fips: place.state_fips || null,
  geography_vintage: 2025, evidence_source: relationship === "adjacent" ? "census_boundary_adjacency" : relationship === "intersects" ? "census_boundary_intersection" : "exact_census_code_hierarchy",
  overlap_area_m2: null, overlap_weight: null, ...extra });
export const RELATED = {
  "us:1": [], // the nation's states are listed by the page itself
  "state:55": [link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }), link("part_of", AREAS.division), link("part_of", AREAS.region)],
  "state:27": [link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" })],
  "state:55|county:025": [
    link("adjacent", COUNTIES[1]), link("adjacent", COUNTIES[2]),
    link("intersects", CROSSING, { overlap_weight: 0.6, overlap_area_m2: 6e6 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
    link("part_of", AREAS.metro), link("part_of", AREAS.division), link("part_of", AREAS.region),
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
    // Its metro, through Dane County: Rock County is in no metro here.
    link("intersects", AREAS.metro, { overlap_weight: 0.6, overlap_area_m2: 6e6 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
  "state:55|place:48000": [
    link("intersects", COUNTIES[0], { overlap_weight: 1, overlap_area_m2: 2e8 }),
    link("part_of", STATES[0]), link("part_of", { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" }),
  ],
};

// Per-capita and share pairings (web-per-capita-toggle) as the catalog lists
// them: the registry's own entries, every one approved (2026-10-31) and
// served. ACS entries exist once per dataset.
// Both runners start in apps/web or at the repository root.
const RATIOS_PATH = ["../../docs/semantics/derived_ratios.json", "docs/semantics/derived_ratios.json"]
  .map((path) => resolve(process.cwd(), path))
  .find((path) => existsSync(path));
const REGISTRY = JSON.parse(readFileSync(RATIOS_PATH, "utf8")).entries;
export const DERIVED_RATIOS = REGISTRY.flatMap((entry) => (entry.datasets || [null]).map((dataset) => ({
  ratio_id: dataset ? `${entry.ratio_id}:${dataset}` : entry.ratio_id, kind: entry.kind, derived: entry.kind !== "provider_rate",
  label: entry.label, unit: entry.unit, scale: entry.scale,
  numerator: dataset ? entry.numerator.replace("{dataset}", dataset) : entry.numerator,
  denominator: entry.denominator ? (dataset ? entry.denominator.replace("{dataset}", dataset) : entry.denominator) : null,
  provider_rate: entry.provider_rate || null, reads_beside: entry.reads_beside || null, geo_levels: entry.geo_levels || [],
  dataset, denominator_label: entry.denominator_label, universe: entry.universe,
  numerator_is_subset: Boolean(entry.numerator_is_subset), status: entry.status, version: entry.version, reviewer: entry.reviewer,
  served: true, refusal_reason: null, limitations: entry.limitations || [], citations: entry.citations || [],
})));

// Synthetic derived values. Rock County's survey count has no same-period
// denominator in this fixture, so its shares answer `different_period`.
function derivedRows(ratio, geoId) {
  const place = [NATION, ...STATES, ...COUNTIES, ...PLACES].find((item) => item.geo_id === geoId);
  if (!place) return [];
  if (ratio.kind === "agency_rate") return agencyRateRows(ratio, place);
  const metric = metrics[ratio.provider_rate || ratio.numerator];
  if (ratio.kind === "provider_rate" && metric && !metric.valid_geo_grains.includes(place.geo_level)) return [];
  if (ratio.numerator.startsWith("FBI_UCR:") && !["STATE", "NATIONAL"].includes(place.geo_level)) return [];
  const provider = ratio.kind === "provider_rate";
  const fbi = ratio.numerator.startsWith("FBI_UCR:");
  const missing = !provider && geoId === "state:55|county:105";
  const value = fbi ? { "state:55": "297.4", "state:27": "281.0", "us:1": "363.8" }[geoId] || "300.0" : (10 + (scale[geoId] || 1) / 10).toFixed(2);
  const input = (code, amount) => ({ metric_code: code, metric_label: `${code.split(":").at(-1)} (UI fixture)`, release: fbi ? "fixture-2025" : "2023",
    period_start: fbi ? "2024-01-01" : "2019-01-01", period_end: fbi ? "2024-12-31" : "2023-12-31", value: amount, value_status: "valid", margin_of_error: provider ? null : "40", margin_of_error_controlled: false });
  return [{
    ratio_id: ratio.ratio_id, derived: !provider, pairing_status: ratio.status, pairing_version: ratio.version,
    geo_id: geoId, geo_level: place.geo_level, geo_name: place.geo_name,
    period_start: fbi ? "2024-01-01" : "2019-01-01", period_end: fbi ? "2024-12-31" : "2023-12-31", release: fbi ? "fixture-2025" : "2023",
    value: missing ? null : value, unit: ratio.unit, scale: ratio.scale,
    margin_of_error: provider || missing ? null : "0.80", margin_of_error_method: provider ? "provider" : missing ? "unavailable" : "proportion",
    unavailable_reason: missing ? "different_period" : null,
    numerator: provider ? null : input(ratio.numerator, "1200"), denominator: provider || missing ? null : input(ratio.denominator, "11000"),
    provider_rate: provider ? input(ratio.provider_rate, value) : null,
  }];
}

// A county rate computed from agency reports (ADR-0012), as the gold view
// serves it: Dane County's single-county agencies cover all their people;
// in Rock County only one small agency reported, under the 50 % floor, so the
// month is refused and still shows its coverage. Counties only.
function agencyRateRows(ratio, place) {
  if (place.geo_level !== "COUNTY") return [];
  const refused = place.geo_id === "state:55|county:105";
  const coverage = refused
    ? { covered_population: "9000", linked_population: "41000", coverage_share: "0.2195", coverage_floor: "0.5",
      contributing_agencies: ["WI0530300"], excluded_multi_county_agencies: ["WI0540300"], non_reporting_agencies: ["WI0530000", "WI0530100"], unknown_population_agencies: [] }
    : { covered_population: "115648", linked_population: "115648", coverage_share: "1.0000", coverage_floor: "0.5",
      contributing_agencies: ["WI0130000", "WI0137000"], excluded_multi_county_agencies: ["WI0540300"], non_reporting_agencies: [], unknown_population_agencies: [] };
  return [{
    ratio_id: ratio.ratio_id, derived: true, pairing_status: ratio.status, pairing_version: ratio.version,
    geo_id: place.geo_id, geo_level: "COUNTY", geo_name: place.geo_name,
    period_start: "2024-12-01", period_end: "2024-12-31", release: "fixture-2025",
    value: refused ? null : "11.24", unit: ratio.unit, scale: ratio.scale, margin_of_error: null, margin_of_error_method: "none",
    unavailable_reason: refused ? "coverage_below_floor" : null,
    numerator: { metric_code: ratio.numerator, release: "fixture-2025", period_start: "2024-12-01", period_end: "2024-12-31", value: refused ? "3" : "13", value_status: "reported" },
    denominator: null, provider_rate: null, coverage,
  }];
}

// The place search (A-1) as the API answers it: each group is one matched
// name followed by the county and metro it brings along. Austin's rows
// demonstrate the build-out's example (Texas has no page fixture); Madison's
// and Rock's open the Wisconsin pages above.
const searchRow = (geo_id, geo_level, geo_name, short_name, state, page_path, population, extra = {}) => ({
  geo_id, geo_level, geo_name, short_name, match: "exact", matched_via: null,
  state_fips: state?.[0] ?? null, state_name: state?.[1] ?? null, county_geo_id: null, county_name: null,
  population, population_year: population === null ? null : 2023, population_metric_code: population === null ? null : "CENSUS_ACS:acs5:B01003_001",
  page_path, ...extra,
});
const via = (city) => ({ match: "contains_match", matched_via: { geo_id: city.geo_id, geo_name: city.geo_name } });
const TX = ["48", "Texas"];
const WI = ["55", "Wisconsin"];
const AUSTIN_CITY = searchRow("state:48|place:05000", "PLACE", "Austin city", "Austin", TX, "/us/texas/austin-city", 967862);
const MADISON_CITY = searchRow("state:55|place:48000", "PLACE", "Madison city", "Madison", WI, "/us/wisconsin/madison-city", 269196);
export const SEARCH_GROUPS = [
  [AUSTIN_CITY,
    searchRow("state:48|county:453", "COUNTY", "Travis County", "Travis", TX, "/us/texas/travis-county", 1290188, via(AUSTIN_CITY)),
    searchRow("cbsa:12420", "METRO", "Austin-Round Rock-San Marcos, TX Metro Area", "Austin-Round Rock-San Marcos", TX, "/metro/austin-round-rock-san-marcos-tx-metro-area", 2421115, via(AUSTIN_CITY))],
  [searchRow("state:48|county:015", "COUNTY", "Austin County", "Austin", TX, "/us/texas/austin-county", 30707)],
  [MADISON_CITY,
    searchRow("state:55|county:025", "COUNTY", "Dane County", "Dane", WI, "/us/wisconsin/dane-county", 561504, via(MADISON_CITY)),
    searchRow("cbsa:31540", "METRO", "Madison, WI Metro Area", "Madison", WI, "/metro/madison-wi-metro-area", 680796, via(MADISON_CITY))],
  [searchRow("state:55|county:105", "COUNTY", "Rock County", "Rock", WI, "/us/wisconsin/rock-county", 163687)],
  [searchRow("state:55|place:99999", "PLACE", "Crossing city", "Crossing", WI, "/us/wisconsin/crossing-city", 12000)],
  [searchRow("state:55", "STATE", "Wisconsin", "Wisconsin", WI, "/us/wisconsin", 5882128)],
];

// Where "use my location" points land: inside Madison, in Rock County
// outside every city, and anywhere else outside the covered states.
export const LOCATIONS = {
  madison: { latitude: 43.074678, longitude: -89.384123 },
  rockCounty: { latitude: 42.701234, longitude: -89.155678 },
  atSea: { latitude: 40, longitude: -40 },
};
function locateAnswer(lat, lon) {
  const state = { geo_id: "state:55", geo_level: "STATE", geo_name: "Wisconsin", boundary_vintage: 2024, page_path: "/us/wisconsin" };
  const dane = { geo_id: "state:55|county:025", geo_level: "COUNTY", geo_name: "Dane County", boundary_vintage: 2024, page_path: "/us/wisconsin/dane-county" };
  const rock = { geo_id: "state:55|county:105", geo_level: "COUNTY", geo_name: "Rock County", boundary_vintage: 2024, page_path: "/us/wisconsin/rock-county" };
  if (Math.abs(lat - 43.07) < 0.1 && Math.abs(lon + 89.4) < 0.1) {
    const place = { geo_id: "state:55|place:48000", geo_level: "PLACE", geo_name: "Madison city", boundary_vintage: 2024, page_path: "/us/wisconsin/madison-city" };
    return { landing: place, state, county: dane, place, tract: null, zcta: null, metro: { geo_id: "cbsa:31540", geo_level: "METRO", geo_name: "Madison, WI Metro Area", boundary_vintage: 2023, page_path: "/metro/madison-wi-metro-area" } };
  }
  if (Math.abs(lat - 42.7) < 0.1 && Math.abs(lon + 89.15) < 0.1) return { landing: rock, state, county: rock, place: null, tract: null, zcta: null, metro: null };
  return null;
}

export async function installPlaceFixtures(page, { nationLagsMedianAge = true, withoutSource = null, searchUnavailable = false, onLocate = null, onBatchItem = null, onHistoryBatch = null } = {}) {
  // One `/observations` read, as the single route answers it; the batch
  // route answers each of its pairs from the same function (API-180).
  const answerObservations = (params) => {
    if (COST_METRICS[params.get("metric_code")]) {
      const code = params.get("metric_code");
      let items = costRows(code, params.get("geo_id"));
      if (params.get("limit") === "1" || params.get("newest_per_geography") === "true") items = items.slice(-1);
      return { status: 200, json: { metric_code: code, source_code: code.split(":")[0], scope: params.get("scope") || "latest", total: items.length, offset: 0, limit: Number(params.get("limit") || 500), items } };
    }
    {
      const code = params.get("metric_code");
      const metric = metrics[code];
      const geo = params.get("geo_id");
      const place = [NATION, ...STATES, ...COUNTIES, ...PLACES, AREAS.metro].find((item) => item.geo_id === geo);
      if (!metric || !place) return { status: 404, json: { detail: "No reviewed fixture" } };
      // NASS publishes no cell for Dane County in this fixture: the Land and
      // Farms chapter must be omitted there and named in the footer.
      const empty = !metric.valid_geo_grains.includes(place.geo_level) || (code.startsWith("USDA_NASS") && geo === "state:55|county:025");
      const newest = params.get("limit") === "1" || params.get("newest_per_geography") === "true";
      const lastYear = nationLagsMedianAge && geo === "us:1" && code.endsWith("B01002_001") ? 2023 : 2024;
      // BEA withholds mining earnings for Dane County in this fixture, as it
      // does for a sector that would disclose one employer; QCEW withholds a
      // sector it cannot disclose, and Mining is withheld here too.
      const withheld = (code === "BEA:CAINC5N:200" || code === "BLS_QCEW:employment:21:5") && geo === "state:55|county:025";
      // Madison's home value is published from its third year only, as the
      // Bureau's curated tables grew (web-city-history): the city's line
      // starts there and the earlier years read "Not published that year".
      const firstIndex = geo === "state:55|place:48000" && code.endsWith("B25077_001") ? 2 : 0;
      let items = empty ? [] : Array.from({ length: 6 }, (_, index) => {
        const year = lastYear - 5 + index;
        const monthly = code.endsWith(":monthly");
        const base = rates.test(code) ? 40 + (scale[geo] || 1) : 1000 * (scale[geo] || 1);
        return { metric_code: code, source_code: metric.source_code, geo_id: geo, geo_level: place.geo_level, geo_name: place.geo_name,
          value: withheld ? null : String(Math.round(base * (1 + index * 0.02) * 100) / 100), value_status: withheld ? "withheld" : "valid", unit: metric.units,
          period_start: monthly ? `2024-${String(index + 1).padStart(2, "0")}-01` : `${year}-01-01`,
          period_end: monthly ? `2024-${String(index + 1).padStart(2, "0")}-28` : `${year}-12-31`,
          release: "fixture-release-2025", as_of: "2025-09-01",
          dimensions: code.startsWith("BEA:")
            ? { dollar_basis: code === "BEA:CAGDP1:1" ? "chained_dollars" : "current_dollars", value_source: withheld ? "(D)" : "",
              derivation: place.geo_level === "METRO" ? "ratio_of_member_county_sums" : null }
            : code.startsWith("CENSUS_BPS:") ? { reported_value: String(Math.round(base * 0.9)), observation_basis: "authorized by building permits" } : {},
          uncertainty: code.startsWith("CENSUS_ACS") ? { margin_of_error: "12" }
            : code.startsWith("CDC") ? { confidence_lower: "30.1", confidence_upper: "33.4" }
            : code.startsWith("CENSUS_SAIPE_SAHIE") ? { margin_of_error: "1.4", confidence_lower: "39.6", confidence_upper: "42.4" }
            : null,
          ...(code.startsWith("CENSUS_SAIPE_SAHIE") ? { dimensions: { estimate_method: "model-based annual estimate" } } : {}),
          coverage: null };
      }).slice(firstIndex);
      if (newest) items = items.slice(-1);
      return { status: 200, json: { metric_code: code, source_code: metric.source_code, scope: "latest", total: items.length, offset: 0, limit: Number(params.get("limit") || 500), items } };
    }
  };
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: sources.length + 1, items: [...sources, "EIA"].map((source_code) => ({ source_code,
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
    if (path === "/api/v1/catalog/geographies/metros") {
      const items = params.get("state_fips") === "55" ? [METRO_PAGE.metro] : [];
      return route.fulfill({ json: { total: items.length, limit: Number(params.get("limit") || 100), offset: 0, items } });
    }
    if (path.startsWith("/api/v1/catalog/geographies/metros/")) {
      const segment = decodeURIComponent(path.slice("/api/v1/catalog/geographies/metros/".length));
      if (segment !== METRO_PAGE.metro.segment && segment !== METRO_PAGE.metro.cbsa_code) return route.fulfill({ status: 404, json: { detail: "metro not found" } });
      return route.fulfill({ json: { ...METRO_PAGE, requested_segment: segment } });
    }
    if (path === "/api/v1/place/distinctive") {
      const geoId = params.get("geo_id");
      if (![...STATES, ...COUNTIES].some((item) => item.geo_id === geoId)) return route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
      const rank = (metric_code, name, value, below, extra = {}) => ({ metric_code, metric_display_name: `${name} (UI fixture)`, metric_label: DEFINED.get(metric_code)?.[0] ?? null, metric_label_status: DEFINED.get(metric_code)?.[1] ?? null, source_code: metric_code.split(":")[0], units: null,
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
    if (path === "/api/v1/catalog/geographies/search") {
      if (searchUnavailable) return route.fulfill({ status: 503, json: { detail: "database unavailable" } });
      const typed = (params.get("q") || "").trim().toLowerCase();
      const limit = Number(params.get("limit") || 10);
      const items = SEARCH_GROUPS.filter(([primary]) => primary.short_name.toLowerCase().startsWith(typed)).flat().slice(0, limit);
      return route.fulfill({ json: { query: typed, limit, items } });
    }
    if (path === "/api/v1/catalog/geographies/locate") {
      onLocate?.(url.search);
      const answer = locateAnswer(Number(params.get("lat")), Number(params.get("lon")));
      return answer ? route.fulfill({ json: answer }) : route.fulfill({ status: 404, json: { detail: "no geography contains this point" } });
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
    if (path === "/api/v1/catalog/derived-ratios") {
      const items = DERIVED_RATIOS.filter((ratio) => !params.get("numerator") || ratio.numerator === params.get("numerator"));
      return route.fulfill({ json: { total: items.length, items } });
    }
    if (path === "/api/v1/observations/derived") {
      const ratio = DERIVED_RATIOS.find((item) => item.ratio_id === params.get("ratio_id"));
      if (!ratio) return route.fulfill({ status: 404, json: { detail: "ratio_id not found" } });
      const items = derivedRows(ratio, params.get("geo_id"));
      return route.fulfill({ json: { ratio, scope: "latest", release: null, total: items.length, limit: Number(params.get("limit") || 100), offset: 0, items } });
    }
    if (path === "/api/v1/catalog/metrics" && params.get("metric_codes")) {
      // The named rows, as the per-code route answers each (API-178).
      const items = params.get("metric_codes").split(",").flatMap((code) => {
        if (COST_METRICS[code]) return [COST_METRICS[code]];
        const withheld = withoutSource && code.startsWith(`${withoutSource}:`);
        return metrics[code] && !withheld ? [metrics[code]] : [];
      });
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/catalog/metrics") return route.fulfill({ json: { total: 0, limit: 6, offset: 0, items: [] } });
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      const withheld = withoutSource && code.startsWith(`${withoutSource}:`);
      if (COST_METRICS[code]) return route.fulfill({ json: COST_METRICS[code] });
      return metrics[code] && !withheld ? route.fulfill({ json: metrics[code] }) : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
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
    if (path === "/api/v1/observations/newest/batch") {
      const codes = params.get("metric_codes").split(",");
      const geoIds = params.get("geo_ids").split(",");
      const items = codes.flatMap((code) => geoIds.map((geo) => {
        const single = new URLSearchParams({ metric_code: code, scope: "latest", limit: "1", newest_per_geography: "true", geo_id: geo });
        const answer = answerObservations(single);
        return answer.status === 200
          ? { metric_code: code, geo_id: geo, status: 200, detail: null, observations: answer.json }
          : { metric_code: code, geo_id: geo, status: answer.status, detail: answer.json.detail, observations: null };
      }));
      if (onBatchItem) items.forEach(onBatchItem);
      return route.fulfill({ json: { metric_codes: codes, geo_ids: geoIds, items } });
    }
    if (path === "/api/v1/observations/history/batch") {
      const codes = params.get("metric_codes").split(",");
      const geoIds = params.get("geo_ids").split(",");
      const items = codes.flatMap((code) => geoIds.map((geo) => {
        const single = new URLSearchParams({ metric_code: code, scope: "as_released", newest_release_per_period: "true", limit: "1000", geo_id: geo });
        const answer = answerObservations(single);
        return answer.status === 200
          ? { metric_code: code, geo_id: geo, status: 200, detail: null, observations: answer.json }
          : { metric_code: code, geo_id: geo, status: answer.status, detail: answer.json.detail, observations: null };
      }));
      onHistoryBatch?.(items);
      return route.fulfill({ json: { metric_codes: codes, geo_ids: geoIds, items } });
    }
    if (path === "/api/v1/observations" && params.get("geo_level") !== "TRACT") {
      const answer = answerObservations(params);
      return route.fulfill({ status: answer.status, json: answer.json });
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
