// Deterministic tract-page fixtures (tract pages, D-4). Values are synthetic
// UI fixtures, not provider-published statistics. Travis County, Texas: 2010
// tract 18.05 was split in 2020 into tracts 18.31 and 18.32; tract 11 kept its
// boundary.
import { tractMetricCodes } from "../../../apps/web/lib/tractPage.ts";

export const NATION = { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "us:1" };
export const TEXAS = { geo_id: "state:48", geo_level: "STATE", geo_name: "Texas", state_fips: "48", state_name: "Texas" };
export const TRAVIS = {
  geo_id: "state:48|county:453", geo_level: "COUNTY", geo_name: "Travis County", state_fips: "48", county_fips: "453",
  county_name: "Travis County", state_name: "Texas",
};
const tract = (code, name) => ({
  geo_id: `state:48|county:453|tract:${code}`, geo_level: "TRACT", state_fips: "48", county_fips: "453",
  county_name: "Travis County", area_name: name, geo_name: name,
});
export const SPLIT = tract("001831", "Census Tract 18.31");
export const SIBLING = tract("001832", "Census Tract 18.32");
export const KEPT = tract("001100", "Census Tract 11");
export const OLD = "state:48|county:453|tract:001805";
export const TRACTS = [KEPT, SPLIT, SIBLING];

const SCALE = { [SPLIT.geo_id]: 1, [SIBLING.geo_id]: 1.1, [KEPT.geo_id]: 0.9, [TRAVIS.geo_id]: 300, [TEXAS.geo_id]: 7000, "us:1": 80000 };
const MEDIANS = /B19013|B19301|B25064|B25077|B01002/;

function newestItems(code, geo) {
  const scale = SCALE[geo];
  if (!scale) return [];
  const value = MEDIANS.test(code)
    ? (code.endsWith("B01002_001") ? 34 + (scale > 1 ? 3 : 0) : 60000 + Math.round(Math.log10(scale + 1) * 4000))
    : Math.round(1500 * scale);
  return [{
    metric_code: code, source_code: "CENSUS_ACS", geo_id: geo, geo_level: geo === "us:1" ? "NATIONAL" : geo.includes("tract") ? "TRACT" : geo.includes("county") ? "COUNTY" : "STATE",
    value: String(value), value_status: "valid", unit: /B19013|B19301|B25064|B25077/.test(code) ? "dollars" : null,
    period_start: "2020-01-01", period_end: "2024-12-31", release: "2024", as_of: "2025-12-11",
    dimensions: {}, uncertainty: { margin_of_error: geo.includes("tract") ? "410" : "35" }, coverage: null,
  }];
}

const point = (vintage, value, status, extra = {}) => ({
  vintage_year: vintage, observation_date: `${vintage}-01-01`, value, margin_of_error: status === "published" || status === "unchanged" ? 380 : null,
  status, derived: status === "apportioned" || status === "incomplete", weight_basis: status === "apportioned" ? "share of land area" : null,
  source_geo_ids: status === "apportioned" || status === "incomplete" ? [OLD] : [], ...extra,
});

function historyFor(geoId, codes) {
  if (geoId === SPLIT.geo_id) {
    return {
      geo_id: geoId, geo_name: "Census Tract 18.31", target_tract_vintage: 2020, derivation: "decade_crosswalk",
      method: "Values from 2020 on are the Census Bureau's ACS 5-year estimates for this tract's 2020 boundary. Before 2020 a count is DERIVED (UI fixture).",
      boundary: {
        status: "split",
        sentence: "In 2020 Census Tract 18.05 was split, and Census Tract 18.31 is 46% of its land. The rest became Census Tract 18.32. Earlier figures for Census Tract 18.31 are therefore derived from Census Tract 18.05's.",
        earlier: [{ geo_id: OLD, name: "Census Tract 18.05", vintage: 2010, share_of_that_tract: 0.46, share_of_this_tract: 1 }],
        also_became: [{ geo_id: SIBLING.geo_id, name: "Census Tract 18.32", vintage: 2020, share_of_that_tract: 0.54, share_of_this_tract: 1 }],
        slivers_ignored: 0,
      },
      series: codes.map((code) => MEDIANS.test(code)
        ? { metric_code: code, note: "Before 2020 this measure was published on different tract boundaries and it is not a count that can be apportioned, so its history here starts in 2020.", items: [point(2020, 71000, "published"), point(2024, 82000, "published")] }
        : { metric_code: code, note: null, items: [point(2012, null, "incomplete"), point(2015, 1210, "apportioned"), point(2019, 1388, "apportioned"), point(2020, 1420, "published"), point(2024, 1500, "published")] }),
    };
  }
  if (geoId === KEPT.geo_id || geoId === SIBLING.geo_id) {
    const kept = geoId === KEPT.geo_id;
    return {
      geo_id: geoId, geo_name: kept ? "Census Tract 11" : "Census Tract 18.32", target_tract_vintage: 2020, derivation: "decade_crosswalk",
      method: "UI fixture method.",
      boundary: kept
        ? { status: "unchanged", sentence: "Census Tract 11 kept its boundary in 2020, so its figures from earlier releases describe the same area.", earlier: [{ geo_id: KEPT.geo_id, name: "Census Tract 11", vintage: 2010, share_of_that_tract: 1, share_of_this_tract: 1 }], also_became: [], slivers_ignored: 0 }
        : { status: "split", sentence: "In 2020 Census Tract 18.05 was split, and Census Tract 18.32 is 54% of its land. The rest became Census Tract 18.31.", earlier: [], also_became: [{ geo_id: SPLIT.geo_id, name: "Census Tract 18.31", vintage: 2020, share_of_that_tract: 0.46, share_of_this_tract: 1 }], slivers_ignored: 0 },
      series: codes.map((code) => ({ metric_code: code, note: null, items: [point(2015, 1300, kept ? "unchanged" : "apportioned"), point(2024, 1350, "published")] })),
    };
  }
  return null;
}

/** Answer every API request a tract page makes; `seen` collects each path and query. */
export async function installTractFixtures(page, { seen = [] } = {}) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    seen.push(`${path}${url.search}`);
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/geographies") {
      const grain = params.get("geo_level");
      const items = grain === "NATIONAL" ? [NATION]
        : grain === "STATE" ? [TEXAS]
        : grain === "COUNTY" ? (params.get("state_fips") === "48" ? [TRAVIS] : [])
        : grain === "TRACT" ? (params.get("state_fips") === "48" && params.get("county_fips") === "453" ? TRACTS : [])
        : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/observations/newest/batch") {
      const codes = params.get("metric_codes").split(",");
      const geoIds = params.get("geo_ids").split(",");
      const items = codes.flatMap((code) => geoIds.map((geo) => {
        const rows = newestItems(code, geo);
        return { metric_code: code, geo_id: geo, status: 200, detail: null,
          observations: { metric_code: code, source_code: "CENSUS_ACS", scope: "latest", total: rows.length, offset: 0, limit: 1, items: rows } };
      }));
      return route.fulfill({ json: { metric_codes: codes, geo_ids: geoIds, items } });
    }
    if (path === "/api/v1/place/tract-history") {
      const answer = historyFor(params.get("geo_id"), params.get("metric_codes").split(","));
      return answer ? route.fulfill({ json: answer }) : route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
    }
    if (path === "/api/v1/age-structure") return route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}

export const TRACT_CODES = tractMetricCodes();
