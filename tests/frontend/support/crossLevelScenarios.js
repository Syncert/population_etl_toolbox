// Cross-level comparison fixtures (web-cross-level-compare). On top of the
// place fixtures, `/comparison/places` answers by ADR-0010's rules the way
// the API does: a total across levels is refused with `counts_differ_in_size`
// and the served pairings for it; anything else is set side by side in one
// period with how the places relate, and a difference only for two
// independent places. Values are synthetic UI fixtures, not published
// statistics. The Madison metro publishes the survey and the unemployment
// rate here, so a metro beside the nation has rows to show.
import { AREAS, COUNTIES, DERIVED_RATIOS, NATION, PLACES, RELATED, STATES, installPlaceFixtures, metrics } from "./placeScenarios.js";

const METRO = AREAS.metro;
// The API names the nation "United States" (#187).
const KNOWN = [{ ...NATION, geo_name: "United States" }, ...STATES, ...COUNTIES, ...PLACES, METRO];
const SCALE = { "us:1": 600, "state:55": 10, "state:27": 9.5, "state:55|county:025": 1, "state:55|county:105": 0.3, "state:27|county:053": 2.2, "state:55|place:99999": 0.05, "state:55|place:48000": 0.5, "cbsa:31540": 1.2 };
// The same split as the place fixtures: medians, means, rates and indexes
// compare across levels; everything else is a total.
const NOT_A_TOTAL = /B01002|B19013|B19301|B19083|B25064|B25077|BLS:|CDC:|FBI_UCR:|PEP:R|SAEPOVRT|SAEMHI|PCTUI/;

// What the catalog lists for each metric. As on OFFICE_LAB, neither the
// survey nor LAUS lists both the metro and the nation: the survey lists no
// metro grain though it publishes metro rows, and LAUS has no national row.
function grainsFor(code) {
  if (code.startsWith("BLS:")) return ["COUNTY", "STATE", "METRO"];
  if (code.startsWith("CDC:")) return ["COUNTY", "NATIONAL"];
  if (code.startsWith("FBI_UCR:")) return ["AGENCY", "STATE", "NATIONAL"];
  if (code.startsWith("CENSUS_ACS:")) return ["PLACE", "COUNTY", "STATE", "NATIONAL"];
  if (code.startsWith("CENSUS_PEP:POPESTIMATE")) return ["PLACE", "COUNTY", "STATE", "NATIONAL"];
  return ["COUNTY", "STATE", "NATIONAL"];
}

// Where each metric has rows. LAUS publishes metros but no national row.
function publishedAt(code) {
  if (code.startsWith("CENSUS_ACS:")) return [...grainsFor(code), "METRO"];
  return grainsFor(code);
}

/** How a relates to b, from a's side, as the reference records it. */
function containment(a, b) {
  if (b.geo_id === "us:1") return { relationship: "part_of", smaller_side: "a", share_of_smaller_inside_larger: 1 };
  if (a.geo_id === "us:1") return { relationship: "contains", smaller_side: "b", share_of_smaller_inside_larger: 1 };
  const forward = (RELATED[a.geo_id] || []).find((row) => row.geo_id === b.geo_id);
  const backward = (RELATED[b.geo_id] || []).find((row) => row.geo_id === a.geo_id);
  const row = forward || backward;
  if (!row || row.relationship === "adjacent") return { relationship: row ? "adjacent" : "none_recorded" };
  if (row.relationship === "intersects") {
    const smaller = [a, b].find((item) => item.geo_level === "PLACE") === a ? "a" : "b";
    return { relationship: "intersects", smaller_side: smaller, share_of_smaller_inside_larger: row.overlap_weight };
  }
  const aInsideB = forward ? row.relationship === "part_of" : row.relationship === "contains";
  return { relationship: aInsideB ? "part_of" : "contains", smaller_side: aInsideB ? "a" : "b", share_of_smaller_inside_larger: 1 };
}

function side(place, code, value, period) {
  const published = value !== null;
  return {
    geo_id: place.geo_id, geo_level: place.geo_level, geo_name: place.geo_name, metric_code: code, published,
    period_start: published ? period[0] : null, period_end: published ? period[1] : null, release: published ? "fixture-2025" : null,
    value: published ? String(value) : null, value_status: published ? "valid" : null,
    margin_of_error: published ? "1.5" : null,
  };
}

export function placesAnswer(params) {
  const a = KNOWN.find((item) => item.geo_id === params.get("geo_id_a"));
  const b = KNOWN.find((item) => item.geo_id === params.get("geo_id_b"));
  const ratio = params.get("ratio_id") ? DERIVED_RATIOS.find((item) => item.ratio_id === params.get("ratio_id")) : null;
  const code = ratio ? ratio.numerator : params.get("metric_code");
  if (!a || !b) return { status: 404, json: { detail: "geo_id_a not found" } };
  if (params.get("ratio_id") && !ratio) return { status: 404, json: { detail: "ratio_id not found" } };
  const total = !ratio && !NOT_A_TOTAL.test(code);
  const crossLevel = a.geo_level !== b.geo_level;
  const base = {
    metric_code: ratio ? null : code, metric_code_b: null, ratio_id: ratio?.ratio_id || null,
    measure_kind: ratio ? (ratio.kind === "share" ? "share" : "rate") : total ? "count" : code.includes("B19013") ? "median" : "rate",
    measure_kind_status: "draft", cross_level: crossLevel, derived: Boolean(ratio), derivations: [], caveats: [],
    containment: containment(a, b),
  };
  if (total && crossLevel) {
    const alternatives = DERIVED_RATIOS.filter((item) => item.served && item.numerator === code)
      .map((item) => ({ ratio_id: item.ratio_id, label: item.label, unit: item.unit, href: `/api/v1/comparison/places?ratio_id=${item.ratio_id}`, pairing_status: item.status }));
    return { json: { ...base, comparable: false, period_basis: null,
      refusal: { reason: "counts_differ_in_size", detail: `${code} is a count, and a ${a.geo_level.toLowerCase()} and a ${b.geo_level.toLowerCase()} differ in size (UI fixture)`, alternatives },
      side_a: { geo_id: a.geo_id, geo_level: a.geo_level, geo_name: a.geo_name, metric_code: code, published: null },
      side_b: { geo_id: b.geo_id, geo_level: b.geo_level, geo_name: b.geo_name, metric_code: code, published: null },
      difference: null, intervals_overlap: null, notes: [] } };
  }
  const period = code.startsWith("CENSUS_PEP") ? ["2024-01-01", "2024-12-31"] : ["2019-01-01", "2023-12-31"];
  const valueFor = (place) => {
    if (!publishedAt(code).includes(place.geo_level)) return null;
    const scale = SCALE[place.geo_id] || 1;
    return ratio ? Number((10 + scale / 10).toFixed(2)) : total ? 1000 * scale : 40 + scale;
  };
  const sideA = side(a, code, valueFor(a), period);
  const sideB = side(b, code, valueFor(b), period);
  const nested = ["contains", "part_of", "intersects"].includes(base.containment.relationship);
  const both = sideA.published && sideB.published;
  if (!both) {
    // A side with no value is refused, not compared (#187, ADR-0010 rule 3).
    const missing = [sideA, sideB].filter((item) => !item.published).map((item) => `there is no published value for ${item.geo_name} in this series`);
    return { json: { ...base, comparable: false, period_basis: "newest_common",
      refusal: { reason: "not_published", detail: `${missing.join("; ")}, so there is nothing to compare it with`, alternatives: [] },
      side_a: sideA, side_b: sideB, difference: null, intervals_overlap: null, notes: [] } };
  }
  const notes = [
    ...[sideA, sideB].filter((item) => !item.published).map((item) => ({ reason: "not_published", detail: `${item.geo_name} has no published value; it is not zero` })),
    ...(nested ? [{ reason: base.containment.relationship === "intersects" ? "overlapping_places" : "contained", detail: "the places overlap (UI fixture)" }] : []),
    ...(total ? [{ reason: "counts_differ_in_size", detail: `${code} is a count (UI fixture)` }] : []),
    ...(both && Math.abs(Number(sideA.value) - Number(sideB.value)) <= 3 ? [{ reason: "intervals_overlap", detail: "the 90 percent intervals overlap" }] : []),
  ];
  return { json: { ...base, comparable: true, refusal: null, period_basis: "newest_common", side_a: sideA, side_b: sideB,
    difference: both && !nested ? String(Number(sideA.value) - Number(sideB.value)) : null,
    intervals_overlap: both ? notes.some((note) => note.reason === "intervals_overlap") : null, notes } };
}

function withMetroGrain(code) {
  return code.startsWith("CENSUS_ACS:") || code.startsWith("BLS:LAU") ? { ...metrics[code], valid_geo_grains: grainsFor(code) } : metrics[code];
}

/** `/comparison/places/batch`: each item as the single route answers it (API-178). */
export function placesBatch(params) {
  const ask = (extra) => placesAnswer(new URLSearchParams({ geo_id_a: params.get("geo_id_a"), geo_id_b: params.get("geo_id_b"), ...extra }));
  const split = (name) => (params.get(name) || "").split(",").filter(Boolean);
  const pair = ask({ metric_code: "CENSUS_ACS:acs5:B19013_001" });
  if (pair.status === 404) return pair;
  const item = (asked, answered) => {
    if (answered.status) return { ...asked, status: answered.status, detail: answered.json.detail, answer: null, instead: null };
    const answer = answered.json;
    const alternative = answer.refusal?.reason === "counts_differ_in_size" ? answer.refusal.alternatives?.[0] : null;
    const instead = alternative ? ask({ ratio_id: alternative.ratio_id }) : null;
    return { ...asked, status: 200, detail: null, answer, instead: instead && !instead.status ? instead.json : null };
  };
  const items = [
    ...split("metric_codes").map((code) => item({ metric_code: code, ratio_id: null }, ask({ metric_code: code }))),
    ...split("ratio_ids").map((ratio) => item({ metric_code: null, ratio_id: ratio }, ask({ ratio_id: ratio }))),
  ];
  return { json: { geo_id_a: params.get("geo_id_a"), geo_id_b: params.get("geo_id_b"), items } };
}

// "Find your place" (A-1): a name offers every grain it matches, a metro
// with no page yet.
const SEARCHABLE = [
  ...STATES.map((item) => ({ ...item, page_path: `/us/${item.geo_name.toLowerCase()}` })),
  { ...COUNTIES[0], geo_name: "Dane County, Wisconsin", page_path: "/us/wisconsin/dane-county" },
  { ...PLACES[1], geo_name: "Madison city, Wisconsin", page_path: "/us/wisconsin/madison-city" },
  { ...METRO, page_path: null },
];

export async function installCrossLevelFixtures(page, options = {}) {
  await installPlaceFixtures(page, options);
  // Registered after the place fixtures, so these answer first.
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/comparison/places") return route.fulfill(placesAnswer(params));
    if (path === "/api/v1/comparison/places/batch") return route.fulfill(placesBatch(params));
    if (path === "/api/v1/catalog/geographies" && params.get("geo_level") === "METRO") {
      return route.fulfill({ json: { total: 1, limit: 1000, offset: 0, items: [METRO] } });
    }
    if (path === "/api/v1/catalog/geographies/search") {
      const needle = String(params.get("q") || "").toLowerCase();
      const grains = (params.get("grain") || "").split(",").filter(Boolean);
      const items = SEARCHABLE.filter((item) => item.geo_name.toLowerCase().includes(needle))
        .filter((item) => !params.get("state_fips") || item.state_fips === params.get("state_fips"))
        .filter((item) => !grains.length || grains.includes(item.geo_level)).map((item) => ({
        geo_id: item.geo_id, geo_level: item.geo_level, geo_name: item.geo_name, short_name: item.geo_name, match: "prefix",
        state_fips: item.state_fips || null, state_name: item.state_name || null, page_path: item.page_path,
      }));
      return route.fulfill({ json: { query: needle, limit: Number(params.get("limit") || 10), items } });
    }
    if (path === "/api/v1/catalog/metrics" && params.get("metric_codes")) {
      // The metro grain here too, and every other row as the place fixtures list it.
      const items = params.get("metric_codes").split(",").flatMap((code) => (metrics[code] ? [withMetroGrain(code)] : []));
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      // The metro grain, which the place fixtures' catalog does not list.
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      if (metrics[code] && (code.startsWith("CENSUS_ACS:") || code.startsWith("BLS:LAU"))) {
        return route.fulfill({ json: withMetroGrain(code) });
      }
    }
    return route.fallback();
  });
}
