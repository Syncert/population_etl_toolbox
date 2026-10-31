// Age and sex structure fixtures (web-age-and-sex-structure). `/age-structure`,
// `/age-structure/history` and `/age-structure/cohorts` answered in the exact
// shape `docs/plans/to_do/API_AGE_AND_SEX_STRUCTURE_PLAN.md` fixes. Values are
// synthetic UI fixtures shaped like Madison, Dane County, Wisconsin and the
// nation; they are not published statistics. The web computes nothing the
// API serves, so every share, margin and ratio is worked out here, the way
// the API would serve it.
//
// - A Dane County tract has one unreliable band and one suppressed band.
// - "Crossing city" sits 60 % in Dane County, so its county option names the overlap.
// - Minnesota has no 2023 release: a reference that is `not_published`.

import { COUNTIES, DANE_TRACTS, PLACES, STATES } from "./placeScenarios.js";

export const BANDS = [
  [0, 4], [5, 9], [10, 14], [15, 19], [20, 24], [25, 29], [30, 34], [35, 39], [40, 44],
  [45, 49], [50, 54], [55, 59], [60, 64], [65, 69], [70, 74], [75, 79], [80, 84], [85, null],
];
// Census lines per band, men then women (B01001_003..025 and _027..049).
const LINE_GROUPS = [[3], [4], [5], [6, 7], [8, 9, 10], [11], [12], [13], [14], [15], [16], [17], [18, 19], [20, 21], [22], [23], [24], [25]];
const line = (n) => `B01001_${String(n).padStart(3, "0")}`;

// Male shares per band before normalising, and women per man by band.
const SHAPES = {
  madison: [2.5, 2.3, 2.2, 4.3, 8.1, 5.7, 4.6, 3.6, 3.0, 2.5, 2.4, 2.4, 2.4, 2.1, 1.6, 1.0, 0.6, 0.5],
  dane: [2.6, 2.8, 2.9, 3.5, 5.1, 4.3, 3.9, 3.5, 3.2, 2.8, 2.8, 2.9, 2.9, 2.5, 1.9, 1.2, 0.7, 0.55],
  wi: [2.7, 3.0, 3.1, 3.3, 3.2, 3.1, 3.1, 3.1, 3.0, 2.9, 3.1, 3.4, 3.5, 3.0, 2.4, 1.6, 0.9, 0.8],
  us: [2.9, 3.0, 3.2, 3.3, 3.3, 3.4, 3.5, 3.3, 3.2, 3.0, 3.1, 3.1, 3.1, 2.7, 2.2, 1.5, 0.9, 0.7],
  mn: [3.0, 3.1, 3.2, 3.2, 3.1, 3.2, 3.3, 3.2, 3.0, 2.8, 3.0, 3.3, 3.3, 2.9, 2.3, 1.5, 0.9, 0.8],
  rock: [2.9, 3.1, 3.2, 3.3, 3.0, 3.0, 3.1, 3.0, 2.9, 2.9, 3.2, 3.5, 3.6, 3.1, 2.5, 1.6, 0.9, 0.7],
};
const WOMEN_PER_MAN = [0.955, 0.955, 0.955, 0.97, 0.98, 0.98, 0.99, 1.0, 1.0, 1.01, 1.02, 1.04, 1.06, 1.09, 1.13, 1.2, 1.32, 1.75];
const MEDIANS = { madison: [31.6, 31.0, 32.3], dane: [35.4, 34.6, 36.3], wi: [40.1, 39.2, 41.1], us: [38.7, 37.5, 39.9], mn: [38.5, 37.6, 39.4], rock: [40.4, 39.3, 41.5] };

const NAMES = {
  "us:1": ["United States", "NATIONAL"],
  "state:55": ["Wisconsin", "STATE"],
  "state:27": ["Minnesota", "STATE"],
  "state:55|county:025": ["Dane County, Wisconsin", "COUNTY"],
  "state:55|county:105": ["Rock County, Wisconsin", "COUNTY"],
  "state:27|county:053": ["Hennepin County, Minnesota", "COUNTY"],
  "state:55|place:48000": ["Madison city, Wisconsin", "PLACE"],
  "state:55|place:99999": ["Crossing city, Wisconsin", "PLACE"],
  [DANE_TRACTS[0].geo_id]: ["Census Tract 1, Dane County, Wisconsin", "TRACT"],
};
const PROFILE = {
  "us:1": ["us", 332_400_000],
  "state:55": ["wi", 5_882_000],
  "state:27": ["mn", 5_700_000],
  "state:55|county:025": ["dane", 563_000],
  "state:55|county:105": ["rock", 163_000],
  "state:27|county:053": ["mn", 1_270_000],
  "state:55|place:48000": ["madison", 269_000],
  "state:55|place:99999": ["rock", 4_200],
  [DANE_TRACTS[0].geo_id]: ["madison", 3_900],
};

const fixed = (value, digits) => value.toFixed(digits);
const rss = (values) => Math.sqrt(values.reduce((sum, value) => sum + value * value, 0));

/** One place's block, for a release. Older releases shift the shape a little. */
export function ageBlock(geoId, { release = "2023", role = "place", referenceLevel = null, overlapWeight = null } = {}) {
  const [name, level] = NAMES[geoId];
  const [shapeKey, baseTotal] = PROFILE[geoId];
  const age = (2023 - Number(release)) / 5;
  const shape = SHAPES[shapeKey].map((value, index) => value * (1 + (index > 12 ? -0.08 : index < 4 ? 0.04 : 0) * age));
  const total = Math.round(baseTotal * (1 - 0.06 * age));
  const women = shape.map((value, index) => value * WOMEN_PER_MAN[index]);
  const sum = [...shape, ...women].reduce((a, b) => a + b, 0);
  const small = level === "TRACT";
  const moeOf = (count) => (level === "NATIONAL" ? Math.round(Math.sqrt(count) * 3) : Math.round(1.645 * Math.sqrt(count) * (small ? 4.2 : 1.9) + (small ? 20 : 60)));
  const sides = { men: shape, women };
  const bandCounts = { men: [], women: [] };
  const bands = BANDS.map(([start, end], index) => {
    const block = {};
    for (const sex of ["men", "women"]) {
      const share = (sides[sex][index] / sum) * 100;
      const count = Math.round((share / 100) * total);
      const moe = moeOf(count);
      const cv = count ? ((moe / 1.645) / count) * 100 : null;
      bandCounts[sex][index] = count;
      block[sex] = {
        count: String(count), count_moe: String(moe), share: fixed(share, 2), share_moe: fixed((moe / total) * 100, 2),
        cv_percent: cv === null ? null : fixed(cv, 1), reliable: cv !== null && cv <= 30, unavailable_reason: null,
      };
    }
    const offset = 0;
    return {
      band_id: end === null ? `${start}+` : `${start}-${end}`, age_start: start, age_end: end,
      label: end === null ? `${start} and over` : `${start} to ${end}`,
      lines: { men: LINE_GROUPS[index].map((n) => line(n + offset)), women: LINE_GROUPS[index].map((n) => line(n + 24)) },
      ...block,
    };
  });
  if (small) {
    // An unreliable band (CV over 30 %) and a suppressed one, on the tract only.
    bands[16].men = { ...bands[16].men, count: "14", count_moe: "19", cv_percent: "82.5", reliable: false };
    bands[17].women = { count: null, count_moe: null, share: null, share_moe: null, cv_percent: null, reliable: false, unavailable_reason: "suppressed_input" };
  }
  const both = (index) => bandCounts.men[index] + bandCounts.women[index];
  const group = (indices, fraction = []) => {
    const count = indices.reduce((sum, index) => sum + both(index), 0) + fraction.reduce((sum, [index, part]) => sum + both(index) * part, 0);
    const moe = rss(indices.map((index) => moeOf(both(index))));
    return { count: String(Math.round(count)), count_moe: String(Math.round(moe)), share: fixed((count / total) * 100, 1), share_moe: fixed((moe / total) * 100, 1) };
  };
  const under18 = group([0, 1, 2], [[3, 0.6]]);
  const old = group([13, 14, 15, 16, 17]);
  const working = Number(total) - Number(under18.count) - Number(old.count);
  const ratio = (numerator) => ({ value: fixed((Number(numerator) / working) * 100, 1), moe: "0.9", moe_method: "ratio" });
  const menTotal = bandCounts.men.reduce((a, b) => a + b, 0);
  const womenTotal = bandCounts.women.reduce((a, b) => a + b, 0);
  const [all, menMedian, womenMedian] = MEDIANS[shapeKey].map((value) => value - 0.4 * age);
  return {
    geo_id: geoId, geo_level: level, geo_name: name, role, reference_level: referenceLevel, overlap_weight: overlapWeight,
    release, period_start: `${Number(release) - 4}-01-01`, period_end: `${release}-12-31`, unavailable_reason: null,
    total: { count: String(total), count_moe: level === "NATIONAL" || level === "STATE" ? null : "22" },
    men: { count: String(menTotal), count_moe: String(moeOf(menTotal)) },
    women: { count: String(womenTotal), count_moe: String(moeOf(womenTotal)) },
    bands,
    median_age: { all: { value: fixed(all, 1), moe: "0.2" }, men: { value: fixed(menMedian, 1), moe: "0.3" }, women: { value: fixed(womenMedian, 1), moe: "0.3" } },
    indicators: {
      under_18: under18,
      age_18_64: { count: String(working), count_moe: "410", share: fixed((working / total) * 100, 1), share_moe: "0.3" },
      age_65_plus: old,
      age_85_plus: group([17]),
      child_dependency_ratio: ratio(under18.count),
      old_age_dependency_ratio: ratio(old.count),
      total_dependency_ratio: ratio(Number(under18.count) + Number(old.count)),
      sex_ratio: { value: fixed((menTotal / womenTotal) * 100, 1), moe: "0.6", moe_method: "ratio" },
    },
    derived: true,
  };
}

function notPublished(geoId, role, referenceLevel, release) {
  const [name, level] = NAMES[geoId];
  return {
    geo_id: geoId, geo_level: level, geo_name: name, role, reference_level: referenceLevel, overlap_weight: null,
    release, period_start: null, period_end: null, unavailable_reason: "not_published",
    total: null, men: null, women: null, bands: [], median_age: null, indicators: null, derived: true,
  };
}

/** Each place's references, next level up first, as ADR-0013 rule 4 orders them. */
export const REFERENCES = {
  "us:1": [],
  "state:55": [["us:1", "NATIONAL"]],
  "state:27": [["us:1", "NATIONAL"]],
  "state:55|county:025": [["state:55", "STATE"], ["us:1", "NATIONAL"]],
  "state:55|county:105": [["state:55", "STATE"], ["us:1", "NATIONAL"]],
  "state:27|county:053": [["state:27", "STATE"], ["us:1", "NATIONAL"]],
  "state:55|place:48000": [["state:55|county:025", "COUNTY", 1], ["state:55", "STATE"], ["us:1", "NATIONAL"]],
  "state:55|place:99999": [["state:55|county:025", "COUNTY", 0.6], ["state:55", "STATE"], ["us:1", "NATIONAL"]],
  [DANE_TRACTS[0].geo_id]: [["state:55|county:025", "COUNTY"], ["state:55", "STATE"], ["us:1", "NATIONAL"]],
};
// Minnesota and its county publish no 2023 release here.
const UNPUBLISHED = new Set(["state:27"]);

export const SCHEME = { scheme_id: "acs_b01001_five_year", version: 1, status: "draft" };

export function ageStructureAnswer(geoId, { compare = [], release = "2023" } = {}) {
  const [name, level] = NAMES[geoId];
  const references = REFERENCES[geoId] || [];
  const block = (id, role, referenceLevel = null, overlap = null) =>
    UNPUBLISHED.has(id) ? notPublished(id, role, referenceLevel, release) : ageBlock(id, { release, role, referenceLevel, overlapWeight: overlap });
  return {
    geo_id: geoId, geo_level: level, geo_name: name, dataset: "acs5", scheme: SCHEME,
    default_reference_geo_id: references[0]?.[0] ?? null,
    places: [
      ageBlock(geoId, { release }),
      ...references.map(([id, referenceLevel, overlap]) => block(id, "reference", referenceLevel, overlap ?? null)),
      ...compare.map((id) => block(id, "compare")),
    ],
  };
}

export function ageHistoryAnswer(geoId, { includeBands = false } = {}) {
  const [name, level] = NAMES[geoId];
  return {
    geo_id: geoId, geo_level: level, geo_name: name, dataset: "acs5", scheme: SCHEME, periods: "non_overlapping",
    items: ["2023", "2018", "2013"].map((release) => {
      const item = ageBlock(geoId, { release });
      return includeBands ? item : { ...item, bands: [] };
    }),
  };
}

export function ageCohortsAnswer(geoId, from, to) {
  const period = (release) => ({ release, period_start: `${Number(release) - 4}-01-01`, period_end: `${release}-12-31` });
  const gap = Number(to) - Number(from);
  const base = {
    geo_id: geoId, dataset: "acs5", scheme: SCHEME, from: period(from), to: period(to), years_apart: gap,
    caveat: "A change between two surveys mixes births abroad, deaths, moves and sampling error; it is not one group of people followed over time.",
  };
  if (gap < 5) return { ...base, comparable: false, refusal: { reason: "overlapping_periods", detail: "2019-2023 and 2015-2019 share years." }, items: [] };
  if (gap !== 5) return { ...base, comparable: false, refusal: { reason: "period_gap_not_five_years", detail: "The periods end more than five years apart." }, items: [] };
  const before = ageBlock(geoId, { release: from });
  const after = ageBlock(geoId, { release: to });
  const items = before.bands.slice(0, 16).map((band, index) => {
    const next = after.bands[index + 1];
    const side = (sex) => {
      const a = Number(band[sex].count);
      const b = Number(next[sex].count);
      const moe = rss([Number(band[sex].count_moe), Number(next[sex].count_moe)]);
      const overlap = Math.abs(b - a) <= Number(band[sex].count_moe) + Number(next[sex].count_moe);
      return { from_count: String(a), to_count: String(b), change: String(b - a), change_moe: String(Math.round(moe)), intervals_overlap: overlap };
    };
    return { from_band: band.band_id, to_band: next.band_id, men: side("men"), women: side("women") };
  });
  return { ...base, comparable: true, refusal: null, items };
}

const KNOWN = new Set(Object.keys(NAMES));

/**
 * Answers the three age routes, on top of whatever the page's other
 * fixtures answer (registered later, so these win). `seen` records each
 * request's path and query so a test can count them.
 */
export async function installAgeFixtures(page, { seen = [] } = {}) {
  await page.route(/\/api\/v1\/age-structure(\/history|\/cohorts)?(\?|$)/, (route) => {
    const url = new URL(route.request().url());
    seen.push(`${url.pathname}${url.search}`);
    const geoId = url.searchParams.get("geo_id") || "";
    if (!KNOWN.has(geoId)) return route.fulfill({ status: 404, json: { detail: "geo_id not found" } });
    if (url.pathname.endsWith("/history")) {
      return route.fulfill({ json: ageHistoryAnswer(geoId, { includeBands: url.searchParams.get("include_bands") === "true" }) });
    }
    if (url.pathname.endsWith("/cohorts")) {
      return route.fulfill({ json: ageCohortsAnswer(geoId, url.searchParams.get("from_release"), url.searchParams.get("to_release")) });
    }
    const compare = (url.searchParams.get("compare_geo_ids") || "").split(",").filter(Boolean);
    return route.fulfill({ json: ageStructureAnswer(geoId, { compare }) });
  });
}

export { COUNTIES, PLACES, STATES };
