import { describe, expect, it } from "vitest";

import {
  INDUSTRY_MIX_SECTORS,
  MIGRATION_POPULATION_NOTE,
  PLACE_CHAPTERS,
  PLACE_SEGMENT,
  buildTrend,
  chapterHasValues,
  countySegment,
  citySegment,
  measureBasis,
  omissionLine,
  placeChapterMetricCodes,
  placePath,
  placeSlug,
  resolveCitySegment,
  resolveCountySegment,
  resolvePlaceChapters,
  resolveStateSegment,
  stateSegment,
  threeLevelCard,
} from "../../../apps/web/lib/placeChapters";

// Covers: WEB-125 — place-pages: the chapter contract every place page shares, the address
// rules, the three-level card's one-period rule, and the trend's stated index.

const states = [
  { geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin" },
  { geo_id: "state:27", geo_level: "STATE", state_fips: "27", state_name: "Minnesota" },
  { geo_id: "state:72", geo_level: "STATE", state_fips: "72", state_name: "Puerto Rico" },
];
const counties = [
  { geo_id: "state:55|county:025", geo_level: "COUNTY", state_fips: "55", county_fips: "025", county_name: "Dane County" },
  { geo_id: "state:55|county:105", geo_level: "COUNTY", state_fips: "55", county_fips: "105", county_name: "Rock County" },
  // Two county equivalents whose names slug the same way inside one state.
  { geo_id: "state:24|county:005", geo_level: "COUNTY", state_fips: "24", county_fips: "005", county_name: "Baltimore County" },
  { geo_id: "state:24|county:510", geo_level: "COUNTY", state_fips: "24", county_fips: "510", county_name: "Baltimore city" },
  { geo_id: "state:51|county:600", geo_level: "COUNTY", state_fips: "51", county_fips: "600", county_name: "Fairfax city" },
  { geo_id: "state:51|county:610", geo_level: "COUNTY", state_fips: "51", county_fips: "610", county_name: "Fairfax City" },
];

const metric = (code, grains) => [code, { metric_code: code, source_code: code.split(":")[0], valid_geo_grains: grains }];

// Covers: WEB-133 — census-saipe-sahie: a survey estimate and a model-based
// estimate are labelled apart, and neither is the other's fallback.
describe("how a figure was made", () => {
  it("labels the ACS as a survey and SAIPE and SAHIE as model-based", () => {
    expect(measureBasis("CENSUS_ACS:acs5:B19013_001")).toBe("Survey estimate (American Community Survey)");
    expect(measureBasis("CENSUS_SAIPE_SAHIE:saipe:SAEMHI")).toBe("Model-based annual estimate (SAIPE)");
    expect(measureBasis("CENSUS_SAIPE_SAHIE:sahie:PCTUI")).toBe("Model-based annual estimate (SAHIE)");
    expect(measureBasis("BLS:LAU:UNEMP_RATE")).toBeNull();
    expect(measureBasis(null)).toBeNull();
  });

  it("never lists an ACS identity and a SAIPE/SAHIE identity in one slot", () => {
    for (const chapter of PLACE_CHAPTERS) {
      for (const measure of [...chapter.headline, ...chapter.depth]) {
        const sources = new Set(measure.candidates.map((code) => code.split(":")[0]));
        expect(sources.has("CENSUS_ACS") && sources.has("CENSUS_SAIPE_SAHIE"), measure.id).toBe(false);
      }
    }
  });
});

describe("the chapter contract", () => {
  it("pins the chapter order every place page reads in", () => {
    expect(PLACE_CHAPTERS.map((chapter) => chapter.title)).toEqual([
      "People", "Work and Money", "Housing", "Health", "Safety", "Land and Farms", "Change",
    ]);
  });

  it("pins each chapter's headline and depth candidates", () => {
    const pinned = Object.fromEntries(PLACE_CHAPTERS.map((chapter) => [chapter.id, {
      headline: chapter.headline.map((measure) => measure.candidates[0]),
      depth: chapter.depth.map((measure) => measure.candidates[0]),
      trend: chapter.trend,
    }]));
    expect(pinned).toEqual({
      people: {
        headline: ["CENSUS_PEP:POPESTIMATE", "CENSUS_ACS:acs5:B01002_001"],
        depth: ["B01003_001", "B09001_001", "B09020_001", "B11001_008", "B12001_003", "B12001_012", "B07001_017", "B07001_033", "B05002_013", "B21001_002"].map((variable) => `CENSUS_ACS:acs5:${variable}`),
        trend: { measureId: "population-estimate", scale: "index" },
      },
      "work-money": {
        headline: ["CENSUS_ACS:acs5:B19013_001", "BEA:CAINC1:3", "BEA:CAGDP1:1", "BLS:LAU:UNEMP_RATE", "BLS_QCEW:employment:10:0", "CENSUS_SAIPE_SAHIE:saipe:SAEMHI", "BLS_QCEW:avg_weekly_wage:10:0", "CENSUS_SAIPE_SAHIE:saipe:SAEPOVRTALL"],
        depth: [
          ...["B19301_001", "B19083_001", "B17001_002"].map((variable) => `CENSUS_ACS:acs5:${variable}`),
          "CENSUS_SAIPE_SAHIE:saipe:SAEPOVALL",
          "CENSUS_SAIPE_SAHIE:saipe:SAEPOVRT0_17",
          ...["B19001_002", "B19001_017", "B23025_002", "B23025_005", "C24050_001", "B08301_021", "B08301_010", "B08303_013"].map((variable) => `CENSUS_ACS:acs5:${variable}`),
          ...["81", "100", "200", "300", "400", "500", "600", "700", "800", "900", "1000", "1100", "1200", "1300", "1400", "1500", "1600", "1700", "1800", "1900", "2000"].map((line) => `BEA:CAINC5N:${line}`),
          ...INDUSTRY_MIX_SECTORS.map(([code]) => `BLS_QCEW:employment:${code}:5`),
        ],
        trend: { measureId: "unemployment-rate", scale: "level" },
      },
      housing: {
        headline: ["CENSUS_ACS:acs5:B25064_001", "CENSUS_ACS:acs5:B25077_001", "CENSUS_BPS:units:1_unit:annual", "CENSUS_BPS:units:5_plus_units:annual", "CENSUS_BPS:units:1_unit:monthly"],
        depth: ["B25001_001", "B25003_002", "B25003_003", "B25002_003", "B25024_002", "B25034_002", "B08201_002", "B28002_004", "B28002_013"].map((variable) => `CENSUS_ACS:acs5:${variable}`),
        trend: { measureId: "median-gross-rent", scale: "level" },
      },
      health: {
        headline: ["CDC:places_county:OBESITY:AgeAdjPrv", "CDC:places_county:DIABETES:AgeAdjPrv", "CENSUS_SAIPE_SAHIE:sahie:PCTUI"],
        depth: [
          ...["B27010_017", "B27010_033", "B27010_050", "B27010_066"].map((variable) => `CENSUS_ACS:acs5:${variable}`),
          "CENSUS_SAIPE_SAHIE:sahie:NUI",
        ],
        trend: { measureId: "obesity", scale: "level" },
      },
      safety: {
        headline: ["FBI_UCR:summarized_violent_crime:V:offense:rate", "FBI_UCR:summarized_property_crime:P:offense:rate"],
        depth: [],
        trend: { measureId: "violent-crime-rate", scale: "level" },
      },
      "land-farms": {
        headline: ["USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11"],
        depth: [],
        trend: { measureId: "corn-acres-harvested", scale: "index" },
      },
      change: {
        headline: ["CENSUS_PEP:RNETMIG", "CENSUS_PEP:RNATURALCHG"],
        depth: ["BIRTHS", "DEATHS", "DOMESTICMIG", "INTERNATIONALMIG", "NPOPCHG"].map((name) => `CENSUS_PEP:${name}`),
        trend: { measureId: "net-migration-rate", scale: "level" },
      },
    });
  });

  it("labels every ACS count with its Census universe and every trend names a headline", () => {
    for (const chapter of PLACE_CHAPTERS) {
      expect(chapter.headline.map((measure) => measure.id)).toContain(chapter.trend.measureId);
      for (const measure of chapter.depth) {
        if (measure.candidates[0].startsWith("CENSUS_ACS") && !/B19301|B19083/.test(measure.candidates[0])) {
          expect(measure.universe, measure.id).toBeTruthy();
        }
      }
    }
  });

  it("asks the catalog for each candidate once", () => {
    const codes = placeChapterMetricCodes();
    expect(new Set(codes).size).toBe(codes.length);
    expect(codes).toContain("CENSUS_ACS:acs1:B28002_004");
  });
});

describe("addresses", () => {
  it("slugs names and accepts only those segments", () => {
    expect(placeSlug("Dane County")).toBe("dane-county");
    expect(placeSlug("Doña Ana County")).toBe("dona-ana-county");
    expect(placeSlug("St. Mary's County")).toBe("st-mary-s-county");
    expect(PLACE_SEGMENT.test("dane-county")).toBe(true);
    expect(PLACE_SEGMENT.test("Dane%20County")).toBe(false);
    expect(PLACE_SEGMENT.test("../etc")).toBe(false);
  });

  it("resolves a state by its slug or FIPS and names the canonical form", () => {
    expect(resolveStateSegment("wisconsin", states)).toEqual({ place: states[0], canonical: null });
    expect(resolveStateSegment("55", states)).toEqual({ place: states[0], canonical: "wisconsin" });
    expect(resolveStateSegment("puerto-rico", states).place).toBe(states[2]);
    expect(resolveStateSegment("wi", states)).toEqual({ place: null, canonical: null });
  });

  it("falls back to FIPS for county names that slug alike in one state", () => {
    const fairfax = counties.filter((county) => county.state_fips === "51");
    expect(countySegment(fairfax[0], fairfax)).toBe("51600");
    expect(countySegment(fairfax[1], fairfax)).toBe("51610");
    const maryland = counties.filter((county) => county.state_fips === "24");
    expect(countySegment(maryland[0], maryland)).toBe("baltimore-county");
    expect(countySegment(maryland[1], maryland)).toBe("baltimore-city");
    expect(resolveCountySegment("51610", fairfax).place).toBe(fairfax[1]);
    expect(resolveCountySegment("fairfax-city", fairfax).place).toBeNull();
  });

  it("resolves a county by slug, five-digit FIPS, or county FIPS", () => {
    const wisconsin = counties.filter((county) => county.state_fips === "55");
    expect(resolveCountySegment("dane-county", wisconsin)).toEqual({ place: wisconsin[0], canonical: null });
    expect(resolveCountySegment("55025", wisconsin).canonical).toBe("dane-county");
    expect(resolveCountySegment("025", wisconsin).canonical).toBe("dane-county");
    expect(resolveCountySegment("dane", wisconsin).place).toBeNull();
    expect(stateSegment(states[0], states)).toBe("wisconsin");
    expect(placePath()).toBe("/us");
    expect(placePath("wisconsin", "dane-county")).toBe("/us/wisconsin/dane-county");
  });
});

describe("chapters on one page", () => {
  const catalog = new Map([
    metric("CENSUS_PEP:POPESTIMATE", ["COUNTY", "STATE", "NATIONAL"]),
    metric("CDC:places_county:OBESITY:AgeAdjPrv", ["COUNTY", "NATIONAL"]),
    metric("FBI_UCR:summarized_violent_crime:V:offense:rate", ["STATE", "NATIONAL"]),
  ]);

  it("shows a chapter published at the grain and names the rest once", () => {
    const { shown, omitted } = resolvePlaceChapters("STATE", catalog);
    expect(shown.map((entry) => entry.chapter.id)).toEqual(["people", "safety"]);
    expect(omitted.map(omissionLine)).toContain("Health: no published state values for this place");
    expect(omitted.map(omissionLine)).toContain("Land and Farms: no published state values for this place");
  });

  it("reads a county's safety chapter as state context", () => {
    const { shown } = resolvePlaceChapters("COUNTY", catalog);
    const safety = shown.find((entry) => entry.chapter.id === "safety");
    expect(safety).toMatchObject({ readGrain: "STATE", stateContext: true });
    expect(shown.find((entry) => entry.chapter.id === "health")).toMatchObject({ readGrain: "COUNTY", stateContext: false });
  });

  it("omits a chapter whose measures answered nothing for this place", () => {
    const { shown } = resolvePlaceChapters("COUNTY", catalog);
    const people = shown.find((entry) => entry.chapter.id === "people");
    expect(chapterHasValues(people, () => null)).toBe(false);
    expect(chapterHasValues(people, () => ({ value: null, value_status: "suppressed" }))).toBe(false);
    expect(chapterHasValues(people, () => ({ value: "0" }))).toBe(true);
  });
});

describe("the three-level card", () => {
  const levels = [
    { level: "COUNTY", geoId: "c", name: "Dane County", role: "this place" },
    { level: "STATE", geoId: "s", name: "Wisconsin", role: "parent" },
    { level: "NATIONAL", geoId: "n", name: "United States", role: "parent" },
  ];
  const row = (year, value = "10", extra = {}) => ({ period_start: `${year}-01-01`, period_end: `${year}-12-31`, value, ...extra });

  it("shows one period for every row, and says so where a parent lacks it", () => {
    const card = threeLevelCard(levels, new Map([
      ["c", { row: row(2024) }], ["s", { row: row(2024) }], ["n", { row: row(2023) }],
    ]), () => true);
    expect(card.period).toBe("2024-01-01 – 2024-12-31");
    expect(card.rows.map((entry) => entry.message)).toEqual(["", "", "Not published for 2024-01-01 – 2024-12-31 (newest published: 2023-01-01 – 2023-12-31)"]);
    expect(card.rows[2].row).toBeNull();
  });

  it("never turns a missing or withheld value into a number", () => {
    const card = threeLevelCard(levels, new Map([
      ["c", { row: row(2024, null, { value_status: "suppressed" }) }], ["s", { row: null }], ["n", { error: "503" }],
    ]), (level) => level !== "STATE" || true);
    expect(card.rows[0]).toMatchObject({ message: "Published without a value: suppressed" });
    expect(card.rows[1]).toMatchObject({ row: null, message: "Not published for this place" });
    expect(card.rows[2]).toMatchObject({ row: null, message: "503" });
  });

  it("names a grain the measure does not publish", () => {
    const card = threeLevelCard(levels, new Map([["c", { row: row(2024) }]]), (level) => level === "COUNTY");
    expect(card.rows[1].message).toBe("Not published at state grain");
  });
});

describe("the trend", () => {
  const levels = [
    { level: "COUNTY", geoId: "c", name: "Dane County", role: "this place" },
    { level: "NATIONAL", geoId: "n", name: "United States", role: "parent" },
  ];
  const rows = (values, start = 2019) => values.map((value, index) => ({ period_start: `${start + index}-01-01`, period_end: `${start + index}-12-31`, value }));

  it("indexes counts at the first period every line publishes, and states it", () => {
    const trend = buildTrend(levels, new Map([["c", rows(["100", "110", "121"])], ["n", rows(["1000", "1100"], 2020)]]), "index");
    expect(trend.basePeriod).toBe("2020-01-01 – 2020-12-31");
    expect(trend.lines[0].points.map((point) => Number(point.value.toFixed(6)))).toEqual([90.909091, 100, 110]);
    expect(trend.lines[1].points.map((point) => Number(point.value.toFixed(6)))).toEqual([100, 110]);
  });

  it("draws levels as published and counts unpublished periods", () => {
    const trend = buildTrend(levels, new Map([["c", rows(["4.1", null, "3.9"])], ["n", rows(["3.5"])]]), "level");
    expect(trend.basePeriod).toBe("");
    expect(trend.lines[0].points.map((point) => point.value)).toEqual([4.1, 3.9]);
    expect(trend.lines[0].unpublished).toBe(1);
  });

  it("falls back to the primary line alone when no period is shared", () => {
    const trend = buildTrend(levels, new Map([["c", rows(["1", "2"], 2019)], ["n", rows(["5"], 2024)]]), "index");
    expect(trend.lines.map((line) => line.place.name)).toEqual(["Dane County"]);
    expect(trend.unindexed).toEqual(["United States"]);
  });
});

describe("place titles", () => {
  it("are built from accepted segments only", async () => {
    const { placeRouteTitle } = await import("../../../apps/web/lib/routeTitles");
    expect(placeRouteTitle()).toBe("United States");
    expect(placeRouteTitle("wisconsin")).toBe("Wisconsin");
    expect(placeRouteTitle("wisconsin", "dane-county")).toBe("Dane County, Wisconsin");
    expect(placeRouteTitle("55", "025")).toBe("025, 55");
    expect(placeRouteTitle("<script>")).toBe("United States");
  });
});

// Covers: WEB-134 — acs-place-grain: a city or town shares the county's
// address space, so a county keeps its slug and a clashing place takes its
// seven-digit FIPS; chapters no source publishes for places are omitted.
describe("cities and towns", () => {
  const maryland = [
    { geo_id: "state:24|county:005", geo_level: "COUNTY", state_fips: "24", county_fips: "005", county_name: "Baltimore County" },
    { geo_id: "state:24|county:510", geo_level: "COUNTY", state_fips: "24", county_fips: "510", county_name: "Baltimore city" },
  ];
  const places = [
    { geo_id: "state:24|place:04000", geo_level: "PLACE", state_fips: "24", place_fips: "04000", place_name: "Baltimore city" },
    { geo_id: "state:24|place:01600", geo_level: "PLACE", state_fips: "24", place_fips: "01600", place_name: "Annapolis city" },
    { geo_id: "state:24|place:31175", geo_level: "PLACE", state_fips: "24", place_fips: "31175", place_name: "Glen Burnie CDP" },
  ];

  it("gives a place its name's slug unless a county or another place holds it", () => {
    expect(citySegment(places[1], places, maryland)).toBe("annapolis-city");
    expect(citySegment(places[2], places, maryland)).toBe("glen-burnie-cdp");
    // Baltimore city, the county equivalent, keeps `baltimore-city`.
    expect(citySegment(places[0], places, maryland)).toBe("2404000");
  });

  it("resolves a place by slug or seven-digit code, and names the canonical form", () => {
    expect(resolveCitySegment("annapolis-city", places, maryland)).toEqual({ place: places[1], canonical: null });
    expect(resolveCitySegment("2401600", places, maryland)).toEqual({ place: places[1], canonical: "annapolis-city" });
    expect(resolveCitySegment("2404000", places, maryland)).toEqual({ place: places[0], canonical: null });
    expect(resolveCitySegment("atlantis-city", places, maryland)).toEqual({ place: null, canonical: null });
  });

  it("omits the chapters no source publishes for places, with the reason", () => {
    const published = new Map([
      metric("CENSUS_ACS:acs5:B19013_001", ["PLACE", "COUNTY", "STATE", "NATIONAL"]),
      metric("CENSUS_PEP:POPESTIMATE", ["PLACE", "COUNTY", "STATE", "NATIONAL"]),
      metric("FBI_UCR:summarized_violent_crime:V:offense:rate", ["AGENCY", "STATE", "NATIONAL"]),
      metric("BLS:LAU:UNEMP_RATE", ["COUNTY", "STATE"]),
    ]);
    const { shown, omitted } = resolvePlaceChapters("PLACE", published);
    expect(shown.map((resolved) => resolved.chapter.id)).toEqual(["people", "work-money"]);
    const safety = omitted.find((entry) => entry.chapter.id === "safety");
    expect(omissionLine(safety)).toBe("Safety: no published city or town values for this place");
    expect(shown.every((resolved) => !resolved.stateContext)).toBe(true);
  });
});
// Covers: WEB-139 — sub-county-geography: the tract measures are the
// tables the warehouse loads at tract grain, and the legend says how many
// tracts are left uncoloured rather than painting them as zero.
describe("within this county", () => {
  it("offers only the tract tables and states the uncoloured count", async () => {
    const { TRACT_MEASURES, uncolouredSentence } = await import("../../../apps/web/components/WithinCounty.tsx");
    expect(TRACT_MEASURES.map((entry) => entry.code.split(":")[2].split("_")[0])).toEqual([
      "B19013",
      "B01003",
      "B17001",
      "B25064",
      "B25077",
    ]);
    expect(uncolouredSentence(3, 2)).toBe("1 of 3 tracts have no published value and are left uncoloured, not shown as zero.");
    expect(uncolouredSentence(4, 4)).toBe("All 4 tracts have a published value.");
  });
});

// Covers: WEB-137 — bea-regional-accounts: Work and Money shows BEA per capita
// income and real GDP beside the ACS income, each labelled with how it was
// counted, and earnings by industry as BEA's own figures.
describe("BEA income and GDP", () => {
  const work = PLACE_CHAPTERS.find((chapter) => chapter.id === "work-money");
  const slot = (id) => [...work.headline, ...work.depth].find((measure) => measure.id === id);

  it("labels the account and the survey apart on every income slot", () => {
    expect(slot("bea-per-capita-income").basis).toBe("BEA personal income account, current dollars");
    expect(slot("median-household-income").basis).toBe("Survey estimate (American Community Survey)");
    expect(slot("per-capita-income").basis).toBe("Survey estimate (American Community Survey)");
    expect(slot("bea-real-gdp").basis).toBe("BEA county GDP, chained 2017 dollars");
    expect(slot("bea-real-gdp").candidates).toEqual(["BEA:CAGDP1:1"]);
  });

  it("groups the earnings sectors into one table and states that nothing is combined", () => {
    const earnings = work.depth.filter((measure) => measure.group === "bea-earnings");
    expect(earnings).toHaveLength(21);
    expect(earnings.every((measure) => measure.candidates[0].startsWith("BEA:CAINC5N:"))).toBe(true);
    expect(work.caveat).toContain("never combined with the survey's income");
  });
});

// Covers: WEB-135 — bls-qcew-county-wages: jobs located here and residents
// who work are labelled with how each was counted, and never share a slot.
describe("jobs located here beside residents who work", () => {
  const work = PLACE_CHAPTERS.find((chapter) => chapter.id === "work-money");
  const byId = Object.fromEntries([...work.headline, ...work.depth].map((measure) => [measure.id, measure]));

  it("labels the QCEW and LAUS cards with their bases", () => {
    expect(byId["qcew-jobs"].basis).toBe("Jobs located here, counted by employers (QCEW)");
    expect(byId["unemployment-rate"].basis).toBe("Residents who work, counted where they live (LAUS)");
    expect(byId["qcew-weekly-wage"].basis).toBe(byId["qcew-jobs"].basis);
  });

  it("never lists a QCEW and a LAUS identity in one slot", () => {
    for (const measure of [...work.headline, ...work.depth]) {
      const sources = new Set(measure.candidates.map((code) => code.split(":")[0]));
      expect(sources.has("BLS") && sources.has("BLS_QCEW"), measure.id).toBe(false);
    }
  });

  it("builds the industry mix from private sector employment only", () => {
    const mix = work.depth.filter((measure) => measure.group === "industry-mix");
    expect(mix).toHaveLength(21);
    expect(mix.every((measure) => /^BLS_QCEW:employment:[0-9-]+:5$/.test(measure.candidates[0]))).toBe(true);
    expect(mix.find((measure) => measure.id === "qcew-sector-62").label).toBe("Health care and social assistance");
  });
});

// Covers: WEB-136 — census-building-permits: the Housing chapter shows homes
// authorized beside the stock, labelled as authorizations, with a monthly trend.
describe("homes authorized", () => {
  const housing = PLACE_CHAPTERS.find((chapter) => chapter.id === "housing");

  it("labels every permits slot as an authorization and states what was reported", () => {
    const permits = housing.headline.filter((measure) => measure.candidates[0].startsWith("CENSUS_BPS:"));
    expect(permits.map((measure) => measure.id)).toEqual(["bps-single-family-year", "bps-multifamily-year", "bps-single-family-month"]);
    expect(permits.every((measure) => measure.basis === "Authorized by building permits, not started or completed")).toBe(true);
    expect(permits.every((measure) => measure.showReported)).toBe(true);
  });

  it("draws the monthly permits as a second trend after the rent trend", () => {
    expect(housing.trend).toEqual({ measureId: "median-gross-rent", scale: "level" });
    expect(housing.moreTrends).toEqual([{ measureId: "bps-single-family-month", scale: "level" }]);
    expect(housing.headline.some((measure) => measure.id === housing.moreTrends[0].measureId)).toBe(true);
  });
});

// Covers: WEB-138 — irs-county-migration: the note shown beside PEP net
// migration says the IRS counts tax returns and the two populations differ.
describe("migration population note", () => {
  it("names both sources and refuses a net figure", () => {
    expect(MIGRATION_POPULATION_NOTE).toContain("tax returns");
    expect(MIGRATION_POPULATION_NOTE).toContain("Population Estimates Program");
    expect(MIGRATION_POPULATION_NOTE).toContain("different populations");
    expect(MIGRATION_POPULATION_NOTE).toContain("no net figure");
  });
});
