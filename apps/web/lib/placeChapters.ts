// The place almanac's chapter contract (place-pages).
//
// Every place page -- the nation, a state, a county -- reads the same chapters
// in the same order, so a reader who learned one page can read any other.
// Like `lib/productTemplates.ts`, this is presentation over published catalog
// identities: it names candidate metric codes, resolves each against the live
// catalog, and computes nothing a source did not publish. The one arithmetic
// it performs is the trend's index (a value divided by the same series' own
// base-period value), and only for count measures, with the base stated.
//
// A chapter whose measures publish nothing for the page's grain is omitted
// and named once in the footer. It is never shown empty, and a missing value
// is never a zero.

import type { GeographySummary, MetricSummary } from "./api/types";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { observationPeriodLabel } from "./observationAccess";

/** The grains a place page exists for, broadest first. */
export const PLACE_LEVELS = ["NATIONAL", "STATE", "COUNTY", "PLACE"] as const;
export type PlaceLevel = (typeof PLACE_LEVELS)[number];

/**
 * How a chapter's trend draws its reference lines.
 *
 * `index`: the measure is a count, so a county and the nation differ by
 * orders of magnitude and their raw lines cannot share an axis. Each line is
 * divided by its own value in the first period all of them publish, which is
 * stated as the base. `level`: the measure is already a rate, a median, or a
 * share, so the published values share an axis as they are.
 */
export type TrendScale = "index" | "level";

export interface PlaceMeasure {
  /** Stable slot identity within the chapter. */
  id: string;
  /** What this measure is, in the reader's terms. */
  label: string;
  /** Catalog identities that can answer, in preference order. */
  candidates: string[];
  /**
   * Who or what the published number counts (the Census universe), shown
   * beside the value so a count of households is never read as people.
   */
  universe?: string;
  note?: string;
  /**
   * How the figure was counted, when two measures of one subject are counted
   * differently -- an account of a county's income (BEA) against a survey of
   * its households (ACS). Shown on the card so neither is read as the other
   * (bea-regional-accounts).
   */
  basis?: string;
  /** Depth rows rendered together as one table rather than a list. */
  group?: "bea-earnings" | "industry-mix";
  /**
   * The card also states what was reported directly, read from the row's
   * `reported_value` dimension, beside an estimate that imputes for
   * non-reporters (census-building-permits).
   */
  showReported?: boolean;
}

export interface PlaceChapter {
  id: string;
  title: string;
  description: string;
  /** Shown as three-level cards: this place, its state, and the nation. */
  headline: PlaceMeasure[];
  /** Shown for this place alone, each with its universe and period. */
  depth: PlaceMeasure[];
  /** The headline measure the chapter's trend draws. */
  trend: { measureId: string; scale: TrendScale };
  /** Further headline measures drawn as their own trend, after the first. */
  moreTrends?: { measureId: string; scale: TrendScale }[];
  /** The caveat every footer repeats, in the source's own terms. */
  caveat: string;
  /**
   * A county page shows the state's figures, labelled as state context, when
   * the program publishes no county grain (the FBI's summarized offenses).
   */
  stateContextAtCounty?: boolean;
}

/** BEA regional accounts: one table and line (bea-regional-accounts). */
const bea = (table: string, line: string): string[] => [`BEA:${table}:${line}`];

const SURVEY_BASIS = "Survey of households (ACS)";
const ACCOUNT_BASIS = "BEA personal income account, current dollars";

/** CAINC5N earnings by place of work, by sector, in BEA's order. */
export const BEA_EARNINGS_SECTORS: readonly (readonly [string, string])[] = [
  ["81", "Farm"],
  ["100", "Forestry, fishing, and related activities"],
  ["200", "Mining, quarrying, and oil and gas extraction"],
  ["300", "Utilities"],
  ["400", "Construction"],
  ["500", "Manufacturing"],
  ["600", "Wholesale trade"],
  ["700", "Retail trade"],
  ["800", "Transportation and warehousing"],
  ["900", "Information"],
  ["1000", "Finance and insurance"],
  ["1100", "Real estate and rental and leasing"],
  ["1200", "Professional, scientific, and technical services"],
  ["1300", "Management of companies and enterprises"],
  ["1400", "Administrative and waste services"],
  ["1500", "Educational services"],
  ["1600", "Health care and social assistance"],
  ["1700", "Arts, entertainment, and recreation"],
  ["1800", "Accommodation and food services"],
  ["1900", "Other services"],
  ["2000", "Government and government enterprises"],
];

/** QCEW: one measure for one industry and ownership (bls-qcew-county-wages). */
const qcew = (measure: string, industry: string, ownership: string): string[] => [
  `BLS_QCEW:${measure}:${industry}:${ownership}`,
];

const ESTABLISHMENT_BASIS = "Jobs located here, counted by employers (QCEW)";
const HOUSEHOLD_BASIS = "Residents who work, counted where they live (LAUS)";

/** The NAICS sectors of the industry-mix table, in QCEW's order. */
export const INDUSTRY_MIX_SECTORS: readonly (readonly [string, string])[] = [
  ["11", "Agriculture, forestry, fishing and hunting"],
  ["21", "Mining, quarrying, and oil and gas extraction"],
  ["22", "Utilities"],
  ["23", "Construction"],
  ["31-33", "Manufacturing"],
  ["42", "Wholesale trade"],
  ["44-45", "Retail trade"],
  ["48-49", "Transportation and warehousing"],
  ["51", "Information"],
  ["52", "Finance and insurance"],
  ["53", "Real estate and rental and leasing"],
  ["54", "Professional, scientific, and technical services"],
  ["55", "Management of companies and enterprises"],
  ["56", "Administrative and waste services"],
  ["61", "Educational services"],
  ["62", "Health care and social assistance"],
  ["71", "Arts, entertainment, and recreation"],
  ["72", "Accommodation and food services"],
  ["81", "Other services"],
  ["92", "Public administration"],
  ["99", "Unclassified"],
];

/** Building Permits Survey: one measure, structure type and frequency. */
const bps = (measure: string, structure: string, frequency: string): string[] => [
  `CENSUS_BPS:${measure}:${structure}:${frequency}`,
];

const AUTHORIZED_BASIS = "Authorized by building permits, not started or completed";

const acs = (variable: string): string[] => [
  `CENSUS_ACS:acs5:${variable}`,
  `CENSUS_ACS:acs1:${variable}`,
];

/**
 * Census SAIPE and SAHIE: model-based annual estimates for every county
 * (census-saipe-sahie). They resemble ACS figures and are a different method,
 * so they are their own slots with their own candidates -- never an ACS
 * slot's fallback, and never replaced by one.
 */
const saipe = (measure: string): string[] => [`CENSUS_SAIPE_SAHIE:saipe:${measure}`];
const sahie = (measure: string): string[] => [`CENSUS_SAIPE_SAHIE:sahie:${measure}`];

/**
 * How a published figure was made, in the reader's terms, from the identity
 * that answered. A survey estimate and a model-based estimate of the same
 * quantity sit side by side on a place page; this label is what keeps a
 * reader from taking one for the other. `null` for a source whose method the
 * card's own note already states.
 */
export function measureBasis(metricCode: string | null | undefined): string | null {
  if (!metricCode) return null;
  if (metricCode.startsWith("CENSUS_ACS:")) return "Survey estimate (American Community Survey)";
  if (metricCode.startsWith("CENSUS_SAIPE_SAHIE:saipe:")) return "Model-based annual estimate (SAIPE)";
  if (metricCode.startsWith("CENSUS_SAIPE_SAHIE:sahie:")) return "Model-based annual estimate (SAHIE)";
  return null;
}

const SAE_CAVEAT =
  "SAIPE and SAHIE figures are the Census Bureau's model-based estimates for a single year, shown with their 90 percent interval; they are not the survey estimates beside them and are never substituted for them.";

const ACS_CAVEAT =
  "American Community Survey estimates carry a margin of error, shown beside each value. A value labelled with its universe counts that universe only; no share is computed here.";

/**
 * The chapters, in reading order. Order and candidates are pinned by
 * `tests/frontend/unit/place-chapters.test.js`; the studio and the compare
 * page reuse this module unchanged.
 */
export const PLACE_CHAPTERS: readonly PlaceChapter[] = [
  {
    id: "people",
    title: "People",
    description: "Who lives here, as the Census Bureau publishes it.",
    headline: [
      {
        id: "population-estimate",
        label: "Resident population estimate",
        candidates: ["CENSUS_PEP:POPESTIMATE"],
        universe: "people",
        note: "Population Estimates Program vintage, a different method from the survey estimates below.",
      },
      {
        id: "median-age",
        label: "Median age",
        candidates: acs("B01002_001"),
      },
    ],
    depth: [
      { id: "total-population", label: "Total population (survey estimate)", candidates: acs("B01003_001"), universe: "people" },
      { id: "under-18", label: "People under 18", candidates: acs("B09001_001"), universe: "people under 18" },
      { id: "65-plus", label: "People 65 and over", candidates: acs("B09020_001"), universe: "people 65 and over" },
      { id: "living-alone", label: "Householders living alone", candidates: acs("B11001_008"), universe: "households" },
      { id: "never-married-men", label: "Men 15 and over who never married", candidates: acs("B12001_003"), universe: "people 15 and over" },
      { id: "never-married-women", label: "Women 15 and over who never married", candidates: acs("B12001_012"), universe: "people 15 and over" },
      { id: "same-house", label: "People in the same house as a year ago", candidates: acs("B07001_017"), universe: "people 1 year and over" },
      { id: "moved-within-county", label: "People who moved within the county in the past year", candidates: acs("B07001_033"), universe: "people 1 year and over" },
      { id: "foreign-born", label: "Foreign-born residents", candidates: acs("B05002_013"), universe: "people" },
      { id: "veterans", label: "Veterans", candidates: acs("B21001_002"), universe: "civilians 18 and over" },
    ],
    trend: { measureId: "population-estimate", scale: "index" },
    caveat: `Population estimates and survey estimates come from different methods and vintages and are shown separately. ${ACS_CAVEAT}`,
  },
  {
    id: "work-money",
    title: "Work and Money",
    description: "Earnings, income, and work, each in its own survey universe.",
    headline: [
      {
        id: "median-household-income",
        label: "Median household income",
        candidates: acs("B19013_001"),
        note: "In the survey year's inflation-adjusted dollars.",
        basis: SURVEY_BASIS,
      },
      {
        id: "bea-per-capita-income",
        label: "Per capita personal income",
        candidates: bea("CAINC1", "3"),
        note: "All personal income, including transfer receipts and employer contributions, divided by BEA's own population; not the survey income above.",
        basis: ACCOUNT_BASIS,
      },
      {
        id: "bea-real-gdp",
        label: "Gross domestic product, real",
        candidates: bea("CAGDP1", "1"),
        note: "Thousands of chained 2017 dollars, so years compare without inflation; never added to current-dollar figures.",
        basis: "BEA county GDP, chained 2017 dollars",
      },
      {
        id: "unemployment-rate",
        label: "Unemployment rate",
        candidates: ["BLS:LAU:UNEMP_RATE"],
        note: "Local Area Unemployment Statistics, a different universe from the survey counts below.",
        basis: HOUSEHOLD_BASIS,
      },
      {
        id: "qcew-jobs",
        label: "Jobs located here",
        candidates: qcew("employment", "10", "0"),
        note: "Every covered job at an employer in this place, wherever its worker lives; not the residents who work.",
        basis: ESTABLISHMENT_BASIS,
      },
      {
        id: "saipe-median-household-income",
        label: "Median household income, single year",
        candidates: saipe("SAEMHI"),
        note: "The Bureau's one-year model-based figure, beside the survey estimate.",
      },
      {
        id: "qcew-weekly-wage",
        label: "Average weekly wage of jobs located here",
        candidates: qcew("avg_weekly_wage", "10", "0"),
        basis: ESTABLISHMENT_BASIS,
      },
      {
        id: "saipe-poverty-rate",
        label: "People in poverty, all ages",
        candidates: saipe("SAEPOVRTALL"),
        universe: "people whose poverty status is determined",
      },
    ],
    depth: [
      { id: "per-capita-income", label: "Per capita income", candidates: acs("B19301_001"), basis: SURVEY_BASIS },
      { id: "gini", label: "Gini index of income inequality", candidates: acs("B19083_001"), note: "0 is perfect equality and 1 is one household holding all income." },
      { id: "below-poverty", label: "People with income below the poverty level", candidates: acs("B17001_002"), universe: "people whose poverty status is determined" },
      { id: "saipe-poverty-count", label: "People in poverty, all ages, single year", candidates: saipe("SAEPOVALL"), universe: "people whose poverty status is determined" },
      { id: "saipe-child-poverty-rate", label: "Children under 18 in poverty", candidates: saipe("SAEPOVRT0_17"), universe: "related children and others under 18" },
      { id: "households-under-10k", label: "Households with income under $10,000", candidates: acs("B19001_002"), universe: "households" },
      { id: "households-200k-plus", label: "Households with income of $200,000 or more", candidates: acs("B19001_017"), universe: "households" },
      { id: "labor-force", label: "People in the labor force", candidates: acs("B23025_002"), universe: "people 16 and over" },
      { id: "unemployed", label: "Unemployed people (survey count)", candidates: acs("B23025_005"), universe: "people 16 and over" },
      { id: "employed-by-industry", label: "Employed residents (industry and occupation table total)", candidates: acs("C24050_001"), universe: "civilian employed people 16 and over" },
      { id: "worked-from-home", label: "Workers who worked from home", candidates: acs("B08301_021"), universe: "workers 16 and over" },
      { id: "public-transportation", label: "Workers commuting by public transportation", candidates: acs("B08301_010"), universe: "workers 16 and over" },
      { id: "commute-90-minutes", label: "Workers commuting 90 minutes or more", candidates: acs("B08303_013"), universe: "workers 16 and over who did not work from home" },
      ...BEA_EARNINGS_SECTORS.map(([line, title]) => ({
        id: `bea-earnings-${line}`,
        label: title,
        candidates: bea("CAINC5N", line),
        universe: "earnings by place of work",
        basis: ACCOUNT_BASIS,
        group: "bea-earnings" as const,
      })),
      ...INDUSTRY_MIX_SECTORS.map(([code, title]) => ({
        id: `qcew-sector-${code}`,
        label: title,
        candidates: qcew("employment", code, "5"),
        universe: "private jobs located here",
        basis: ESTABLISHMENT_BASIS,
        group: "industry-mix" as const,
      })),
    ],
    trend: { measureId: "unemployment-rate", scale: "level" },
    caveat: `The unemployment rate is BLS's count of residents; the jobs and the industry mix are QCEW's count of jobs located here, by employer, and the two are never added or subtracted. The survey counts are the ACS's, on a different universe. Personal income, earnings and GDP are BEA's accounts, in current or chained dollars as each card says; they are never combined with the survey's income. ${ACS_CAVEAT} ${SAE_CAVEAT}`,
  },
  {
    id: "housing",
    title: "Housing",
    description: "What housing costs and what housing exists here.",
    headline: [
      {
        id: "median-gross-rent",
        label: "Median gross rent",
        candidates: acs("B25064_001"),
        note: "Rent plus the utilities the renter pays.",
      },
      {
        id: "median-home-value",
        label: "Median home value",
        candidates: acs("B25077_001"),
        note: "The owner's own estimate, which is what the ACS asks.",
      },
      {
        id: "bps-single-family-year",
        label: "Single-family homes authorized, newest year",
        candidates: bps("units", "1_unit", "annual"),
        basis: AUTHORIZED_BASIS,
        showReported: true,
      },
      {
        id: "bps-multifamily-year",
        label: "Homes in buildings of 5 or more units authorized, newest year",
        candidates: bps("units", "5_plus_units", "annual"),
        basis: AUTHORIZED_BASIS,
        showReported: true,
      },
      {
        id: "bps-single-family-month",
        label: "Single-family homes authorized, by month",
        candidates: bps("units", "1_unit", "monthly"),
        basis: AUTHORIZED_BASIS,
        showReported: true,
      },
    ],
    depth: [
      { id: "housing-units", label: "Housing units", candidates: acs("B25001_001"), universe: "housing units" },
      { id: "owner-occupied", label: "Owner-occupied housing units", candidates: acs("B25003_002"), universe: "occupied housing units" },
      { id: "renter-occupied", label: "Renter-occupied housing units", candidates: acs("B25003_003"), universe: "occupied housing units" },
      { id: "vacant", label: "Vacant housing units", candidates: acs("B25002_003"), universe: "housing units", note: "Includes seasonal and other vacancies, not only units for rent or sale." },
      { id: "single-family-detached", label: "Single-family detached units", candidates: acs("B25024_002"), universe: "housing units" },
      { id: "built-2020-or-later", label: "Units built in 2020 or later", candidates: acs("B25034_002"), universe: "housing units", note: "Census redraws the year-built bands between vintages; the band is the one this vintage published." },
      { id: "no-vehicle", label: "Households with no vehicle available", candidates: acs("B08201_002"), universe: "households" },
      { id: "broadband", label: "Households with a broadband subscription of any type", candidates: acs("B28002_004"), universe: "households" },
      { id: "no-internet", label: "Households with no internet access", candidates: acs("B28002_013"), universe: "households" },
    ],
    trend: { measureId: "median-gross-rent", scale: "level" },
    moreTrends: [{ measureId: "bps-single-family-month", scale: "level" }],
    caveat: `${ACS_CAVEAT} Permit figures count homes authorized, not started or completed; the Bureau's estimate imputes for jurisdictions that did not report, and what was reported directly is stated beside it.`,
  },
  {
    id: "health",
    title: "Health",
    description: "Published health measures, each with its own interval.",
    headline: [
      {
        id: "obesity",
        label: "Obesity among adults (age-adjusted)",
        candidates: ["CDC:places_county:OBESITY:AgeAdjPrv"],
      },
      {
        id: "diabetes",
        label: "Diagnosed diabetes among adults (age-adjusted)",
        candidates: ["CDC:places_county:DIABETES:AgeAdjPrv"],
      },
      {
        id: "sahie-uninsured-rate",
        label: "Uninsured people under 65",
        candidates: sahie("PCTUI"),
        universe: "people under 65, all incomes",
      },
    ],
    depth: [
      { id: "uninsured-under-19", label: "People under 19 with no health insurance", candidates: acs("B27010_017"), universe: "civilian noninstitutionalized people" },
      { id: "uninsured-19-34", label: "People 19 to 34 with no health insurance", candidates: acs("B27010_033"), universe: "civilian noninstitutionalized people" },
      { id: "uninsured-35-64", label: "People 35 to 64 with no health insurance", candidates: acs("B27010_050"), universe: "civilian noninstitutionalized people" },
      { id: "uninsured-65-plus", label: "People 65 and over with no health insurance", candidates: acs("B27010_066"), universe: "civilian noninstitutionalized people" },
      { id: "sahie-uninsured-count", label: "Uninsured people under 65, single year", candidates: sahie("NUI"), universe: "people under 65, all incomes" },
    ],
    trend: { measureId: "obesity", scale: "level" },
    caveat:
      `CDC PLACES values are model-based estimates among adults with a published confidence interval, not clinical counts. Insurance counts are ACS estimates with a margin of error. ${SAE_CAVEAT}`,
  },
  {
    id: "safety",
    title: "Safety",
    description: "Reported crime from the FBI Uniform Crime Reporting Program, bounded by which agencies reported.",
    headline: [
      {
        id: "violent-crime-rate",
        label: "Violent crime rate, as the program published it",
        candidates: ["FBI_UCR:summarized_violent_crime:V:offense:rate"],
        note: "The program's own rate on its own denominator; nothing here divides a count by a population.",
      },
      {
        id: "property-crime-rate",
        label: "Property crime rate, as the program published it",
        candidates: ["FBI_UCR:summarized_property_crime:P:offense:rate"],
      },
    ],
    depth: [],
    trend: { measureId: "violent-crime-rate", scale: "level" },
    caveat:
      "A period no agency reported is not zero crime. The FBI publishes these offenses for states and the nation, not for counties.",
    stateContextAtCounty: true,
  },
  {
    id: "land-farms",
    title: "Land and Farms",
    description: "Agricultural measures from USDA NASS, subject to its disclosure suppression.",
    headline: [
      {
        id: "corn-acres-harvested",
        label: "Corn for grain, acres harvested",
        candidates: [
          "USDA_NASS:corn_survey_annual:cfc67a954a17ac5a60541c90c59b5add41171f8b4b41f246db1e54eddfd65a11",
        ],
        universe: "acres",
      },
    ],
    depth: [],
    trend: { measureId: "corn-acres-harvested", scale: "index" },
    caveat: "NASS withholds small cells for disclosure; a withheld value is not a zero harvest.",
  },
  {
    id: "change",
    title: "Change",
    description: "The components of population change the Population Estimates Program publishes.",
    headline: [
      {
        id: "net-migration-rate",
        label: "Net migration rate",
        candidates: ["CENSUS_PEP:RNETMIG"],
        note: "Per 1,000 residents, as the program published it.",
      },
      {
        id: "natural-change-rate",
        label: "Natural change rate (births minus deaths)",
        candidates: ["CENSUS_PEP:RNATURALCHG"],
        note: "Per 1,000 residents, as the program published it.",
      },
    ],
    depth: [
      { id: "births", label: "Births", candidates: ["CENSUS_PEP:BIRTHS"], universe: "people" },
      { id: "deaths", label: "Deaths", candidates: ["CENSUS_PEP:DEATHS"], universe: "people" },
      { id: "domestic-migration", label: "Net domestic migration", candidates: ["CENSUS_PEP:DOMESTICMIG"], universe: "people" },
      { id: "international-migration", label: "Net international migration", candidates: ["CENSUS_PEP:INTERNATIONALMIG"], universe: "people" },
      { id: "population-change", label: "Numeric population change", candidates: ["CENSUS_PEP:NPOPCHG"], universe: "people" },
    ],
    trend: { measureId: "net-migration-rate", scale: "level" },
    caveat: "Components are estimates for the year ending July 1 of the vintage, and the residual the program publishes is not shown as a component.",
  },
];

/** Every candidate identity the chapters could ask the catalog for. */
export function placeChapterMetricCodes(
  chapters: readonly PlaceChapter[] = PLACE_CHAPTERS,
): string[] {
  return [
    ...new Set(
      chapters.flatMap((chapter) =>
        [...chapter.headline, ...chapter.depth].flatMap((measure) => measure.candidates),
      ),
    ),
  ];
}

// --- Addresses ---------------------------------------------------------------

/**
 * The URL segment for a name: lower case, letters and digits, hyphens.
 *
 * Presentation only. The address resolves by matching this against the
 * slugs of the catalog's own rows, and the identity a page reads is the
 * catalog row's FIPS-based `geo_id`.
 */
export function placeSlug(name: string | null | undefined): string {
  return String(name || "")
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/** A segment the routes accept at all; anything else is a server 404. */
export const PLACE_SEGMENT = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

export function stateName(state: GeographySummary): string {
  return String(state.state_name || state.geo_name || state.geo_id);
}

export function countyName(county: GeographySummary): string {
  return String(county.county_name || county.geo_name || county.geo_id);
}

/**
 * A state's segment. Its name's slug, unless another state's name slugs the
 * same way, in which case its two-digit FIPS code (never ambiguous).
 */
export function stateSegment(state: GeographySummary, states: readonly GeographySummary[]): string {
  const slug = placeSlug(stateName(state));
  const clash = states.some(
    (other) => other.geo_id !== state.geo_id && placeSlug(stateName(other)) === slug,
  );
  return slug && !clash ? slug : String(state.state_fips || "");
}

/** A county's segment within its state, by the same rule with its FIPS. */
export function countySegment(county: GeographySummary, counties: readonly GeographySummary[]): string {
  const slug = placeSlug(countyName(county));
  const clash = counties.some(
    (other) =>
      other.geo_id !== county.geo_id &&
      other.state_fips === county.state_fips &&
      placeSlug(countyName(other)) === slug,
  );
  return slug && !clash ? slug : `${county.state_fips || ""}${county.county_fips || ""}`;
}

/** A city, town, village or census-designated place, as the catalog names it. */
export function cityName(place: GeographySummary): string {
  return String(place.place_name || place.geo_name || place.geo_id);
}

/**
 * The address segment of a city or town (acs-place-grain).
 *
 * Its name's slug, unless that slug is taken in its state -- by another place
 * or by a county's own segment, because the two share `/us/<state>/<segment>`
 * and a county keeps the plain slug it already had. Baltimore city is both a
 * county equivalent and a place. A taken slug falls back to the seven-digit
 * state-and-place FIPS, which cannot meet a county's five-digit one.
 */
export function citySegment(
  place: GeographySummary,
  places: readonly GeographySummary[],
  counties: readonly GeographySummary[],
): string {
  const slug = placeSlug(cityName(place));
  const fallback = `${place.state_fips || ""}${place.place_fips || ""}`;
  if (!slug) return fallback;
  const placeClash = places.some(
    (other) =>
      other.geo_id !== place.geo_id &&
      other.state_fips === place.state_fips &&
      placeSlug(cityName(other)) === slug,
  );
  const countyClash = counties.some(
    (county) => county.state_fips === place.state_fips && countySegment(county, counties) === slug,
  );
  return placeClash || countyClash ? fallback : slug;
}

export interface SegmentMatch {
  place: GeographySummary | null;
  /** The canonical segment, when the address used another accepted form. */
  canonical: string | null;
}

/** Resolve a state segment against the catalog's states: slug or FIPS. */
export function resolveStateSegment(
  segment: string,
  states: readonly GeographySummary[],
): SegmentMatch {
  const found =
    states.find((state) => stateSegment(state, states) === segment) ||
    states.find((state) => state.state_fips === segment) ||
    null;
  if (!found) return { place: null, canonical: null };
  const canonical = stateSegment(found, states);
  return { place: found, canonical: canonical === segment ? null : canonical };
}

/**
 * Resolve a county segment against the catalog's counties of one state:
 * slug, its five-digit FIPS, or its three-digit county FIPS.
 */
export function resolveCountySegment(
  segment: string,
  counties: readonly GeographySummary[],
): SegmentMatch {
  const found =
    counties.find((county) => countySegment(county, counties) === segment) ||
    counties.find((county) => `${county.state_fips}${county.county_fips}` === segment) ||
    counties.find((county) => county.county_fips === segment) ||
    null;
  if (!found) return { place: null, canonical: null };
  const canonical = countySegment(found, counties);
  return { place: found, canonical: canonical === segment ? null : canonical };
}

/** Resolve a city or town segment: its slug, or its seven-digit FIPS. */
export function resolveCitySegment(
  segment: string,
  places: readonly GeographySummary[],
  counties: readonly GeographySummary[],
): SegmentMatch {
  const found =
    places.find((place) => citySegment(place, places, counties) === segment) ||
    places.find((place) => `${place.state_fips}${place.place_fips}` === segment) ||
    null;
  if (!found) return { place: null, canonical: null };
  const canonical = citySegment(found, places, counties);
  return { place: found, canonical: canonical === segment ? null : canonical };
}

export function placePath(stateSeg?: string | null, countySeg?: string | null): string {
  if (!stateSeg) return "/us";
  return countySeg ? `/us/${stateSeg}/${countySeg}` : `/us/${stateSeg}`;
}

// --- Chapters for one page --------------------------------------------------

export interface ResolvedPlaceMeasure {
  measure: PlaceMeasure;
  metric: MetricSummary | null;
  metricCode: string;
  /** Whether the catalog says the measure publishes at the grain asked. */
  publishedAtGrain: boolean;
}

export interface ResolvedChapter {
  chapter: PlaceChapter;
  headline: ResolvedPlaceMeasure[];
  depth: ResolvedPlaceMeasure[];
  /** The grain the chapter's values are read at for this page. */
  readGrain: PlaceLevel;
  /** True when a county page reads its state's values as context. */
  stateContext: boolean;
}

export interface OmittedChapter {
  chapter: PlaceChapter;
  reason: string;
}

const GRAIN_WORDS: Record<PlaceLevel, string> = {
  NATIONAL: "national",
  STATE: "state",
  COUNTY: "county",
  PLACE: "city or town",
};

function resolveMeasure(
  measure: PlaceMeasure,
  metricsByCode: ReadonlyMap<string, MetricSummary>,
  grain: PlaceLevel,
): ResolvedPlaceMeasure {
  const code = measure.candidates.find((candidate) => metricsByCode.has(candidate)) || "";
  const metric = code ? metricsByCode.get(code) || null : null;
  const grains = metric?.valid_geo_grains;
  return {
    measure,
    metric,
    metricCode: code,
    publishedAtGrain: Boolean(metric) && (!Array.isArray(grains) || grains.includes(grain)),
  };
}

/**
 * Which chapters this page shows, and which it names as omitted.
 *
 * Decided from the catalog's published grains. A chapter whose every measure
 * is published at the grain but answers nothing for this place is omitted
 * later, by `chapterHasValues`, once the answers are in.
 */
export function resolvePlaceChapters(
  level: PlaceLevel,
  metricsByCode: ReadonlyMap<string, MetricSummary>,
  chapters: readonly PlaceChapter[] = PLACE_CHAPTERS,
): { shown: ResolvedChapter[]; omitted: OmittedChapter[] } {
  const shown: ResolvedChapter[] = [];
  const omitted: OmittedChapter[] = [];
  for (const chapter of chapters) {
    const atGrain = [...chapter.headline, ...chapter.depth].map((measure) =>
      resolveMeasure(measure, metricsByCode, level),
    );
    if (atGrain.some((entry) => entry.publishedAtGrain)) {
      shown.push({
        chapter,
        headline: atGrain.slice(0, chapter.headline.length),
        depth: atGrain.slice(chapter.headline.length),
        readGrain: level,
        stateContext: false,
      });
      continue;
    }
    if (level === "COUNTY" && chapter.stateContextAtCounty) {
      const atState = [...chapter.headline, ...chapter.depth].map((measure) =>
        resolveMeasure(measure, metricsByCode, "STATE"),
      );
      if (atState.some((entry) => entry.publishedAtGrain)) {
        shown.push({
          chapter,
          headline: atState.slice(0, chapter.headline.length),
          depth: atState.slice(chapter.headline.length),
          readGrain: "STATE",
          stateContext: true,
        });
        continue;
      }
    }
    omitted.push({
      chapter,
      reason: `no published ${GRAIN_WORDS[level]} values for this place`,
    });
  }
  return { shown, omitted };
}

/** The footer's one line per omitted chapter. */
export function omissionLine(omitted: OmittedChapter): string {
  return `${omitted.chapter.title}: ${omitted.reason}`;
}

// --- Three-level cards ------------------------------------------------------

export interface LevelPlace {
  level: PlaceLevel;
  geoId: string;
  name: string;
  /** "state context" when a county page reads its state's value. */
  role: "this place" | "state context" | "parent";
}

export interface LevelAnswer {
  row: ObservationRow | null;
  /** An error the request raised, stated rather than shown as a gap. */
  error?: string;
}

export interface CardRow {
  place: LevelPlace;
  /** The published row shown, or null with `message` saying why. */
  row: ObservationRow | null;
  message: string;
}

export interface ThreeLevelCard {
  period: string;
  rows: CardRow[];
}

/**
 * One measure for this place and its parents, in one period.
 *
 * The period is the first row's (this place's, or its state's on a county
 * page reading state context). A parent whose newest published period is a
 * different one says so and shows no number, because a different period
 * beside the first would read as the same year.
 */
export function threeLevelCard(
  levels: readonly LevelPlace[],
  answers: ReadonlyMap<string, LevelAnswer>,
  publishedAt: (level: PlaceLevel) => boolean,
): ThreeLevelCard {
  const anchor = levels.find((place) => answers.get(place.geoId)?.row) || null;
  const period = anchor ? observationPeriodLabel(answers.get(anchor.geoId)!.row) : "";
  const rows = levels.map((place): CardRow => {
    const answer = answers.get(place.geoId);
    if (!publishedAt(place.level)) {
      return { place, row: null, message: `Not published at ${GRAIN_WORDS[place.level]} grain` };
    }
    if (answer?.error) return { place, row: null, message: answer.error };
    const row = answer?.row ?? null;
    if (!row) return { place, row: null, message: "Not published for this place" };
    const own = observationPeriodLabel(row);
    if (period && own !== period) {
      return { place, row: null, message: `Not published for ${period} (newest published: ${own})` };
    }
    if (publishedNumber(row.value) === null) {
      return {
        place,
        row,
        message: row.value_status ? `Published without a value: ${String(row.value_status)}` : "Published without a value",
      };
    }
    return { place, row, message: "" };
  });
  return { period, rows };
}

/** Whether any measure in the chapter answered a value for this place. */
export function chapterHasValues(
  resolved: ResolvedChapter,
  valueFor: (measure: ResolvedPlaceMeasure) => ObservationRow | null,
): boolean {
  return [...resolved.headline, ...resolved.depth].some((measure) => {
    const row = valueFor(measure);
    return Boolean(row) && publishedNumber(row!.value) !== null;
  });
}

// --- Trend ------------------------------------------------------------------

export interface TrendPoint {
  period: string;
  time: number;
  value: number;
  published: number;
}

export interface TrendLine {
  place: LevelPlace;
  points: TrendPoint[];
  /** Periods this place published without a number, counted. */
  unpublished: number;
}

export interface TrendModel {
  lines: TrendLine[];
  scale: TrendScale;
  /** For an index, the base period every line is divided at. */
  basePeriod: string;
  /** Lines that could not be indexed, because they lack the base period. */
  unindexed: string[];
}

function rowTime(row: ObservationRow): number {
  const raw = String(row.period_start || row.observation_date || "");
  const time = Date.parse(raw);
  return Number.isFinite(time) ? time : Number.NaN;
}

/**
 * The chapter trend: this place as the primary line, its parents as
 * reference lines.
 *
 * For a count (`index`), each line is divided by its own value in the
 * earliest period every line publishes, times 100, and that period is
 * returned as the stated base; a line without it is left out and named.
 * For anything else the published values are drawn as they are.
 */
export function buildTrend(
  levels: readonly LevelPlace[],
  histories: ReadonlyMap<string, ObservationRow[]>,
  scale: TrendScale,
): TrendModel {
  const raw = levels.map((place) => {
    const rows = histories.get(place.geoId) || [];
    const points = rows
      .map((row) => ({ row, number: publishedNumber(row.value), time: rowTime(row) }))
      .filter((entry) => entry.number !== null && Number.isFinite(entry.time))
      .map((entry) => ({
        period: observationPeriodLabel(entry.row),
        time: entry.time,
        value: entry.number as number,
        published: entry.number as number,
      }))
      .sort((left, right) => left.time - right.time);
    return { place, points, unpublished: rows.length - points.length };
  }).filter((line) => line.points.length > 0 || line.unpublished > 0);

  if (scale === "level") {
    return { lines: raw, scale, basePeriod: "", unindexed: [] };
  }
  const drawn = raw.filter((line) => line.points.length > 0);
  const shared = drawn.length
    ? drawn
        .map((line) => new Set(line.points.map((point) => point.period)))
        .reduce((common, periods) => new Set([...common].filter((period) => periods.has(period))))
    : new Set<string>();
  const primary = drawn[0];
  const basePoint = primary?.points.find((point) => shared.has(point.period)) || null;
  if (!basePoint) {
    return {
      lines: primary ? [primary] : [],
      scale: "level",
      basePeriod: "",
      unindexed: drawn.slice(1).map((line) => line.place.name),
    };
  }
  const unindexed: string[] = [];
  const lines: TrendLine[] = [];
  for (const line of drawn) {
    const base = line.points.find((point) => point.period === basePoint.period);
    if (!base || base.published === 0) {
      unindexed.push(line.place.name);
      continue;
    }
    lines.push({
      ...line,
      points: line.points.map((point) => ({
        ...point,
        value: (point.published / base.published) * 100,
      })),
    });
  }
  return { lines, scale, basePeriod: basePoint.period, unindexed };
}
