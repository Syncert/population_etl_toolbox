// A Census tract's page (tract pages, D-4).
//
// A tract sits under its county's address: `/us/<state>/<county>/tract-18-05`
// is Census Tract 18.05. Its page reads the ACS 5-year tables the warehouse
// loads at tract grain (`AcsConfig.tract_tables`), beside the tract's county,
// state and the nation, and its history on today's (2020) boundary from
// `/place/tract-history` (API-190): published values from 2020 on, and before
// 2020 either the published values of a tract that kept its boundary or the
// DERIVED counts the API apportions from the 2010 tracts. This module names
// the measures and words; it computes nothing a source did not publish.

import type { GeographySummary } from "./api/types";

/** One measure the tract page shows. */
export interface TractMeasure {
  id: string;
  /** The reader's words for it. */
  label: string;
  metricCode: string;
  /** Who or what the number counts, stated beside it. */
  universe?: string;
  /**
   * A count, so the API can carry it across the 2020 redraw by apportioning
   * earlier tracts' counts; a median or a per-person figure cannot be.
   */
  count: boolean;
}

export interface TractChapter {
  id: string;
  title: string;
  description: string;
  measures: TractMeasure[];
}

const acs5 = (variable: string) => `CENSUS_ACS:acs5:${variable}`;

/** The chapters, in the order every place page reads them. */
export const TRACT_CHAPTERS: readonly TractChapter[] = [
  {
    id: "people",
    title: "People",
    description: "Who lives in this tract.",
    measures: [
      { id: "population", label: "Total population", metricCode: acs5("B01003_001"), universe: "People", count: true },
      { id: "median-age", label: "Median age", metricCode: acs5("B01002_001"), universe: "People", count: false },
      { id: "households", label: "Households", metricCode: acs5("B11001_001"), universe: "Households", count: true },
    ],
  },
  {
    id: "income",
    title: "Income and poverty",
    description: "What households here earn, and how many people live below the poverty line.",
    measures: [
      { id: "median-household-income", label: "Median household income", metricCode: acs5("B19013_001"), universe: "Households", count: false },
      { id: "per-capita-income", label: "Income per person", metricCode: acs5("B19301_001"), universe: "People", count: false },
      {
        id: "below-poverty",
        label: "People below the poverty line",
        metricCode: acs5("B17001_002"),
        universe: "People whose poverty status is determined",
        count: true,
      },
    ],
  },
  {
    id: "housing",
    title: "Housing",
    description: "What homes here cost and who owns or rents them.",
    measures: [
      { id: "median-rent", label: "Median gross rent", metricCode: acs5("B25064_001"), universe: "Renter households paying cash rent", count: false },
      { id: "median-home-value", label: "Median home value", metricCode: acs5("B25077_001"), universe: "Owner-occupied homes", count: false },
      { id: "housing-units", label: "Housing units", metricCode: acs5("B25001_001"), universe: "Housing units", count: true },
      { id: "vacant", label: "Vacant homes", metricCode: acs5("B25002_003"), universe: "Housing units", count: true },
      { id: "owner-occupied", label: "Homes lived in by their owners", metricCode: acs5("B25003_002"), universe: "Occupied homes", count: true },
      { id: "renter-occupied", label: "Homes lived in by renters", metricCode: acs5("B25003_003"), universe: "Occupied homes", count: true },
    ],
  },
  {
    id: "work",
    title: "Work and school",
    description: "Who is working or looking for work, and how much schooling adults here have.",
    measures: [
      { id: "labor-force", label: "People in the civilian labor force", metricCode: acs5("B23025_003"), universe: "People 16 and over", count: true },
      { id: "unemployed", label: "Unemployed people", metricCode: acs5("B23025_005"), universe: "People 16 and over", count: true },
      { id: "bachelors", label: "Adults whose highest degree is a bachelor's", metricCode: acs5("B15003_022"), universe: "People 25 and over", count: true },
    ],
  },
  {
    id: "internet",
    title: "Internet at home",
    description: "Which households have a broadband subscription.",
    measures: [
      { id: "broadband", label: "Households with broadband", metricCode: acs5("B28002_004"), universe: "Households", count: true },
      { id: "no-internet", label: "Households with no internet access", metricCode: acs5("B28002_013"), universe: "Households", count: true },
    ],
  },
];

export const TRACT_MEASURE_LIST: readonly TractMeasure[] = TRACT_CHAPTERS.flatMap((chapter) => chapter.measures);

/** Every metric code the page reads, once each, in chapter order. */
export function tractMetricCodes(): string[] {
  return [...new Set(TRACT_MEASURE_LIST.map((measure) => measure.metricCode))];
}

/** The six-digit tract code from a tract `geo_id`, or "". */
export function tractCode(geoId: string | null | undefined): string {
  const match = /tract:(\d{6})$/.exec(String(geoId || ""));
  return match ? match[1]! : "";
}

/** "18.05" from "001805"; "11" from "001100". */
export function tractNumber(code: string): string {
  if (!/^\d{6}$/.test(code)) return code;
  const whole = Number(code.slice(0, 4));
  const fraction = code.slice(4);
  return fraction === "00" ? String(whole) : `${whole}.${fraction}`;
}

/** "Census Tract 18.05" from a tract identity. */
export function tractName(tract: GeographySummary | string | null | undefined): string {
  const geoId = typeof tract === "string" ? tract : String(tract?.geo_id || "");
  const code = tractCode(geoId);
  if (code) return `Census Tract ${tractNumber(code)}`;
  return typeof tract === "string" ? tract : String(tract?.area_name || tract?.geo_name || geoId);
}

/**
 * The address segment of a tract within its county: `tract-18-05` for tract
 * 18.05, `tract-11` for tract 11. The API builds the same path
 * (`apps/api/services/place_paths.py`, shared fixture).
 */
export function tractSegment(geoId: string): string {
  const code = tractCode(geoId);
  if (!code) return "";
  const whole = Number(code.slice(0, 4));
  const fraction = code.slice(4);
  return fraction === "00" ? `tract-${whole}` : `tract-${whole}-${fraction}`;
}

/** A segment the tract route accepts: `tract-18-05`, `tract-11` or `tract-001805`. */
export const TRACT_SEGMENT = /^tract-(?:\d{6}|\d{1,4}(?:-\d{2})?)$/;

/**
 * The six-digit code a tract segment names, or "" when it names none.
 * `tract-001805` is the code itself; `tract-18-05` and `tract-18` are the
 * tract's number.
 */
export function tractCodeFromSegment(segment: string): string {
  if (!TRACT_SEGMENT.test(segment)) return "";
  const body = segment.slice("tract-".length);
  if (/^\d{6}$/.test(body)) return body;
  const [whole, fraction = "00"] = body.split("-");
  return `${String(Number(whole)).padStart(4, "0")}${fraction}`;
}

/** Resolve a tract segment against one county's tracts. */
export function resolveTractSegment(
  segment: string,
  tracts: readonly GeographySummary[],
): { tract: GeographySummary | null; canonical: string | null } {
  const code = tractCodeFromSegment(segment);
  const found = code ? tracts.find((tract) => tractCode(String(tract.geo_id)) === code) || null : null;
  if (!found) return { tract: null, canonical: null };
  const canonical = tractSegment(String(found.geo_id));
  return { tract: found, canonical: canonical === segment ? null : canonical };
}

/** The API's boundary answer (`/place/tract-history`). */
export interface TractPart {
  geo_id: string;
  name: string;
  vintage: number;
  share_of_that_tract: number | null;
  share_of_this_tract: number | null;
}

export type BoundaryStatus = "unchanged" | "split" | "merged" | "redrawn" | "not_recorded";

export interface TractBoundary {
  status: BoundaryStatus;
  sentence: string;
  earlier: TractPart[];
  also_became: TractPart[];
  slivers_ignored: number;
}

export type HistoryStatus = "published" | "unchanged" | "apportioned" | "incomplete";

export interface TractHistoryPoint {
  vintage_year: number;
  observation_date: string;
  value: number | null;
  margin_of_error: number | null;
  status: HistoryStatus;
  derived: boolean;
  weight_basis: string | null;
  source_geo_ids: string[];
}

export interface TractHistorySeries {
  metric_code: string;
  note: string | null;
  items: TractHistoryPoint[];
}

export interface TractHistoryResponse {
  geo_id: string;
  geo_name: string | null;
  target_tract_vintage: number;
  derivation: string;
  method: string;
  boundary: TractBoundary;
  series: TractHistorySeries[];
}

/** The notice's heading, by how the tract came through the 2020 redraw. */
export function boundaryHeading(status: BoundaryStatus): string {
  switch (status) {
    case "unchanged":
      return "Same boundary since 2010";
    case "split":
      return "This tract was split off in 2020";
    case "merged":
      return "This tract was formed by joining tracts in 2020";
    case "redrawn":
      return "This tract was redrawn in 2020";
    default:
      return "Boundary history not recorded";
  }
}

/** The five-year period an ACS 5-year release covers: "2016–2020". */
export function acsPeriod(vintage: number): string {
  return `${vintage - 4}–${vintage}`;
}

/** How one history point was counted, in the reader's terms. */
export function pointBasis(point: TractHistoryPoint): string {
  switch (point.status) {
    case "published":
      return "Published for this tract";
    case "unchanged":
      return "Published for this tract (same boundary)";
    case "apportioned": {
      const sources = point.source_geo_ids.map((geoId) => tractName(geoId));
      const from = sources.length ? ` from ${sources.join(", ")}` : "";
      return `Derived${from}, by ${point.weight_basis || "the relationship file"}`;
    }
    default:
      return "Not available: an earlier tract's estimate was not published";
  }
}

/** The page title: "Census Tract 18.31, Travis County, Texas". */
export function tractTitle(tract: GeographySummary | null, countyLabel: string, stateLabel: string): string {
  if (!tract) return "";
  return [tractName(tract), countyLabel, stateLabel].filter(Boolean).join(", ");
}
