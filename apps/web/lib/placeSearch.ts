// Find your place, over the API's place search and point lookup (D-1).
//
// The search box asks `/catalog/geographies/search`, which matches cities,
// counties, metros, states and ZIP code areas by name and ranks them by
// population, so "Austin" offers Austin city, Travis County and the Austin
// metro. "Use my location" asks `/catalog/geographies/locate`, which answers
// the city the point lies in (or its county outside every city). Neither
// rule is repeated here: this module only turns the API's answers into what
// the finder shows, and keeps the old in-browser county match as the
// fallback where the API's search is unavailable.

import type { GeographyLocateResponse, GeographySearchItem } from "./api/types";
import { formatNumber } from "./format";
import { PLACE_SEGMENT } from "./placeChapters";
import type { PlaceEntry } from "./placeDirectory";

export interface FoundPlace {
  geoId: string;
  level: string;
  name: string;
  /** The page to open, or null for a grain with no page yet (metro, ZIP code area). */
  href: string | null;
  /** What kind of place this is, in a reader's words. */
  kind: string;
  /** Why a place that does not carry the typed name was offered. */
  via: string | null;
  /** "123,456 people (2019–2023 survey)", or null where none is served. */
  population: string | null;
}

const KIND: Record<string, string> = {
  NATIONAL: "nation",
  STATE: "state",
  COUNTY: "county",
  PLACE: "city or town",
  METRO: "metro area",
  ZCTA: "ZIP code area",
};

export function kindOf(level: string): string {
  return KIND[level] || level.toLowerCase();
}

/**
 * A page address the finder may open: the site's own `/us/...` shape, each
 * segment the address rule's. Anything else is treated as "no page", so a
 * malformed answer can never send a reader off the site.
 */
export function safePagePath(path: unknown): string | null {
  if (typeof path !== "string") return null;
  if (path === "/us") return path;
  if (!path.startsWith("/us/")) return null;
  const segments = path.slice(4).split("/");
  return segments.length <= 2 && segments.every((segment) => PLACE_SEGMENT.test(segment)) ? path : null;
}

function populationText(item: GeographySearchItem): string | null {
  if (typeof item.population !== "number" || !Number.isFinite(item.population)) return null;
  const count = formatNumber(Math.round(item.population));
  const year = item.population_year;
  if (!year) return `${count} people`;
  // The ranking population is a five-year survey estimate; say which years.
  const fiveYear = String(item.population_metric_code || "").includes(":acs5:");
  return `${count} people (${fiveYear ? `${year - 4}–${year} survey` : year})`;
}

function displayName(item: GeographySearchItem): string {
  const name = item.geo_name || item.short_name || item.geo_id;
  if (item.geo_level === "ZCTA") return `ZIP code ${item.short_name || name}`;
  if ((item.geo_level === "COUNTY" || item.geo_level === "PLACE") && item.state_name) {
    return `${name}, ${item.state_name}`;
  }
  return name;
}

/** The finder's rows for one search answer, in the API's order. */
export function foundPlaces(items: readonly GeographySearchItem[]): FoundPlace[] {
  return items.map((item) => ({
    geoId: item.geo_id,
    level: item.geo_level,
    name: displayName(item),
    href: safePagePath(item.page_path),
    kind: kindOf(item.geo_level),
    via: item.match === "contains_match" && item.matched_via ? `includes ${item.matched_via.geo_name}` : null,
    population: populationText(item),
  }));
}

const NATION_WORDS = ["united states", "usa", "us", "nation", "america"];

/**
 * The nation's page, when the query names it. The search answers states and
 * smaller; the nation has one page and is offered from here.
 */
export function nationMatch(query: string): FoundPlace | null {
  const typed = query.trim().toLowerCase().replace(/[.,]/g, "").replace(/\s+/g, " ");
  if (typed.length < 2) return null;
  if (!NATION_WORDS.some((word) => word.startsWith(typed) || typed === word)) return null;
  return { geoId: "us:1", level: "NATIONAL", name: "United States", href: "/us", kind: kindOf("NATIONAL"), via: null, population: null };
}

/** The in-browser directory's rows as finder rows: the fallback search. */
export function directoryPlaces(entries: readonly PlaceEntry[]): FoundPlace[] {
  return entries.map((entry) => ({
    geoId: entry.geoId, level: entry.level, name: entry.name, href: entry.href, kind: kindOf(entry.level), via: null, population: null,
  }));
}

/** At least two letters or digits: the API's own minimum for a query. */
export function searchable(query: string): boolean {
  return (query.normalize("NFKD").match(/[\p{L}\p{N}]/gu) || []).length >= 2;
}

/**
 * Four decimal places is about 11 meters: precise enough to land on the
 * right side of a city line, and no more precise than that.
 */
export function roundCoordinate(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}

export interface Landing {
  href: string;
  message: string;
}

/**
 * Where "use my location" goes: the city or town the point is in, or its
 * county outside every city, or its state where neither has a page.
 */
export function locateLanding(located: GeographyLocateResponse): Landing | null {
  const landing = located.landing;
  const href = safePagePath(landing?.page_path);
  if (landing && href) {
    const name = landing.geo_name || "your place";
    const message = landing.geo_level === "PLACE"
      ? `Opening ${name}, the city or town you are in.`
      : `Opening ${name}. You are not inside a city or town, so this is your county.`;
    return { href, message };
  }
  for (const fallback of [located.county, located.state]) {
    const fallbackHref = safePagePath(fallback?.page_path);
    if (fallback && fallbackHref) return { href: fallbackHref, message: `Opening ${fallback.geo_name || "your place"}, the ${kindOf(fallback.geo_level)} you are in.` };
  }
  return null;
}
