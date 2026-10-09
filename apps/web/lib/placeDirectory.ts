// Finding a place: the home page's search, map, and "use my location", and
// the public data page's source cards (find-your-place-home).
//
// Everything here works over rows the geography catalog and the freshness
// resource published. Nothing calls a third-party service: a coordinate is
// matched to the nearest county the catalog publishes, by the catalog's own
// internal points, in the browser.

import type { GeographySummary, SourceFreshness, SourceSummary } from "./api/types";
import { observationJoinValue } from "./explorerViewModel";
import { countyName, countySegment, placePath, stateName, stateSegment } from "./placeChapters";

export interface PlaceEntry {
  geoId: string;
  level: "NATIONAL" | "STATE" | "COUNTY";
  name: string;
  href: string;
  /** Lower-cased words the search matches against. */
  haystack: string;
}

/** Every place page the catalog's rows address, nation first. */
export function buildPlaceDirectory(
  nation: GeographySummary | null,
  states: readonly GeographySummary[],
  counties: readonly GeographySummary[],
): PlaceEntry[] {
  const entries: PlaceEntry[] = [];
  if (nation) {
    entries.push({ geoId: nation.geo_id, level: "NATIONAL", name: "United States", href: "/us", haystack: "united states usa us nation america" });
  }
  const stateByFips = new Map(states.map((state) => [state.state_fips, state]));
  for (const state of states) {
    entries.push({
      geoId: state.geo_id,
      level: "STATE",
      name: stateName(state),
      href: placePath(stateSegment(state, states)),
      haystack: stateName(state).toLowerCase(),
    });
  }
  const countiesByState = new Map<string, GeographySummary[]>();
  for (const county of counties) {
    const key = String(county.state_fips || "");
    countiesByState.set(key, [...(countiesByState.get(key) || []), county]);
  }
  for (const county of counties) {
    const state = stateByFips.get(county.state_fips);
    if (!state) continue;
    const name = `${countyName(county)}, ${stateName(state)}`;
    entries.push({
      geoId: county.geo_id,
      level: "COUNTY",
      name,
      href: placePath(stateSegment(state, states), countySegment(county, countiesByState.get(String(county.state_fips)) || [])),
      haystack: name.toLowerCase(),
    });
  }
  return entries;
}

/**
 * Places matching what a reader typed: every word must appear, names that
 * start with the query first, then broader grains before counties.
 */
export function searchPlaces(entries: readonly PlaceEntry[], query: string, limit = 8): PlaceEntry[] {
  const words = query.toLowerCase().split(/[\s,]+/).filter(Boolean);
  if (!words.length) return [];
  const order = { NATIONAL: 0, STATE: 1, COUNTY: 2 };
  return entries
    .filter((entry) => words.every((word) => entry.haystack.includes(word)))
    .sort((left, right) => {
      const leftStarts = left.haystack.startsWith(words[0]!) ? 0 : 1;
      const rightStarts = right.haystack.startsWith(words[0]!) ? 0 : 1;
      return leftStarts - rightStarts || order[left.level] - order[right.level] || left.name.localeCompare(right.name);
    })
    .slice(0, limit);
}

function coordinate(value: unknown): number | null {
  const number = typeof value === "number" ? value : typeof value === "string" && value.trim() ? Number(value) : Number.NaN;
  return Number.isFinite(number) ? number : null;
}

/**
 * The catalog county whose internal point is nearest a coordinate, by
 * great-circle distance. An approximation stated as one: the nearest county
 * center is not always the county a point lies in.
 */
export function nearestCounty(
  latitude: number,
  longitude: number,
  counties: readonly GeographySummary[],
): GeographySummary | null {
  const radians = (degrees: number) => (degrees * Math.PI) / 180;
  let best: GeographySummary | null = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const county of counties) {
    const lat = coordinate(county.geo_latitude ?? county.latitude);
    const lon = coordinate(county.geo_longitude ?? county.longitude);
    if (lat === null || lon === null) continue;
    const dLat = radians(lat - latitude);
    const dLon = radians(lon - longitude);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(radians(latitude)) * Math.cos(radians(lat)) * Math.sin(dLon / 2) ** 2;
    const distance = 2 * Math.asin(Math.min(1, Math.sqrt(a)));
    if (distance < bestDistance) {
      bestDistance = distance;
      best = county;
    }
  }
  return best;
}

/**
 * The place page a clicked map feature addresses, matched through the same
 * join key the explorer colours by, or null for a feature no catalog row
 * claims.
 */
export function featurePlaceHref(
  properties: Record<string, unknown> | null | undefined,
  joinKey: string,
  counties: readonly GeographySummary[],
  entries: readonly PlaceEntry[],
): string | null {
  if (!properties) return null;
  const value = properties[joinKey];
  if (value === null || value === undefined || value === "") return null;
  const county = counties.find((row) => String(observationJoinValue(row, joinKey)) === String(value));
  if (!county) return null;
  return entries.find((entry) => entry.geoId === county.geo_id)?.href || null;
}

/** The rules every number on the site follows, in a reader's words. */
export const SITE_RULES: readonly { id: string; rule: string; detail: string }[] = [
  { id: "suppressed", rule: "Suppressed is not zero.", detail: "A value a publisher withheld, or never published, is shown as withheld or missing. It is never filled in, and never drawn as zero." },
  { id: "no-score", rule: "No composite score.", detail: "Measures are shown one at a time with their own sources. Nothing here adds them into an index, a grade, or a ranking of places." },
  { id: "association", rule: "Association is not causation.", detail: "Two measures moving together are shown side by side. Nothing here says that one explains the other." },
  { id: "period-source", rule: "Every number names its period and source.", detail: "A value appears with the period it describes and the program that published it, so it can be checked at the source." },
  { id: "revisions", rule: "Revisions are shown, not overwritten.", detail: "When a publisher revises a number, the release it came from is kept, and the as-released view shows what was published at the time." },
];

export interface SourceCard {
  sourceCode: string;
  name: string;
  referenceUrl: string;
  reported: boolean;
  lastRefresh: string | null;
  newestPublication: string | null;
  grains: string[];
  metricCount: number | null;
  staleCount: number | null;
}

/**
 * One card per published source. A source the freshness resource does not
 * report is "not reported" -- never presented as fresh.
 */
export function sourceCards(
  sources: readonly SourceSummary[],
  freshness: readonly SourceFreshness[],
): SourceCard[] {
  const byCode = new Map(freshness.map((item) => [item.source_code, item]));
  return [...sources]
    .sort((left, right) => String(left.source_code).localeCompare(String(right.source_code)))
    .map((source) => {
      const row = byCode.get(String(source.source_code));
      const grains = Array.isArray(row?.geo_grains) ? (row!.geo_grains as unknown[]).map(String) : [];
      return {
        sourceCode: String(source.source_code),
        name: String(source.source_name || source.source_code),
        referenceUrl: String(source.reference_url || ""),
        reported: Boolean(row),
        lastRefresh: row?.latest_harvested_at || null,
        newestPublication: row?.latest_publication_time || null,
        grains,
        metricCount: row ? row.metric_count : null,
        staleCount: row ? row.stale_count : null,
      };
    });
}
