// "Nearby and related" on a place page (nearby-and-related-places).
//
// Groups the rows `/catalog/geographies/{geo_id}/related` serves into what a
// reader looks for next: the places inside a county, the counties around
// it, and what it is part of. Every link is built from the row's own
// identity; nothing is matched by name.

import type { GeographySummary } from "./api/types";
import { countySegment, placePath, stateName, stateSegment } from "./placeChapters";

export interface RelatedGeography {
  relationship: "contains" | "part_of" | "intersects" | "adjacent" | string;
  geo_id: string;
  geo_level?: string | null;
  geo_name?: string | null;
  state_fips?: string | null;
  geography_vintage: number;
  evidence_source: string;
  overlap_area_m2?: number | null;
  overlap_weight?: number | null;
}

export interface RelatedResponse {
  geo_id: string;
  geo_level?: string | null;
  total: number;
  items: RelatedGeography[];
}

export interface NearbyEntry {
  geoId: string;
  name: string;
  /** A place page, or null where the grain has none yet. */
  href: string | null;
  /** For a place inside a county: the share of the place that lies in it. */
  share: number | null;
  /** True when part of the place lies in another county. */
  crossesCounty: boolean;
  vintage: number;
  evidence: string;
}

export interface NearbyGroups {
  within: NearbyEntry[];
  neighbours: NearbyEntry[];
  partOf: NearbyEntry[];
  /** On a city or town's page: the counties it lies in, with its share of each. */
  counties: NearbyEntry[];
}

export function relatedPath(geoId: string): string {
  return `/catalog/geographies/${encodeURIComponent(geoId)}/related`;
}

const COUNTY_ID = /^state:(\d{2})\|county:(\d{3})$/;
const PLACE_ID = /^state:(\d{2})\|place:(\d{5})$/;

/**
 * The place-page address of a related geography. A county in this page's own
 * state uses its slug (checked against that state's counties); a county in
 * another state uses its five-digit FIPS, which the place route accepts and
 * settles on the named address, so no other state's list is needed.
 */
function hrefFor(
  row: RelatedGeography,
  states: readonly GeographySummary[],
  ownStateCounties: readonly GeographySummary[],
): string | null {
  if (row.geo_level === "NATIONAL") return "/us";
  const state = states.find((item) => item.state_fips === row.state_fips);
  if (row.geo_level === "STATE") return state ? placePath(stateSegment(state, states)) : null;
  if (row.geo_level === "PLACE" && state) {
    // A city or town is addressed by its seven-digit FIPS here; the place
    // route settles it on its named address, so no list of the state's
    // places is needed to link to one.
    const place = PLACE_ID.exec(row.geo_id);
    return place ? placePath(stateSegment(state, states), `${place[1]}${place[2]}`) : null;
  }
  if (row.geo_level !== "COUNTY" || !state) return null;
  const own = ownStateCounties.find((county) => county.geo_id === row.geo_id);
  if (own) return placePath(stateSegment(state, states), countySegment(own, ownStateCounties));
  const match = COUNTY_ID.exec(row.geo_id);
  return match ? placePath(stateSegment(state, states), `${match[1]}${match[2]}`) : null;
}

function nameOf(row: RelatedGeography, states: readonly GeographySummary[]): string {
  const base = String(row.geo_name || row.geo_id);
  if (row.geo_level === "NATIONAL") return "United States";
  if (row.geo_level === "STATE") return base;
  const state = states.find((item) => item.state_fips === row.state_fips);
  return state ? `${base}, ${stateName(state)}` : base;
}

export function groupNearby(
  response: RelatedResponse | null,
  states: readonly GeographySummary[],
  ownStateCounties: readonly GeographySummary[],
): NearbyGroups {
  const groups: NearbyGroups = { within: [], neighbours: [], partOf: [], counties: [] };
  for (const row of response?.items || []) {
    const entry: NearbyEntry = {
      geoId: row.geo_id,
      name: nameOf(row, states),
      href: hrefFor(row, states, ownStateCounties),
      share: null,
      crossesCounty: false,
      vintage: row.geography_vintage,
      evidence: row.evidence_source,
    };
    if (row.relationship === "intersects" && row.geo_level === "PLACE") {
      const share = typeof row.overlap_weight === "number" ? row.overlap_weight : null;
      groups.within.push({ ...entry, share, crossesCounty: share !== null && share < 0.995 });
    } else if (row.relationship === "intersects" && row.geo_level === "COUNTY") {
      const share = typeof row.overlap_weight === "number" ? row.overlap_weight : null;
      groups.counties.push({ ...entry, share, crossesCounty: share !== null && share < 0.995 });
    } else if (row.relationship === "adjacent") {
      groups.neighbours.push(entry);
    } else if (row.relationship === "part_of") {
      groups.partOf.push(entry);
    }
  }
  const byName = (left: NearbyEntry, right: NearbyEntry) => left.name.localeCompare(right.name);
  groups.within.sort((left, right) => (right.share ?? 0) - (left.share ?? 0) || byName(left, right));
  groups.counties.sort((left, right) => (right.share ?? 0) - (left.share ?? 0) || byName(left, right));
  groups.neighbours.sort(byName);
  groups.partOf.sort((left, right) => (left.geoId.startsWith("us") ? 1 : 0) - (right.geoId.startsWith("us") ? 1 : 0));
  return groups;
}

export function isEmpty(groups: NearbyGroups): boolean {
  return !groups.within.length && !groups.neighbours.length && !groups.partOf.length && !groups.counties.length;
}

/**
 * Which counties a city or town lies in, from the published overlap weights:
 * "Lies in Kent County." or "Lies in two counties: Kent County (62%) and
 * Sussex County (38%)." Empty when the reference records none.
 */
export function crossCountyNote(name: string, counties: readonly NearbyEntry[]): string {
  if (!counties.length) return "";
  const bare = (entry: NearbyEntry) => entry.name.replace(/, [^,]+$/, "");
  if (counties.length === 1) return `${name} lies in ${bare(counties[0]!)}.`;
  const parts = counties.map((entry) =>
    entry.share === null ? bare(entry) : `${bare(entry)} (${Math.round(entry.share * 100)}%)`,
  );
  const listed = parts.length === 2 ? parts.join(" and ") : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;
  return `${name} crosses county lines: it lies in ${listed}. A county's figures describe the whole county, not this place.`;
}

/** "62% of Crossing city lies in this county", from the published weight. */
export function shareText(entry: NearbyEntry): string {
  if (entry.share === null) return "";
  const percent = Math.round(entry.share * 100);
  return entry.crossesCounty
    ? `${percent}% of it lies in this county; the rest is in another county`
    : "all of it lies in this county";
}
