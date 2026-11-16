// Whether an address under `/us` or `/metro` names a published place, asked
// of the API from the web server (website UX review F6). The page resolved
// its address in the browser, so an address that names nothing answered
// HTTP 200 with a "not found" page, which a search engine indexes. The
// server now asks the catalog the same questions with the page's own
// resolvers and answers 404 when the catalog says no.
//
// Only a positive "no" is a 404. Any failure (the API down, slow, cut short
// or answering something unexpected) is "unknown", and the page settles the
// address in the browser as before.

import { serverApiOrigin } from "./metroRedirect";
import { resolveCitySegment, resolveCountySegment, resolveStateSegment } from "./placeChapters";
import type { GeographySummary } from "./api/types";

const TIMEOUT_MS = 1500;
const PAGE_SIZE = 1000;
const MAX_PAGES = 10;
/** The catalog changes when a load publishes; an hour is soon enough. */
const REVALIDATE_SECONDS = 3600;

type Options = { fetcher?: typeof fetch; origin?: string };

/** Every active geography of one level, or null when the read is not whole. */
async function geographies(
  query: Record<string, string>,
  fetcher: typeof fetch,
  origin: string,
): Promise<GeographySummary[] | null> {
  const items: GeographySummary[] = [];
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const params = new URLSearchParams({ active_only: "true", ...query, limit: String(PAGE_SIZE), offset: String(items.length) });
    const response = await fetcher(`${origin}/api/v1/catalog/geographies?${params}`, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
      next: { revalidate: REVALIDATE_SECONDS },
    } as RequestInit);
    if (!response.ok) return null;
    const body = await response.json();
    if (!Array.isArray(body?.items) || typeof body?.total !== "number") return null;
    items.push(...body.items);
    if (items.length >= body.total) return items;
    if (body.items.length === 0) return null;
  }
  return null;
}

/**
 * True only when the catalog answered in full and no state, county or city
 * is published at `/us/<state>[/<county>]`.
 */
export async function placeAddressIsUnknown(
  state: string,
  county: string | undefined,
  { fetcher = fetch, origin = serverApiOrigin() }: Options = {},
): Promise<boolean> {
  try {
    const states = await geographies({ geo_level: "STATE" }, fetcher, origin);
    if (!states || !states.length) return false;
    const stateMatch = resolveStateSegment(state, states).place;
    if (!stateMatch) return true;
    if (county === undefined) return false;
    const stateFips = String(stateMatch.state_fips || "");
    if (!stateFips) return false;
    const counties = (await geographies({ geo_level: "COUNTY", state_fips: stateFips }, fetcher, origin))
      ?.filter((item) => item.state_fips === stateMatch.state_fips);
    if (!counties) return false;
    if (resolveCountySegment(county, counties).place) return false;
    const places = (await geographies({ geo_level: "PLACE", state_fips: stateFips }, fetcher, origin))
      ?.filter((item) => item.state_fips === stateMatch.state_fips);
    if (!places) return false;
    return !resolveCitySegment(county, places, counties).place;
  } catch {
    return false;
  }
}

/** True only when the API answered 404 for `/metro/<segment>`. */
export async function metroAddressIsUnknown(
  segment: string,
  { fetcher = fetch, origin = serverApiOrigin() }: Options = {},
): Promise<boolean> {
  try {
    const response = await fetcher(`${origin}/api/v1/catalog/geographies/metros/${encodeURIComponent(segment)}`, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
      next: { revalidate: REVALIDATE_SECONDS },
    } as RequestInit);
    return response.status === 404;
  } catch {
    return false;
  }
}
