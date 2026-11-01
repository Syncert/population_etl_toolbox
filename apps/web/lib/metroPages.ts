// Metro pages (web-metro-pages).
//
// A metro (a CBSA) is built by OMB from whole counties and can span states,
// so its page is `/metro/<segment>` rather than a step under `/us/<state>`.
// One API read names everything the page needs before it asks for a figure:
// the metro, its member counties, the states they lie in (the principal
// state first: the state of its most populous member county) and the areas
// that state lies in. The rules are the API's; this module only shapes them.

import { apiFetch } from "./api/client";
import type { GeographySummary } from "./api/types";
import type { RelatedResponse } from "./placeRelationships";

export interface MetroSummary {
  geo_id: string;
  geo_name: string;
  short_name: string;
  cbsa_code: string;
  segment: string;
  page_path: string;
  principal_state_fips: string | null;
  principal_state_name: string | null;
  population: number | null;
  population_year: number | null;
  population_metric_code: string | null;
}

export interface MetroMemberCounty {
  geo_id: string;
  geo_name: string;
  state_fips: string | null;
  state_name: string | null;
  page_path: string | null;
  population: number | null;
  population_year: number | null;
}

export interface MetroState {
  geo_id: string;
  state_fips: string;
  state_name: string | null;
  page_path: string | null;
  member_counties: number;
  principal: boolean;
}

export interface MetroArea {
  geo_id: string;
  geo_level: string;
  geo_name: string | null;
}

export interface MetroPageResponse {
  requested_segment: string;
  metro: MetroSummary;
  delineation_vintage: number | null;
  evidence_source: string | null;
  member_counties: MetroMemberCounty[];
  states: MetroState[];
  principal_state_areas: MetroArea[];
}

export interface MetroListResponse {
  total: number;
  limit: number;
  offset: number;
  items: MetroSummary[];
}

export function getMetroPage(segment: string, { signal }: { signal?: AbortSignal } = {}): Promise<MetroPageResponse> {
  return apiFetch<MetroPageResponse>(`/catalog/geographies/metros/${encodeURIComponent(segment)}`, { signal });
}

export function getMetrosInState(stateFips: string, { signal }: { signal?: AbortSignal } = {}): Promise<MetroListResponse> {
  return apiFetch<MetroListResponse>("/catalog/geographies/metros", { params: { state_fips: stateFips, limit: 1000 }, signal });
}

/** The metro as the page's other helpers read a geography. */
export function metroGeography(metro: MetroSummary): GeographySummary {
  return { geo_id: metro.geo_id, geo_level: "METRO", geo_name: metro.geo_name } as GeographySummary;
}

/**
 * What the groceries-and-gas card reads a metro's larger areas from: the
 * principal state and the areas it lies in, as `part_of` rows. Nothing is
 * matched by name; every row is the API's.
 */
export function metroContainingRows(page: MetroPageResponse): RelatedResponse {
  const principal = page.states.find((state) => state.principal) || null;
  const items = [
    ...(principal ? [{ relationship: "part_of", geo_id: principal.geo_id, geo_level: "STATE", geo_name: principal.state_name, state_fips: principal.state_fips }] : []),
    ...page.principal_state_areas.map((area) => ({ relationship: "part_of", geo_id: area.geo_id, geo_level: area.geo_level, geo_name: area.geo_name, state_fips: null })),
  ].map((row) => ({ ...row, geography_vintage: page.delineation_vintage ?? 0, evidence_source: page.evidence_source || "" }));
  return { geo_id: page.metro.geo_id, geo_level: "METRO", total: items.length, items };
}

function listAnd(parts: readonly string[]): string {
  if (parts.length <= 1) return parts[0] ?? "";
  return parts.length === 2 ? parts.join(" and ") : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;
}

/**
 * Said when a metro reaches into more than one state: which state its cards
 * read beside it, and why that one.
 */
export function multiStateNote(page: MetroPageResponse): string {
  if (page.states.length <= 1) return "";
  const principal = page.states.find((state) => state.principal);
  const names = page.states.map((state) => state.state_name || state.state_fips);
  return `${page.metro.geo_name} reaches into ${page.states.length} states: ${listAnd(names)}. Its figures are read beside ${principal?.state_name || "the state"}'s, the state of its most populous county.`;
}

/** "From the 2023 CBSA delineation." -- the reference's own vintage. */
export function delineationText(page: MetroPageResponse): string {
  return page.delineation_vintage ? `Member counties from the ${page.delineation_vintage} CBSA delineation the geography reference holds.` : "Member counties from the CBSA delineation the geography reference holds.";
}

/**
 * How a BEA figure for a metro was made, from the row's own `derivation`
 * dimension (bea-metro-income-gdp): BEA publishes no metro rows for county
 * income, so the warehouse sums the member counties as BEA does. `null` for a
 * figure BEA published itself.
 */
export function derivationText(dimensions: Record<string, unknown> | null | undefined): string | null {
  const derivation = String(dimensions?.derivation || "");
  if (derivation === "sum_of_member_counties") return "Derived: the sum of its member counties' BEA figures, not a figure BEA published for the metro";
  if (derivation === "ratio_of_member_county_sums") return "Derived: its member counties' summed income over their summed population, not a figure BEA published for the metro";
  return null;
}
