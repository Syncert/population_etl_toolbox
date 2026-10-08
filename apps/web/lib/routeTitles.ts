// Per-route document titles, built from the public URL state and nothing else.
//
// The product's value is a shareable link: `lib/urlState.ts` serialises the
// explorer, comparison, catalog, profile and workbench state into the address
// bar so a link reproduces a view. Every one of those links landed in a tab, a
// bookmark, a history entry and a link preview titled "Economic Data Studio",
// because `app/layout.js` held the only `metadata` export and its `%s | ...`
// template had nothing to fill it with.
//
// **What may appear in a title.** Only what the URL already carries, and only
// after `urlState.ts`'s own parsers have accepted it. That is the whole rule,
// and it is why these builders take the parsed state rather than the raw
// query: a title is copied into a bookmark, read aloud by a screen reader,
// sent to a link previewer and written to browser history, so a key the URL
// vocabulary does not define must not reach one. An observation value, a
// saved-analysis name or id, or a token would each be a leak; none of them is
// in the vocabulary, so none of them can arrive here.

import { PLACE_SEGMENT } from "./placeChapters";
import {
  parseComparisonState,
  parseExplorerState,
  parseProfileState,
  parseWorkbenchState,
} from "./urlState";

/** The fixed title of every route whose address carries no state. */
export const STATIC_ROUTE_TITLES: Readonly<Record<string, string>> = {
  "/": "Economic Data Studio",
  "/catalog": "Data catalog",
  "/use-cases": "Analytics use cases",
  "/quality": "Data quality",
  "/saved": "Saved analyses",
  "/builder": "Evidence packet builder",
  "/articles": "Composed article",
  "/explain": "Explainers",
  "/us": "United States",
};

/**
 * How many series a workbench title names before it stops counting them out.
 *
 * A composition may carry eight. A title that lists all of them is truncated
 * by every surface that shows it, and the truncation falls in an arbitrary
 * place, so the title says how many there are instead.
 */
export const MAX_TITLED_SERIES = 2;

/** A query string, from whatever shape the caller holds it in. */
export function searchString(
  search: string | Record<string, string | string[] | undefined> | null | undefined,
): string {
  if (typeof search === "string") return search;
  if (!search) return "";
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(search)) {
    if (Array.isArray(value)) {
      for (const item of value) params.append(key, item);
    } else if (value !== undefined) {
      params.append(key, value);
    }
  }
  return params.toString();
}

/** Join the parts a title carries, dropping the ones the URL did not. */
function titleOf(screen: string, parts: (string | undefined)[]): string {
  const present = parts.filter((part): part is string => Boolean(part));
  return present.length > 0 ? `${screen}: ${present.join(" · ")}` : screen;
}

/** Where a view is scoped to, as a title says it. */
function place(geoLevel?: string, stateFips?: string, geoId?: string): string | undefined {
  if (geoId) return geoId;
  if (stateFips) return `state ${stateFips}`;
  return geoLevel;
}

export function explorerTitle(search: Parameters<typeof searchString>[0]): string {
  const state = parseExplorerState(searchString(search));
  return titleOf("Explore", [
    state.source,
    state.metric,
    place(state.geoLevel, state.stateFips, state.geoId),
  ]);
}

export function comparisonTitle(search: Parameters<typeof searchString>[0]): string {
  const state = parseComparisonState(searchString(search));
  const pair =
    state.metricA && state.metricB
      ? `${state.metricA} against ${state.metricB}`
      : state.metricA || state.metricB;
  return titleOf("Compare", [pair, place(state.geoLevel, state.stateFips)]);
}

export function profileTitle(search: Parameters<typeof searchString>[0]): string {
  const state = parseProfileState(searchString(search));
  return titleOf("Place profile", [state.template, state.geoId]);
}

export function workbenchTitle(search: Parameters<typeof searchString>[0]): string {
  const state = parseWorkbenchState(searchString(search));
  const series = state.series || [];
  const named = series.slice(0, MAX_TITLED_SERIES).map((entry) => entry.metricCode);
  const measures =
    series.length > MAX_TITLED_SERIES
      ? `${named.join(" · ")} and ${series.length - MAX_TITLED_SERIES} more`
      : named.join(" · ");
  return titleOf("Workbench", [
    measures || undefined,
    state.presentation,
    place(state.alignmentGeoLevel, state.stateFips),
  ]);
}

/**
 * A place page's title, from its address segments alone.
 *
 * The segments are the slugs the place routes accept (`PLACE_SEGMENT`); a
 * segment outside that vocabulary never reaches a title. The words are the
 * slug's, capitalised -- the catalog's own spelling of the name is the page's
 * heading, which needs the catalog; a title is built before it answers.
 */
export function placeRouteTitle(stateSegment?: string, countySegment?: string): string {
  const words = (segment: string) =>
    /^[0-9]+$/.test(segment)
      ? segment
      : segment.split("-").map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(" ");
  const parts = [countySegment, stateSegment].filter(
    (segment): segment is string => Boolean(segment) && PLACE_SEGMENT.test(segment as string),
  );
  return parts.length ? parts.map(words).join(", ") : STATIC_ROUTE_TITLES["/us"]!;
}

/** A one-measure map's title: the catalog identity its address names. */
export function measureMapTitle(metricCode: string): string {
  return titleOf("Map", [/^[A-Za-z0-9_.:-]{1,200}$/.test(metricCode) ? metricCode : undefined]);
}
