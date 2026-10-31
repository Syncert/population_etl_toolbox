"use client";

// Two places, the same chapters (compare-two-places). Each side resolves
// through the geography catalog like a place page; each headline measure is
// offered only as far as `/comparison/preflight` allows and both places
// published it for one shared period. See `lib/placeComparison.ts`.
//
// Two places at different levels -- a city beside its county, a county
// beside its state, a metro beside the nation -- are answered measure by
// measure by `/comparison/places` instead (web-cross-level-compare,
// ADR-0010), which refuses a total across levels and offers its share or
// per-resident view. Each chapter's "Show counts as" switch (D-6) asks the
// same route for the chosen view at either level.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { ArrowLeftRight } from "lucide-react";
import StatusPill from "./StatusPill";
import {
  apiErrorMessage,
  apiFetch,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getComparisonPreflight,
  getMetricsByCode,
} from "../lib/api/client";
import type { CollectionResponse, ComparisonPreflight, GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, marginOfErrorText, observationUnit } from "../lib/explorerViewModel";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildNewestValueRequest,
  normalizeObservationRows,
} from "../lib/observationAccess";
import { buildUseCaseHistory, verifyUseCaseRows } from "../lib/useCaseAnalysis";
import {
  PLACE_CHAPTERS,
  buildTrend,
  placeChapterMetricCodes,
  cityName,
  citySegment,
  countyName,
  countySegment,
  placePath,
  resolveCitySegment,
  resolveCountySegment,
  resolveStateSegment,
  stateName,
  stateSegment,
} from "../lib/placeChapters";
import type { LevelPlace, PlaceChapter, PlaceLevel, PlaceMeasure } from "../lib/placeChapters";
import { COMPARE_GEO_ID, barPosition, compareHref, compareMeasure, intervalsOverlap, pairPath } from "../lib/placeComparison";
import type { ComparedPlace, ComparedRow, NotComparableRow } from "../lib/placeComparison";
import type { CompareLevel } from "../lib/crossLevelComparison";
import { getPlacesComparisonBatch, isCrossLevel, levelWords } from "../lib/crossLevelComparison";
import { getDerivedRatios, pairingFor, viewChoices } from "../lib/derivedViews";
import type { DerivedRatioPairing, ViewChoice } from "../lib/derivedViews";
import { groupNearby, relatedPath } from "../lib/placeRelationships";
import type { RelatedResponse } from "../lib/placeRelationships";
import { comparisonHref } from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";
import { ViewToggle } from "./DerivedView";
import PlacesAnswer, { askedFor } from "./PlacesAnswer";
import type { ChapterBatch } from "./PlacesAnswer";

const PlaceTrend = dynamic(() => import("./PlaceTrend"));

/**
 * One side of a comparison: a place-page address (no `state` is the nation)
 * or, for a grain with no page yet (a metro), its geography identity.
 */
export type SideAddress = { state?: string; county?: string; geoId?: undefined } | { geoId: string; state?: undefined; county?: undefined };

interface ResolvedSide {
  place: ComparedPlace;
  /** Its place page, or null where the grain has none yet (a metro). */
  path: string | null;
  state: GeographySummary | null;
  county: GeographySummary | null;
}

const LEVEL_OF_ID: [RegExp, CompareLevel][] = [
  [/^us:1$/, "NATIONAL"],
  [/^cbsa:\d{5}$/, "METRO"],
  [/^state:\d{2}$/, "STATE"],
  [/^state:\d{2}\|county:\d{3}$/, "COUNTY"],
  [/^state:\d{2}\|place:\d{5}$/, "PLACE"],
];

function levelOfGeoId(geoId: string): CompareLevel | null {
  return LEVEL_OF_ID.find(([pattern]) => pattern.test(geoId))?.[1] ?? null;
}

const keyOf = (code: string, geoId: string) => `${code}|${geoId}`;
const unitText = (row: ObservationRow) => (observationUnit(row) === "value" ? "" : observationUnit(row));

const NATION_NAME = "United States";

const linkOf = (side: ResolvedSide) => ({ geoId: side.place.geoId, path: side.path });

const listAt = (geoLevel: string, signal: AbortSignal, stateFips?: string | null) =>
  fetchAllPages<GeographySummary>("/catalog/geographies", {
    params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: geoLevel, ...(stateFips ? { state_fips: String(stateFips) } : {}) },
    pageSize: 1000,
    signal,
  }).then((items) => (stateFips ? items.filter((item) => item.state_fips === stateFips) : items));

async function resolveSide(
  address: SideAddress,
  states: GeographySummary[],
  nation: GeographySummary | null,
  signal: AbortSignal,
): Promise<ResolvedSide | null> {
  if (address.geoId !== undefined) return resolveGeoId(address.geoId, states, nation, signal);
  if (!address.state) {
    return nation ? { place: { geoId: nation.geo_id, name: NATION_NAME, level: "NATIONAL" }, path: "/us", state: null, county: null } : null;
  }
  const stateMatch = resolveStateSegment(address.state, states);
  if (!stateMatch.place) return null;
  const state = stateMatch.place;
  const stateSeg = stateSegment(state, states);
  if (!address.county) {
    return { place: { geoId: state.geo_id, name: stateName(state), level: "STATE" }, path: placePath(stateSeg), state, county: null };
  }
  // The place search answers with each place's own page address (A-1), so a
  // canonical segment resolves in one read; paging every county and city of
  // a state took a minute on Texas. Any other form falls through to the lists.
  const found = await searchSegment(address.county, stateSeg, state, signal);
  if (found) return found;
  // A county keeps its address; otherwise the segment names a city or town
  // in the state, as on the place page (acs-place-grain).
  const counties = await listAt("COUNTY", signal, state.state_fips);
  const countyMatch = resolveCountySegment(address.county, counties);
  if (countyMatch.place) {
    const county = countyMatch.place;
    return {
      place: { geoId: county.geo_id, name: `${countyName(county)}, ${stateName(state)}`, level: "COUNTY" },
      path: placePath(stateSeg, countySegment(county, counties)),
      state,
      county,
    };
  }
  const places = await listAt("PLACE", signal, state.state_fips);
  const cityMatch = resolveCitySegment(address.county, places, counties);
  if (!cityMatch.place) return null;
  const city = cityMatch.place;
  return {
    place: { geoId: city.geo_id, name: `${cityName(city)}, ${stateName(state)}`, level: "PLACE" },
    path: placePath(stateSeg, citySegment(city, places, counties)),
    state,
    county: null,
  };
}

/** One row of `/catalog/geographies/search` (place-search, A-1). */
interface SearchItem {
  geo_id: string;
  geo_level: string;
  geo_name: string;
  short_name?: string | null;
  state_name?: string | null;
  page_path?: string | null;
}

async function searchSegment(
  segment: string,
  stateSeg: string,
  state: GeographySummary,
  signal: AbortSignal,
): Promise<ResolvedSide | null> {
  const path = placePath(stateSeg, segment);
  let items: SearchItem[] = [];
  try {
    const payload = await apiFetch<{ items: SearchItem[] }>("/catalog/geographies/search", {
      params: { q: segment.replace(/-/g, " "), state_fips: String(state.state_fips || ""), grain: "COUNTY,PLACE", limit: 10 },
      signal,
    });
    items = payload.items || [];
  } catch (error) {
    if (signal.aborted) throw error;
    return null;
  }
  const item = items.find((entry) => entry.page_path === path && (entry.geo_level === "COUNTY" || entry.geo_level === "PLACE"));
  if (!item) return null;
  const own = String(item.geo_name || item.short_name || item.geo_id).replace(new RegExp(`,\\s*${stateName(state)}$`), "");
  const summary: GeographySummary = { geo_id: item.geo_id, geo_level: item.geo_level, state_fips: state.state_fips, state_name: stateName(state) };
  return {
    place: { geoId: item.geo_id, name: `${own}, ${stateName(state)}`, level: item.geo_level as CompareLevel },
    path,
    state,
    county: item.geo_level === "COUNTY" ? summary : null,
  };
}

async function resolveGeoId(
  geoId: string,
  states: GeographySummary[],
  nation: GeographySummary | null,
  signal: AbortSignal,
): Promise<ResolvedSide | null> {
  const level = COMPARE_GEO_ID.test(geoId) ? levelOfGeoId(geoId) : null;
  if (level === "NATIONAL") return resolveSide({}, states, nation, signal);
  if (level === "METRO") {
    const metro = (await listAt("METRO", signal)).find((item) => item.geo_id === geoId);
    return metro ? { place: { geoId, name: String(metro.geo_name || geoId), level: "METRO" }, path: null, state: null, county: null } : null;
  }
  const state = states.find((item) => geoId === item.geo_id || geoId.startsWith(`${item.geo_id}|`));
  if (!level || !state) return null;
  const stateSeg = stateSegment(state, states);
  if (level === "STATE") return resolveSide({ state: stateSeg }, states, nation, signal);
  const counties = await listAt("COUNTY", signal, state.state_fips);
  if (level === "COUNTY") {
    const county = counties.find((item) => item.geo_id === geoId);
    return county ? resolveSide({ state: stateSeg, county: countySegment(county, counties) }, states, nation, signal) : null;
  }
  const places = await listAt("PLACE", signal, state.state_fips);
  const city = places.find((item) => item.geo_id === geoId);
  return city ? resolveSide({ state: stateSeg, county: citySegment(city, places, counties) }, states, nation, signal) : null;
}

export default function ComparePlacesPage({ a: addressA, b: addressB }: { a: SideAddress; b: SideAddress }) {
  const router = useRouter();
  const [sides, setSides] = useState<{ a: ResolvedSide; b: ResolvedSide } | null>(null);
  const [nation, setNation] = useState<GeographySummary | null>(null);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [problem, setProblem] = useState("");
  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [metrics, setMetrics] = useState<Map<string, MetricSummary>>(new Map());
  // The per-capita and share pairings the API lists (ADR-0009): each
  // chapter's switch offers the served ones.
  const [pairings, setPairings] = useState<DerivedRatioPairing[]>([]);
  const [preflights, setPreflights] = useState<Map<string, ComparisonPreflight | string>>(new Map());
  const [answers, setAnswers] = useState<Map<string, ObservationRow | null>>(new Map());
  const [histories, setHistories] = useState<Map<string, ObservationRow[]>>(new Map());
  const [status, setStatus] = useState({ state: "loading", message: "resolving both places" });

  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    getDerivedRatios({}, { signal })
      .then((payload) => { if (!signal.aborted) setPairings(Array.isArray(payload?.items) ? payload.items : []); })
      .catch(() => {
        // No pairings listed: every chapter shows its figures as published, and no switch.
      });
    (async () => {
      try {
        const [nations, stateItems, capabilities] = await Promise.all([
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "NATIONAL" }, pageSize: 1000, signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" }, pageSize: 1000, signal }),
          getCapabilities({ signal }),
        ]);
        const us = nations[0] || null;
        setNation(us);
        setStates(stateItems);
        setSources(buildExplorerSources(capabilities.items));
        const [a, b] = await Promise.all([resolveSide(addressA, stateItems, us, signal), resolveSide(addressB, stateItems, us, signal)]);
        if (signal.aborted) return;
        if (!a || !b) {
          setProblem("One of these addresses names no published place.");
          return;
        }
        if (a.place.geoId === b.place.geoId) {
          setProblem(`Both sides name ${a.place.name}. Pick a second place to compare it with.`);
          return;
        }
        // An identity address of two places that both have pages settles on
        // the place-page form, as a place page settles on its canonical name.
        if ((addressA.geoId !== undefined || addressB.geoId !== undefined) && a.path && b.path) {
          router.replace(compareHref(linkOf(a), linkOf(b)));
        }
        setSides({ a, b });
        // One catalog read for every chapter's codes, not one per code.
        const found = await getMetricsByCode(placeChapterMetricCodes(), { signal });
        if (!signal.aborted) setMetrics(found);
      } catch (error) {
        if (!signal.aborted) setProblem(apiErrorMessage(error));
      }
    })();
    return () => controller.abort();
    // `router` is stable; the sides are the page's identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [addressA, addressB]);

  const cross = sides ? isCrossLevel(sides.a.place, sides.b.place) : false;
  const levels = useMemo<CompareLevel[]>(() => (sides ? [...new Set([sides.a.place.level, sides.b.place.level])] : ["COUNTY"]), [sides]);
  const level = levels[0]!;

  const measures = useMemo(() => {
    const published = (measure: PlaceMeasure): ChapterMeasure[] => {
      const code = measure.candidates.find((candidate) => metrics.has(candidate));
      const metric = code ? metrics.get(code)! : null;
      const grains = metric?.valid_geo_grains;
      // Across levels a measure is asked when the catalog lists either side's
      // grain: the API then says plainly which place publishes nothing
      // (`not_published`), where a filter on both left a metro beside the
      // nation with no rows at all and no word why.
      const listed = (grain: CompareLevel) => !Array.isArray(grains) || grains.includes(grain);
      return metric && code && (levels.length > 1 ? levels.some(listed) : levels.every(listed)) ? [{ measure, code, metric }] : [];
    };
    return PLACE_CHAPTERS.map((chapter) => ({
      chapter,
      measures: chapter.headline.flatMap(published),
      // The chapter's counts with a served share or per-resident view
      // (ADR-0009): the way a total compares across places of any size.
      viewable: chapter.depth
        .filter((measure) => !measure.group)
        .flatMap(published)
        .filter(({ code }) => VIEWABLE.some((choice) => pairingFor(pairings, code, choice))),
    }));
  }, [metrics, levels, pairings]);

  const parentsFor = (pair: { a: ResolvedSide; b: ResolvedSide }): LevelPlace[] => {
    const parents: LevelPlace[] = [];
    if (pair.a.place.level === "COUNTY" || pair.a.place.level === "PLACE") {
      for (const state of [pair.a.state, pair.b.state]) {
        if (state && !parents.some((parent) => parent.geoId === state.geo_id)) {
          parents.push({ level: "STATE", geoId: state.geo_id, name: stateName(state), role: "parent" });
        }
      }
    }
    if (nation && pair.a.place.level !== "NATIONAL") parents.push({ level: "NATIONAL", geoId: nation.geo_id, name: NATION_NAME, role: "parent" });
    return parents;
  };

  // A metro's place in the chapters' level list; the trend and history
  // readers take the grain as the catalog names it.
  const asLevel = (side: ResolvedSide) => side.place.level as PlaceLevel;

  useEffect(() => {
    if (!sides || !metrics.size) return;
    if (cross) {
      // Each row asks `/comparison/places` itself.
      setStatus({ state: "ok", message: "each measure is asked of both places" });
      return;
    }
    if (!sources.length) return;
    const controller = new AbortController();
    const signal = controller.signal;
    const nextPreflights = new Map<string, ComparisonPreflight | string>();
    const nextAnswers = new Map<string, ObservationRow | null>();
    const nextHistories = new Map<string, ObservationRow[]>();
    const parents = parentsFor(sides);
    const sourceFor = (metric: MetricSummary) => sources.find((item) => item.sourceCode === metric.source_code) || null;
    const publishes = (metric: MetricSummary, grain: PlaceLevel) => !Array.isArray(metric.valid_geo_grains) || metric.valid_geo_grains.includes(grain);
    (async () => {
      setStatus({ state: "loading", message: "checking each measure and reading both places" });
      const tasks: Promise<void>[] = [];
      for (const { chapter, measures: chapterMeasures } of measures) {
        for (const { measure, code, metric } of chapterMeasures) {
          const source = sourceFor(metric);
          tasks.push(
            getComparisonPreflight({ metric_code_a: code, metric_code_b: code }, { signal })
              .then((verdict) => { nextPreflights.set(code, verdict); })
              .catch((error) => { nextPreflights.set(code, apiErrorMessage(error)); }),
          );
          if (!source) continue;
          const places: LevelPlace[] = [
            { level: asLevel(sides.a), geoId: sides.a.place.geoId, name: sides.a.place.name, role: "this place" },
            { level: asLevel(sides.b), geoId: sides.b.place.geoId, name: sides.b.place.name, role: "this place" },
            ...parents,
          ];
          for (const place of places) {
            if (!publishes(metric, place.level)) continue;
            tasks.push((async () => {
              try {
                const { resource, params } = buildNewestValueRequest(source, { metricCode: code, geoId: place.geoId });
                const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params, signal });
                const rows = normalizeObservationRows(source, payload.items || []);
                verifyUseCaseRows(rows, code, place.geoId);
                nextAnswers.set(keyOf(code, place.geoId), rows.at(-1) ?? null);
              } catch {
                nextAnswers.set(keyOf(code, place.geoId), null);
              }
            })());
          }
          if (measure.id === chapter.trend.measureId) {
            for (const place of [places[0]!, places[1]!, ...parents.filter((parent) => parent.level === "NATIONAL")]) {
              const reading = buildUseCaseHistory(source, metric, place.geoId, place.level);
              if (!reading.request) continue;
              tasks.push(
                fetchCollectionPages<Observation>(reading.request.resource, { params: reading.request.params, pageSize: 500, maxPages: 2, signal })
                  .then((result) => { nextHistories.set(keyOf(code, place.geoId), normalizeObservationRows(source, result.items)); })
                  .catch(() => undefined),
              );
            }
          }
        }
      }
      await Promise.all(tasks);
      if (signal.aborted) return;
      setPreflights(nextPreflights);
      setAnswers(nextAnswers);
      setHistories(nextHistories);
      setStatus({ state: "ok", message: "both places read" });
    })();
    return () => controller.abort();
    // `parentsFor` reads only `sides` and `nation`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sides, cross, sources, metrics, measures, nation]);

  if (problem) {
    return (
      <main className="page-shell compact-page" data-testid="compare-places-problem">
        <header className="page-heading">
          <div className="section-kicker">Compare two places</div>
          <h1>These places cannot be compared here</h1>
          <p>{problem}</p>
        </header>
        <p><Link className="text-link" href="/us">Find a place</Link></p>
      </main>
    );
  }

  const title = sides ? `${sides.a.place.name} and ${sides.b.place.name}` : "Comparing two places…";
  const swapped = sides ? compareHref(linkOf(sides.b), linkOf(sides.a)) : "";
  const parents = sides && !cross ? parentsFor(sides) : [];
  const sameLevelOffer = sides && cross ? sameLevelPair(sides.a, sides.b, states) : null;

  return (
    <main
      className="page-shell compare-places-page"
      data-testid="compare-places"
      data-ready={status.state === "ok" ? "true" : "false"}
      data-cross-level={cross ? "true" : "false"}
    >
      <header className="page-heading">
        <div className="section-kicker">Compare two places</div>
        <h1>{title}</h1>
        {sides && cross ? (
          <p data-testid="compare-places-levels">
            {sides.a.place.name} is {levelWords(sides.a.place.level)} and {sides.b.place.name} is {levelWords(sides.b.place.level)}. Averages, medians
            and rates are set side by side. Totals are not, because a bigger place nearly always has a bigger total; where a share or per-resident
            version is published, it is shown instead.
          </p>
        ) : (
          <p>The place page&apos;s chapters, both places side by side in one shared period, with {parents.map((parent) => parent.name).join(" and ") || "their parents"} as reference marks.</p>
        )}
        {sides ? (
          <div className="command-row">
            <Link className="button secondary" href={swapped} data-testid="compare-places-swap">
              <ArrowLeftRight size={15} aria-hidden="true" /> Swap sides
            </Link>
            {sides.a.path ? <Link className="text-link" href={sides.a.path}>{sides.a.place.name}</Link> : null}
            {sides.b.path ? <Link className="text-link" href={sides.b.path}>{sides.b.place.name}</Link> : null}
          </div>
        ) : null}
        {sameLevelOffer ? (
          <p>
            <Link className="text-link" href={sameLevelOffer.href} data-testid="compare-places-parent-offer">
              Or compare {sameLevelOffer.name} with {sameLevelOffer.other} at one level
            </Link>
          </p>
        ) : null}
        {sides ? <ComparePicker base={sides.a} states={states} /> : null}
      </header>
      <section className="status-row" role="status">
        <StatusPill state={status.state} label="Comparison" message={status.message} testId="compare-places-status" />
      </section>

      {sides && status.state === "ok"
        ? measures.map(({ chapter, measures: chapterMeasures, viewable }) => (
            <CompareChapter
              key={chapter.id}
              chapter={chapter}
              measures={chapterMeasures}
              viewable={viewable}
              cross={cross}
              a={sides.a}
              b={sides.b}
              pairings={pairings}
              preflights={preflights}
              answers={answers}
              histories={histories}
              parents={parents}
            />
          ))
        : null}

      {sides && status.state === "ok" && !measures.some((entry) => entry.measures.length || entry.viewable.length) ? (
        <p className="analysis-panel" data-testid="compare-places-empty">
          No measure on these pages is published for {sides.a.place.name} or {sides.b.place.name}, so there is nothing to set side by side here yet.
        </p>
      ) : null}

      <footer className="place-footer" data-testid="compare-places-footnote">
        <h2>What this comparison cannot say</h2>
        <ul>
          <li>It does not say why the two places differ. Two numbers side by side are not a cause.</li>
          <li>It is not a ranking. Each row is one measure; nothing here adds them up.</li>
          <li>Every row names its period, and a measure the two places did not publish for the same period is listed rather than shown.</li>
          {cross ? (
            <>
              <li>A measure its publisher reports for only one of the two places says so, rather than showing a zero.</li>
              <li>When one place lies inside or overlaps the other, the two figures partly count the same people, so no difference is worked out.</li>
              <li>Whether each measure is a total, an average or a rate decides what may be set side by side; those classifications are awaiting review.</li>
            </>
          ) : null}
        </ul>
        {sides ? (
          <p>
            <Link
              className="text-link"
              href={comparisonHref({ metricA: measures.flatMap((entry) => entry.measures)[0]?.code, metricB: measures.flatMap((entry) => entry.measures)[0]?.code, geoLevel: level as GeoLevel })}
              data-testid="compare-places-workspace"
            >
              Open the comparison workspace
            </Link>
          </p>
        ) : null}
      </footer>
    </main>
  );
}

/**
 * Beside a county or city compared with another state: the same question at
 * one level, its state against that state, which compares totals as well.
 */
function sameLevelPair(a: ResolvedSide, b: ResolvedSide, states: GeographySummary[]): { href: string; name: string; other: string } | null {
  const finer = [a, b].find((side) => side.place.level === "COUNTY" || side.place.level === "PLACE");
  const coarser = finer === a ? b : a;
  if (!finer?.state || coarser.place.level !== "STATE" || coarser.place.geoId === finer.state.geo_id || !coarser.path) return null;
  const statePath = placePath(stateSegment(finer.state, states));
  return {
    href: finer === a ? pairPath(statePath, coarser.path) : pairPath(coarser.path, statePath),
    name: stateName(finer.state),
    other: coarser.place.name,
  };
}

interface ChapterMeasure {
  measure: PlaceMeasure;
  code: string;
  metric: MetricSummary;
}

const VIEWABLE: readonly ViewChoice[] = ["per_capita", "share"];

function CompareChapter({
  chapter,
  measures,
  viewable,
  cross,
  a,
  b,
  pairings,
  preflights,
  answers,
  histories,
  parents,
}: {
  chapter: PlaceChapter;
  measures: ChapterMeasure[];
  /** Counts beyond the headline that a served pairing divides. */
  viewable: ChapterMeasure[];
  cross: boolean;
  a: ResolvedSide;
  b: ResolvedSide;
  pairings: DerivedRatioPairing[];
  preflights: Map<string, ComparisonPreflight | string>;
  answers: Map<string, ObservationRow | null>;
  histories: Map<string, ObservationRow[]>;
  parents: LevelPlace[];
}) {
  // The chapter's "Show counts as" choice (D-6): a count with a served
  // pairing for it is asked as that view, at one level or across two.
  const [view, setView] = useState<ViewChoice>("count");
  const all = [...measures, ...viewable];
  const choices = viewChoices(pairings, all.map(({ code }) => code));
  const viewPairings = all.flatMap(({ code }) => VIEWABLE.flatMap((choice) => pairingFor(pairings, code, choice) || []));
  const switched = (code: string) => pairingFor(pairings, code, view);
  // A count beyond the headline is shown as its share or per-resident view:
  // across levels always (its total is refused there and the API offers the
  // view), at one level when the switch chooses one.
  const shownViewable = cross ? viewable : viewable.filter(({ code }) => switched(code));

  // Every row this chapter asks `/comparison/places` for, asked in one
  // request: one request per row paid for the pair and its reads each time,
  // and the browser sends only six requests at a time to one host.
  const askedRows = (cross ? all : [...measures.filter(({ code }) => switched(code)), ...shownViewable]).map(({ code }) =>
    askedFor(code, switched(code)),
  );
  const askedKey = askedRows.map((asked) => asked.ratioId ? `r:${asked.ratioId}` : `m:${asked.metricCode}`).join(",");
  const [batch, setBatch] = useState<ChapterBatch>({ state: "loading" });
  useEffect(() => {
    if (!askedRows.length) return;
    const controller = new AbortController();
    setBatch({ state: "loading" });
    getPlacesComparisonBatch(
      {
        geo_id_a: a.place.geoId,
        geo_id_b: b.place.geoId,
        metricCodes: askedRows.flatMap((asked) => (asked.metricCode ? [asked.metricCode] : [])),
        ratioIds: askedRows.flatMap((asked) => (asked.ratioId ? [asked.ratioId] : [])),
      },
      { signal: controller.signal },
    )
      .then((items) => { if (!controller.signal.aborted) setBatch({ state: "ok", items }); })
      // An API without the batch route: each row asks for itself.
      .catch(() => { if (!controller.signal.aborted) setBatch({ state: "failed" }); });
    return () => controller.abort();
    // `askedKey` names exactly what `askedRows` asks.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [askedKey, a.place.geoId, b.place.geoId]);

  if (!all.length) return null;
  const toggle = (
    <ViewToggle choices={choices} value={view} onChange={setView} pairings={viewPairings} label="Show counts as" testId={`compare-chapter-${chapter.id}-view`} />
  );

  if (cross) {
    return (
      <section className="analysis-panel" aria-labelledby={`compare-${chapter.id}`} data-testid={`compare-chapter-${chapter.id}`} data-view={view}>
        <h2 id={`compare-${chapter.id}`}>{chapter.title}</h2>
        {toggle}
        {all.map(({ measure, code, metric }) => (
          <PlacesAnswer
            key={measure.id}
            measureId={measure.id}
            label={measure.label}
            metricCode={code}
            pairing={switched(code)}
            unit={metricUnit(metric)}
            a={a.place}
            b={b.place}
            batch={batch}
          />
        ))}
      </section>
    );
  }

  const outcomes = measures.map(({ measure, code }) => {
    const verdict = preflights.get(code);
    return {
      measure,
      code,
      outcome: compareMeasure(
        {
          measureId: measure.id,
          label: measure.label,
          metricCode: code,
          preflight: typeof verdict === "object" ? verdict : null,
          preflightError: typeof verdict === "string" ? verdict : undefined,
          a: answers.get(keyOf(code, a.place.geoId)) ?? null,
          b: answers.get(keyOf(code, b.place.geoId)) ?? null,
          parents: parents.map((parent) => ({ name: parent.name, row: answers.get(keyOf(code, parent.geoId)) ?? null })),
        },
        a.place,
        b.place,
      ),
    };
  });
  const viewed = outcomes.filter(({ code }) => switched(code));
  const rows = outcomes.filter(({ code }) => !switched(code)).flatMap(({ outcome }) => (outcome.row ? [outcome.row] : []));
  const refused = outcomes.filter(({ code }) => !switched(code)).flatMap(({ outcome }) => (outcome.refused ? [outcome.refused] : []));
  if (!rows.length && !refused.length && !viewed.length && !shownViewable.length) return null;
  const trendMeasure = measures.find(({ measure }) => measure.id === chapter.trend.measureId);
  const trendRow = trendMeasure ? outcomes.find(({ code }) => code === trendMeasure.code)?.outcome.row ?? null : null;
  const trendLevels: LevelPlace[] = [
    { level: a.place.level as PlaceLevel, geoId: a.place.geoId, name: a.place.name, role: "this place" },
    { level: b.place.level as PlaceLevel, geoId: b.place.geoId, name: b.place.name, role: "this place" },
    ...parents.filter((parent) => parent.level === "NATIONAL"),
  ];
  return (
    <section className="analysis-panel" aria-labelledby={`compare-${chapter.id}`} data-testid={`compare-chapter-${chapter.id}`} data-view={view}>
      <h2 id={`compare-${chapter.id}`}>{chapter.title}</h2>
      {toggle}
      {outcomes.map(({ measure, code, outcome }) => {
        const pairing = switched(code);
        if (pairing) {
          return (
            <PlacesAnswer
              key={measure.id}
              measureId={measure.id}
              label={measure.label}
              metricCode={code}
              pairing={pairing}
              unit={pairing.unit}
              a={a.place}
              b={b.place}
              batch={batch}
            />
          );
        }
        return outcome.row ? <PairedRow key={measure.id} row={outcome.row} a={a.place.name} b={b.place.name} /> : null;
      })}
      {shownViewable.map(({ measure, code }) => (
        <PlacesAnswer key={measure.id} measureId={measure.id} label={measure.label} metricCode={code} pairing={switched(code)} unit="" a={a.place} b={b.place} batch={batch} />
      ))}
      {trendRow && trendMeasure ? (
        <PlaceTrend
          model={buildTrend(trendLevels, new Map(trendLevels.map((place) => [place.geoId, histories.get(keyOf(trendMeasure.code, place.geoId)) || []])), chapter.trend.scale)}
          label={`${trendMeasure.measure.label} trend`}
          unit={unitText(trendRow.a.row)}
          testId={`compare-chapter-${chapter.id}-trend`}
        />
      ) : null}
      {refused.length ? <NotComparable rows={refused} chapterId={chapter.id} /> : null}
    </section>
  );
}

function metricUnit(metric: MetricSummary): string {
  const unit = observationUnit({ unit: metric.units } as ObservationRow);
  return unit === "value" ? "" : unit;
}

function PairedRow({ row, a, b }: { row: ComparedRow; a: string; b: string }) {
  const unit = unitText(row.a.row);
  const overlap = intervalsOverlap(row.a.row, row.b.row);
  return (
    <div className="paired-row" data-testid={`compare-row-${row.measureId}`} data-period={row.period}>
      <h3>{row.label}</h3>
      <p className="subtle"><span title={row.metricCode} data-metric-code={row.metricCode}>{row.period}</span>{unit ? ` · ${unit}` : ""}</p>
      {[{ name: a, entry: row.a }, { name: b, entry: row.b }].map(({ name, entry }) => {
        const margin = marginOfErrorText(entry.row);
        return (
          <div key={name} className="paired-bar">
            <span className="paired-name">{name}</span>
            <span className="paired-track" aria-hidden="true">
              <span className="paired-fill" style={{ width: `${Math.max(2, barPosition(entry.value, row))}%` }} />
              {row.ticks.map((tick) => (
                <span key={tick.name} className="paired-tick" style={{ left: `${barPosition(tick.value, row)}%` }} title={tick.name} />
              ))}
            </span>
            <span className="paired-value">
              {`${formatObservationValue(entry.row.value)} ${unit}`.trim()}
              {margin && margin !== "Not provided" ? <span className="subtle paired-margin"> {margin}</span> : null}
            </span>
          </div>
        );
      })}
      {overlap ? (
        <p className="subtle" data-testid={`compare-row-${row.measureId}-overlap`}>
          The two margins of error overlap, so these places may not truly differ on this measure.
        </p>
      ) : null}
      {row.ticks.length ? (
        <p className="subtle">
          Reference marks: {row.ticks.map((tick) => `${tick.name} ${formatObservationValue(tick.value)}`).join(" · ")}
          {row.ticks.some((tick) => tick.value > Math.max(row.a.value, row.b.value)) ? " (a mark beyond the bar sits at its end)" : ""}
        </p>
      ) : null}
    </div>
  );
}

function NotComparable({ rows, chapterId }: { rows: NotComparableRow[]; chapterId: string }) {
  return (
    <div className="not-comparable" data-testid={`compare-chapter-${chapterId}-refused`}>
      <h3>Not comparable here</h3>
      <ul>
        {rows.map((row) => (
          <li key={row.measureId} data-testid={`compare-refused-${row.measureId}`}>
            <strong>{row.label}:</strong> {row.reasons.join(" ")}
          </li>
        ))}
      </ul>
    </div>
  );
}

interface PickerOption {
  geoId: string;
  name: string;
  path: string | null;
  note: string;
}


const PICKABLE = new Set(["NATIONAL", "STATE", "COUNTY", "PLACE", "METRO"]);
const SEARCH_NOTES: Record<string, string> = { STATE: "state", COUNTY: "county", PLACE: "city or town", METRO: "metro area" };

/**
 * What to compare a place with: the places it borders, lies in or contains
 * first (nearby-and-related-places), then any place a typed name finds, at
 * any level. If the search cannot be read, the same-level list from the
 * catalog answers instead.
 */
function ComparePicker({ base, states }: { base: ResolvedSide; states: GeographySummary[] }) {
  const router = useRouter();
  const level = base.place.level;
  const [query, setQuery] = useState("");
  const [related, setRelated] = useState<PickerOption[]>([]);
  const [found, setFound] = useState<PickerOption[] | null>(null);
  const [counties, setCounties] = useState<GeographySummary[] | null>(null);
  const [searchFailed, setSearchFailed] = useState(false);
  useEffect(() => {
    if (level === "NATIONAL") return;
    const controller = new AbortController();
    apiFetch<RelatedResponse>(relatedPath(base.place.geoId), { signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        const groups = groupNearby(payload, states, []);
        const levelOf = (geoId: string) => payload.items.find((item) => item.geo_id === geoId)?.geo_level || "";
        const entries = [
          ...groups.neighbours.map((entry) => ({ entry, note: "neighbouring county" })),
          ...groups.counties.map((entry) => ({ entry, note: "a county it lies in" })),
          ...groups.partOf.map((entry) => ({ entry, note: "it lies within this" })),
          ...groups.within.map((entry) => ({ entry, note: "a place within it" })),
        ];
        setRelated(
          entries
            .filter(({ entry }) => PICKABLE.has(levelOf(entry.geoId)) && (entry.href || levelOf(entry.geoId) === "METRO"))
            .map(({ entry, note }) => ({ geoId: entry.geoId, name: entry.name, path: entry.href, note })),
        );
      })
      .catch(() => undefined);
    return () => controller.abort();
  }, [level, base.place.geoId, states]);
  const needle = query.trim().toLowerCase();
  useEffect(() => {
    if (needle.length < 2 || searchFailed) {
      setFound(null);
      return;
    }
    const controller = new AbortController();
    const timer = setTimeout(() => {
      apiFetch<{ items: SearchItem[] }>("/catalog/geographies/search", { params: { q: needle, limit: 8 }, signal: controller.signal })
        .then((payload) => {
          if (controller.signal.aborted) return;
          setFound(
            (payload.items || [])
              .filter((item) => item.geo_id !== base.place.geoId && PICKABLE.has(item.geo_level) && (item.page_path || item.geo_level === "METRO"))
              .map((item) => ({ geoId: item.geo_id, name: item.geo_name, path: item.page_path || null, note: SEARCH_NOTES[item.geo_level] || "" })),
          );
        })
        .catch(() => { if (!controller.signal.aborted) setSearchFailed(true); });
    }, 200);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [needle, base.place.geoId, searchFailed]);
  useEffect(() => {
    if (!searchFailed || level !== "COUNTY" || counties || needle.length < 2) return;
    const controller = new AbortController();
    fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY" }, pageSize: 1000, signal: controller.signal })
      .then((items) => { if (!controller.signal.aborted) setCounties(items); })
      .catch(() => undefined);
    return () => controller.abort();
  }, [searchFailed, level, counties, needle]);
  const fallback: PickerOption[] = !searchFailed || !needle
    ? []
    : level === "COUNTY"
      ? (counties || []).filter((county) => county.geo_id !== base.place.geoId).map((county): PickerOption | null => {
          const state = states.find((item) => item.state_fips === county.state_fips);
          const sameState = (counties || []).filter((item) => item.state_fips === county.state_fips);
          return state ? { geoId: county.geo_id, name: `${countyName(county)}, ${stateName(state)}`, path: placePath(stateSegment(state, states), countySegment(county, sameState)), note: "" } : null;
        }).filter((entry): entry is PickerOption => Boolean(entry) && entry!.name.toLowerCase().includes(needle)).slice(0, 8)
      : states.filter((state) => state.geo_id !== base.place.geoId && stateName(state).toLowerCase().includes(needle)).map((state) => ({ geoId: state.geo_id, name: stateName(state), path: placePath(stateSegment(state, states)), note: "" })).slice(0, 8);
  const nearby = related.filter((entry) => !needle || entry.name.toLowerCase().includes(needle));
  const nation: PickerOption[] = level !== "NATIONAL" && (!needle || "united states".includes(needle)) && !nearby.some((entry) => entry.geoId === "us:1")
    ? [{ geoId: "us:1", name: NATION_NAME, path: "/us", note: "the nation" }]
    : [];
  const options = [...nearby, ...nation, ...(found || fallback)].filter(
    (option, index, all) => all.findIndex((other) => other.geoId === option.geoId) === index,
  );
  return (
    <div className="measure-switcher">
      <label>
        Compare {base.place.name} with
        <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="a county, city, state or metro" data-testid="compare-places-picker" />
      </label>
      {options.length ? (
        <ul className="place-index" data-testid="compare-places-picker-results">
          {options.map((option) => (
            <li key={option.geoId} data-geo-id={option.geoId}>
              <button type="button" className="text-link" onClick={() => router.push(compareHref(linkOf(base), option))}>{option.name}</button>
              {option.note ? <span className="subtle"> · {option.note}</span> : null}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
