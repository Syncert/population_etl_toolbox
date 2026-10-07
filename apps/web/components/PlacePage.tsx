"use client";

// One permanent page per place: the nation, a state, or a county (place-pages).
//
// The place is resolved through the geography catalog -- the address's
// segments are matched against the catalog's own rows, and the identity read
// is the row's FIPS-based `geo_id` -- and every value comes from the published
// observation routes. The chapters, their order, their measures, and the rules
// for omitting one are `lib/placeChapters.ts`; this component fetches and
// renders. It computes nothing beyond the trend index that module defines.

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { ArrowRight } from "lucide-react";
import StatusPill from "./StatusPill";
import {
  ApiError,
  apiErrorMessage,
  apiFetch,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getMetric,
} from "../lib/api/client";
import type { CollectionResponse, GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, marginOfErrorText, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName } from "../lib/format";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  OBSERVATION_UNCERTAINTY_BEYOND_MARGIN,
  buildNewestValueRequest,
  describeStratification,
  normalizeObservationRows,
  observationPeriodLabel,
  observationUncertaintyLabel,
  seriesDimensionNames,
} from "../lib/observationAccess";
import { buildUseCaseHistory, verifyUseCaseRows } from "../lib/useCaseAnalysis";
import {
  PLACE_CHAPTERS,
  buildTrend,
  chapterHasValues,
  countyName,
  countySegment,
  cityName,
  citySegment,
  omissionLine,
  placeChapterMetricCodes,
  placePath,
  resolveCitySegment,
  resolveCountySegment,
  resolvePlaceChapters,
  resolveStateSegment,
  stateName,
  stateSegment,
  threeLevelCard,
} from "../lib/placeChapters";
import type {
  LevelAnswer,
  LevelPlace,
  PlaceLevel,
  ResolvedChapter,
  ResolvedPlaceMeasure,
} from "../lib/placeChapters";
import { crossCountyNote, groupNearby, isEmpty, relatedPath, shareText } from "../lib/placeRelationships";
import type { NearbyGroups, RelatedResponse } from "../lib/placeRelationships";
import { rankSentence, standouts } from "../lib/distinctive";
import type { DistinctiveMeasure, DistinctiveResponse } from "../lib/distinctive";
import { explorerHref } from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";

const PlaceTrend = dynamic(() => import("./PlaceTrend"));
const WithinCounty = dynamic(() => import("./WithinCounty"));

const CATALOG_PAGE_SIZE = 1000;
/** How many observation requests one page keeps in flight at once. */
const REQUEST_CONCURRENCY = 6;

const LEVEL_WORDS: Record<PlaceLevel, string> = {
  NATIONAL: "Nation",
  STATE: "State",
  COUNTY: "County",
  PLACE: "City or town",
};

type Resolution =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "not-found"; within: GeographySummary | null }
  | { state: "found" };

/** Run `tasks` with at most `limit` in flight. */
async function inPool(tasks: (() => Promise<void>)[], limit: number): Promise<void> {
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, tasks.length) }, async () => {
    while (next < tasks.length) {
      const task = tasks[next];
      next += 1;
      await task!();
    }
  });
  await Promise.all(workers);
}

/** The row's published unit, or nothing: `observationUnit`'s "value" is a placeholder. */
function publishedUnit(row: ObservationRow): string {
  const unit = observationUnit(row);
  return unit === "value" ? "" : unit;
}

const answerKey = (metricCode: string, geoId: string) => `${metricCode}|${geoId}`;

function placeTitle(place: GeographySummary | null, level: PlaceLevel, state: GeographySummary | null): string {
  if (!place) return "";
  if (level === "COUNTY") return `${countyName(place)}, ${state ? stateName(state) : String(place.state_name || "")}`.replace(/, $/, "");
  if (level === "PLACE") return `${cityName(place)}, ${state ? stateName(state) : String(place.state_name || "")}`.replace(/, $/, "");
  if (level === "STATE") return stateName(place);
  return "United States";
}

export default function PlacePage({
  stateSegment: requestedState,
  countySegment: requestedCounty,
}: {
  stateSegment?: string;
  countySegment?: string;
}) {
  const router = useRouter();
  // The third segment names a county or, failing that, a city or town; which
  // one is known once the address resolves (acs-place-grain).
  const [localLevel, setLocalLevel] = useState<"COUNTY" | "PLACE">("COUNTY");
  const level: PlaceLevel = requestedCounty ? localLevel : requestedState ? "STATE" : "NATIONAL";

  const [resolution, setResolution] = useState<Resolution>({ state: "loading" });
  const [nation, setNation] = useState<GeographySummary | null>(null);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [state, setState] = useState<GeographySummary | null>(null);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [county, setCounty] = useState<GeographySummary | null>(null);
  const [city, setCity] = useState<GeographySummary | null>(null);
  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [metricsByCode, setMetricsByCode] = useState<Map<string, MetricSummary>>(new Map());
  const [catalogStatus, setCatalogStatus] = useState({ state: "loading", message: "resolving published measures" });
  const [answers, setAnswers] = useState<Map<string, LevelAnswer>>(new Map());
  const [histories, setHistories] = useState<Map<string, ObservationRow[]>>(new Map());
  const [observationStatus, setObservationStatus] = useState({ state: "idle", message: "waiting for the place" });
  const [search, setSearch] = useState("");
  const [nearby, setNearby] = useState<NearbyGroups | null>(null);
  const [nearbyNote, setNearbyNote] = useState("");
  const [distinctive, setDistinctive] = useState<DistinctiveResponse | null>(null);
  const [distinctiveNote, setDistinctiveNote] = useState("");
  const settled = useRef(false);

  // Resolve the address through the catalog.
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    setResolution({ state: "loading" });
    (async () => {
      try {
        const [nationalItems, stateItems] = await Promise.all([
          fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "NATIONAL" },
            pageSize: CATALOG_PAGE_SIZE,
            signal,
          }),
          fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" },
            pageSize: CATALOG_PAGE_SIZE,
            signal,
          }),
        ]);
        if (signal.aborted) return;
        const sortedStates = [...stateItems].sort((left, right) => stateName(left).localeCompare(stateName(right)));
        setNation(nationalItems[0] || null);
        setStates(sortedStates);
        if (level === "NATIONAL") {
          setResolution(nationalItems[0] ? { state: "found" } : { state: "not-found", within: null });
          return;
        }
        const stateMatch = resolveStateSegment(requestedState || "", sortedStates);
        if (!stateMatch.place) {
          setResolution({ state: "not-found", within: null });
          return;
        }
        setState(stateMatch.place);
        const countyItems = await fetchAllPages<GeographySummary>("/catalog/geographies", {
          params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY", state_fips: String(stateMatch.place.state_fips || "") },
          pageSize: CATALOG_PAGE_SIZE,
          signal,
        });
        if (signal.aborted) return;
        // The filter is the API's; the rows are checked against it anyway, so a
        // response that ignored it cannot list another state's counties here.
        const ownCounties = countyItems
          .filter((item) => item.state_fips === stateMatch.place!.state_fips)
          .sort((left, right) => countyName(left).localeCompare(countyName(right)));
        setCounties(ownCounties);
        if (level === "STATE") {
          if (stateMatch.canonical) router.replace(placePath(stateMatch.canonical));
          setResolution({ state: "found" });
          return;
        }
        const countyMatch = resolveCountySegment(requestedCounty || "", ownCounties);
        if (!countyMatch.place) {
          const placeItems = await fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "PLACE", state_fips: String(stateMatch.place.state_fips || "") },
            pageSize: CATALOG_PAGE_SIZE,
            signal,
          });
          if (signal.aborted) return;
          const ownPlaces = placeItems.filter((item) => item.state_fips === stateMatch.place!.state_fips);
          const cityMatch = resolveCitySegment(requestedCounty || "", ownPlaces, ownCounties);
          if (!cityMatch.place) {
            setResolution({ state: "not-found", within: stateMatch.place });
            return;
          }
          setCity(cityMatch.place);
          setLocalLevel("PLACE");
          if (stateMatch.canonical || cityMatch.canonical) {
            router.replace(
              placePath(
                stateSegment(stateMatch.place, sortedStates),
                citySegment(cityMatch.place, ownPlaces, ownCounties),
              ),
            );
          }
          setResolution({ state: "found" });
          return;
        }
        setLocalLevel("COUNTY");
        setCounty(countyMatch.place);
        if (stateMatch.canonical || countyMatch.canonical) {
          router.replace(
            placePath(
              stateSegment(stateMatch.place, sortedStates),
              countySegment(countyMatch.place, ownCounties),
            ),
          );
        }
        setResolution({ state: "found" });
      } catch (error) {
        if (!signal.aborted) setResolution({ state: "error", message: apiErrorMessage(error) });
      }
    })();
    return () => controller.abort();
    // `level` follows the resolution; the address alone decides what to fetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [requestedState, requestedCounty, router]);

  // The sources' declared access shapes, and the chapters' candidate
  // identities. A 404 is the API's answer for "not published".
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const payload = await getCapabilities({ signal: controller.signal });
        if (!controller.signal.aborted) setSources(buildExplorerSources(payload.items));
      } catch {
        // Every measure then reports that its source's access shape is unknown.
      }
    })();
    (async () => {
      const codes = placeChapterMetricCodes();
      const found = new Map<string, MetricSummary>();
      let failures = 0;
      await inPool(
        codes.map((code) => async () => {
          try {
            const metric = await getMetric(code, { signal: controller.signal });
            if (metric?.metric_code) found.set(metric.metric_code, metric);
          } catch (error) {
            if (!(error instanceof ApiError && error.status === 404)) failures += 1;
          }
        }),
        REQUEST_CONCURRENCY * 2,
      );
      if (controller.signal.aborted) return;
      setMetricsByCode(found);
      setCatalogStatus(
        failures
          ? { state: "warn", message: `${failures} of ${codes.length} measures could not be checked` }
          : { state: "ok", message: `${found.size} of ${codes.length} candidate measures are published` },
      );
    })();
    return () => controller.abort();
  }, []);

  const place = level === "PLACE" ? city : level === "COUNTY" ? county : level === "STATE" ? state : nation;
  const title = placeTitle(place, level, state);

  // What this place contains, borders and is part of, as the reference
  // recorded it (nearby-and-related-places).
  useEffect(() => {
    if (resolution.state !== "found" || !place) return;
    const controller = new AbortController();
    apiFetch<RelatedResponse>(relatedPath(place.geo_id), { signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        const groups = groupNearby(payload, states, counties);
        setNearby(groups);
        setNearbyNote(isEmpty(groups) ? "Nearby and related: the geography reference records no relationships for this place" : "");
      })
      .catch((error) => {
        if (!controller.signal.aborted) setNearbyNote(`Nearby and related: ${apiErrorMessage(error)}`);
      });
    return () => controller.abort();
  }, [resolution.state, place, states, counties]);
  // Where this place stands among its siblings, one measure at a time, as the
  // API derives it (what-makes-this-place-distinctive).
  useEffect(() => {
    if (resolution.state !== "found" || !place || level === "NATIONAL") return;
    const controller = new AbortController();
    apiFetch<DistinctiveResponse>("/place/distinctive", { params: { geo_id: place.geo_id }, signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        setDistinctive(payload);
        setDistinctiveNote(payload.ranked.length ? "" : "What stands out: no measure could be ranked for this place");
      })
      .catch((error) => {
        if (!controller.signal.aborted) setDistinctiveNote(`What stands out: ${apiErrorMessage(error)}`);
      });
    return () => controller.abort();
  }, [resolution.state, place, level]);

  const levels = useMemo((): LevelPlace[] => {
    const chain: LevelPlace[] = [];
    if (city && level === "PLACE") chain.push({ level: "PLACE", geoId: city.geo_id, name: cityName(city), role: "this place" });
    if (county) chain.push({ level: "COUNTY", geoId: county.geo_id, name: countyName(county), role: "this place" });
    if (state && level !== "NATIONAL") chain.push({ level: "STATE", geoId: state.geo_id, name: stateName(state), role: level === "STATE" ? "this place" : "parent" });
    if (nation) chain.push({ level: "NATIONAL", geoId: nation.geo_id, name: "United States", role: level === "NATIONAL" ? "this place" : "parent" });
    return chain;
  }, [city, county, state, nation, level]);

  const { shown, omitted } = useMemo(
    () => (metricsByCode.size ? resolvePlaceChapters(level, metricsByCode) : { shown: [], omitted: [] }),
    [level, metricsByCode],
  );

  const chapterLevels = (resolved: ResolvedChapter): LevelPlace[] =>
    resolved.stateContext
      ? levels.filter((entry) => entry.level !== "COUNTY").map((entry) => (entry.level === "STATE" ? { ...entry, role: "state context" } : entry))
      : levels;

  const sourceFor = (metric: MetricSummary | null) =>
    sources.find((source) => source.sourceCode === metric?.source_code) || null;

  // Every value the page shows: headline measures for each level, depth
  // measures for the chapter's own place, and one history per trend line.
  useEffect(() => {
    if (resolution.state !== "found" || !shown.length || !sources.length || !levels.length) return;
    const controller = new AbortController();
    const signal = controller.signal;
    const nextAnswers = new Map<string, LevelAnswer>();
    const nextHistories = new Map<string, ObservationRow[]>();
    const tasks: (() => Promise<void>)[] = [];
    let failures = 0;

    const askNewest = (measure: ResolvedPlaceMeasure, geoId: string) => async () => {
      const source = sourceFor(measure.metric);
      const key = answerKey(measure.metricCode, geoId);
      if (!source) {
        nextAnswers.set(key, { row: null, error: `No declared access shape for ${measure.metric?.source_code || "this source"}` });
        return;
      }
      try {
        const { resource, params } = buildNewestValueRequest(source, { metricCode: measure.metricCode, geoId });
        const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params, signal });
        const rows = normalizeObservationRows(source, Array.isArray(payload.items) ? payload.items : []);
        verifyUseCaseRows(rows, measure.metricCode, geoId);
        if (describeStratification(rows, seriesDimensionNames(source, "latest")).stratified) {
          nextAnswers.set(key, { row: null, error: "Several published strata describe this place; open the explorer to choose one." });
          return;
        }
        nextAnswers.set(key, { row: rows.at(-1) ?? null });
      } catch (error) {
        if (signal.aborted) return;
        failures += 1;
        nextAnswers.set(key, { row: null, error: apiErrorMessage(error) });
      }
    };

    const askHistory = (measure: ResolvedPlaceMeasure, place: LevelPlace) => async () => {
      const source = sourceFor(measure.metric);
      const reading = buildUseCaseHistory(source, measure.metric, place.geoId, place.level);
      if (!reading.request || !source) return;
      try {
        const result = await fetchCollectionPages<Observation>(reading.request.resource, {
          params: reading.request.params,
          pageSize: 500,
          maxPages: 2,
          signal,
        });
        const rows = normalizeObservationRows(source, result.items);
        verifyUseCaseRows(rows, measure.metricCode, place.geoId);
        if (!describeStratification(rows, seriesDimensionNames(source, "latest")).stratified) {
          nextHistories.set(answerKey(measure.metricCode, place.geoId), rows);
        }
      } catch {
        // The trend draws the lines that arrived; the cards state any error.
      }
    };

    for (const resolved of shown) {
      const chain = chapterLevels(resolved);
      const own = chain[0];
      for (const measure of resolved.headline) {
        if (!measure.metric) continue;
        for (const entry of chain) {
          if (publishesAt(measure, entry.level)) tasks.push(askNewest(measure, entry.geoId));
        }
        const trended = [resolved.chapter.trend, ...(resolved.chapter.moreTrends || [])].map((item) => item.measureId);
        if (trended.includes(measure.measure.id)) {
          for (const entry of chain) {
            if (publishesAt(measure, entry.level)) tasks.push(askHistory(measure, entry));
          }
        }
      }
      if (own) {
        for (const measure of resolved.depth) {
          if (measure.metric && publishesAt(measure, own.level)) tasks.push(askNewest(measure, own.geoId));
        }
      }
    }

    setObservationStatus({ state: "loading", message: `asking ${tasks.length} published series` });
    (async () => {
      await inPool(tasks, REQUEST_CONCURRENCY);
      if (signal.aborted) return;
      settled.current = true;
      setAnswers(nextAnswers);
      setHistories(nextHistories);
      setObservationStatus(
        failures
          ? { state: "warn", message: `${failures} of ${tasks.length} requests failed; each is stated where it belongs` }
          : { state: "ok", message: `${tasks.length} published series read` },
      );
    })();
    return () => controller.abort();
    // `chapterLevels` and `sourceFor` read only the dependencies listed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resolution.state, shown, sources, levels]);

  if (resolution.state === "not-found" || resolution.state === "error") {
    const within = resolution.state === "not-found" ? resolution.within : null;
    const candidates = within
      ? counties.map((item) => ({ name: `${countyName(item)}, ${stateName(within)}`, href: placePath(stateSegment(within, states), countySegment(item, counties)) }))
      : states.map((item) => ({ name: stateName(item), href: placePath(stateSegment(item, states)) }));
    const needle = search.trim().toLowerCase();
    const matches = needle ? candidates.filter((entry) => entry.name.toLowerCase().includes(needle)).slice(0, 20) : [];
    return (
      <main className="page-shell compact-page" data-testid="place-not-found">
        <div className="page-heading">
          <div className="section-kicker">Not found</div>
          <h1>There is no place at this address</h1>
          <p>
            {resolution.state === "error"
              ? `The geography catalog could not be read: ${resolution.message}`
              : within
                ? `${stateName(within)} has no county published under that name or code.`
                : "No state is published under that name or code."}
          </p>
        </div>
        <label className="place-search">
          Search places
          <input type="search" value={search} onChange={(event) => setSearch(event.target.value)} data-testid="place-search" />
        </label>
        {needle ? (
          <ul className="place-index" data-testid="place-search-results">
            {matches.map((entry) => <li key={entry.href}><Link href={entry.href}>{entry.name}</Link></li>)}
            {!matches.length ? <li>No published place matches “{search}”.</li> : null}
          </ul>
        ) : null}
        <p><Link className="text-link" href="/us">All states <ArrowRight size={13} /></Link></p>
      </main>
    );
  }

  const visible = shown.filter(
    (resolved) =>
      !settled.current ||
      chapterHasValues(resolved, (measure) => answers.get(answerKey(measure.metricCode, chapterLevels(resolved)[0]?.geoId || ""))?.row ?? null),
  );
  const emptied = settled.current ? shown.filter((resolved) => !visible.includes(resolved)) : [];
  const omissions = [
    ...(nearbyNote ? [nearbyNote] : []),
    ...(distinctiveNote ? [distinctiveNote] : []),
    ...omitted.map(omissionLine),
    ...emptied.map((resolved) => `${resolved.chapter.title}: no published values for this place`),
  ].sort((left, right) => chapterOrder(left) - chapterOrder(right));

  return (
    <main
      className="page-shell place-page"
      data-testid="place-page"
      data-level={level}
      data-geo-id={place?.geo_id || ""}
      data-ready={resolution.state === "found" && settled.current ? "true" : "false"}
    >
      <header className="page-heading">
        <div className="section-kicker">{LEVEL_WORDS[level]}</div>
        <h1>{title || "Loading place…"}</h1>
        <nav aria-label="Place hierarchy" className="place-breadcrumbs">
          <ol>
            <li><Link href="/us">United States</Link></li>
            {state && level !== "NATIONAL" ? <li><Link href={placePath(stateSegment(state, states))}>{stateName(state)}</Link></li> : null}
            {county ? <li aria-current="page">{countyName(county)}</li> : null}
            {city && level === "PLACE" ? <li aria-current="page">{cityName(city)}</li> : null}
          </ol>
        </nav>
        <p>
          The same chapters in the same order on every place page. Each headline number is shown beside
          {level === "NATIONAL" ? " its own history" : level === "STATE" ? " the nation's" : " its state's and the nation's"},
          {" "}with its period, source, and caveats. Nothing here is a score or a ranking.
        </p>
      </header>

      <section className="status-row" role="status">
        <StatusPill state={resolution.state === "found" ? "ok" : "loading"} label="Place" message={resolution.state === "found" ? place?.geo_id || "" : "resolving the address"} testId="place-status" />
        <StatusPill state={catalogStatus.state} label="Measures" message={catalogStatus.message} testId="place-catalog-status" />
        <StatusPill state={observationStatus.state} label="Observations" message={observationStatus.message} testId="place-observation-status" />
      </section>

      {distinctive && distinctive.ranked.length ? (
        <WhatStandsOut response={distinctive} siblingsName={level === "COUNTY" && state ? `${stateName(state)} counties` : "states"} />
      ) : null}

      {visible.length ? (
        <nav className="chapter-rail" aria-label="Chapters" data-testid="chapter-rail">
          <ol>
            {visible.map((resolved) => <li key={resolved.chapter.id}><a href={`#${resolved.chapter.id}`}>{resolved.chapter.title}</a></li>)}
          </ol>
        </nav>
      ) : null}

      {visible.map((resolved) => (
        <Chapter
          key={resolved.chapter.id}
          resolved={resolved}
          levels={chapterLevels(resolved)}
          pageLevel={level}
          county={county}
          state={state}
          answers={answers}
          histories={histories}
          sourceFor={sourceFor}
        />
      ))}

      {level === "PLACE" && city && nearby?.counties.length ? (
        <p className="place-cross-county" data-testid="place-cross-county">
          {crossCountyNote(cityName(city), nearby.counties)}
        </p>
      ) : null}

      {nearby && !isEmpty(nearby) ? (
        <section className="analysis-panel place-nearby" aria-labelledby="place-nearby-heading" data-testid="place-nearby">
          <h2 id="place-nearby-heading">Nearby and related</h2>
          {nearby.within.length ? (
            <div data-testid="place-nearby-within">
              <h3>Within this county</h3>
              <p className="subtle">Places whose boundaries overlap this county. A place can extend into another county.</p>
              <ul className="place-index">
                {nearby.within.map((entry) => (
                  <li key={entry.geoId} data-geo-id={entry.geoId}>
                    {entry.href ? <Link href={entry.href}>{entry.name}</Link> : entry.name}
                    {entry.share !== null ? <span className="subtle"> · {shareText(entry)}</span> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          {nearby.neighbours.length ? (
            <div data-testid="place-nearby-neighbours">
              <h3>Neighbouring counties</h3>
              <ul className="place-index">
                {nearby.neighbours.map((entry) => (
                  <li key={entry.geoId}>{entry.href ? <Link href={entry.href}>{entry.name}</Link> : entry.name}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {nearby.counties.length ? (
            <div data-testid="place-nearby-counties">
              <h3>Counties it lies in</h3>
              <ul className="place-index">
                {nearby.counties.map((entry) => (
                  <li key={entry.geoId}>
                    {entry.href ? <Link href={entry.href}>{entry.name}</Link> : entry.name}
                    {entry.share !== null ? <span className="subtle"> · {Math.round(entry.share * 100)}% of this place</span> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          {nearby.partOf.length ? (
            <div data-testid="place-nearby-part-of">
              <h3>Part of</h3>
              <ul className="place-index">
                {nearby.partOf.map((entry) => (
                  <li key={entry.geoId}>{entry.href ? <Link href={entry.href}>{entry.name}</Link> : entry.name}</li>
                ))}
              </ul>
            </div>
          ) : null}
          <p className="subtle">
            From the Census geography reference, vintage {[...new Set([...nearby.within, ...nearby.neighbours, ...nearby.partOf, ...nearby.counties].map((entry) => entry.vintage))].join(", ")}.
          </p>
        </section>
      ) : null}

      {level === "COUNTY" && county && resolution.state === "found" ? <WithinCounty county={county} /> : null}

      {(level === "NATIONAL" || level === "STATE") && resolution.state === "found" ? (
        <section className="analysis-panel place-children" aria-labelledby="place-children-heading">
          <h2 id="place-children-heading">{level === "NATIONAL" ? "States" : `Counties in ${stateName(state!)}`}</h2>
          <ul className="place-index" data-testid="place-children">
            {(level === "NATIONAL" ? states : counties).map((item) => (
              <li key={item.geo_id}>
                <Link href={level === "NATIONAL" ? placePath(stateSegment(item, states)) : placePath(stateSegment(state!, states), countySegment(item, counties))}>
                  {level === "NATIONAL" ? stateName(item) : countyName(item)}
                </Link>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <footer className="place-footer" data-testid="place-omissions">
        {omissions.length ? (
          <>
            <h2>Not on this page</h2>
            <ul>{omissions.map((line) => <li key={line}>{line}</li>)}</ul>
          </>
        ) : null}
        <p>
          <Link className="text-link" href="/data#rules" data-testid="place-rules-link">The rules every number here follows</Link>
        </p>
      </footer>
    </main>
  );
}

function chapterOrder(line: string): number {
  const index = PLACE_CHAPTERS.findIndex((chapter) => line.startsWith(`${chapter.title}:`));
  return index < 0 ? PLACE_CHAPTERS.length : index;
}

function publishesAt(measure: ResolvedPlaceMeasure, level: PlaceLevel): boolean {
  const grains = measure.metric?.valid_geo_grains;
  return Boolean(measure.metric) && (!Array.isArray(grains) || grains.includes(level));
}

function Chapter({
  resolved,
  levels,
  pageLevel,
  county,
  state,
  answers,
  histories,
  sourceFor,
}: {
  resolved: ResolvedChapter;
  levels: LevelPlace[];
  pageLevel: PlaceLevel;
  county: GeographySummary | null;
  state: GeographySummary | null;
  answers: Map<string, LevelAnswer>;
  histories: Map<string, ObservationRow[]>;
  sourceFor: (metric: MetricSummary | null) => ExplorerSource | null;
}) {
  const { chapter } = resolved;
  const own = levels[0];
  const trendMeasure = resolved.headline.find((measure) => measure.measure.id === chapter.trend.measureId) || null;
  const primary = trendMeasure?.metric ? trendMeasure : resolved.headline.find((measure) => measure.metric) || null;
  const primaryRow = primary && own ? answers.get(answerKey(primary.metricCode, own.geoId))?.row ?? null : null;
  const trend = trendMeasure?.metric
    ? buildTrend(
        levels.filter((entry) => publishesAt(trendMeasure, entry.level)),
        new Map(levels.map((entry) => [entry.geoId, histories.get(answerKey(trendMeasure.metricCode, entry.geoId)) || []])),
        chapter.trend.scale,
      )
    : null;
  const moreTrends = (chapter.moreTrends || []).flatMap((spec) => {
    const measure = resolved.headline.find((entry) => entry.measure.id === spec.measureId);
    if (!measure?.metric) return [];
    const model = buildTrend(
      levels.filter((entry) => publishesAt(measure, entry.level)),
      new Map(levels.map((entry) => [entry.geoId, histories.get(answerKey(measure.metricCode, entry.geoId)) || []])),
      spec.scale,
    );
    return model ? [{ measure, model }] : [];
  });
  const sourcesShown = [...new Set([...resolved.headline, ...resolved.depth].map((measure) => measure.metric?.source_code).filter(Boolean))];
  const periods = [...new Set(
    [...resolved.headline, ...resolved.depth]
      .map((measure) => (own ? answers.get(answerKey(measure.metricCode, own.geoId))?.row : null))
      .filter((row): row is ObservationRow => Boolean(row))
      .map((row) => observationPeriodLabel(row)),
  )];
  const exploreLink = primary && own
    ? explorerHref({
        metric: primary.metricCode,
        source: primary.metric?.source_code || undefined,
        geoId: own.geoId,
        geoLevel: own.level as GeoLevel,
        stateFips: (own.level === "STATE" ? state?.state_fips : own.level === "COUNTY" ? county?.state_fips : undefined) || undefined,
      })
    : "";

  return (
    <section className="analysis-panel place-chapter" id={chapter.id} aria-labelledby={`${chapter.id}-heading`} data-testid={`chapter-${chapter.id}`} data-read-grain={resolved.readGrain}>
      <div className="panel-heading">
        <div>
          <h2 id={`${chapter.id}-heading`}>{chapter.title}</h2>
          <p className="subtle">{chapter.description}</p>
        </div>
      </div>
      {resolved.stateContext ? (
        <p className="notice" data-testid={`chapter-${chapter.id}-state-context`}>
          State context: the program publishes these offenses for states and the nation, not for counties.
          The figures below are {state ? stateName(state) : "the state"}&apos;s, not {county ? countyName(county) : "this county"}&apos;s.
        </p>
      ) : null}
      <div className="place-cards">
        {resolved.headline.map((measure) => (
          <HeadlineCard key={measure.measure.id} measure={measure} levels={levels} answers={answers} pageLevel={pageLevel} stateContext={resolved.stateContext} county={county} />
        ))}
      </div>
      {trend && trendMeasure ? (
        <PlaceTrend
          model={trend}
          label={`${trendMeasure.measure.label} trend`}
          unit={primaryRow ? publishedUnit(primaryRow) : String(trendMeasure.metric?.units || "")}
          testId={`chapter-${chapter.id}-trend`}
        />
      ) : null}
      {moreTrends.map(({ measure, model }) => (
        <PlaceTrend
          key={measure.measure.id}
          model={model}
          label={`${measure.measure.label} trend`}
          unit={String(measure.metric?.units || "")}
          testId={`chapter-${chapter.id}-trend-${measure.measure.id}`}
        />
      ))}
      {resolved.depth.some((measure) => !measure.measure.group) && own ? (
        <dl className="place-depth" data-testid={`chapter-${chapter.id}-depth`}>
          {resolved.depth.filter((measure) => !measure.measure.group).map((measure) => (
            <DepthRow key={measure.measure.id} measure={measure} place={own} answer={answers.get(answerKey(measure.metricCode, own.geoId))} />
          ))}
        </dl>
      ) : null}
      {own ? (
        <EarningsByIndustry
          rows={resolved.depth.filter((measure) => measure.measure.group === "bea-earnings" && measure.metric)}
          place={own}
          answers={answers}
        />
      ) : null}
      {own ? (
        <IndustryMix
          rows={resolved.depth.filter((measure) => measure.measure.group === "industry-mix" && measure.metric)}
          place={own}
          answers={answers}
        />
      ) : null}
      <footer className="place-chapter-footer" data-testid={`chapter-${chapter.id}-footer`}>
        <p>
          <strong>Period:</strong> {periods.length ? periods.join("; ") : "none published for this place"} ·{" "}
          <strong>Source:</strong> {sourcesShown.join(", ") || "not published"}
        </p>
        <p>{chapter.caveat}</p>
        {primary?.metric && publishesAt(primary, "COUNTY") ? (
          <p>
            <Link className="text-link" href={`/map/${encodeURIComponent(primary.metricCode)}`} data-testid={`chapter-${chapter.id}-map`}>
              See {primary.measure.label.toLowerCase()} for every county on a map <ArrowRight size={13} />
            </Link>
          </p>
        ) : null}
        {exploreLink ? (
          <p>
            <Link className="text-link" href={exploreLink} data-testid={`chapter-${chapter.id}-explore`}>
              Explore {primary?.measure.label.toLowerCase()} for {own?.name} <ArrowRight size={13} />
            </Link>
          </p>
        ) : null}
      </footer>
    </section>
  );
}

function HeadlineCard({
  measure,
  levels,
  answers,
  pageLevel,
  stateContext,
  county,
}: {
  measure: ResolvedPlaceMeasure;
  levels: LevelPlace[];
  answers: Map<string, LevelAnswer>;
  pageLevel: PlaceLevel;
  stateContext: boolean;
  county: GeographySummary | null;
}) {
  if (!measure.metric) {
    return (
      <div className="place-card" data-testid={`card-${measure.measure.id}`} data-available="false">
        <h3>{measure.measure.label}</h3>
        <p className="subtle">Not published by this warehouse (looked for {measure.measure.candidates.join(", ")}).</p>
      </div>
    );
  }
  const byGeo = new Map(levels.map((entry) => [entry.geoId, answers.get(answerKey(measure.metricCode, entry.geoId)) || { row: null }]));
  const card = threeLevelCard(levels, byGeo, (level) => publishesAt(measure, level));
  const unit = card.rows.find((entry) => entry.row)?.row;
  return (
    <div className="place-card" data-testid={`card-${measure.measure.id}`} data-available="true" data-period={card.period}>
      <h3>{measure.measure.label}</h3>
      <p className="subtle">
        {displayMetricName(measure.metric)} · {measure.metricCode}
        {card.period ? ` · ${card.period}` : ""}
        {unit ? ` · ${publishedUnit(unit) || "unit not published"}` : ""}
      </p>
      <table className="place-card-table">
        <caption className="sr-only">{measure.measure.label}{card.period ? `, ${card.period}` : ""}</caption>
        <tbody>
          {stateContext && pageLevel === "COUNTY" ? (
            <tr data-testid={`card-${measure.measure.id}-county`}>
              <th scope="row">{county ? countyName(county) : "This county"}</th>
              <td colSpan={2}>Not published at county grain</td>
            </tr>
          ) : null}
          {card.rows.map((entry) => (
            <tr key={entry.place.geoId} data-testid={`card-${measure.measure.id}-${entry.place.level.toLowerCase()}`}>
              <th scope="row">
                {entry.place.name}
                {entry.place.role === "state context" ? <span className="place-role"> (state context)</span> : null}
              </th>
              <td>
                {entry.row && !entry.message ? `${formatObservationValue(entry.row.value)} ${publishedUnit(entry.row)}`.trim() : entry.message}
              </td>
              <td className="subtle">
                {entry.row && !entry.message ? uncertaintyText(entry.row) : ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {measure.measure.basis ? (
        <p className="place-basis" data-testid={`card-${measure.measure.id}-basis`}>{measure.measure.basis}</p>
      ) : null}
      {measure.measure.showReported ? <ReportedLine measure={measure} row={card.rows[0]?.row ?? null} /> : null}
      {measure.measure.note ? <p className="subtle">{measure.measure.note}</p> : null}
    </div>
  );
}

/**
 * Earnings by place of work, by NAICS sector (bea-regional-accounts).
 *
 * Each row is BEA's own published figure for its newest year; nothing here
 * sums the sectors or computes a share, so a withheld sector reads as
 * withheld rather than as a gap someone filled with arithmetic.
 */
function EarningsByIndustry({
  rows,
  place,
  answers,
}: {
  rows: ResolvedPlaceMeasure[];
  place: LevelPlace;
  answers: Map<string, LevelAnswer>;
}) {
  if (!rows.length) return null;
  const read = rows.map((measure) => ({ measure, answer: answers.get(answerKey(measure.metricCode, place.geoId)) }));
  const period = read.map((entry) => (entry.answer?.row ? observationPeriodLabel(entry.answer.row) : "")).find(Boolean) || "";
  const unit = read.map((entry) => (entry.answer?.row ? publishedUnit(entry.answer.row) : "")).find(Boolean) || "";
  return (
    <div className="table-scroll" data-testid="bea-earnings">
      <table className="place-industry-mix">
        <caption>
          Earnings from work located in {place.name}, by industry{period ? `, ${period}` : ""}
          <span className="place-basis" data-testid="bea-earnings-basis"> · {rows[0]!.measure.basis}</span>
        </caption>
        <thead>
          <tr>
            <th scope="col">Industry</th>
            <th scope="col">Earnings{unit ? ` (${unit})` : ""}</th>
          </tr>
        </thead>
        <tbody>
          {read.map(({ measure, answer }) => {
            const row = answer?.row ?? null;
            const published = row && row.value !== null && row.value !== undefined;
            return (
              <tr key={measure.measure.id} data-testid={`bea-earnings-${measure.measure.id}`}>
                <th scope="row">{measure.measure.label}</th>
                <td>
                  {!publishesAt(measure, place.level)
                    ? `Not published at ${place.level.toLowerCase()} grain`
                    : answer?.error
                      ? answer.error
                      : published
                        ? formatObservationValue(row!.value)
                        : row?.value_status
                          ? `Published without a value: ${String(row.value_status)}`
                          : "Not published for this place"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Private jobs located in this place, by NAICS sector (bls-qcew-county-wages).
 *
 * Each row is QCEW's own published figure for its newest month; nothing here
 * sums the sectors or computes a share, so a withheld sector reads as
 * withheld rather than as a gap someone filled with arithmetic.
 */
function IndustryMix({
  rows,
  place,
  answers,
}: {
  rows: ResolvedPlaceMeasure[];
  place: LevelPlace;
  answers: Map<string, LevelAnswer>;
}) {
  if (!rows.length) return null;
  const read = rows.map((measure) => ({ measure, answer: answers.get(answerKey(measure.metricCode, place.geoId)) }));
  const period = read.map((entry) => (entry.answer?.row ? observationPeriodLabel(entry.answer.row) : "")).find(Boolean) || "";
  return (
    <div className="table-scroll" data-testid="industry-mix">
      <table className="place-industry-mix">
        <caption>
          Private jobs located in {place.name}, by industry{period ? `, ${period}` : ""}
          <span className="place-basis" data-testid="industry-mix-basis"> · {rows[0]!.measure.basis}</span>
        </caption>
        <thead>
          <tr>
            <th scope="col">Industry</th>
            <th scope="col">Jobs</th>
          </tr>
        </thead>
        <tbody>
          {read.map(({ measure, answer }) => {
            const row = answer?.row ?? null;
            const published = row && row.value !== null && row.value !== undefined;
            return (
              <tr key={measure.measure.id} data-testid={`industry-mix-${measure.measure.id}`}>
                <th scope="row">{measure.measure.label}</th>
                <td>
                  {!publishesAt(measure, place.level)
                    ? `Not published at ${place.level.toLowerCase()} grain`
                    : answer?.error
                      ? answer.error
                      : published
                        ? formatObservationValue(row!.value)
                        : row?.value_status
                          ? `Published without a value: ${String(row.value_status)}`
                          : "Not published for this place"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * What jurisdictions reported directly, beside the Bureau's estimate
 * (census-building-permits). Read from the row as published; nothing is
 * subtracted, so the imputed part is stated rather than computed.
 */
function ReportedLine({ measure, row }: { measure: ResolvedPlaceMeasure; row: ObservationRow | null }) {
  const dimensions = (row?.dimensions || {}) as Record<string, unknown>;
  const reported = dimensions["reported_value"];
  if (!row || row.value === null || row.value === undefined || reported === null || reported === undefined || reported === "") {
    return null;
  }
  return (
    <p className="subtle" data-testid={`card-${measure.measure.id}-reported`}>
      Reported directly by permit offices: {formatObservationValue(reported as string)} of {formatObservationValue(row.value)}; the Bureau estimates the rest for offices that did not report.
    </p>
  );
}

function uncertaintyText(row: ObservationRow): string {
  const margin = marginOfErrorText(row);
  const beyond = observationUncertaintyLabel(row, OBSERVATION_UNCERTAINTY_BEYOND_MARGIN);
  return [margin !== "Not provided" ? `margin of error ${margin}` : "", beyond].filter(Boolean).join(" · ");
}

function DepthRow({
  measure,
  place,
  answer,
}: {
  measure: ResolvedPlaceMeasure;
  place: LevelPlace;
  answer: LevelAnswer | undefined;
}) {
  const row = answer?.row ?? null;
  const published = row && row.value !== null && row.value !== undefined;
  return (
    <div data-testid={`depth-${measure.measure.id}`}>
      <dt>
        {measure.measure.label}
        {measure.measure.universe ? <span className="place-universe"> · Universe: {measure.measure.universe}</span> : null}
        {measure.measure.basis ? <span className="place-basis" data-testid={`depth-${measure.measure.id}-basis`}> · {measure.measure.basis}</span> : null}
      </dt>
      <dd>
        {!measure.metric
          ? "Not published by this warehouse"
          : !publishesAt(measure, place.level)
            ? `Not published at ${place.level.toLowerCase()} grain`
            : answer?.error
              ? answer.error
              : published
                ? `${formatObservationValue(row!.value)} ${publishedUnit(row!)}`.trim()
                : row?.value_status
                  ? `Published without a value: ${String(row.value_status)}`
                  : "Not published for this place"}
        {published ? <span className="subtle"> · {observationPeriodLabel(row)}{uncertaintyText(row!) ? ` · ${uncertaintyText(row!)}` : ""}</span> : null}
      </dd>
    </div>
  );
}

function StandoutRow({ measure, siblingsName }: { measure: DistinctiveMeasure; siblingsName: string }) {
  const unit = measure.units && measure.units !== "value" ? ` ${measure.units}` : "";
  return (
    <li data-testid={`standout-${measure.metric_code}`}>
      <strong>{measure.metric_display_name || measure.metric_code}</strong>
      <span> · {formatObservationValue(measure.value)}{unit}{measure.period_start ? ` · period beginning ${measure.period_start}` : ""}</span>
      <p className="subtle">{rankSentence(measure, siblingsName)}.</p>
      {measure.caveats.map((caveat) => <p key={caveat} className="subtle">{caveat}</p>)}
      <Link className="text-link" href={`/map/${encodeURIComponent(measure.metric_code)}`}>See every county on a map</Link>
    </li>
  );
}

function WhatStandsOut({ response, siblingsName }: { response: DistinctiveResponse; siblingsName: string }) {
  const { highest, lowest, rankedCount } = standouts(response);
  return (
    <section className="analysis-panel place-standout" aria-labelledby="standout-heading" data-testid="place-standout">
      <h2 id="standout-heading">What stands out</h2>
      <p className="subtle" data-testid="place-standout-count">
        {rankedCount} measure{rankedCount === 1 ? "" : "s"} could be ranked among {siblingsName}, each on its own. A rank
        says where this place falls, not how far apart the values are, and nothing here adds measures together.
        Places whose values carry overlapping margins of error may not truly differ.
      </p>
      <div className="place-standout-groups">
        <div>
          <h3>Among the highest</h3>
          <ul data-testid="place-standout-highest">{highest.map((measure) => <StandoutRow key={measure.metric_code} measure={measure} siblingsName={siblingsName} />)}</ul>
        </div>
        {lowest.length ? (
          <div>
            <h3>Among the lowest</h3>
            <ul data-testid="place-standout-lowest">{lowest.map((measure) => <StandoutRow key={measure.metric_code} measure={measure} siblingsName={siblingsName} />)}</ul>
          </div>
        ) : null}
      </div>
    </section>
  );
}
