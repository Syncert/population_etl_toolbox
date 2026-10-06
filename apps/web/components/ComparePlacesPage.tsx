"use client";

// Two places, the same chapters (compare-two-places). Each side resolves
// through the geography catalog like a place page; each headline measure is
// offered only as far as `/comparison/preflight` allows and both places
// published it for one shared period. See `lib/placeComparison.ts`.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { ArrowLeftRight } from "lucide-react";
import StatusPill from "./StatusPill";
import {
  ApiError,
  apiErrorMessage,
  apiFetch,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getComparisonPreflight,
  getMetric,
} from "../lib/api/client";
import type { CollectionResponse, ComparisonPreflight, GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, observationUnit } from "../lib/explorerViewModel";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildNewestValueRequest,
  normalizeObservationRows,
} from "../lib/observationAccess";
import { buildUseCaseHistory, verifyUseCaseRows } from "../lib/useCaseAnalysis";
import {
  PLACE_CHAPTERS,
  buildTrend,
  countyName,
  countySegment,
  placePath,
  resolveCountySegment,
  resolveStateSegment,
  stateName,
  stateSegment,
} from "../lib/placeChapters";
import type { LevelPlace, PlaceLevel } from "../lib/placeChapters";
import { barPosition, compareMeasure, grainMismatch, pairPath } from "../lib/placeComparison";
import type { ComparedPlace, ComparedRow, NotComparableRow } from "../lib/placeComparison";
import { comparisonHref } from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";

const PlaceTrend = dynamic(() => import("./PlaceTrend"));

export interface SideAddress {
  state: string;
  county?: string;
}

interface ResolvedSide {
  place: ComparedPlace;
  path: string;
  state: GeographySummary;
  county: GeographySummary | null;
}

const keyOf = (code: string, geoId: string) => `${code}|${geoId}`;
const unitText = (row: ObservationRow) => (observationUnit(row) === "value" ? "" : observationUnit(row));

async function resolveSide(
  address: SideAddress,
  states: GeographySummary[],
  signal: AbortSignal,
): Promise<ResolvedSide | null> {
  const stateMatch = resolveStateSegment(address.state, states);
  if (!stateMatch.place) return null;
  const state = stateMatch.place;
  const statePath = placePath(stateSegment(state, states));
  if (!address.county) {
    return { place: { geoId: state.geo_id, name: stateName(state), level: "STATE" }, path: statePath, state, county: null };
  }
  const counties = (await fetchAllPages<GeographySummary>("/catalog/geographies", {
    params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY", state_fips: String(state.state_fips || "") },
    pageSize: 1000,
    signal,
  })).filter((item) => item.state_fips === state.state_fips);
  const countyMatch = resolveCountySegment(address.county, counties);
  if (!countyMatch.place) return null;
  const county = countyMatch.place;
  return {
    place: { geoId: county.geo_id, name: `${countyName(county)}, ${stateName(state)}`, level: "COUNTY" },
    path: placePath(stateSegment(state, states), countySegment(county, counties)),
    state,
    county,
  };
}

export default function ComparePlacesPage({ a: addressA, b: addressB }: { a: SideAddress; b: SideAddress }) {
  const [sides, setSides] = useState<{ a: ResolvedSide; b: ResolvedSide } | null>(null);
  const [nation, setNation] = useState<GeographySummary | null>(null);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [problem, setProblem] = useState("");
  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [metrics, setMetrics] = useState<Map<string, MetricSummary>>(new Map());
  const [preflights, setPreflights] = useState<Map<string, ComparisonPreflight | string>>(new Map());
  const [answers, setAnswers] = useState<Map<string, ObservationRow | null>>(new Map());
  const [histories, setHistories] = useState<Map<string, ObservationRow[]>>(new Map());
  const [status, setStatus] = useState({ state: "loading", message: "resolving both places" });

  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    (async () => {
      try {
        const [nations, stateItems, capabilities] = await Promise.all([
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "NATIONAL" }, pageSize: 1000, signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" }, pageSize: 1000, signal }),
          getCapabilities({ signal }),
        ]);
        setNation(nations[0] || null);
        setStates(stateItems);
        setSources(buildExplorerSources(capabilities.items));
        const [a, b] = await Promise.all([resolveSide(addressA, stateItems, signal), resolveSide(addressB, stateItems, signal)]);
        if (signal.aborted) return;
        if (!a || !b) {
          setProblem("One of these addresses names no published place.");
          return;
        }
        setSides({ a, b });
        const found = new Map<string, MetricSummary>();
        await Promise.all(
          PLACE_CHAPTERS.flatMap((chapter) => chapter.headline).flatMap((measure) => measure.candidates).map(async (code) => {
            try {
              const metric = await getMetric(code, { signal });
              if (metric?.metric_code) found.set(metric.metric_code, metric);
            } catch (error) {
              if (!(error instanceof ApiError && error.status === 404)) throw error;
            }
          }),
        );
        if (!signal.aborted) setMetrics(found);
      } catch (error) {
        if (!signal.aborted) setProblem(apiErrorMessage(error));
      }
    })();
    return () => controller.abort();
  }, [addressA, addressB]);

  const mismatch = sides ? grainMismatch(sides.a.place, sides.b.place) : null;
  const level: PlaceLevel = sides?.a.place.level || "COUNTY";

  const measures = useMemo(
    () =>
      PLACE_CHAPTERS.map((chapter) => ({
        chapter,
        measures: chapter.headline.flatMap((measure) => {
          const code = measure.candidates.find((candidate) => metrics.has(candidate));
          const metric = code ? metrics.get(code)! : null;
          const grains = metric?.valid_geo_grains;
          return metric && code && (!Array.isArray(grains) || grains.includes(level)) ? [{ measure, code, metric }] : [];
        }),
      })),
    [metrics, level],
  );

  const parentsFor = (pair: { a: ResolvedSide; b: ResolvedSide }): LevelPlace[] => {
    const parents: LevelPlace[] = [];
    if (pair.a.place.level === "COUNTY") {
      parents.push({ level: "STATE", geoId: pair.a.state.geo_id, name: stateName(pair.a.state), role: "parent" });
      if (pair.b.state.geo_id !== pair.a.state.geo_id) {
        parents.push({ level: "STATE", geoId: pair.b.state.geo_id, name: stateName(pair.b.state), role: "parent" });
      }
    }
    if (nation) parents.push({ level: "NATIONAL", geoId: nation.geo_id, name: "United States", role: "parent" });
    return parents;
  };

  useEffect(() => {
    if (!sides || mismatch || !sources.length || !metrics.size) return;
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
            { level: sides.a.place.level, geoId: sides.a.place.geoId, name: sides.a.place.name, role: "this place" },
            { level: sides.b.place.level, geoId: sides.b.place.geoId, name: sides.b.place.name, role: "this place" },
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
  }, [sides, mismatch, sources, metrics, measures, nation]);

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

  if (sides && mismatch) {
    const finer = sides.a.place.level === "COUNTY" ? sides.a : sides.b;
    const coarser = finer === sides.a ? sides.b : sides.a;
    const offer = finer === sides.a ? pairPath(placePath(stateSegment(finer.state, states)), coarser.path) : pairPath(coarser.path, placePath(stateSegment(finer.state, states)));
    return (
      <main className="page-shell compact-page" data-testid="compare-places-grain">
        <header className="page-heading">
          <div className="section-kicker">Compare two places</div>
          <h1>These places are not the same kind of place</h1>
          <p>{mismatch}</p>
        </header>
        <p>
          <Link className="text-link" href={offer} data-testid="compare-places-parent-offer">
            Compare {stateName(finer.state)} with {coarser.place.name} instead
          </Link>
        </p>
      </main>
    );
  }

  const title = sides ? `${sides.a.place.name} and ${sides.b.place.name}` : "Comparing two places…";
  const swapped = sides ? pairPath(sides.b.path, sides.a.path) : "";
  const parents = sides ? parentsFor(sides) : [];

  return (
    <main className="page-shell compare-places-page" data-testid="compare-places" data-ready={status.state === "ok" ? "true" : "false"}>
      <header className="page-heading">
        <div className="section-kicker">Compare two places</div>
        <h1>{title}</h1>
        <p>The place page&apos;s chapters, both places side by side in one shared period, with {parents.map((parent) => parent.name).join(" and ") || "their parents"} as reference marks.</p>
        {sides ? (
          <div className="command-row">
            <Link className="button secondary" href={swapped} data-testid="compare-places-swap">
              <ArrowLeftRight size={15} aria-hidden="true" /> Swap sides
            </Link>
            <Link className="text-link" href={sides.a.path}>{sides.a.place.name}</Link>
            <Link className="text-link" href={sides.b.path}>{sides.b.place.name}</Link>
          </div>
        ) : null}
        {sides ? <ComparePicker base={sides.a} level={level} states={states} /> : null}
      </header>
      <section className="status-row" role="status">
        <StatusPill state={status.state} label="Comparison" message={status.message} testId="compare-places-status" />
      </section>

      {sides && status.state === "ok"
        ? measures.map(({ chapter, measures: chapterMeasures }) => {
            const outcomes = chapterMeasures.map(({ measure, code }) => {
              const verdict = preflights.get(code);
              return compareMeasure(
                {
                  measureId: measure.id,
                  label: measure.label,
                  metricCode: code,
                  preflight: typeof verdict === "object" ? verdict : null,
                  preflightError: typeof verdict === "string" ? verdict : undefined,
                  a: answers.get(keyOf(code, sides.a.place.geoId)) ?? null,
                  b: answers.get(keyOf(code, sides.b.place.geoId)) ?? null,
                  parents: parents.map((parent) => ({ name: parent.name, row: answers.get(keyOf(code, parent.geoId)) ?? null })),
                },
                sides.a.place,
                sides.b.place,
              );
            });
            const rows = outcomes.flatMap((outcome) => (outcome.row ? [outcome.row] : []));
            const refused = outcomes.flatMap((outcome) => (outcome.refused ? [outcome.refused] : []));
            if (!rows.length && !refused.length) return null;
            const trendMeasure = chapterMeasures.find(({ measure }) => measure.id === chapter.trend.measureId);
            const trendRow = trendMeasure ? rows.find((row) => row.metricCode === trendMeasure.code) : null;
            const trendLevels: LevelPlace[] = [
              { level: sides.a.place.level, geoId: sides.a.place.geoId, name: sides.a.place.name, role: "this place" },
              { level: sides.b.place.level, geoId: sides.b.place.geoId, name: sides.b.place.name, role: "this place" },
              ...parents.filter((parent) => parent.level === "NATIONAL"),
            ];
            return (
              <section key={chapter.id} className="analysis-panel" aria-labelledby={`compare-${chapter.id}`} data-testid={`compare-chapter-${chapter.id}`}>
                <h2 id={`compare-${chapter.id}`}>{chapter.title}</h2>
                {rows.map((row) => <PairedRow key={row.measureId} row={row} a={sides.a.place.name} b={sides.b.place.name} />)}
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
          })
        : null}

      <footer className="place-footer" data-testid="compare-places-footnote">
        <h2>What this comparison cannot say</h2>
        <ul>
          <li>It does not say why the two places differ. Two numbers side by side are not a cause.</li>
          <li>It is not a ranking. Each row is one measure; nothing here adds them up.</li>
          <li>Every row names its period, and a measure the two places did not publish for the same period is listed rather than shown.</li>
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

function PairedRow({ row, a, b }: { row: ComparedRow; a: string; b: string }) {
  const unit = unitText(row.a.row);
  return (
    <div className="paired-row" data-testid={`compare-row-${row.measureId}`} data-period={row.period}>
      <h3>{row.label}</h3>
      <p className="subtle">{row.metricCode} · {row.period}{unit ? ` · ${unit}` : ""}</p>
      {[{ name: a, entry: row.a }, { name: b, entry: row.b }].map(({ name, entry }) => (
        <div key={name} className="paired-bar">
          <span className="paired-name">{name}</span>
          <span className="paired-track" aria-hidden="true">
            <span className="paired-fill" style={{ width: `${Math.max(2, barPosition(entry.value, row))}%` }} />
            {row.ticks.map((tick) => (
              <span key={tick.name} className="paired-tick" style={{ left: `${barPosition(tick.value, row)}%` }} title={tick.name} />
            ))}
          </span>
          <span className="paired-value">{`${formatObservationValue(entry.row.value)} ${unit}`.trim()}</span>
        </div>
      ))}
      {row.ticks.length ? (
        <p className="subtle">Reference marks: {row.ticks.map((tick) => `${tick.name} ${formatObservationValue(tick.value)}`).join(" · ")}</p>
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

function ComparePicker({ base, level, states }: { base: ResolvedSide; level: PlaceLevel; states: GeographySummary[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [counties, setCounties] = useState<GeographySummary[] | null>(null);
  useEffect(() => {
    if (level !== "COUNTY" || counties || query.trim().length < 2) return;
    const controller = new AbortController();
    fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY" }, pageSize: 1000, signal: controller.signal })
      .then((items) => { if (!controller.signal.aborted) setCounties(items); })
      .catch(() => undefined);
    return () => controller.abort();
  }, [level, counties, query]);
  const needle = query.trim().toLowerCase();
  const options = !needle
    ? []
    : level === "COUNTY"
      ? (counties || []).filter((county) => county.geo_id !== base.place.geoId).map((county) => {
          const state = states.find((item) => item.state_fips === county.state_fips);
          const sameState = (counties || []).filter((item) => item.state_fips === county.state_fips);
          return state ? { name: `${countyName(county)}, ${stateName(state)}`, path: placePath(stateSegment(state, states), countySegment(county, sameState)) } : null;
        }).filter((entry): entry is { name: string; path: string } => Boolean(entry) && entry!.name.toLowerCase().includes(needle)).slice(0, 8)
      : states.filter((state) => state.geo_id !== base.place.geoId && stateName(state).toLowerCase().includes(needle)).map((state) => ({ name: stateName(state), path: placePath(stateSegment(state, states)) })).slice(0, 8);
  return (
    <div className="measure-switcher">
      <label>
        Compare {base.place.name} with
        <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} data-testid="compare-places-picker" />
      </label>
      {options.length ? (
        <ul className="place-index" data-testid="compare-places-picker-results">
          {options.map((option) => (
            <li key={option.path}>
              <button type="button" className="text-link" onClick={() => router.push(pairPath(base.path, option.path))}>{option.name}</button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
