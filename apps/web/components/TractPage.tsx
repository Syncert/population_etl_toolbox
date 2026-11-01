"use client";

// One Census tract's page (tract pages, D-4).
//
// The address `/us/<state>/<county>/tract-18-05` is resolved through the
// geography catalog the way every place page is: the state and county
// segments against the catalog's own rows, the tract against the county's
// tracts (a read scoped to that one county, never the grain). Only then is
// anything about the tract read, in two requests: the chapters' newest values
// for the tract, its county, its state and the nation as one batch, and the
// tract's history on its 2020 boundary (`/place/tract-history`, API-187),
// which also says whether the tract's code named the same area before 2020.
// A number the API derived across the redraw is marked as derived wherever it
// is shown. The measures and words are `lib/tractPage.ts`.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { ArrowRight } from "lucide-react";
import StatusPill from "./StatusPill";
import { apiErrorMessage, apiFetch, fetchAllPages } from "../lib/api/client";
import type { GeographySummary } from "../lib/api/types";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, marginOfErrorText, observationUnit, unitText } from "../lib/explorerViewModel";
import { getNewestValuesBatch, newestKey } from "../lib/newestValues";
import type { NewestBatchItem } from "../lib/newestValues";
import { ACTIVE_GEOGRAPHIES_ONLY, normalizeNeutralObservationRows, observationPeriodLabel } from "../lib/observationAccess";
import {
  countyName,
  countySegment,
  placePath,
  resolveCountySegment,
  resolveStateSegment,
  stateName,
  stateSegment,
} from "../lib/placeChapters";
import {
  TRACT_CHAPTERS,
  TRACT_MEASURE_LIST,
  acsPeriod,
  boundaryHeading,
  pointBasis,
  resolveTractSegment,
  tractMetricCodes,
  tractName,
  tractSegment,
  tractTitle,
} from "../lib/tractPage";
import type { TractHistoryResponse, TractMeasure } from "../lib/tractPage";

const AgeAndSexSection = dynamic(() => import("./AgeAndSexSection"));

const CATALOG_PAGE_SIZE = 1000;

type Resolution =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "not-found"; reason: string; county: GeographySummary | null; countyHref: string | null }
  | { state: "found" };

type Load<T> = { state: "loading" } | { state: "error"; message: string } | { state: "ready"; value: T };

interface Level {
  geoId: string;
  name: string;
  role: string;
}

function publishedUnit(row: ObservationRow): string {
  const unit = observationUnit(row);
  return unit === "value" ? "" : unitText(unit);
}

function newestRow(item: NewestBatchItem | undefined): ObservationRow | null {
  if (!item || item.status !== 200 || !item.observations) return null;
  const rows = normalizeNeutralObservationRows((item.observations.items || []) as unknown as ObservationRow[]);
  return rows.at(-1) ?? null;
}

function hasValue(row: ObservationRow | null): boolean {
  return Boolean(row && row.value !== null && row.value !== undefined && row.value !== "");
}

export default function TractPage({
  stateSegment: requestedState,
  countySegment: requestedCounty,
  tractSegment: requestedTract,
}: {
  stateSegment: string;
  countySegment: string;
  tractSegment: string;
}) {
  const router = useRouter();
  const [resolution, setResolution] = useState<Resolution>({ state: "loading" });
  const [nation, setNation] = useState<GeographySummary | null>(null);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [state, setState] = useState<GeographySummary | null>(null);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [county, setCounty] = useState<GeographySummary | null>(null);
  const [tracts, setTracts] = useState<GeographySummary[]>([]);
  const [tract, setTract] = useState<GeographySummary | null>(null);
  const [newest, setNewest] = useState<Load<Map<string, NewestBatchItem>>>({ state: "loading" });
  const [history, setHistory] = useState<Load<TractHistoryResponse>>({ state: "loading" });

  // Resolve the address: the state, its counties, then the one county's tracts.
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
        setNation(nationalItems[0] || null);
        setStates(stateItems);
        const stateMatch = resolveStateSegment(requestedState, stateItems);
        if (!stateMatch.place) {
          setResolution({ state: "not-found", reason: "No state is published under that name or code.", county: null, countyHref: null });
          return;
        }
        setState(stateMatch.place);
        const stateFips = String(stateMatch.place.state_fips || "");
        const countyItems = (
          await fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY", state_fips: stateFips },
            pageSize: CATALOG_PAGE_SIZE,
            signal,
          })
        ).filter((item) => item.state_fips === stateMatch.place!.state_fips);
        if (signal.aborted) return;
        setCounties(countyItems);
        const countyMatch = resolveCountySegment(requestedCounty, countyItems);
        if (!countyMatch.place) {
          setResolution({
            state: "not-found",
            reason: `${stateName(stateMatch.place)} has no county published under that name or code. Census tracts are listed under their county.`,
            county: null,
            countyHref: null,
          });
          return;
        }
        setCounty(countyMatch.place);
        const stateSeg = stateSegment(stateMatch.place, stateItems);
        const countySeg = countySegment(countyMatch.place, countyItems);
        const tractItems = (
          await fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: {
              ...ACTIVE_GEOGRAPHIES_ONLY,
              geo_level: "TRACT",
              state_fips: stateFips,
              county_fips: String(countyMatch.place.county_fips || ""),
            },
            pageSize: CATALOG_PAGE_SIZE,
            signal,
          })
        ).filter((item) => item.state_fips === stateMatch.place!.state_fips && item.county_fips === countyMatch.place!.county_fips);
        if (signal.aborted) return;
        setTracts(tractItems);
        const tractMatch = resolveTractSegment(requestedTract, tractItems);
        if (!tractMatch.tract) {
          setResolution({
            state: "not-found",
            reason: `${countyName(countyMatch.place)} has no current Census tract under that number.`,
            county: countyMatch.place,
            countyHref: placePath(stateSeg, countySeg),
          });
          return;
        }
        setTract(tractMatch.tract);
        if (stateMatch.canonical || countyMatch.canonical || tractMatch.canonical) {
          router.replace(`${placePath(stateSeg, countySeg)}/${tractSegment(String(tractMatch.tract.geo_id))}`);
        }
        setResolution({ state: "found" });
      } catch (error) {
        if (!signal.aborted) setResolution({ state: "error", message: apiErrorMessage(error) });
      }
    })();
    return () => controller.abort();
  }, [requestedState, requestedCounty, requestedTract, router]);

  const levels = useMemo((): Level[] => {
    const chain: Level[] = [];
    if (tract) chain.push({ geoId: String(tract.geo_id), name: tractName(tract), role: "this tract" });
    if (county) chain.push({ geoId: String(county.geo_id), name: countyName(county), role: "county" });
    if (state) chain.push({ geoId: String(state.geo_id), name: stateName(state), role: "state" });
    if (nation) chain.push({ geoId: String(nation.geo_id), name: "United States", role: "nation" });
    return chain;
  }, [tract, county, state, nation]);

  // The page's two reads, fired only once the real tract is known.
  useEffect(() => {
    if (resolution.state !== "found" || !tract) return;
    const controller = new AbortController();
    const signal = controller.signal;
    const codes = tractMetricCodes();
    setNewest({ state: "loading" });
    setHistory({ state: "loading" });
    getNewestValuesBatch(codes, levels.map((level) => level.geoId), { signal })
      .then((found) => { if (!signal.aborted) setNewest({ state: "ready", value: found }); })
      .catch((error) => { if (!signal.aborted) setNewest({ state: "error", message: apiErrorMessage(error) }); });
    apiFetch<TractHistoryResponse>("/place/tract-history", {
      params: { geo_id: String(tract.geo_id), metric_codes: codes.join(",") },
      signal,
    })
      .then((payload) => { if (!signal.aborted) setHistory({ state: "ready", value: payload }); })
      .catch((error) => { if (!signal.aborted) setHistory({ state: "error", message: apiErrorMessage(error) }); });
    return () => controller.abort();
  }, [resolution.state, tract, levels]);

  if (resolution.state === "not-found" || resolution.state === "error") {
    return (
      <main className="page-shell compact-page" data-testid="tract-not-found">
        <div className="page-heading">
          <div className="section-kicker">Not found</div>
          <h1>There is no tract at this address</h1>
          <p>{resolution.state === "error" ? `The geography catalog could not be read: ${resolution.message}` : resolution.reason}</p>
        </div>
        {resolution.state === "not-found" && resolution.countyHref ? (
          <p>
            <Link className="text-link" href={`${resolution.countyHref}#within-county-heading`}>
              Every tract in {resolution.county ? countyName(resolution.county) : "the county"} <ArrowRight size={13} />
            </Link>
          </p>
        ) : null}
        <p><Link className="text-link" href="/us">All states <ArrowRight size={13} /></Link></p>
      </main>
    );
  }

  const stateSeg = state ? stateSegment(state, states) : "";
  const countySeg = county ? countySegment(county, counties) : "";
  const countyHref = state && county ? placePath(stateSeg, countySeg) : "";
  const title = tractTitle(tract, county ? countyName(county) : "", state ? stateName(state) : "");
  const ready = resolution.state === "found" && newest.state !== "loading" && history.state !== "loading";
  const historyValue = history.state === "ready" ? history.value : null;
  const sameCounty = (geoId: string) => tracts.some((item) => String(item.geo_id) === geoId);
  const tractHref = (geoId: string) => (countyHref && sameCounty(geoId) ? `${countyHref}/${tractSegment(geoId)}` : "");

  return (
    <main
      className="page-shell place-page tract-page"
      data-testid="tract-page"
      data-level="TRACT"
      data-geo-id={tract?.geo_id || ""}
      data-ready={ready ? "true" : "false"}
    >
      <header className="page-heading">
        <div className="section-kicker">Census tract</div>
        <h1>{title || "Loading tract…"}</h1>
        <nav aria-label="Place hierarchy" className="place-breadcrumbs">
          <ol>
            <li><Link href="/us">United States</Link></li>
            {state ? <li><Link href={placePath(stateSeg)}>{stateName(state)}</Link></li> : null}
            {county ? <li><Link href={countyHref}>{countyName(county)}</Link></li> : null}
            {tract ? <li aria-current="page">{tractName(tract)}</li> : null}
          </ol>
        </nav>
        <p>
          A Census tract is a small, stable area of about 4,000 people that the Census Bureau draws for statistics. Each
          number here is the American Community Survey&apos;s 5-year estimate, shown beside its county&apos;s, its
          state&apos;s and the nation&apos;s, with its margin of error. Tract estimates rest on small samples: read the
          margin before comparing.
        </p>
      </header>

      <section className="status-row" role="status">
        <StatusPill state={resolution.state === "found" ? "ok" : "loading"} label="Tract" message={resolution.state === "found" ? tractName(tract) : "resolving the address"} testId="tract-status" />
        <StatusPill
          state={newest.state === "ready" ? "ok" : newest.state === "error" ? "warn" : "loading"}
          label="Newest values"
          message={newest.state === "ready" ? `${TRACT_MEASURE_LIST.length} measures read` : newest.state === "error" ? newest.message : "reading"}
          testId="tract-newest-status"
        />
        <StatusPill
          state={history.state === "ready" ? "ok" : history.state === "error" ? "warn" : "loading"}
          label="History"
          message={history.state === "ready" ? "read on the 2020 boundary" : history.state === "error" ? history.message : "reading"}
          testId="tract-history-status"
        />
      </section>

      {historyValue ? (
        <section
          className={`analysis-panel tract-boundary tract-boundary-${historyValue.boundary.status}`}
          aria-labelledby="tract-boundary-heading"
          data-testid="tract-boundary"
          data-status={historyValue.boundary.status}
        >
          <h2 id="tract-boundary-heading">{boundaryHeading(historyValue.boundary.status)}</h2>
          <p>{historyValue.boundary.sentence}</p>
          {historyValue.boundary.also_became.length ? (
            <p data-testid="tract-boundary-also-became">
              Today&apos;s tracts from the same land:{" "}
              {historyValue.boundary.also_became.map((part, index) => {
                const href = tractHref(part.geo_id);
                return (
                  <span key={part.geo_id}>
                    {index ? ", " : ""}
                    {href ? <Link href={href}>{part.name}</Link> : part.name}
                  </span>
                );
              })}
              .
            </p>
          ) : null}
          {historyValue.boundary.status !== "unchanged" && historyValue.boundary.status !== "not_recorded" ? (
            <p className="subtle">
              Earlier counts on this page are marked <strong>Derived</strong>: the Census Bureau did not publish them for
              this tract. They are its earlier tracts&apos; published counts, split by the Bureau&apos;s relationship
              file. Medians and incomes cannot be split, so their history starts in 2020.
            </p>
          ) : null}
        </section>
      ) : null}

      {TRACT_CHAPTERS.map((chapter) => (
        <section key={chapter.id} className="analysis-panel place-chapter" id={chapter.id} aria-labelledby={`${chapter.id}-heading`} data-testid={`tract-chapter-${chapter.id}`}>
          <div className="panel-heading">
            <div>
              <h2 id={`${chapter.id}-heading`}>{chapter.title}</h2>
              <p className="subtle">{chapter.description}</p>
            </div>
          </div>
          <div className="place-cards">
            {chapter.measures.map((measure) => (
              <TractCard key={measure.id} measure={measure} levels={levels} newest={newest} />
            ))}
          </div>
        </section>
      ))}

      {history.state === "error" ? (
        <p className="subtle" data-testid="tract-history-error">The tract&apos;s history could not be read: {history.message}</p>
      ) : null}
      {historyValue ? <TractHistory response={historyValue} /> : null}

      {tract && resolution.state === "found" ? <AgeAndSexSection geoId={String(tract.geo_id)} testId="tract-age-section" /> : null}

      {county && tracts.length > 1 ? (
        <section className="analysis-panel place-nearby" aria-labelledby="tract-siblings-heading" data-testid="tract-siblings">
          <h2 id="tract-siblings-heading">Other tracts in {countyName(county)}</h2>
          <p>
            <Link className="text-link" href={`${countyHref}#within-county-heading`} data-testid="tract-county-map-link">
              See every tract in {countyName(county)} on one map <ArrowRight size={13} />
            </Link>
          </p>
          <details>
            <summary>All {tracts.length} tracts</summary>
            <ul className="place-index">
              {[...tracts]
                .sort((left, right) => String(left.geo_id).localeCompare(String(right.geo_id)))
                .map((item) => (
                  <li key={String(item.geo_id)}>
                    {item.geo_id === tract?.geo_id ? (
                      <span aria-current="page">{tractName(item)}</span>
                    ) : (
                      <Link href={`${countyHref}/${tractSegment(String(item.geo_id))}`}>{tractName(item)}</Link>
                    )}
                  </li>
                ))}
            </ul>
          </details>
        </section>
      ) : null}

      <footer className="place-footer">
        <p>
          <Link className="text-link" href="/data#rules">The rules every number here follows</Link>
        </p>
      </footer>
    </main>
  );
}

function TractCard({
  measure,
  levels,
  newest,
}: {
  measure: TractMeasure;
  levels: Level[];
  newest: Load<Map<string, NewestBatchItem>>;
}) {
  const rows = levels.map((level) => ({
    level,
    row: newest.state === "ready" ? newestRow(newest.value.get(newestKey(measure.metricCode, level.geoId))) : null,
  }));
  const first = rows.find((entry) => hasValue(entry.row))?.row || null;
  const period = first ? observationPeriodLabel(first) : "";
  const unit = first ? publishedUnit(first) : "";
  return (
    <div className="place-card" data-testid={`tract-card-${measure.id}`} data-metric-code={measure.metricCode}>
      <h3>{measure.label}</h3>
      <p className="subtle">
        {measure.universe ? `For ${measure.universe.charAt(0).toLowerCase()}${measure.universe.slice(1)}` : ""}
        {period ? ` · ${period}` : ""}
        {unit ? ` · ${unit}` : ""}
      </p>
      <table className="place-card-table">
        <caption className="sr-only">{measure.label}{period ? `, ${period}` : ""}</caption>
        <tbody>
          {rows.map(({ level, row }) => (
            <tr key={level.geoId} data-testid={`tract-card-${measure.id}-${level.role.replace(/\s+/g, "-")}`}>
              <th scope="row">
                {level.name}
                <span className="subtle place-role"> · {level.role}</span>
              </th>
              <td>
                {newest.state === "loading"
                  ? "Reading…"
                  : hasValue(row)
                    ? formatObservationValue(row!.value)
                    : newest.state === "error"
                      ? "Could not be read"
                      : "Not published"}
              </td>
              <td className="subtle">{hasValue(row) ? marginOfErrorText(row!) : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TractHistory({ response }: { response: TractHistoryResponse }) {
  const withItems = TRACT_MEASURE_LIST.filter((measure) =>
    response.series.some((series) => series.metric_code === measure.metricCode && series.items.length),
  );
  const [chosen, setChosen] = useState(withItems[0]?.metricCode || "");
  const measure = withItems.find((entry) => entry.metricCode === chosen) || withItems[0];
  if (!measure) return null;
  const series = response.series.find((entry) => entry.metric_code === measure.metricCode);
  const items = series?.items || [];
  const derivedCount = items.filter((point) => point.derived).length;
  return (
    <section className="analysis-panel tract-history" aria-labelledby="tract-history-heading" data-testid="tract-history">
      <h2 id="tract-history-heading">Over time, on today&apos;s boundary</h2>
      <p className="subtle">
        Every value is for the area this tract covers today (its 2020 boundary). Each 5-year estimate overlaps the ones
        beside it, so compare estimates five years apart.
      </p>
      <label className="within-county-measure">
        Measure
        <select value={measure.metricCode} onChange={(event) => setChosen(event.target.value)} data-testid="tract-history-measure">
          {withItems.map((entry) => (
            <option key={entry.metricCode} value={entry.metricCode}>{entry.label}</option>
          ))}
        </select>
      </label>
      {series?.note ? <p className="notice" data-testid="tract-history-note">{series.note}</p> : null}
      <div className="table-scroll">
        <table className="within-county-table" data-testid="tract-history-table">
          <caption>{measure.label}, {response.geo_name ? tractName(response.geo_id) : "this tract"}{derivedCount ? `; ${derivedCount} derived` : ""}</caption>
          <thead>
            <tr>
              <th scope="col">Survey years</th>
              <th scope="col">Value</th>
              <th scope="col">Margin of error</th>
              <th scope="col">How it was counted</th>
            </tr>
          </thead>
          <tbody>
            {[...items].reverse().map((point) => (
              <tr
                key={point.vintage_year}
                data-testid={`tract-history-row-${point.vintage_year}`}
                data-status={point.status}
                data-derived={point.derived ? "true" : "false"}
                className={point.derived ? "tract-history-derived" : undefined}
              >
                <th scope="row">{acsPeriod(point.vintage_year)}</th>
                <td>
                  {point.value === null ? "No value" : formatObservationValue(point.value)}
                  {point.derived ? <span className="derived-tag" data-testid="derived-tag"> Derived</span> : null}
                </td>
                <td className="subtle">{point.margin_of_error === null ? (point.derived ? "None: derived" : "") : `±${formatObservationValue(point.margin_of_error)}`}</td>
                <td>{pointBasis(point)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <details>
        <summary>How the earlier figures were made</summary>
        <p className="subtle">{response.method}</p>
      </details>
    </section>
  );
}
