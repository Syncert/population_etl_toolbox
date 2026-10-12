"use client";

// `/map/<metric_code>`: one measure across every county, in one period
// (one-measure-map). The map, its legend and the table read one response;
// counties without a published value are counted and listed, never painted
// and never printed as zero. There is no second layer and no combination of
// measures: switching measure replaces the page's one measure.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import StatusPill from "./StatusPill";
import {
  ApiError,
  apiErrorMessage,
  apiFetch,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getMetric,
  getSources,
  searchMetrics,
} from "../lib/api/client";
import type { GeographySummary, MetricSummary, Observation, SourceSummary } from "../lib/api/types";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import { formatObservationValue, marginOfErrorText, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName, formatNumber } from "../lib/format";
import { buildMeasureMapView, rankedPage } from "../lib/oneMeasureMap";
import type { RankedCounty } from "../lib/oneMeasureMap";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  OBSERVATION_UNCERTAINTY_BEYOND_MARGIN,
  buildLatestObservationRequest,
  buildPeriodListRequest,
  normalizeObservationRows,
  observationUncertaintyLabel,
} from "../lib/observationAccess";
import { PLACE_CHAPTERS, countySegment, placePath, stateSegment } from "../lib/placeChapters";
import { discoverTileMetadata } from "../lib/tiles";
import type { ObservationRow } from "../lib/explorerViewModel";

const ChoroplethMap = dynamic(() => import("./ChoroplethMap"), { ssr: false });

const PAGE_SIZE = 50;
const EXTREMES = 10;

interface PeriodItem {
  period_start: string;
  period_end?: string | null;
  observation_count?: number | null;
}

type Load =
  | { state: "loading"; message: string }
  | { state: "unknown"; message: string }
  | { state: "no-county"; message: string }
  | { state: "error"; message: string }
  | { state: "ready"; message: string };

function formatBound(value: number): string {
  return formatNumber(value, { maximumFractionDigits: 2 });
}

export default function MeasureMapPage({ metricCode, requestedPeriod }: { metricCode: string; requestedPeriod?: string }) {
  const [load, setLoad] = useState<Load>({ state: "loading", message: "reading the measure" });
  const [metric, setMetric] = useState<MetricSummary | null>(null);
  const [source, setSource] = useState<ExplorerSource | null>(null);
  const [sourceRow, setSourceRow] = useState<SourceSummary | null>(null);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [periods, setPeriods] = useState<PeriodItem[]>([]);
  const [period, setPeriod] = useState(requestedPeriod || "");
  const [rows, setRows] = useState<ObservationRow[]>([]);
  const [rowsComplete, setRowsComplete] = useState(true);
  const [rowsLoaded, setRowsLoaded] = useState(false);
  const [tileMetadata, setTileMetadata] = useState<Awaited<ReturnType<typeof discoverTileMetadata>> | null>(null);
  const [page, setPage] = useState(0);

  // The measure, its source, the counties, and its published periods.
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    (async () => {
      try {
        let found: MetricSummary;
        try {
          found = await getMetric(metricCode, { signal });
        } catch (error) {
          if (error instanceof ApiError && error.status === 404) {
            setLoad({ state: "unknown", message: `No measure is published under ${metricCode}.` });
            return;
          }
          throw error;
        }
        setMetric(found);
        const grains = found.valid_geo_grains;
        if (Array.isArray(grains) && !grains.includes("COUNTY")) {
          setLoad({ state: "no-county", message: `${displayMetricName(found)} is not published for counties (published grains: ${grains.join(", ") || "none"}).` });
          return;
        }
        const [capabilities, sourceItems, stateItems, countyItems] = await Promise.all([
          getCapabilities({ signal }),
          getSources({ signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" }, pageSize: 1000, signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY" }, pageSize: 1000, signal }),
        ]);
        const explorerSource = buildExplorerSources(capabilities.items).find((item) => item.sourceCode === found.source_code) || null;
        setSource(explorerSource);
        setSourceRow(sourceItems.find((item) => item.source_code === found.source_code) || null);
        setStates(stateItems);
        setCounties(countyItems);
        const periodRequest = buildPeriodListRequest(explorerSource, { metricCode, limit: 200 });
        if (periodRequest) {
          const payload = await apiFetch<{ items?: PeriodItem[] }>(periodRequest.resource, { params: periodRequest.params, signal });
          const items = Array.isArray(payload.items) ? payload.items : [];
          setPeriods(items);
          setPeriod((current) => (current && items.some((item) => item.period_start === current) ? current : items[0]?.period_start || ""));
        }
        setLoad({ state: "ready", message: "" });
      } catch (error) {
        if (!signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) });
      }
    })();
    discoverTileMetadata().then(setTileMetadata).catch(() => setTileMetadata(null));
    return () => controller.abort();
  }, [metricCode]);

  // The one period's county values.
  useEffect(() => {
    if (load.state !== "ready" || !source) return;
    const controller = new AbortController();
    setRows([]);
    setRowsLoaded(false);
    const request = buildLatestObservationRequest(source, {
      metricCode,
      geoLevel: "COUNTY",
      periodStart: period || undefined,
      newestPerGeography: true,
      limit: "1000",
    });
    fetchCollectionPages<Observation>(request.resource, { params: request.params, pageSize: 1000, maxPages: 5, signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted) return;
        setRows(normalizeObservationRows(source, result.items).filter((row) => row.geo_level === undefined || row.geo_level === "COUNTY" || String(row.geo_id).includes("county:")));
        setRowsComplete(result.complete);
        setRowsLoaded(true);
        setPage(0);
      })
      .catch((error) => {
        if (!controller.signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, [load.state, source, metricCode, period]);

  // The period is public state, so it lives in the address. Replaced in
  // place, not navigated: the rows are already being read for it.
  useEffect(() => {
    if (!period) return;
    const params = new URLSearchParams(window.location.search);
    if (params.get("period") === period) return;
    params.set("period", period);
    window.history.replaceState(null, "", `${window.location.pathname}?${params}`);
  }, [period]);

  const view = useMemo(() => (metric && rows.length ? buildMeasureMapView(metricCode, rows, counties) : null), [metric, metricCode, rows, counties]);
  const name = metric ? displayMetricName(metric) : metricCode;
  const unit = rows.find((row) => row.unit || row.units) ? observationUnit(rows.find((row) => row.unit || row.units)!) : String(metric?.units || "");

  if (load.state === "unknown" || load.state === "no-county" || load.state === "error" || (load.state === "ready" && rowsLoaded && (!view || view.withValue === 0))) {
    return (
      <main className="page-shell compact-page measure-map-page" data-testid="measure-map-unavailable">
        <header className="page-heading">
          <div className="section-kicker">Map</div>
          <h1>{metric ? name : "This measure cannot be mapped"}</h1>
          <p data-testid="measure-map-reason">
            {load.state === "ready" ? `No county has a published value for ${name} in this period.` : load.message}
          </p>
        </header>
        <MeasureSwitcher current={metricCode} />
        <CountyMeasures />
      </main>
    );
  }

  const linkFor = (entry: RankedCounty) => {
    const state = states.find((item) => item.state_fips === entry.county.state_fips);
    if (!state) return null;
    const sameState = counties.filter((item) => item.state_fips === entry.county.state_fips);
    return placePath(stateSegment(state, states), countySegment(entry.county, sameState));
  };
  const withValues = view?.ranked.filter((entry) => entry.value !== null) || [];
  const highest = withValues.slice(0, EXTREMES);
  const lowest = withValues.slice(-EXTREMES).reverse();
  const pages = view ? Math.max(1, Math.ceil(view.ranked.length / PAGE_SIZE)) : 1;

  return (
    <main className="page-shell measure-map-page" data-testid="measure-map" data-metric={metricCode} data-period={view?.period || ""}>
      <header className="page-heading">
        <div className="section-kicker">One measure, every county</div>
        <h1>{name}</h1>
        <p>
          One measure at a time, in one period. Counties without a published value are counted and listed,
          not painted. Nothing here combines measures or scores places.
        </p>
      </header>

      <section className="profile-controls">
        <label>
          Period
          <select value={period} onChange={(event) => setPeriod(event.target.value)} disabled={!periods.length} data-testid="measure-map-period">
            {periods.length ? null : <option value="">Latest publication</option>}
            {periods.map((item) => (
              <option key={item.period_start} value={item.period_start}>
                {item.period_end && item.period_end !== item.period_start ? `${item.period_start} – ${item.period_end}` : item.period_start}
              </option>
            ))}
          </select>
        </label>
        <MeasureSwitcher current={metricCode} />
      </section>

      <section className="status-row" role="status">
        <StatusPill
          state={load.state === "ready" && view ? (rowsComplete ? "ok" : "warn") : "loading"}
          label="Counties"
          message={view ? `${formatNumber(view.withValue)} with a published value, ${formatNumber(view.withoutValue)} without${rowsComplete ? "" : "; the page bound cut the read short"}` : "reading county values"}
          testId="measure-map-status"
        />
      </section>

      {view ? (
        <>
          <section className="analysis-panel" aria-labelledby="map-heading">
            <h2 id="map-heading">{name}, {view.period}</h2>
            {tileMetadata ? (
              <ChoroplethMap
                rows={rows}
                tileMetadata={tileMetadata}
                geoLevel="COUNTY"
                legendTitle={`${name}, ${view.period}`}
                missingLabel={`No published value (${formatNumber(view.withoutValue)} counties)`}
                testId="measure-map-canvas"
                distribution={view.distribution}
              />
            ) : (
              <p className="subtle" role="status">The map is not available here; every value is in the table below.</p>
            )}
            <p className="subtle" data-testid="measure-map-banding">
              Each colour holds about the same number of counties, so the bands show where a county
              stands among the others rather than how far apart the values are.
            </p>
            <ul className="measure-legend" data-testid="measure-map-legend" aria-label="Legend">
              {view.bins.map((bin) => (
                <li key={bin.binIndex} data-count={bin.count}>
                  {formatBound(bin.lowerBound)} to {formatBound(bin.upperBound)}{unit ? ` ${unit}` : ""}: {formatNumber(bin.count)} counties
                </li>
              ))}
              <li data-count={view.withoutValue}>Uncoloured, no published value: {formatNumber(view.withoutValue)} counties</li>
            </ul>
          </section>

          <section className="analysis-panel" aria-labelledby="extremes-heading">
            <h2 id="extremes-heading">Highest and lowest published values</h2>
            <p className="subtle">
              Where a margin of error is published it is shown; counties whose intervals overlap may not differ.
            </p>
            <div className="measure-extremes">
              <CountyTable caption="Highest published values" entries={highest} linkFor={linkFor} testId="measure-map-highest" />
              <CountyTable caption="Lowest published values" entries={lowest} linkFor={linkFor} testId="measure-map-lowest" />
            </div>
          </section>

          <section className="analysis-panel" aria-labelledby="all-heading">
            <h2 id="all-heading">Every county</h2>
            <CountyTable caption={`Every county, page ${page + 1} of ${pages}`} entries={rankedPage(view, page, PAGE_SIZE)} linkFor={linkFor} testId="measure-map-all" />
            <div className="command-row">
              <button type="button" className="button secondary" disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button>
              <button type="button" className="button secondary" disabled={page + 1 >= pages} onClick={() => setPage(page + 1)}>Next</button>
            </div>
          </section>

          <section className="analysis-panel" aria-labelledby="coverage-heading" data-testid="measure-map-coverage">
            <h2 id="coverage-heading">What this measure covers</h2>
            <dl className="place-depth">
              <div><dt>Period</dt><dd>{view.period}</dd></div>
              <div><dt>Unit</dt><dd>{unit || "not published"}</dd></div>
              <div><dt>Source</dt><dd>{String(sourceRow?.source_name || metric?.source_code || "not published")}</dd></div>
              <div><dt>Measure kind</dt><dd>{String(metric?.measure_kind || "not published")}</dd></div>
              <div><dt>How it may be combined</dt><dd>{String(metric?.aggregation_characteristic || "not published")}</dd></div>
              <div><dt>Denominator and universe</dt><dd>As the source defines it; the catalog publishes no separate denominator for this measure.</dd></div>
            </dl>
            {sourceRow?.reference_url ? (
              <p><a className="text-link" href={String(sourceRow.reference_url)} rel="noopener noreferrer" target="_blank">The source&apos;s own definitions and changes</a></p>
            ) : null}
          </section>
        </>
      ) : (
        <p className="subtle" role="status">Reading county values…</p>
      )}
    </main>
  );
}

function CountyTable({ caption, entries, linkFor, testId }: { caption: string; entries: RankedCounty[]; linkFor: (entry: RankedCounty) => string | null; testId: string }) {
  return (
    <div className="table-scroll">
      <table className="place-card-table" data-testid={testId}>
        <caption>{caption}</caption>
        <thead>
          <tr><th scope="col">County</th><th scope="col">Value</th><th scope="col">Uncertainty</th></tr>
        </thead>
        <tbody>
          {entries.map((entry) => {
            const href = linkFor(entry);
            const label = `${String(entry.county.county_name || entry.county.geo_name || entry.county.geo_id)}, ${String(entry.county.state_name || "")}`.replace(/, $/, "");
            return (
              <tr key={entry.county.geo_id} data-geo-id={entry.county.geo_id} data-gap={entry.gap || undefined}>
                <th scope="row">{href ? <Link href={href}>{label}</Link> : label}</th>
                <td>{entry.value !== null && entry.row ? `${formatObservationValue(entry.row.value)} ${observationUnit(entry.row) === "value" ? "" : observationUnit(entry.row)}`.trim() : entry.gap === "missing" ? "Not published" : `Withheld${entry.gap.includes(":") ? ` (${entry.gap.split(": ")[1]})` : ""}`}</td>
                <td className="subtle">{entry.row && entry.value !== null ? [marginOfErrorText(entry.row) !== "Not provided" ? marginOfErrorText(entry.row) : "", observationUncertaintyLabel(entry.row, OBSERVATION_UNCERTAINTY_BEYOND_MARGIN)].filter(Boolean).join(" · ") : ""}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Replace the page's one measure with another; never add a second. */
function MeasureSwitcher({ current }: { current: string }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<MetricSummary[]>([]);
  useEffect(() => {
    if (query.trim().length < 3) {
      setResults([]);
      return;
    }
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      searchMetrics({ q: query.trim(), active_only: "true", limit: "8" }, { signal: controller.signal })
        .then((payload) => { if (!controller.signal.aborted) setResults((payload.items || []).filter((item) => item.metric_code !== current)); })
        .catch(() => { if (!controller.signal.aborted) setResults([]); });
    }, 250);
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [query, current]);
  return (
    <div className="measure-switcher">
      <label>
        Switch to another measure
        <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} data-testid="measure-map-switch" />
      </label>
      {results.length ? (
        <ul className="place-index" data-testid="measure-map-switch-results">
          {results.map((item) => (
            <li key={item.metric_code}>
              <button type="button" className="text-link" onClick={() => router.push(`/map/${encodeURIComponent(item.metric_code)}`)}>
                {displayMetricName(item)}
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

/** The measures the place pages read at county grain, as places to go instead. */
function CountyMeasures() {
  const measures = PLACE_CHAPTERS.flatMap((chapter) => chapter.headline).filter((measure) => !measure.candidates[0]!.startsWith("FBI_UCR"));
  return (
    <section aria-labelledby="county-measures-heading">
      <h2 id="county-measures-heading">Measures the place pages map by county</h2>
      <ul className="place-index" data-testid="measure-map-alternatives">
        {measures.map((measure) => (
          <li key={measure.id}><Link href={`/map/${encodeURIComponent(measure.candidates[0]!)}`}>{measure.label}</Link></li>
        ))}
      </ul>
    </section>
  );
}

