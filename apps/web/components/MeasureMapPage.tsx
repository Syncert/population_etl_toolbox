"use client";

// `/map/<metric_code>`: one measure across every county, in one period
// (one-measure-map). The map, its legend and the table read one response;
// counties without a published value are counted and listed, never painted
// and never printed as zero. There is no second layer and no combination of
// measures: switching measure replaces the page's one measure.
//
// The same page draws every grain the catalog publishes the measure at
// (web-place-maps): counties and metro areas nationally, cities and towns and
// Census tracts one state at a time. The layer and its boundary vintages come
// from `/catalog/tile-layers`, and the vintage drawn is named.

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
  getTileLayers,
  searchMetrics,
} from "../lib/api/client";
import type { GeographySummary, MetricSummary, Observation, SourceSummary, TileLayerCatalogResponse } from "../lib/api/types";
import { NEUTRAL_OBSERVATIONS_PATH, buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import { formatObservationValue, marginOfErrorText, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName, formatNumber } from "../lib/format";
import { buildMeasureMapView, currentLatestPeriods, earlierReleaseChoices, newestPeriodOfRelease, rankedPage, releaseOfChoice, RELEASE_CHOICE_PREFIX } from "../lib/oneMeasureMap";
import type { MapRelease, RankedCounty } from "../lib/oneMeasureMap";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  OBSERVATION_UNCERTAINTY_BEYOND_MARGIN,
  SCOPE_AS_RELEASED,
  buildLatestObservationRequest,
  buildPeriodListRequest,
  buildReleaseListRequest,
  servesAsReleased,
  normalizeObservationRows,
  observationUncertaintyLabel,
} from "../lib/observationAccess";
import { PLACE_CHAPTERS, citySegment, countySegment, metroCode, metroPath, placePath, stateSegment } from "../lib/placeChapters";
import { discoverTileMetadata } from "../lib/tiles";
import {
  STATE_SCOPED_GRAINS,
  boundaryForGrain,
  boundaryNote,
  boundarySentence,
  capitalised,
  grainLayerFilter,
  initialMapGrain,
  mapGrainChoices,
  newestPeriodYear,
  nounFor,
  rowInState,
  tileMetadataForBoundary,
} from "../lib/measureMapGrains";
import type { MapGrain } from "../lib/measureMapGrains";
import { uncolouredSentence } from "../lib/uncoloured";
import type { ObservationRow } from "../lib/explorerViewModel";
import { denominatorText, derivedMapRow, getDerivedRatios, pairingFor, viewChoices, viewKey } from "../lib/derivedViews";
import type { DerivedRatioObservation, DerivedRatioPairing, ViewChoice } from "../lib/derivedViews";
import { DraftViewMark, ViewToggle } from "./DerivedView";
import { isSingleAgeLine } from "../lib/ageLines";
import { ageMapPath, searchAgeMapIndicators } from "../lib/ageMap";

const ChoroplethMap = dynamic(() => import("./ChoroplethMap"), { ssr: false });

const PAGE_SIZE = 50;
const DERIVED_PATH = "/observations/derived";
/** The most rows `/observations` and `/observations/derived` answer in one page. */
const MAX_ROWS_PER_READ = 5000;
const EXTREMES = 10;

interface PeriodItem {
  period_start: string;
  period_end?: string | null;
  observation_count?: number | null;
}

type Load =
  | { state: "loading"; message: string }
  | { state: "unknown"; message: string }
  | { state: "no-grain"; message: string }
  | { state: "error"; message: string }
  | { state: "ready"; message: string };

function formatBound(value: number): string {
  return formatNumber(value, { maximumFractionDigits: 2 });
}

export default function MeasureMapPage({
  metricCode,
  requestedPeriod,
  requestedRelease,
  requestedView,
  requestedGrain,
  requestedState,
}: {
  metricCode: string;
  requestedPeriod?: string;
  requestedRelease?: string;
  requestedView?: ViewChoice;
  requestedGrain?: string;
  requestedState?: string;
}) {
  const [load, setLoad] = useState<Load>({ state: "loading", message: "reading the measure" });
  const [metric, setMetric] = useState<MetricSummary | null>(null);
  const [source, setSource] = useState<ExplorerSource | null>(null);
  const [sourceRow, setSourceRow] = useState<SourceSummary | null>(null);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  // The grains the measure is offered at, the one shown, and for a grain read
  // one state at a time, the state.
  const [grains, setGrains] = useState<MapGrain[]>([]);
  const [grain, setGrain] = useState<MapGrain>("COUNTY");
  const [stateFips, setStateFips] = useState(requestedState && /^\d{2}$/.test(requestedState) ? requestedState : "");
  // The geographies of the grain shown (the state's, for a state-scoped grain).
  const [geographies, setGeographies] = useState<GeographySummary[] | null>(null);
  // `undefined` while reading; `null` when the route could not be read.
  const [tileCatalog, setTileCatalog] = useState<TileLayerCatalogResponse | null | undefined>(undefined);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [periods, setPeriods] = useState<PeriodItem[]>([]);
  const [releaseChoices, setReleaseChoices] = useState<{ value: string; label: string }[]>([]);
  // A period of the latest publication, or `release:<id>` for an earlier release.
  const [period, setPeriod] = useState(
    requestedRelease ? `${RELEASE_CHOICE_PREFIX}${requestedRelease}` : requestedPeriod || "",
  );
  const [rows, setRows] = useState<ObservationRow[]>([]);
  const [rowsComplete, setRowsComplete] = useState(true);
  const [rowsLoaded, setRowsLoaded] = useState(false);
  const [tileMetadata, setTileMetadata] = useState<Awaited<ReturnType<typeof discoverTileMetadata>> | null>(null);
  const [page, setPage] = useState(0);
  // The pairings that divide this count (ADR-0009), and the view chosen.
  const [pairings, setPairings] = useState<DerivedRatioPairing[] | null>(null);
  const [view, setView] = useState<ViewChoice>(requestedView || "count");
  const pairing = pairings ? pairingFor(pairings, metricCode, view) : null;
  const choices = pairings ? viewChoices(pairings, [metricCode]) : ["count" as ViewChoice];

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
        getDerivedRatios({ numerator: metricCode }, { signal })
          .then((payload) => { if (!signal.aborted) setPairings(Array.isArray(payload?.items) ? payload.items : []); })
          .catch(() => { if (!signal.aborted) setPairings([]); });
        const offered = mapGrainChoices(found.valid_geo_grains);
        if (!offered.length) {
          const published = Array.isArray(found.valid_geo_grains) ? found.valid_geo_grains : [];
          setLoad({ state: "no-grain", message: `${displayMetricName(found)} is not published for counties, metro areas, cities and towns or Census tracts (published grains: ${published.join(", ") || "none"}).` });
          return;
        }
        setGrains(offered);
        setGrain(initialMapGrain(offered, requestedGrain) || offered[0]!);
        // The counties are read with the grain's own geographies, below: a
        // city map needs only its state's, and a metro or tract map none.
        const [capabilities, sourceItems, stateItems] = await Promise.all([
          getCapabilities({ signal }),
          getSources({ signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" }, pageSize: 1000, signal }),
        ]);
        const explorerSource = buildExplorerSources(capabilities.items).find((item) => item.sourceCode === found.source_code) || null;
        setSource(explorerSource);
        setSourceRow(sourceItems.find((item) => item.source_code === found.source_code) || null);
        setStates(stateItems);
        const periodRequest = buildPeriodListRequest(explorerSource, { metricCode, limit: 200 });
        // The latest publication keeps each county's newest value only, so a
        // period every county has a newer estimate for is listed by its
        // release instead, where the source can pin one.
        const releaseRequest =
          explorerSource && servesAsReleased(explorerSource) && explorerSource.supportsReleasePin
            ? buildReleaseListRequest(explorerSource, { metricCode, limit: 200 })
            : null;
        const [periodPayload, releasePayload] = await Promise.all([
          periodRequest ? apiFetch<{ items?: PeriodItem[] }>(periodRequest.resource, { params: periodRequest.params, signal }) : null,
          releaseRequest
            ? apiFetch<{ items?: MapRelease[] }>(releaseRequest.resource, { params: releaseRequest.params, signal }).catch(() => null)
            : null,
        ]);
        const releases = Array.isArray(releasePayload?.items) ? releasePayload.items : [];
        // A period only a few geographies still hold as their newest is read
        // from the release that published it for every geography.
        const { periods: items, readAsReleased } = currentLatestPeriods(
          Array.isArray(periodPayload?.items) ? periodPayload.items : [],
          releases,
        );
        const choices = earlierReleaseChoices(releases);
        setPeriods(items);
        setReleaseChoices(choices);
        setPeriod((asked) => {
          const current = readAsReleased.get(asked) || asked;
          return current && (items.some((item) => item.period_start === current) || choices.some((choice) => choice.value === current))
            ? current
            : items[0]?.period_start || "";
        });
        setLoad({ state: "ready", message: "" });
      } catch (error) {
        if (!signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) });
      }
    })();
    getTileLayers({ signal })
      .then((catalog) => { if (!signal.aborted) setTileCatalog(catalog); })
      .catch(() => {
        if (signal.aborted) return;
        setTileCatalog(null);
        // An API without the layer list still has its county boundary.
        discoverTileMetadata().then(setTileMetadata).catch(() => setTileMetadata(null));
      });
    return () => controller.abort();
    // The grain asked for is read once, with the measure.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metricCode]);

  const scoped = STATE_SCOPED_GRAINS.has(grain);
  const noun = nounFor(grain);
  // A state-scoped grain with no state chosen reads nothing yet.
  const awaitingState = scoped && !stateFips;

  // The geographies of the grain shown: every one is counted and listed,
  // including those without a published value.
  // A city's page address is told apart from its state's counties, so a city
  // map reads that state's counties beside its cities, in parallel.
  useEffect(() => {
    if (load.state !== "ready") return;
    if (awaitingState) {
      setGeographies([]);
      return;
    }
    const controller = new AbortController();
    const signal = controller.signal;
    const read = (geoLevel: string, stateParam?: string) =>
      fetchAllPages<GeographySummary>("/catalog/geographies", {
        params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: geoLevel, state_fips: stateParam },
        pageSize: 1000,
        signal,
      });
    setGeographies(null);
    const reads: Promise<[GeographySummary[], GeographySummary[] | null]> =
      grain === "COUNTY"
        ? read("COUNTY").then((items) => [items, items])
        : grain === "PLACE"
          ? Promise.all([read("PLACE", stateFips), read("COUNTY", stateFips)])
          : read(grain, scoped ? stateFips : undefined).then((items) => [items, null]);
    reads
      .then(([items, countyItems]) => {
        if (signal.aborted) return;
        if (countyItems) setCounties(countyItems);
        setGeographies(items);
      })
      .catch((error) => { if (!signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) }); });
    return () => controller.abort();
  }, [load.state, grain, stateFips, scoped, awaitingState]);

  // The one period's county values.
  useEffect(() => {
    if (load.state !== "ready" || !source) return;
    // A derived view waits for the pairings; one the API does not serve
    // falls back to the count.
    if (view !== "count" && pairings === null) return;
    const controller = new AbortController();
    setRows([]);
    setRowsLoaded(false);
    if (awaitingState) return;
    const release = releaseOfChoice(period);
    const stateParam = scoped ? stateFips : undefined;
    // Tracts in a large state run to several thousand rows.
    const pageBound = release || scoped ? 50 : 5;
    // The neutral and derived routes page up to 5,000 rows, so one state's
    // cities, or every county or metro, is one read rather than several, each
    // of which ranked the metric's rows again.
    const readSize = (resource: string) => (resource === NEUTRAL_OBSERVATIONS_PATH || resource === DERIVED_PATH ? MAX_ROWS_PER_READ : 1000);
    const atGrain = (row: { geo_level?: unknown; geo_id?: unknown }) =>
      (row.geo_level === undefined || row.geo_level === null || String(row.geo_level).toUpperCase() === grain ||
        (grain === "COUNTY" && String(row.geo_id).includes("county:") && !String(row.geo_id).includes("tract:"))) &&
      (!scoped || rowInState(row.geo_id, stateFips));
    if (pairing) {
      const params = release
        ? { ratio_id: pairing.ratio_id, geo_level: grain, state_fips: stateParam, scope: SCOPE_AS_RELEASED, release, limit: "1000" }
        : { ratio_id: pairing.ratio_id, geo_level: grain, state_fips: stateParam, period_start: period || undefined, newest_per_geography: true, limit: "1000" };
      fetchCollectionPages<DerivedRatioObservation>(DERIVED_PATH, { params, pageSize: readSize(DERIVED_PATH), maxPages: pageBound, signal: controller.signal })
        .then((result) => {
          if (controller.signal.aborted) return;
          const countyRows = result.items.filter(atGrain).map((row) => derivedMapRow(row, viewKey(pairing)));
          setRows(release ? newestPeriodOfRelease(countyRows) : countyRows);
          setRowsComplete(result.complete);
          setRowsLoaded(true);
          setPage(0);
        })
        .catch((error) => {
          if (!controller.signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) });
        });
      return () => controller.abort();
    }
    const request = release
      ? buildLatestObservationRequest(source, {
          metricCode,
          geoLevel: grain,
          stateFips: stateParam,
          scope: SCOPE_AS_RELEASED,
          release,
          limit: "1000",
        })
      : buildLatestObservationRequest(source, {
          metricCode,
          geoLevel: grain,
          stateFips: stateParam,
          periodStart: period || undefined,
          newestPerGeography: true,
          limit: "1000",
        });
    fetchCollectionPages<Observation>(request.resource, { params: request.params, pageSize: readSize(request.resource), maxPages: pageBound, signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted) return;
        const countyRows = normalizeObservationRows(source, result.items).filter(atGrain);
        setRows(release ? newestPeriodOfRelease(countyRows) : countyRows);
        setRowsComplete(result.complete);
        setRowsLoaded(true);
        setPage(0);
      })
      .catch((error) => {
        if (!controller.signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
    // `pairing` follows `pairings` and `view`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [load.state, source, metricCode, period, pairings, view, grain, stateFips]);

  // The period is public state, so it lives in the address. Replaced in
  // place, not navigated: the rows are already being read for it.
  useEffect(() => {
    if (!period || load.state !== "ready") return;
    const params = new URLSearchParams(window.location.search);
    const before = params.toString();
    const release = releaseOfChoice(period);
    const [key, value, other] = release ? ["release", release, "period"] : ["period", period, "release"];
    const shownView = view === "count" ? null : view;
    params.set(key, value);
    params.delete(other);
    if (shownView) params.set("view", shownView);
    else params.delete("view");
    // The grain and its state are public state too; the county map is the default.
    if (grain !== "COUNTY") params.set("grain", grain);
    else params.delete("grain");
    if (scoped && stateFips) params.set("state", stateFips);
    else params.delete("state");
    if (params.toString() === before) return;
    window.history.replaceState(null, "", `${window.location.pathname}?${params}`);
  }, [period, view, grain, scoped, stateFips, load.state]);

  const shownKey = pairing ? viewKey(pairing) : metricCode;
  const mapView = useMemo(
    () => (metric && geographies && rows.length && rows[0]?.metric_code === shownKey ? buildMeasureMapView(shownKey, rows, geographies) : null),
    [metric, shownKey, rows, geographies],
  );
  const periodYear = useMemo(() => newestPeriodYear(rows), [rows]);
  // The boundary this grain is drawn on: from the API's layer list, or the
  // discovered county boundary when that list could not be read.
  const boundary = tileCatalog ? boundaryForGrain(tileCatalog, grain, periodYear) : null;
  const drawnMetadata = boundary?.drawn ? tileMetadataForBoundary(boundary, grain, scoped ? stateFips : null) : tileCatalog === null && grain === "COUNTY" ? tileMetadata : null;
  const chosenState = states.find((item) => item.state_fips === stateFips) || null;
  const initialView = useMemo(() => {
    if (!scoped || !chosenState) return null;
    const latitude = Number(chosenState.latitude ?? chosenState.geo_latitude);
    const longitude = Number(chosenState.longitude ?? chosenState.geo_longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
    // A state-scoped layer draws from its minimum zoom; open there.
    return { center: [longitude, latitude] as [number, number], zoom: Math.max(6, boundary?.layer?.minzoom ?? 6) };
  }, [scoped, chosenState, boundary?.layer?.minzoom]);
  const where = scoped && chosenState ? ` in ${String(chosenState.state_name || chosenState.geo_name || "")}` : "";
  const name = pairing ? pairing.label : metric ? displayMetricName(metric) : metricCode;
  const unit = rows.find((row) => row.unit || row.units) ? observationUnit(rows.find((row) => row.unit || row.units)!) : String(metric?.units || "");

  const grainControls = (
    <GrainControls
      grains={grains}
      grain={grain}
      onGrain={(next) => { setGrain(next); setPage(0); }}
      scoped={scoped}
      states={states}
      stateFips={stateFips}
      onState={(next) => { setStateFips(next); setPage(0); }}
    />
  );

  if (load.state === "ready" && awaitingState) {
    return (
      <main className="page-shell compact-page measure-map-page" data-testid="measure-map-choose-state" data-metric={metricCode}>
        <header className="page-heading">
          <div className="section-kicker">Map</div>
          <h1>{name}</h1>
          <p>
            There are tens of thousands of {noun.many} in the country, so they are drawn one state at a time.
            Choose a state.
          </p>
        </header>
        <section className="profile-controls">{grainControls}</section>
      </main>
    );
  }

  if (load.state === "unknown" || load.state === "no-grain" || load.state === "error" || (load.state === "ready" && rowsLoaded && geographies !== null && (!mapView || mapView.withValue === 0))) {
    return (
      <main className="page-shell compact-page measure-map-page" data-testid="measure-map-unavailable">
        <header className="page-heading">
          <div className="section-kicker">Map</div>
          <h1>{metric ? name : "This measure cannot be mapped"}</h1>
          <p data-testid="measure-map-reason">
            {load.state === "ready" ? `No ${noun.one} has a published value for ${name}${where} in this period.` : load.message}
          </p>
        </header>
        {load.state === "ready" ? <section className="profile-controls">{grainControls}</section> : null}
        {view !== "count" ? (
          <ViewToggle choices={choices} value={view} onChange={setView} pairings={pairings || []} label="Show as" testId="measure-map-view" />
        ) : null}
        <MeasureSwitcher current={metricCode} />
        <CountyMeasures />
      </main>
    );
  }

  // Beside every derived value: the column names its denominator.
  const valueHeading = pairing ? `${pairing.unit[0]!.toUpperCase()}${pairing.unit.slice(1)} ${denominatorText(pairing)}` : "Value";
  const linkFor = (entry: RankedCounty) => {
    // Counties, cities and metros have pages; tracts do not yet. A metro is
    // linked by its CBSA code, which its route settles on the named address.
    if (grain === "METRO") return metroCode(entry.county.geo_id) ? metroPath(metroCode(entry.county.geo_id)) : null;
    if (grain !== "COUNTY" && grain !== "PLACE") return null;
    const home = states.find((item) => item.state_fips === entry.county.state_fips);
    if (!home) return null;
    const sameState = counties.filter((item) => item.state_fips === entry.county.state_fips);
    if (grain === "PLACE") {
      const places = (geographies || []).filter((item) => item.state_fips === entry.county.state_fips);
      return placePath(stateSegment(home, states), citySegment(entry.county, places, sameState));
    }
    return placePath(stateSegment(home, states), countySegment(entry.county, sameState));
  };
  const nameFor = (entry: RankedCounty) => {
    const place = entry.county;
    const own = String(place.county_name || place.place_name || place.geo_name || place.geo_id);
    if (grain === "METRO") return own;
    const stateName = place.state_name || states.find((item) => item.state_fips === place.state_fips)?.state_name;
    return stateName ? `${own}, ${String(stateName)}` : own;
  };
  const withValues = mapView?.ranked.filter((entry) => entry.value !== null) || [];
  const highest = withValues.slice(0, EXTREMES);
  const lowest = withValues.slice(-EXTREMES).reverse();
  const pages = mapView ? Math.max(1, Math.ceil(mapView.ranked.length / PAGE_SIZE)) : 1;

  return (
    <main className="page-shell measure-map-page" data-testid="measure-map" data-metric={metricCode} data-view={pairing ? view : "count"} data-period={mapView?.period || ""} data-grain={grain} data-state={scoped ? stateFips : ""}>
      <header className="page-heading">
        <div className="section-kicker">One measure, every {noun.one}{where}</div>
        <h1>{name}</h1>
        <p>
          One measure at a time, in one period. {capitalised(noun.many)} without a published value are counted and listed,
          not painted. Nothing here combines measures or scores places.
        </p>
      </header>

      <section className="profile-controls">
        <label>
          Period
          <select value={period} onChange={(event) => setPeriod(event.target.value)} disabled={!periods.length && !releaseChoices.length} data-testid="measure-map-period">
            {periods.length ? null : <option value="">Latest publication</option>}
            {periods.map((item) => (
              <option key={item.period_start} value={item.period_start}>
                {item.period_end && item.period_end !== item.period_start ? `${item.period_start} – ${item.period_end}` : item.period_start}
              </option>
            ))}
            {releaseChoices.length ? (
              <optgroup label="Earlier releases">
                {releaseChoices.map((choice) => (
                  <option key={choice.value} value={choice.value}>{choice.label}</option>
                ))}
              </optgroup>
            ) : null}
          </select>
        </label>
        <ViewToggle
          choices={choices}
          value={view}
          onChange={setView}
          pairings={pairings || []}
          label="Show as"
          testId="measure-map-view"
        />
        {grainControls}
        <MeasureSwitcher current={metricCode} />
      </section>

      <section className="status-row" role="status">
        <StatusPill
          state={load.state === "ready" && mapView ? (rowsComplete ? "ok" : "warn") : "loading"}
          label={capitalised(noun.many)}
          message={mapView ? `${formatNumber(mapView.withValue)} with a published value, ${formatNumber(mapView.withoutValue)} without${rowsComplete ? "" : "; the page bound cut the read short"}` : `reading ${noun.one} values`}
          testId="measure-map-status"
        />
      </section>

      {mapView ? (
        <>
          <section className="analysis-panel" aria-labelledby="map-heading">
            <h2 id="map-heading">{name}, {mapView.period}</h2>
            {pairing ? (
              <p className="subtle" data-testid="measure-map-denominator" data-ratio-id={pairing.ratio_id}>
                Every value is {pairing.unit} {denominatorText(pairing)}, computed by the API from the count and its
                denominator for the same {noun.one}, period and release. <DraftViewMark pairing={pairing} testId="measure-map-draft" />
              </p>
            ) : null}
            {drawnMetadata ? (
              <ChoroplethMap
                key={`${grain}:${stateFips}:${drawnMetadata.tileTemplate}`}
                rows={rows}
                tileMetadata={drawnMetadata}
                geoLevel={grain}
                layerFilter={grain === "COUNTY" ? undefined : grainLayerFilter(grain, scoped ? stateFips : null)}
                sourceZoom={boundary?.drawn ? { minzoom: boundary.layer.minzoom, maxzoom: boundary.layer.maxzoom } : undefined}
                initialView={initialView}
                boundaryNote={boundary?.drawn ? { text: boundaryNote(boundary, grain), marked: boundary.marked } : null}
                legendTitle={`${name}, ${mapView.period}`}
                missingLabel={`No published value (${formatNumber(mapView.withoutValue)} ${noun.many})`}
                testId="measure-map-canvas"
                ariaLabel={`${name}, ${mapView.period}, by ${noun.one}${where}. The tables below list every ${noun.one}, including those this map leaves uncoloured.`}
                distribution={mapView.distribution}
              />
            ) : (
              <p className="subtle" role="status" data-testid="measure-map-no-map">
                {boundary && !boundary.drawn ? boundarySentence(boundary, grain, periodYear) : "The map is not available here; every value is in the table below."}
              </p>
            )}
            {boundary?.drawn ? (
              <p
                className="subtle"
                data-testid="measure-map-vintage"
                data-vintage={boundary.vintage ?? "current"}
                data-marked={boundary.marked ? "true" : "false"}
                title={boundaryNote(boundary, grain)}
              >
                {boundarySentence(boundary, grain, periodYear)}
              </p>
            ) : null}
            <p className="subtle" data-testid="measure-map-uncoloured">
              {uncolouredSentence(noun, mapView.ranked.length, mapView.withValue)}
              {mapView.offBoundary
                ? ` ${formatNumber(mapView.offBoundary)} published ${mapView.offBoundary === 1 ? "value is" : "values are"} for ${mapView.offBoundary === 1 ? `a ${noun.one}` : noun.many} not on the current list and ${mapView.offBoundary === 1 ? "is" : "are"} neither drawn nor tabled.`
                : ""}
            </p>
            <p className="subtle" data-testid="measure-map-banding">
              {`Each colour holds about the same number of ${noun.many}, so the bands show where a ${noun.one} stands among the others rather than how far apart the values are.`}
            </p>
            <ul className="measure-legend" data-testid="measure-map-legend" aria-label="Legend">
              {mapView.bins.map((bin) => (
                <li key={bin.binIndex} data-count={bin.count}>
                  {formatBound(bin.lowerBound)} to {formatBound(bin.upperBound)}{unit ? ` ${unit}` : ""}: {formatNumber(bin.count)} {noun.many}
                </li>
              ))}
              <li data-count={mapView.withoutValue}>Uncoloured, no published value: {formatNumber(mapView.withoutValue)} {noun.many}</li>
            </ul>
          </section>

          <section className="analysis-panel" aria-labelledby="extremes-heading">
            <h2 id="extremes-heading">Highest and lowest published values</h2>
            <p className="subtle">
              Where a margin of error is published it is shown; {noun.many} whose intervals overlap may not differ.
            </p>
            <div className="measure-extremes">
              <CountyTable caption="Highest published values" heading={capitalised(noun.one)} valueHeading={valueHeading} entries={highest} linkFor={linkFor} nameFor={nameFor} testId="measure-map-highest" />
              <CountyTable caption="Lowest published values" heading={capitalised(noun.one)} valueHeading={valueHeading} entries={lowest} linkFor={linkFor} nameFor={nameFor} testId="measure-map-lowest" />
            </div>
          </section>

          <section className="analysis-panel" aria-labelledby="all-heading">
            <h2 id="all-heading">Every {noun.one}{where}</h2>
            <CountyTable caption={`Every ${noun.one}${where}, page ${page + 1} of ${pages}`} heading={capitalised(noun.one)} valueHeading={valueHeading} entries={rankedPage(mapView, page, PAGE_SIZE)} linkFor={linkFor} nameFor={nameFor} testId="measure-map-all" />
            <div className="command-row">
              <button type="button" className="button secondary" disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button>
              <button type="button" className="button secondary" disabled={page + 1 >= pages} onClick={() => setPage(page + 1)}>Next</button>
            </div>
          </section>

          <section className="analysis-panel" aria-labelledby="coverage-heading" data-testid="measure-map-coverage">
            <h2 id="coverage-heading">What this measure covers</h2>
            <dl className="place-depth">
              <div><dt>Period</dt><dd>{mapView.period}</dd></div>
              <div><dt>Geography</dt><dd>{capitalised(noun.many)}{where}{boundary?.drawn && boundary.vintage !== null ? `, ${boundary.vintage} boundaries` : ""}</dd></div>
              <div><dt>Unit</dt><dd>{unit || "not published"}</dd></div>
              <div><dt>Source</dt><dd>{String(sourceRow?.source_name || metric?.source_code || "not published")}</dd></div>
              <div><dt>Measure kind</dt><dd>{String(metric?.measure_kind || "not published")}</dd></div>
              <div><dt>How it may be combined</dt><dd>{String(metric?.aggregation_characteristic || "not published")}</dd></div>
              <div><dt>Denominator and universe</dt><dd>{pairing ? `${pairing.denominator_label}; the value describes ${pairing.universe}` : "As the source defines it; the catalog publishes no separate denominator for this measure."}</dd></div>
            </dl>
            {sourceRow?.reference_url ? (
              <p><a className="text-link" href={String(sourceRow.reference_url)} rel="noopener noreferrer" target="_blank">The source&apos;s own definitions and changes</a></p>
            ) : null}
          </section>
        </>
      ) : (
        <p className="subtle" role="status">Reading {noun.one} values…</p>
      )}
    </main>
  );
}

function CountyTable({ caption, entries, heading, linkFor, nameFor, testId, valueHeading }: { caption: string; entries: RankedCounty[]; heading: string; linkFor: (entry: RankedCounty) => string | null; nameFor: (entry: RankedCounty) => string; testId: string; valueHeading: string }) {
  return (
    <div className="table-scroll">
      <table className="place-card-table" data-testid={testId}>
        <caption>{caption}</caption>
        <thead>
          <tr><th scope="col">{heading}</th><th scope="col">{valueHeading}</th><th scope="col">Uncertainty</th></tr>
        </thead>
        <tbody>
          {entries.map((entry) => {
            const href = linkFor(entry);
            const label = nameFor(entry);
            return (
              <tr key={entry.county.geo_id} data-geo-id={entry.county.geo_id} data-gap={entry.gap || undefined}>
                <th scope="row">{href ? <Link href={href}>{label}</Link> : label}</th>
                <td>{entry.value !== null && entry.row ? `${formatObservationValue(entry.row.value, entry.row.derived !== undefined ? 2 : 1)} ${observationUnit(entry.row) === "value" ? "" : observationUnit(entry.row)}`.trim() : entry.gap === "missing" ? "Not published" : entry.row?.derived !== undefined ? String(entry.row.value_status) : `Withheld${entry.gap.includes(":") ? ` (${entry.gap.split(": ")[1]})` : ""}`}</td>
                <td className="subtle">{entry.row && entry.value !== null ? [marginOfErrorText(entry.row) !== "Not provided" ? marginOfErrorText(entry.row) : "", observationUncertaintyLabel(entry.row, OBSERVATION_UNCERTAINTY_BEYOND_MARGIN)].filter(Boolean).join(" · ") : ""}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** The grain the measure is drawn at and, for a grain drawn one state at a time, the state. */
function GrainControls({
  grains,
  grain,
  onGrain,
  scoped,
  states,
  stateFips,
  onState,
}: {
  grains: MapGrain[];
  grain: MapGrain;
  onGrain: (grain: MapGrain) => void;
  scoped: boolean;
  states: GeographySummary[];
  stateFips: string;
  onState: (stateFips: string) => void;
}) {
  const sortedStates = [...states].sort((left, right) => String(left.state_name || "").localeCompare(String(right.state_name || "")));
  return (
    <>
      <label>
        Draw by
        <select value={grain} onChange={(event) => onGrain(event.target.value as MapGrain)} disabled={grains.length < 2} data-testid="measure-map-grain">
          {grains.map((item) => (
            <option key={item} value={item}>{capitalised(nounFor(item).many)}</option>
          ))}
        </select>
      </label>
      {scoped ? (
        <label>
          State
          <select value={stateFips} onChange={(event) => onState(event.target.value)} data-testid="measure-map-state">
            <option value="">Choose a state</option>
            {sortedStates.map((item) => (
              <option key={item.geo_id} value={String(item.state_fips || "")}>{String(item.state_name || item.geo_name || "")}</option>
            ))}
          </select>
        </label>
      ) : null}
    </>
  );
}

/** Replace the page's one measure with another; never add a second. */
function MeasureSwitcher({ current }: { current: string }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<MetricSummary[]>([]);
  // The age indicators are not catalog measures; they are found by their words.
  const ageResults = useMemo(() => searchAgeMapIndicators(query), [query]);
  useEffect(() => {
    if (query.trim().length < 3) {
      setResults([]);
      return;
    }
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      searchMetrics({ q: query.trim(), active_only: "true", limit: "8" }, { signal: controller.signal })
        .then((payload) => { if (!controller.signal.aborted) setResults((payload.items || []).filter((item) => item.metric_code !== current && !isSingleAgeLine(item.metric_code))); })
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
      {results.length || ageResults.length ? (
        <ul className="place-index" data-testid="measure-map-switch-results">
          {ageResults.map((item) => (
            <li key={item.id}>
              <button type="button" className="text-link" onClick={() => router.push(ageMapPath(item.id))} data-testid={`measure-map-switch-age-${item.id}`}>
                {`${item.label} (age and sex)`}
              </button>
            </li>
          ))}
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

