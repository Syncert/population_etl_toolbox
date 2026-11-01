"use client";

// `/map/age/<indicator>`: one age indicator for every county or metro area,
// or every city and town or tract in one state (web-age-and-sex-structure,
// API-191). It is the one-measure map's page for a measure that is not in the
// catalog: the same equal-count bands, the same boundary layer and vintage
// rule (the newest boundaries held, named in the legend note and the map's
// tooltip), the same tables of highest, lowest and every geography. The
// nation's own value is named beside the legend as a reference, not painted.
// A place whose value the API withholds is listed with the reason, never as
// zero and never coloured.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import StatusPill from "./StatusPill";
import { apiErrorMessage, fetchAllPages, getTileLayers } from "../lib/api/client";
import type { GeographySummary, TileLayerCatalogResponse } from "../lib/api/types";
import { formatNumber } from "../lib/format";
import { buildMeasureMapView, rankedPage } from "../lib/oneMeasureMap";
import type { RankedCounty } from "../lib/oneMeasureMap";
import { ACTIVE_GEOGRAPHIES_ONLY } from "../lib/observationAccess";
import { citySegment, countySegment, metroCode, metroPath, placePath, stateSegment } from "../lib/placeChapters";
import {
  MAP_GRAINS,
  STATE_SCOPED_GRAINS,
  boundaryForGrain,
  boundaryNote,
  boundarySentence,
  capitalised,
  grainLayerFilter,
  initialMapGrain,
  newestPeriodYear,
  nounFor,
  tileMetadataForBoundary,
} from "../lib/measureMapGrains";
import type { MapGrain } from "../lib/measureMapGrains";
import { uncolouredSentence } from "../lib/uncoloured";
import {
  AGE_MAP_INDICATORS,
  ageMapKey,
  ageMapMarginText,
  ageMapPath,
  ageMapPeriodText,
  ageMapRows,
  ageMapValueText,
  getAgeMap,
} from "../lib/ageMap";
import type { AgeMapIndicator, AgeMapResponse } from "../lib/ageMap";
import { NATION_NAME, num } from "../lib/ageStructure";

const ChoroplethMap = dynamic(() => import("./ChoroplethMap"), { ssr: false });

const PAGE_SIZE = 50;
const EXTREMES = 10;

type Load = { state: "loading" } | { state: "error"; message: string } | { state: "ready" };

export default function AgeMapPage({
  indicator,
  requestedGrain,
  requestedState,
}: {
  indicator: AgeMapIndicator;
  requestedGrain?: string;
  requestedState?: string;
}) {
  const router = useRouter();
  const [grain, setGrain] = useState<MapGrain>(initialMapGrain(MAP_GRAINS, requestedGrain) || "COUNTY");
  const [stateFips, setStateFips] = useState(requestedState && /^\d{2}$/.test(requestedState) ? requestedState : "");
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [geographies, setGeographies] = useState<GeographySummary[] | null>(null);
  // `undefined` while reading; `null` when the route could not be read.
  const [tileCatalog, setTileCatalog] = useState<TileLayerCatalogResponse | null | undefined>(undefined);
  const [answer, setAnswer] = useState<AgeMapResponse | null>(null);
  const [nation, setNation] = useState<AgeMapResponse | null>(null);
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const [page, setPage] = useState(0);

  const scoped = STATE_SCOPED_GRAINS.has(grain);
  const noun = nounFor(grain);
  const awaitingState = scoped && !stateFips;

  // The states (for the picker and the names), the layer list, and the
  // nation's own value as the legend's reference: once per indicator.
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" }, pageSize: 1000, signal })
      .then((items) => { if (!signal.aborted) setStates(items); })
      .catch((error) => { if (!signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) }); });
    getTileLayers({ signal })
      .then((catalog) => { if (!signal.aborted) setTileCatalog(catalog); })
      .catch(() => { if (!signal.aborted) setTileCatalog(null); });
    getAgeMap({ indicator: indicator.id, geo_level: "NATIONAL" }, { signal })
      .then((payload) => { if (!signal.aborted) setNation(payload); })
      .catch(() => { if (!signal.aborted) setNation(null); });
    return () => controller.abort();
  }, [indicator.id]);

  // The grain's geographies (every one is listed, valued or not) and the
  // indicator's values for them, in parallel. A city map also reads its
  // state's counties, which its page addresses are told apart from.
  useEffect(() => {
    if (awaitingState) {
      setGeographies([]);
      setAnswer(null);
      setLoad({ state: "ready" });
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
    setAnswer(null);
    setLoad({ state: "loading" });
    const stateParam = scoped ? stateFips : undefined;
    Promise.all([
      grain === "PLACE" ? Promise.all([read("PLACE", stateFips), read("COUNTY", stateFips)]) : read(grain, stateParam).then((items) => [items, grain === "COUNTY" ? items : null] as const),
      getAgeMap({ indicator: indicator.id, geo_level: grain, state_fips: stateParam }, { signal }),
    ])
      .then(([[items, countyItems], payload]) => {
        if (signal.aborted) return;
        if (countyItems) setCounties(countyItems);
        setGeographies(items);
        setAnswer(payload);
        setPage(0);
        setLoad({ state: "ready" });
      })
      .catch((error) => { if (!signal.aborted) setLoad({ state: "error", message: apiErrorMessage(error) }); });
    return () => controller.abort();
  }, [indicator.id, grain, stateFips, scoped, awaitingState]);

  // The grain and its state are public state: replaced in place, not navigated.
  useEffect(() => {
    const target = ageMapPath(indicator.id, grain, stateFips);
    if (`${window.location.pathname}${window.location.search}` !== target) window.history.replaceState(null, "", target);
  }, [indicator.id, grain, stateFips]);

  const key = ageMapKey(indicator.id);
  const rows = useMemo(() => (answer ? ageMapRows(answer) : []), [answer]);
  const mapView = useMemo(
    () => (answer && geographies && answer.indicator === indicator.id ? buildMeasureMapView(key, rows, geographies) : null),
    [answer, geographies, rows, key, indicator.id],
  );
  const periodYear = useMemo(() => newestPeriodYear(rows), [rows]);
  const boundary = tileCatalog ? boundaryForGrain(tileCatalog, grain, periodYear) : null;
  const drawnMetadata = boundary?.drawn ? tileMetadataForBoundary(boundary) : null;
  const chosenState = states.find((item) => item.state_fips === stateFips) || null;
  const where = scoped && chosenState ? ` in ${String(chosenState.state_name || chosenState.geo_name || "")}` : "";
  const name = answer?.label && answer.indicator === indicator.id ? answer.label : indicator.label;
  const unit = answer?.unit || indicator.unit;
  const period = answer ? ageMapPeriodText(answer) : "";
  const initialView = useMemo(() => {
    if (!scoped || !chosenState) return null;
    const latitude = Number(chosenState.latitude ?? chosenState.geo_latitude);
    const longitude = Number(chosenState.longitude ?? chosenState.geo_longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
    return { center: [longitude, latitude] as [number, number], zoom: Math.max(6, boundary?.layer?.minzoom ?? 6) };
  }, [scoped, chosenState, boundary?.layer?.minzoom]);

  const nationItem = nation?.indicator === indicator.id ? nation.items[0] : undefined;
  const nationValue = num(nationItem?.value ?? null);
  const nationLine =
    nation && nationItem
      ? nationValue === null
        ? `${NATION_NAME}: not available.`
        : `${NATION_NAME}, ${ageMapPeriodText(nation)}: ${ageMapValueText(nationValue, nation.unit)}${nationItem.moe ? ` (${ageMapMarginText(nationItem.moe, nation.unit)})` : ""}.`
      : null;

  const controls = (
    <section className="profile-controls">
      <label>
        Measure
        <select value={indicator.id} onChange={(event) => router.push(ageMapPath(event.target.value, grain, stateFips))} data-testid="age-map-indicator">
          {AGE_MAP_INDICATORS.map((item) => (
            <option key={item.id} value={item.id}>{item.label}</option>
          ))}
        </select>
      </label>
      <label>
        Draw by
        <select value={grain} onChange={(event) => setGrain(event.target.value as MapGrain)} data-testid="age-map-grain">
          {MAP_GRAINS.map((item) => (
            <option key={item} value={item}>{capitalised(nounFor(item).many)}</option>
          ))}
        </select>
      </label>
      {scoped ? (
        <label>
          State
          <select value={stateFips} onChange={(event) => setStateFips(event.target.value)} data-testid="age-map-state">
            <option value="">Choose a state</option>
            {[...states].sort((left, right) => String(left.state_name || "").localeCompare(String(right.state_name || ""))).map((item) => (
              <option key={item.geo_id} value={String(item.state_fips || "")}>{String(item.state_name || item.geo_name || "")}</option>
            ))}
          </select>
        </label>
      ) : null}
    </section>
  );

  const heading = (
    <header className="page-heading">
      <div className="section-kicker">Age and sex, every {noun.one}{where}</div>
      <h1>{name}</h1>
      <p>
        {`${capitalised(indicator.per)}. Computed from the Census Bureau's published age and sex counts (American Community Survey, five-year estimates) under the five-year age grouping. ${capitalised(noun.many)} without a value are counted and listed, not painted.`}
      </p>
    </header>
  );

  if (awaitingState) {
    return (
      <main className="page-shell compact-page measure-map-page" data-testid="age-map-choose-state" data-indicator={indicator.id}>
        {heading}
        <p>There are tens of thousands of {noun.many} in the country, so they are drawn one state at a time. Choose a state.</p>
        {controls}
      </main>
    );
  }

  if (load.state === "error" || (load.state === "ready" && mapView && mapView.withValue === 0)) {
    return (
      <main className="page-shell compact-page measure-map-page" data-testid="age-map-unavailable" data-indicator={indicator.id}>
        {heading}
        <p data-testid="age-map-reason">
          {load.state === "error" ? load.message : `No ${noun.one}${where} has a value for ${name.toLowerCase()} yet.`}
        </p>
        {controls}
      </main>
    );
  }

  const linkFor = (entry: RankedCounty) => {
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
  const pages = mapView ? Math.max(1, Math.ceil(mapView.ranked.length / PAGE_SIZE)) : 1;
  const table = (caption: string, entries: RankedCounty[], testId: string) => (
    <AgeMapTable caption={caption} heading={capitalised(noun.one)} unit={unit} entries={entries} linkFor={linkFor} nameFor={nameFor} testId={testId} />
  );

  return (
    <main className="page-shell measure-map-page" data-testid="age-map" data-indicator={indicator.id} data-grain={grain} data-state={scoped ? stateFips : ""} data-release={answer?.release || ""}>
      {heading}
      {controls}

      <section className="status-row" role="status">
        <StatusPill
          state={mapView ? "ok" : "loading"}
          label={capitalised(noun.many)}
          message={mapView ? `${formatNumber(mapView.withValue)} with a value, ${formatNumber(mapView.withoutValue)} without` : `reading ${noun.one} values`}
          testId="age-map-status"
        />
      </section>

      {mapView ? (
        <>
          <section className="analysis-panel" aria-labelledby="age-map-heading">
            <h2 id="age-map-heading">{name}, {period}</h2>
            {drawnMetadata && boundary?.drawn ? (
              <ChoroplethMap
                key={`${indicator.id}:${grain}:${stateFips}:${drawnMetadata.tileTemplate}`}
                rows={rows}
                tileMetadata={drawnMetadata}
                geoLevel={grain}
                layerFilter={grain === "COUNTY" ? undefined : grainLayerFilter(grain, scoped ? stateFips : null)}
                sourceZoom={{ minzoom: boundary.layer.minzoom, maxzoom: boundary.layer.maxzoom }}
                initialView={initialView}
                boundaryNote={{ text: boundaryNote(boundary, grain), marked: boundary.marked }}
                legendTitle={`${name}, ${period}`}
                missingLabel={`No value (${formatNumber(mapView.withoutValue)} ${noun.many})`}
                testId="age-map-canvas"
                ariaLabel={`${name}, ${period}, by ${noun.one}${where}. The tables below list every ${noun.one}, including those this map leaves uncoloured.`}
                distribution={mapView.distribution}
              />
            ) : (
              <p className="subtle" role="status" data-testid="age-map-no-map">
                {boundary && !boundary.drawn ? boundarySentence(boundary, grain, periodYear) : tileCatalog === undefined ? "Reading the map layers…" : "The map is not available here; every value is in the table below."}
              </p>
            )}
            {boundary?.drawn ? (
              <p className="subtle" data-testid="age-map-vintage" data-vintage={boundary.vintage ?? "current"} data-marked={boundary.marked ? "true" : "false"} title={boundaryNote(boundary, grain)}>
                {boundarySentence(boundary, grain, periodYear)}
              </p>
            ) : null}
            {nationLine ? <p className="subtle" data-testid="age-map-nation">{nationLine}</p> : null}
            <p className="subtle" data-testid="age-map-uncoloured">{uncolouredSentence(noun, mapView.ranked.length, mapView.withValue)}</p>
            <p className="subtle">
              {`Each colour holds about the same number of ${noun.many}, so the bands show where a ${noun.one} stands among the others rather than how far apart the values are.`}
            </p>
            <ul className="measure-legend" data-testid="age-map-legend" aria-label="Legend">
              {mapView.bins.map((bin) => (
                <li key={bin.binIndex} data-count={bin.count}>
                  {ageMapValueText(bin.lowerBound, unit)} to {ageMapValueText(bin.upperBound, unit)}: {formatNumber(bin.count)} {noun.many}
                </li>
              ))}
              <li data-count={mapView.withoutValue}>Uncoloured, no value: {formatNumber(mapView.withoutValue)} {noun.many}</li>
            </ul>
          </section>

          <section className="analysis-panel" aria-labelledby="age-map-extremes-heading">
            <h2 id="age-map-extremes-heading">Highest and lowest</h2>
            <p className="subtle">Each value carries its 90 % margin of error; {noun.many} whose intervals overlap may not differ.</p>
            <div className="measure-extremes">
              {table("Highest values", withValues.slice(0, EXTREMES), "age-map-highest")}
              {table("Lowest values", withValues.slice(-EXTREMES).reverse(), "age-map-lowest")}
            </div>
          </section>

          <section className="analysis-panel" aria-labelledby="age-map-all-heading">
            <h2 id="age-map-all-heading">Every {noun.one}{where}</h2>
            {table(`Every ${noun.one}${where}, page ${page + 1} of ${pages}`, rankedPage(mapView, page, PAGE_SIZE), "age-map-all")}
            <div className="command-row">
              <button type="button" className="button secondary" disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button>
              <button type="button" className="button secondary" disabled={page + 1 >= pages} onClick={() => setPage(page + 1)}>Next</button>
            </div>
          </section>
        </>
      ) : (
        <p className="subtle" role="status">Reading {noun.one} values…</p>
      )}
    </main>
  );
}

function AgeMapTable({
  caption,
  entries,
  heading,
  unit,
  linkFor,
  nameFor,
  testId,
}: {
  caption: string;
  entries: RankedCounty[];
  heading: string;
  unit: string;
  linkFor: (entry: RankedCounty) => string | null;
  nameFor: (entry: RankedCounty) => string;
  testId: string;
}) {
  return (
    <div className="table-scroll">
      <table className="place-card-table" data-testid={testId}>
        <caption>{caption}</caption>
        <thead>
          <tr><th scope="col">{heading}</th><th scope="col">Value</th><th scope="col">Margin of error</th></tr>
        </thead>
        <tbody>
          {entries.map((entry) => {
            const href = linkFor(entry);
            const label = nameFor(entry);
            const reason = entry.row?.value_status ? String(entry.row.value_status) : "no value published";
            return (
              <tr key={entry.county.geo_id} data-geo-id={entry.county.geo_id} data-gap={entry.gap || undefined}>
                <th scope="row">{href ? <Link href={href}>{label}</Link> : label}</th>
                <td>{entry.value !== null ? ageMapValueText(entry.value, unit) : capitalised(reason)}</td>
                <td className="subtle">{entry.value !== null && entry.row?.margin_of_error ? ageMapMarginText(String(entry.row.margin_of_error), unit) : ""}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
