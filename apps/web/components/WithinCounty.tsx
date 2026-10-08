"use client";

/**
 * What varies within one county (sub-county-geography).
 *
 * One ACS 5-year measure at a time over the county's Census tracts: the
 * `tracts` tile layer painted with `buildChoroplethModel` -- the explorer's
 * own rule that a tract without a usable number is left uncoloured rather
 * than coloured as zero -- a legend that counts the tracts left uncoloured,
 * and the same values in a table, so the map is never the only way to read
 * one. The tract list comes from the geography catalog, so a tract with no
 * published value is still counted and named.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import type { ExpressionSpecification, FilterSpecification } from "maplibre-gl";

import ChoroplethLegend from "./ChoroplethLegend";
import { useMapLibre } from "./useMapLibre";
import { apiErrorMessage, fetchAllPages } from "../lib/api/client";
import type { GeographySummary, Observation } from "../lib/api/types";
import { buildChoroplethModel, formatObservationValue, marginOfErrorText } from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import { boundaryTileUrl } from "../lib/mapWiring";
import { ACTIVE_GEOGRAPHIES_ONLY, normalizeObservationRows } from "../lib/observationAccess";
import { discoverTractTiles } from "../lib/tiles";
import type { TractTiles } from "../lib/tiles";

const TRACT_SOURCE = "within-county-tracts";
const TRACT_LAYER = "within-county-tracts-fill";

/** The ACS 5-year measures the warehouse loads at tract grain (`AcsConfig.tract_tables`). */
export const TRACT_MEASURES: readonly { code: string; label: string }[] = [
  { code: "CENSUS_ACS:acs5:B19013_001", label: "Median household income" },
  { code: "CENSUS_ACS:acs5:B01003_001", label: "Total population" },
  { code: "CENSUS_ACS:acs5:B17001_002", label: "People with income below the poverty level" },
  { code: "CENSUS_ACS:acs5:B25064_001", label: "Median gross rent" },
  { code: "CENSUS_ACS:acs5:B25077_001", label: "Median home value" },
];

/** The sentence the legend states about tracts left uncoloured. */
export function uncolouredSentence(total: number, coloured: number): string {
  const missing = Math.max(total - coloured, 0);
  if (!missing) return `All ${total} tracts have a published value.`;
  return `${missing} of ${total} tracts have no published value and are left uncoloured, not shown as zero.`;
}

type Load =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; tracts: GeographySummary[]; rows: ObservationRow[] };

function TractMap({
  rows,
  tiles,
  county,
  stateFips,
  countyFips,
  legendTitle,
  uncoloured,
}: {
  rows: ObservationRow[];
  tiles: TractTiles;
  county: GeographySummary;
  stateFips: string;
  countyFips: string;
  legendTitle: string;
  uncoloured: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const { mapRef, ready, loadFailed } = useMapLibre(containerRef, true);
  const model = useMemo(() => buildChoroplethModel(rows, "geo_id", null, "No published value"), [rows]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    if (!map.getSource(TRACT_SOURCE)) {
      map.addSource(TRACT_SOURCE, {
        type: "vector",
        tiles: [boundaryTileUrl(tiles.tileTemplate, window.location.origin)],
        minzoom: 6,
        maxzoom: 14,
      });
      const latitude = Number(county.latitude ?? county.geo_latitude);
      const longitude = Number(county.longitude ?? county.geo_longitude);
      if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
        map.jumpTo({ center: [longitude, latitude], zoom: 8.5 });
      }
    }
    const expression = model.expression as unknown as ExpressionSpecification;
    const filter = [
      "all",
      ["==", ["get", "geo_level"], "TRACT"],
      ["==", ["get", "state_fips"], stateFips],
      ["==", ["get", "county_fips"], countyFips],
    ] as unknown as FilterSpecification;
    if (!map.getLayer(TRACT_LAYER)) {
      map.addLayer({
        id: TRACT_LAYER,
        type: "fill",
        source: TRACT_SOURCE,
        "source-layer": tiles.sourceLayer,
        filter,
        paint: { "fill-color": expression, "fill-opacity": 0.85, "fill-outline-color": "#ffffff" },
      });
    } else {
      map.setPaintProperty(TRACT_LAYER, "fill-color", expression);
      map.setFilter(TRACT_LAYER, filter);
    }
  }, [mapRef, ready, tiles, model, county, stateFips, countyFips]);

  if (loadFailed) {
    return (
      <p className="status-line" role="status" data-testid="within-county-map-failed">
        The map could not be loaded. Every value it would colour is in the table below.
      </p>
    );
  }
  return (
    <div className="map-shell">
      <div
        className="map-canvas"
        data-testid="within-county-map"
        data-map-ready={ready ? "true" : "false"}
        data-colored-values={model.valueCount}
        ref={containerRef}
        role="region"
        aria-label={`${legendTitle} by Census tract. The table below lists every tract, including those this map leaves uncoloured.`}
      />
      <ChoroplethLegend title={legendTitle} items={model.legendItems} ariaLabel={`${legendTitle} legend`} />
      <p className="subtle" data-testid="within-county-uncoloured">{uncoloured}</p>
    </div>
  );
}

export default function WithinCounty({ county }: { county: GeographySummary }) {
  const stateFips = String(county.state_fips || "");
  const countyFips = String(county.county_fips || "");
  const [measure, setMeasure] = useState(TRACT_MEASURES[0]!.code);
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const [tiles, setTiles] = useState<TractTiles | null>(null);

  useEffect(() => {
    let cancelled = false;
    discoverTractTiles().then((found) => {
      if (!cancelled) setTiles(found);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setLoad({ state: "loading" });
    Promise.all([
      fetchAllPages<GeographySummary>("/catalog/geographies", {
        params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "TRACT", state_fips: stateFips, county_fips: countyFips },
        pageSize: 1000,
      }),
      fetchAllPages<Observation>("/observations", {
        params: {
          metric_code: measure,
          geo_level: "TRACT",
          state_fips: stateFips,
          county_fips: countyFips,
          newest_per_geography: true,
        },
        pageSize: 1000,
      }),
    ])
      .then(([tracts, observations]) => {
        if (!cancelled) setLoad({
            state: "ready",
            tracts,
            rows: normalizeObservationRows(null, observations as unknown as ObservationRow[]),
          });
      })
      .catch((error) => {
        if (!cancelled) setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => {
      cancelled = true;
    };
  }, [measure, stateFips, countyFips]);

  const label = TRACT_MEASURES.find((entry) => entry.code === measure)?.label || measure;
  if (load.state === "ready" && !load.tracts.length) return null;

  const byTract = new Map<string, ObservationRow>();
  if (load.state === "ready") {
    for (const row of load.rows) byTract.set(String(row.geo_id), row);
  }
  const coloured =
    load.state === "ready"
      ? load.tracts.filter((tract) => {
          const row = byTract.get(tract.geo_id);
          return row && row.value !== null && row.value !== undefined && row.value !== "";
        }).length
      : 0;
  const uncoloured = load.state === "ready" ? uncolouredSentence(load.tracts.length, coloured) : "";

  return (
    <section className="analysis-panel within-county" aria-labelledby="within-county-heading" data-testid="within-county">
      <h2 id="within-county-heading">Within this county</h2>
      <p>
        One measure at a time across the county&apos;s Census tracts, from the American Community Survey 5-year
        estimates. Tract estimates carry wide margins of error; read them before comparing two tracts.
      </p>
      <label className="within-county-measure">
        Measure
        <select value={measure} onChange={(event) => setMeasure(event.target.value)} data-testid="within-county-measure">
          {TRACT_MEASURES.map((entry) => (
            <option key={entry.code} value={entry.code}>
              {entry.label}
            </option>
          ))}
        </select>
      </label>
      {load.state === "loading" ? <p className="subtle">Reading the county&apos;s tracts…</p> : null}
      {load.state === "error" ? (
        <p className="subtle" data-testid="within-county-error">The tracts could not be read: {load.message}</p>
      ) : null}
      {load.state === "ready" ? (
        <>
          {tiles ? (
            <TractMap
              rows={load.rows}
              tiles={tiles}
              county={county}
              stateFips={stateFips}
              countyFips={countyFips}
              legendTitle={label}
              uncoloured={uncoloured}
            />
          ) : (
            <p className="subtle" data-testid="within-county-uncoloured">
              The tract map is not available; every value is in the table. {uncoloured}
            </p>
          )}
          <div className="table-scroll">
            <table className="within-county-table" data-testid="within-county-table">
              <caption>{label} by Census tract</caption>
              <thead>
                <tr>
                  <th scope="col">Tract</th>
                  <th scope="col">{label}</th>
                  <th scope="col">Margin of error</th>
                </tr>
              </thead>
              <tbody>
                {load.tracts.map((tract) => {
                  const row = byTract.get(tract.geo_id);
                  const published = row && row.value !== null && row.value !== undefined && row.value !== "";
                  return (
                    <tr key={tract.geo_id} data-testid={`within-county-row-${tract.geo_id}`}>
                      <th scope="row">{String(tract.area_name || tract.geo_name || tract.geo_id)}</th>
                      <td>
                        {published
                          ? formatObservationValue(row!.value)
                          : row?.value_status
                            ? `No value: ${String(row.value_status)}`
                            : "Not published for this tract"}
                      </td>
                      <td>{published ? marginOfErrorText(row!) || "" : ""}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      ) : null}
    </section>
  );
}
