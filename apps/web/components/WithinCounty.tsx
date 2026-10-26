"use client";

/**
 * What varies within one county (sub-county-geography).
 *
 * One ACS 5-year measure at a time over the county's Census tracts, for one
 * published release at a time: the `tracts` tile layer painted with
 * `buildChoroplethModel` -- the explorer's own rule that a tract without a
 * usable number is left uncoloured rather than coloured as zero -- a legend
 * that counts the tracts left uncoloured, and the same values in a table, so
 * the map is never the only way to read one. The tract list comes from the
 * geography catalog, so a tract with no published value is still counted and
 * named.
 *
 * Tract boundaries are redrawn each decade, so an older release is on an
 * older set of tracts. A tract that no longer exists is listed from the
 * release's own rows and marked, a tract that did not yet exist is counted
 * as absent rather than as unpublished, and the map, which draws today's
 * boundaries, says so. Nothing here joins a value across the redraw.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import type { ExpressionSpecification, FilterSpecification } from "maplibre-gl";

import ChoroplethLegend from "./ChoroplethLegend";
import { useMapLibre } from "./useMapLibre";
import { apiErrorMessage, fetchAllPages } from "../lib/api/client";
import type { GeographySummary, MetricPeriod, MetricRelease, Observation } from "../lib/api/types";
import { buildChoroplethModel, formatObservationValue, marginOfErrorText } from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import { boundaryTileUrl } from "../lib/mapWiring";
import { ACTIVE_GEOGRAPHIES_ONLY, normalizeNeutralObservationRows, observationPeriodLabel } from "../lib/observationAccess";
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

/** One release the reader can choose, named by the period it covers. */
export interface ReleaseOption {
  release: string;
  label: string;
}

/**
 * The releases offered, newest first, each named by its period when the
 * periods route publishes one ending in the release's year (an ACS vintage
 * is its period's last year), else by the release alone. A release the
 * periods route does not know is still offered: the rows say their period
 * once read.
 */
export function releaseOptions(releases: readonly MetricRelease[], periods: readonly MetricPeriod[]): ReleaseOption[] {
  const byEndYear = new Map<string, MetricPeriod>();
  for (const period of periods) {
    const end = String(period.period_end || period.period_start || "").slice(0, 4);
    if (end && !byEndYear.has(end)) byEndYear.set(end, period);
  }
  return releases
    .map((entry) => String(entry.release || ""))
    .filter(Boolean)
    .sort((left, right) => right.localeCompare(left))
    .map((release) => {
      const period = byEndYear.get(release);
      const label = period
        ? `${observationPeriodLabel({ period_start: period.period_start, period_end: period.period_end })} (${release} release)`
        : `${release} release`;
      return { release, label };
    });
}

/**
 * The boundary sentence for one release: how many of the release's tracts
 * are gone from today's boundaries (so they are in the table, not on the
 * map) and how many of today's tracts did not exist for it. Empty when the
 * release is on today's boundaries.
 */
export function boundarySentence(periodLabel: string, currentTotal: number, retired: number, absent: number): string {
  if (!retired && !absent) return "";
  const parts: string[] = [];
  if (retired) {
    parts.push(
      `${retired} ${retired === 1 ? "tract" : "tracts"} of that release ${retired === 1 ? "is" : "are"} not in today's boundaries and ${retired === 1 ? "appears" : "appear"} only in the table`,
    );
  }
  if (absent) {
    parts.push(`${absent} of today's ${currentTotal} tracts did not exist then and have no value for it`);
  }
  return `${periodLabel ? `${periodLabel} is` : "This release is"} on the tract boundaries of its own release: ${parts.join("; ")}. A value is never carried across a boundary change.`;
}

/**
 * "Census Tract 1.01" from a tract identity, for a tract the catalog no
 * longer lists: the six-digit code is the tract number times 100.
 */
export function tractNameFromGeoId(geoId: string): string {
  const match = /tract:(\d{6})$/.exec(String(geoId || ""));
  if (!match) return String(geoId || "");
  const whole = Number(match[1]!.slice(0, 4));
  const fraction = match[1]!.slice(4);
  return `Census Tract ${fraction === "00" ? whole : `${whole}.${fraction}`}`;
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
  boundary,
}: {
  rows: ObservationRow[];
  tiles: TractTiles;
  county: GeographySummary;
  stateFips: string;
  countyFips: string;
  legendTitle: string;
  uncoloured: string;
  boundary: string;
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
      {boundary ? <p className="subtle" data-testid="within-county-boundary">{boundary}</p> : null}
    </div>
  );
}

export default function WithinCounty({ county }: { county: GeographySummary }) {
  const stateFips = String(county.state_fips || "");
  const countyFips = String(county.county_fips || "");
  const [measure, setMeasure] = useState(TRACT_MEASURES[0]!.code);
  const [releases, setReleases] = useState<ReleaseOption[] | null>(null);
  const [release, setRelease] = useState("");
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

  // The releases this measure publishes, so the reader can step back through
  // them. The newest is read first; a failure here leaves the newest values
  // readable, with no choice of release.
  useEffect(() => {
    let cancelled = false;
    setReleases(null);
    setRelease("");
    Promise.all([
      fetchAllPages<MetricRelease>("/observations/releases", { params: { metric_code: measure }, pageSize: 200 }),
      fetchAllPages<MetricPeriod>("/observations/periods", { params: { metric_code: measure }, pageSize: 200 }),
    ])
      .then(([published, periods]) => {
        if (cancelled) return;
        const options = releaseOptions(published, periods);
        setReleases(options);
        setRelease(options[0]?.release || "");
      })
      .catch(() => {
        if (!cancelled) setReleases([]);
      });
    return () => {
      cancelled = true;
    };
  }, [measure]);

  useEffect(() => {
    if (releases === null) return;
    let cancelled = false;
    setLoad({ state: "loading" });
    const scoped = release
      ? { scope: "as_released", release }
      : { newest_per_geography: true };
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
          ...scoped,
        },
        pageSize: 1000,
      }),
    ])
      .then(([tracts, observations]) => {
        if (!cancelled) setLoad({
            state: "ready",
            tracts,
            rows: normalizeNeutralObservationRows(observations as unknown as ObservationRow[]),
          });
      })
      .catch((error) => {
        if (!cancelled) setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => {
      cancelled = true;
    };
  }, [measure, release, releases, stateFips, countyFips]);

  const label = TRACT_MEASURES.find((entry) => entry.code === measure)?.label || measure;
  if (load.state === "ready" && !load.tracts.length) return null;

  const byTract = new Map<string, ObservationRow>();
  if (load.state === "ready") {
    for (const row of load.rows) byTract.set(String(row.geo_id), row);
  }
  const hasValue = (row: ObservationRow | undefined) =>
    Boolean(row && row.value !== null && row.value !== undefined && row.value !== "");
  const current = load.state === "ready" ? load.tracts : [];
  const currentIds = new Set(current.map((tract) => String(tract.geo_id)));
  // A tract the release published that today's boundaries no longer hold.
  const retired = load.state === "ready" ? load.rows.filter((row) => !currentIds.has(String(row.geo_id))) : [];
  // A tract of today that the release has no row for did not exist then --
  // but only when the release is on another decade's tracts, which shows as
  // a retired row. On today's boundaries a missing row is unpublished.
  const redrawn = retired.length > 0;
  const absent = redrawn ? current.filter((tract) => !byTract.has(String(tract.geo_id))).length : 0;
  const coloured = current.filter((tract) => hasValue(byTract.get(String(tract.geo_id)))).length;
  const periodLabel = load.state === "ready" ? observationPeriodLabel(load.rows[0]) : "";
  // The uncoloured count is over the tracts that existed for the release: a
  // tract that did not exist then is absent, not unpublished, and the
  // boundary sentence counts it.
  const uncoloured = load.state === "ready" ? uncolouredSentence(current.length - absent, coloured) : "";
  const boundary = load.state === "ready" ? boundarySentence(periodLabel, current.length, retired.length, absent) : "";
  const titled = periodLabel ? `${label}, ${periodLabel}` : label;

  return (
    <section className="analysis-panel within-county" aria-labelledby="within-county-heading" data-testid="within-county">
      <h2 id="within-county-heading">Within this county</h2>
      <p>
        One measure at a time across the county&apos;s Census tracts, from the American Community Survey 5-year
        estimates, one release at a time. Tract estimates carry wide margins of error; read them before comparing
        two tracts.
      </p>
      <div className="within-county-controls">
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
        {releases && releases.length > 0 ? (
          <label className="within-county-release">
            Period
            <select value={release} onChange={(event) => setRelease(event.target.value)} data-testid="within-county-release">
              {releases.map((entry) => (
                <option key={entry.release} value={entry.release}>
                  {entry.label}
                </option>
              ))}
            </select>
          </label>
        ) : null}
      </div>
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
              legendTitle={titled}
              uncoloured={uncoloured}
              boundary={boundary}
            />
          ) : (
            <p className="subtle" data-testid="within-county-uncoloured">
              The tract map is not available; every value is in the table. {uncoloured} {boundary}
            </p>
          )}
          <div className="table-scroll">
            <table className="within-county-table" data-testid="within-county-table">
              <caption>{titled} by Census tract</caption>
              <thead>
                <tr>
                  <th scope="col">Tract</th>
                  <th scope="col">{label}</th>
                  <th scope="col">Margin of error</th>
                </tr>
              </thead>
              <tbody>
                {current.map((tract) => {
                  const row = byTract.get(String(tract.geo_id));
                  const published = hasValue(row);
                  return (
                    <tr key={tract.geo_id} data-testid={`within-county-row-${tract.geo_id}`}>
                      <th scope="row">{String(tract.area_name || tract.geo_name || tract.geo_id)}</th>
                      <td>
                        {published
                          ? formatObservationValue(row!.value)
                          : row?.value_status
                            ? `No value: ${String(row.value_status)}`
                            : row || !redrawn
                              ? "Not published for this tract"
                              : "Did not exist in this release"}
                      </td>
                      <td>{published ? marginOfErrorText(row!) || "" : ""}</td>
                    </tr>
                  );
                })}
                {retired.map((row) => {
                  const published = hasValue(row);
                  return (
                    <tr key={String(row.geo_id)} data-testid={`within-county-row-${row.geo_id}`} data-retired="true">
                      <th scope="row">
                        {tractNameFromGeoId(String(row.geo_id))}
                        <span className="subtle"> · not in today&apos;s boundaries</span>
                      </th>
                      <td>
                        {published
                          ? formatObservationValue(row.value)
                          : row.value_status
                            ? `No value: ${String(row.value_status)}`
                            : "Not published for this tract"}
                      </td>
                      <td>{published ? marginOfErrorText(row) || "" : ""}</td>
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
