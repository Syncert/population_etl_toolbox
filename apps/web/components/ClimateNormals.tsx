"use client";

// Climate averages on a place page (place-climate-normals): NOAA's monthly
// U.S. Climate Normals 1991-2020 for a county, one table of twelve months.
// A county page reads its own four normals (one read each, twelve rows);
// every other page says NOAA publishes them for stations and this site
// shows them for counties, and a city page links the counties it lies in.
// The rules are `lib/climateNormals.ts`. The number in each cell is the
// figure; the bar beside it, on one scale for the whole column, only helps
// compare months.

import Link from "next/link";
import { useEffect, useState } from "react";
import { apiFetch } from "../lib/api/client";
import type { CollectionResponse, GeographySummary, Observation } from "../lib/api/types";
import { NEUTRAL_OBSERVATIONS_PATH } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { normalizeNeutralObservationRows } from "../lib/observationAccess";
import type { PlaceLevel } from "../lib/placeChapters";
import {
  NORMALS_CREDIT,
  NORMALS_MEASURES,
  NOT_A_COUNTY,
  catalogedNormals,
  formatNormal,
  normalsHeadline,
  normalsTable,
  stationText,
} from "../lib/climateNormals";

type State =
  | { status: "reading" }
  | { status: "unloaded" }
  | { status: "failed" }
  | { status: "read"; rows: Record<string, ObservationRow[]> };

export interface ClimateCountyLink {
  geoId: string;
  name: string;
  href?: string | null;
}

export default function ClimateNormals({
  level,
  place,
  catalogued,
  counties = [],
}: {
  level: PlaceLevel;
  place: GeographySummary;
  /** The place page's catalog read: `null` while pending. */
  catalogued?: ReadonlyMap<string, unknown> | null;
  /** A city's counties, from the page's nearby read, linked from the note. */
  counties?: readonly ClimateCountyLink[];
}) {
  const [state, setState] = useState<State>({ status: "reading" });
  const county = level === "COUNTY";
  const codes = catalogedNormals(catalogued);
  const codesKey = codes ? codes.join(",") : null;

  useEffect(() => {
    if (!county) return;
    if (codesKey === null) {
      setState({ status: "reading" });
      return;
    }
    const wanted = codesKey ? codesKey.split(",") : [];
    if (!wanted.length) {
      setState({ status: "unloaded" });
      return;
    }
    const controller = new AbortController();
    setState({ status: "reading" });
    Promise.all(
      wanted.map(async (code) => {
        const payload = await apiFetch<CollectionResponse<Observation>>(NEUTRAL_OBSERVATIONS_PATH, {
          params: { metric_code: code, scope: "latest", geo_id: place.geo_id, limit: "24" },
          signal: controller.signal,
        });
        return [code, normalizeNeutralObservationRows(Array.isArray(payload.items) ? (payload.items as ObservationRow[]) : [])] as const;
      }),
    )
      .then((pairs) => {
        if (!controller.signal.aborted) setState({ status: "read", rows: Object.fromEntries(pairs) });
      })
      .catch(() => {
        if (!controller.signal.aborted) setState({ status: "failed" });
      });
    return () => controller.abort();
  }, [county, codesKey, place.geo_id]);

  const name = String(place.geo_name || "This county");

  return (
    <section className="analysis-panel place-climate" id="climate" aria-labelledby="place-climate-heading" data-testid="place-climate">
      <h2 id="place-climate-heading">Climate averages</h2>
      {!county ? (
        <>
          <p className="subtle" data-testid="place-climate-counties-only">{NOT_A_COUNTY}</p>
          {level === "PLACE" && counties.length ? (
            <p className="subtle" data-testid="place-climate-county-links">
              {counties.length === 1 ? "The county it lies in: " : "The counties it lies in: "}
              {counties.map((entry, index) => (
                <span key={entry.geoId}>
                  {index ? ", " : ""}
                  {entry.href ? <Link className="text-link" href={`${entry.href}#climate`}>{entry.name}</Link> : entry.name}
                </span>
              ))}
              .
            </p>
          ) : null}
        </>
      ) : state.status === "reading" ? (
        <p>Reading…</p>
      ) : state.status === "unloaded" ? (
        <p data-testid="place-climate-unloaded">Not loaded into this site yet.</p>
      ) : state.status === "failed" ? (
        <p data-testid="place-climate-failed">Climate averages could not be read just now.</p>
      ) : (
        <ClimateTable rows={state.rows} geoId={place.geo_id} name={name} />
      )}
    </section>
  );
}

function ClimateTable({ rows, geoId, name }: { rows: Record<string, ObservationRow[]>; geoId: string; name: string }) {
  const table = normalsTable(rows, geoId);
  if (!table.shown) {
    return (
      <p data-testid="place-climate-no-station">
        NOAA has no standard or representative weather station with 1991–2020 normals placed in {name}, so there is no county figure.
      </p>
    );
  }
  const headline = normalsHeadline(table, name);
  const [low, high] = table.scale || [0, 0];
  const span = high - low;
  const at = (value: string) => (span > 0 ? ((Number(value) - low) / span) * 100 : 0);
  const stations = [
    table.temperatureStations ? `temperature from ${stationText(table.temperatureStations)}` : "",
    table.precipitationStations ? `precipitation from ${stationText(table.precipitationStations)}` : "",
  ].filter(Boolean);
  return (
    <>
      {headline ? <p data-testid="place-climate-headline">{headline}</p> : null}
      <table className="place-climate-table" data-testid="place-climate-table">
        <caption>
          Monthly normals, 1991–2020
          <span className="subtle block">
            30-year averages, not the weather of any one year. Source: {NORMALS_CREDIT}.
          </span>
        </caption>
        <thead>
          <tr>
            <th scope="col">Month</th>
            {NORMALS_MEASURES.map((measure) => (
              <th scope="col" key={measure.key}>
                <span className="place-climate-long">{measure.title}</span>
                <span className="place-climate-short">{measure.short}</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.months.map((month) => {
            const { mean, high: hi, low: lo, precipitation } = month.values;
            return (
              <tr key={month.month} data-testid={`place-climate-month-${month.month}`}>
                <th scope="row">
                  <span className="place-climate-long">{month.name}</span>
                  <span className="place-climate-short">{month.name.slice(0, 3)}</span>
                </th>
                {NORMALS_MEASURES.map((measure) => {
                  const value = month.values[measure.key];
                  return value === null ? (
                    <td key={measure.key} data-testid={`place-climate-${month.month}-${measure.key}`} data-missing="true">
                      <span className="subtle">Not published</span>
                    </td>
                  ) : (
                    <td key={measure.key} data-testid={`place-climate-${month.month}-${measure.key}`}>
                      <span className="place-climate-value">{formatNormal(value, measure.key)}</span>
                      {measure.key === "mean" && lo !== null && hi !== null ? (
                        <span className="place-climate-range" aria-hidden="true" title={`${month.name}: low ${formatNormal(lo, "low")}, mean ${formatNormal(mean!, "mean")}, high ${formatNormal(hi, "high")}`}>
                          <span className="place-climate-range-band" style={{ left: `${at(lo)}%`, width: `${Math.max(1, at(hi) - at(lo))}%` }} />
                          <span className="place-climate-range-mean" style={{ left: `${at(mean!)}%` }} />
                        </span>
                      ) : null}
                      {measure.key === "precipitation" && precipitation !== null && table.maxPrecipitation > 0 ? (
                        <span className="place-childcare-bar" aria-hidden="true" title={`${month.name}: ${formatNormal(precipitation, "precipitation")}`}>
                          <span style={{ width: `${Math.max(2, (Number(precipitation) / table.maxPrecipitation) * 100)}%` }} />
                        </span>
                      ) : null}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="subtle" data-testid="place-climate-stations">
        NCEI publishes normals for weather stations. Each figure here is the plain mean of the stations placed in {name} that NCEI flags standard or representative for that measure
        {stations.length ? `: ${stations.join("; ")}` : ""}. The bar under each mean temperature runs from the month&apos;s mean daily low to its mean daily high, with a mark at the mean, on one scale for the year. Figures are rounded to the precision NCEI publishes for a station: 0.1°F and 0.01 in.
      </p>
      {table.placementNotes.length ? (
        <p className="subtle" data-testid="place-climate-placement-note">
          {table.placementNotes.join(" ")}
        </p>
      ) : null}
    </>
  );
}
