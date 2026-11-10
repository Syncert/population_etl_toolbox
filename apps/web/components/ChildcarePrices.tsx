"use client";

// Childcare prices in the "Cost of living" section (county-childcare-prices).
// A county page reads its own eight prices, the newest row of each; every
// other page says the Women's Bureau publishes counties only. The rules for
// the table are `lib/childcare.ts`. The price is the number in each cell;
// the bar beside it, on one scale for the whole table, only helps compare.
// Hours of work (each price over the county's own average hourly wage) are
// read once the prices are, shown under each price, and worked once below.

import { useEffect, useState } from "react";
import { apiFetch } from "../lib/api/client";
import { getDerivedObservations } from "../lib/derivedViews";
import type { CollectionResponse, GeographySummary, Observation } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { buildNewestValueRequest, normalizeObservationRows } from "../lib/observationAccess";
import type { PlaceLevel } from "../lib/placeChapters";
import {
  AGE_GROUPS,
  CARE_TYPES,
  CHILDCARE_CREDIT,
  CHILDCARE_SOURCE,
  NOT_A_COUNTY,
  catalogedCodes,
  childcareMetricCode,
  childcareRatioId,
  childcareTable,
  footnoteReasons,
  formatWeekly,
  workFormula,
  workGap,
  workText,
} from "../lib/childcare";
import type { ChildcareMinutes, InfantCareRead } from "../lib/childcare";

type Newest = Record<string, ObservationRow | null>;
type State = { status: "reading" } | { status: "unloaded" } | { status: "failed" } | { status: "read"; newest: Newest };


export default function ChildcarePrices({
  level,
  place,
  sources,
  catalogued,
  onInfant,
}: {
  level: PlaceLevel;
  place: GeographySummary;
  sources: readonly ExplorerSource[];
  /** The place page's catalog read: `null` while pending (see `catalogedCodes`). */
  catalogued?: ReadonlyMap<string, unknown> | null;
  /** Hands the center infant price and its hours on, for "Starting a family". */
  onInfant?: (geoId: string, read: InfantCareRead) => void;
}) {
  const [state, setState] = useState<State>({ status: "reading" });
  const [minutes, setMinutes] = useState<Record<string, ChildcareMinutes | null>>({});
  const source = sources.find((item) => item.sourceCode === CHILDCARE_SOURCE) || null;
  const county = level === "COUNTY";
  const codes = catalogedCodes(catalogued);
  const codesKey = codes ? codes.join(",") : null;

  useEffect(() => {
    if (!county) return;
    if (codesKey === null) {
      setState({ status: "reading" });
      return;
    }
    const wanted = codesKey ? codesKey.split(",") : [];
    // A source with no catalogued price is not loaded: nothing is asked.
    if (!source || !wanted.length) {
      setState({ status: "unloaded" });
      return;
    }
    const controller = new AbortController();
    setState({ status: "reading" });
    Promise.all(
      wanted.map(async (code) => {
        const { resource, params } = buildNewestValueRequest(source, { metricCode: code, geoId: place.geo_id });
        const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params, signal: controller.signal });
        const rows = normalizeObservationRows(source, Array.isArray(payload.items) ? payload.items : [])
          .filter((row) => row.geo_id === place.geo_id)
          .sort((a, b) => String(a.period_start || "").localeCompare(String(b.period_start || "")));
        return [code, rows.at(-1) || null] as const;
      }),
    )
      .then((pairs) => {
        if (!controller.signal.aborted) setState({ status: "read", newest: Object.fromEntries(pairs) });
      })
      .catch(() => {
        if (!controller.signal.aborted) setState({ status: "failed" });
      });
    return () => controller.abort();
  }, [county, source, codesKey, place.geo_id]);

  // Hours of work for each shown price, at the county's own wage, for the
  // price's own study year. A price not computed shows no hours, never zero.
  useEffect(() => {
    setMinutes({});
    if (!county || state.status !== "read") return;
    const table = childcareTable(state.newest);
    if (table.year === null) return;
    const controller = new AbortController();
    for (const cell of table.cells.filter((item) => item.value !== null)) {
      const key = `${cell.care}_${cell.age}`;
      getDerivedObservations(
        { ratio_id: childcareRatioId(cell.care, cell.age), geo_id: place.geo_id, wage_geo_id: place.geo_id, period_start: `${table.year}-01-01`, limit: 1 },
        { signal: controller.signal },
      )
        .then((page) => {
          if (!controller.signal.aborted) setMinutes((current) => ({ ...current, [key]: page.items[0] || null }));
        })
        .catch(() => {
          if (!controller.signal.aborted) setMinutes((current) => ({ ...current, [key]: null }));
        });
    }
    return () => controller.abort();
  }, [county, state, place.geo_id]);

  // The center infant price and its hours, once each read ends.
  const infantRow = state.status === "read" ? (state.newest[childcareMetricCode("center", "infant")] ?? null) : undefined;
  const infantError = state.status === "failed" ? "Could not be read just now" : state.status === "unloaded" ? "unloaded" : undefined;
  const infantCell = state.status === "read" ? childcareTable(state.newest).cells.find((cell) => cell.care === "center" && cell.age === "infant") : undefined;
  const infantMinutes = minutes.center_infant;
  const infantDone = !infantRow || infantCell?.value === null || infantCell?.value === undefined || "center_infant" in minutes;
  useEffect(() => {
    if (!county || !onInfant) return;
    if (infantError) onInfant(place.geo_id, { row: null, error: infantError, minutes: null });
    else if (infantRow !== undefined) onInfant(place.geo_id, { row: infantRow, minutes: infantDone ? (infantMinutes ?? null) : undefined });
  }, [county, onInfant, place.geo_id, infantRow, infantError, infantMinutes, infantDone]);

  if (!county) {
    return (
      <div className="place-childcare" data-testid="place-childcare">
        <h3>Childcare</h3>
        <p className="subtle" data-testid="place-childcare-counties-only">{NOT_A_COUNTY}</p>
      </div>
    );
  }

  const table = state.status === "read" ? childcareTable(state.newest) : null;
  const shown = table?.cells.filter((cell) => cell.value !== null) || [];
  const headline = table?.cells.find((cell) => cell.care === "center" && cell.age === "infant");
  const reasons = table ? footnoteReasons(table.cells) : [];
  const name = String(place.geo_name || "This county");
  const headlineWork = workText(minutes.center_infant);
  const formula =
    headline?.value !== null && headline?.value !== undefined
      ? workFormula(minutes.center_infant, headline.value, "center-based infant care", name)
      : "";
  const gap = table ? workGap(minutes.center_infant, table.year, name) : "";

  return (
    <div className="place-childcare" data-testid="place-childcare">
      <h3>Childcare</h3>
      {state.status === "reading" ? (
        <p>Reading…</p>
      ) : state.status === "unloaded" || (table && !shown.length) ? (
        <p data-testid="place-childcare-unloaded">Not loaded into this site yet.</p>
      ) : state.status === "failed" || !table ? (
        <p data-testid="place-childcare-failed">Childcare prices could not be read just now.</p>
      ) : (
        <>
          {headline?.value !== null && headline?.value !== undefined ? (
            <p className="place-childcare-headline" data-testid="place-childcare-headline">
              A childcare center charges <strong>{formatWeekly(headline.value)}</strong>
              {headline.imputed ? <sup aria-label="imputed">*</sup> : null} for an infant in {name}, the median full-time price in {table.year}
              {headlineWork ? (
                <>
                  : <span data-testid="place-childcare-headline-work">{headlineWork}</span> at the county&apos;s average wage
                </>
              ) : null}
              .
            </p>
          ) : null}
          <table className="place-childcare-table" data-testid="place-childcare-table">
            <caption>
              Median weekly full-time price, {table.year}
              <span className="subtle block">Source: {CHILDCARE_CREDIT}. Published for counties.</span>
            </caption>
            <thead>
              <tr>
                <th scope="col">Age group</th>
                {CARE_TYPES.map((care) => <th scope="col" key={care.key}>{care.title}</th>)}
              </tr>
            </thead>
            <tbody>
              {AGE_GROUPS.map((age) => (
                <tr key={age.key}>
                  <th scope="row">
                    {age.title}
                    <span className="subtle block">{age.ages}</span>
                  </th>
                  {CARE_TYPES.map((care) => {
                    const cell = table.cells.find((item) => item.care === care.key && item.age === age.key)!;
                    const testId = `place-childcare-${care.key}_${age.key}`;
                    if (cell.value === null) {
                      return (
                        <td key={care.key} data-testid={testId} data-missing="true">
                          <span className="subtle">Not published for {table.year}.</span>
                        </td>
                      );
                    }
                    const width = table.max > 0 ? Math.max(2, (cell.value / table.max) * 100) : 0;
                    const label = `${care.title}, ${age.title.toLowerCase()}: ${formatWeekly(cell.value)}${cell.imputed ? " (imputed)" : ""}`;
                    return (
                      <td key={care.key} data-testid={testId} data-imputed={cell.imputed ? "true" : "false"} title={label}>
                        <span className="place-childcare-value">
                          {formatWeekly(cell.value)}
                          {cell.imputed ? <sup aria-label="imputed">*</sup> : null}
                        </span>
                        <span className="place-childcare-bar" aria-hidden="true">
                          <span style={{ width: `${width}%` }} />
                        </span>
                        {workText(minutes[`${care.key}_${age.key}`]) ? (
                          <span className="subtle block" data-testid={`${testId}-work`}>
                            {workText(minutes[`${care.key}_${age.key}`])}
                          </span>
                        ) : null}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          {formula ? (
            <p className="subtle" data-testid="place-childcare-formula">
              {formula}
            </p>
          ) : null}
          {gap ? (
            <p className="subtle" data-testid="place-childcare-work-gap">
              {gap}
            </p>
          ) : null}
          {reasons.length ? (
            <p className="subtle" data-testid="place-childcare-imputed">
              * The Women&apos;s Bureau filled this price in itself ({reasons.join("; ")}). It is shown as published.
            </p>
          ) : null}
        </>
      )}
    </div>
  );
}
