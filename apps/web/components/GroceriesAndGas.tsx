"use client";

// The "Cost of living" section on a county, metro, state or nation page
// (groceries-and-gas-cards; everyday prices from cost-of-living-section).
// Every number names the area it describes; the rules for choosing the area
// are `lib/groceriesAndGas.ts`. Loaded as its own chunk.

import dynamic from "next/dynamic";
import { useEffect, useState } from "react";
import { ApiError, apiErrorMessage, apiFetch, fetchCollectionPages, getMetric } from "../lib/api/client";
import type { CollectionResponse, GeographySummary, Observation } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { buildNewestValueRequest, normalizeObservationRows, observationPeriodLabel } from "../lib/observationAccess";
import { buildUseCaseHistory } from "../lib/useCaseAnalysis";
import type { PlaceLevel } from "../lib/placeChapters";
import type { RelatedResponse } from "../lib/placeRelationships";
import {
  NO_LOCAL_PRICES,
  NO_METRO_PRICES,
  PRICE_ITEMS,
  SOURCE_NAMES,
  areaLabel,
  changeOverYear,
  containingAreas,
  formatChange,
  formatGas,
  formatParity,
  formatPrice,
  formatRent,
  readingsFor,
  withheldReason,
} from "../lib/groceriesAndGas";
import type { Area, CostMeasure, PriceItem, Reading } from "../lib/groceriesAndGas";

const PlaceSparkline = dynamic(() => import("./PlaceSparkline"));

interface Answer {
  reading: Reading | null;
  text: string;
  period: string;
  /** "up 3.0% over the year", for a price with a monthly history. */
  change?: string;
  /** The published history, for the sparkline. */
  rows?: ObservationRow[];
  /** Why no more local area answered, or why nothing did. */
  notes: string[];
}

const TITLES: Record<"gas" | "food" | "parity" | "rent", string> = {
  gas: "Regular gasoline",
  rent: "Median rent",
  food: "Groceries overall (food at home)",
  parity: "Overall price level",
};

const LINES: ("gas" | "rent" | "food" | "parity")[] = ["gas", "rent", "food", "parity"];
const ITEMS = PRICE_ITEMS.map((spec) => spec.item);
const ORDER: CostMeasure[] = [...ITEMS, ...LINES];

const isItem = (measure: CostMeasure): measure is PriceItem => ITEMS.includes(measure as PriceItem);

async function answer(
  readings: Reading[],
  sources: readonly ExplorerSource[],
  signal: AbortSignal,
): Promise<Answer> {
  const notes: string[] = [];
  for (const reading of readings) {
    const where = `${reading.area.name} (${reading.source})`;
    try {
      const metric = await getMetric(reading.metricCode, { signal });
      const source = sources.find((item) => item.sourceCode === metric?.source_code) || null;
      if (!metric || !source) {
        notes.push(`${where}: not published.`);
        continue;
      }
      if (reading.measure === "food" || isItem(reading.measure)) {
        const history = buildUseCaseHistory(source, metric, reading.area.geoId, reading.area.level);
        if (!history.request) {
          notes.push(`${where}: ${history.reason}`);
          continue;
        }
        const result = await fetchCollectionPages<Observation>(history.request.resource, {
          params: history.request.params,
          pageSize: 500,
          maxPages: 2,
          signal,
        });
        const rows = normalizeObservationRows(source, result.items).filter((row) => row.geo_id === reading.area.geoId);
        const change = changeOverYear(rows);
        if (reading.measure === "food") {
          if (change.percent === null) {
            notes.push(`${where}: ${change.reason}`);
            continue;
          }
          return { reading, text: formatChange(change.percent), period: change.period, notes };
        }
        const spec = PRICE_ITEMS.find((item) => item.item === reading.measure)!;
        const newest = [...rows].reverse().find((row) => row.value !== null && row.value !== undefined && row.value !== "");
        const value = newest ? Number(newest.value) : NaN;
        if (!newest || !Number.isFinite(value)) {
          notes.push(`${where}: ${rows.length ? withheldReason(rows.at(-1)) : "no value published for this area."}`);
          continue;
        }
        return {
          reading,
          text: formatPrice(value, spec),
          period: observationPeriodLabel(newest),
          change: change.percent === null ? "" : formatChange(change.percent),
          rows,
          notes,
        };
      }
      const { resource, params } = buildNewestValueRequest(source, { metricCode: reading.metricCode, geoId: reading.area.geoId });
      const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params, signal });
      const rows: ObservationRow[] = normalizeObservationRows(source, Array.isArray(payload.items) ? payload.items : [])
        .filter((row) => row.geo_id === reading.area.geoId);
      const row = rows.at(-1);
      if (!row) {
        notes.push(`${where}: no value published for this area.`);
        continue;
      }
      const value = row.value === null || row.value === undefined || row.value === "" ? null : Number(row.value);
      if (value === null || !Number.isFinite(value)) {
        notes.push(`${where}: ${withheldReason(row)}`);
        continue;
      }
      const text = reading.measure === "gas" ? formatGas(value) : reading.measure === "rent" ? formatRent(value) : formatParity(value);
      return { reading, text, period: observationPeriodLabel(row), notes };
    } catch (error) {
      if (signal.aborted) throw error;
      notes.push(error instanceof ApiError && error.status === 404 ? `${where}: not published.` : `${where}: ${apiErrorMessage(error)}`);
    }
  }
  return { reading: null, text: "", period: "", notes };
}

function Notes({ notes }: { notes: string[] }) {
  return notes.length ? (
    <ul className="subtle place-cost-notes">
      {notes.map((note) => <li key={note}>{note}</li>)}
    </ul>
  ) : null;
}

export default function GroceriesAndGas({
  level,
  place,
  related,
  sources,
}: {
  level: PlaceLevel;
  place: GeographySummary;
  related: RelatedResponse | null;
  sources: readonly ExplorerSource[];
}) {
  const [answers, setAnswers] = useState<Partial<Record<CostMeasure, Answer>>>({});

  useEffect(() => {
    if (!sources.length) return;
    const controller = new AbortController();
    const areas = containingAreas(level, place, related);
    const own: Area = { geoId: place.geo_id, level, name: String(place.geo_name || place.geo_id) };
    const readings = readingsFor(areas, own);
    setAnswers({});
    for (const measure of ORDER) {
      answer(readings[measure], sources, controller.signal)
        .then((result) => {
          if (!controller.signal.aborted) setAnswers((current) => ({ ...current, [measure]: result }));
        })
        .catch(() => undefined);
    }
    return () => controller.abort();
  }, [level, place, related, sources]);

  // The area most items answered for, stated once above the table; an item
  // answered by another area says so on its own row.
  const answered = ITEMS.map((item) => answers[item]?.reading?.area).filter(Boolean);
  const common = answered.length
    ? answered.reduce((best, area) => (answered.filter((a) => a!.geoId === area!.geoId).length > answered.filter((a) => a!.geoId === best!.geoId).length ? area : best))
    : null;

  return (
    <section className="analysis-panel place-cost" aria-labelledby="place-cost-heading" data-testid="place-groceries-gas">
      <h2 id="place-cost-heading">Cost of living</h2>
      {level !== "NATIONAL" ? <p className="subtle" data-testid="place-cost-no-local">{level === "METRO" ? NO_METRO_PRICES : NO_LOCAL_PRICES}</p> : null}

      <table className="place-cost-items" data-testid="place-cost-items">
        <caption>
          Everyday prices
          {common ? (
            <span className="subtle" data-testid="place-cost-items-area">
              {" "}· {areaLabel(common, place.geo_id)}, urban average. Source: {SOURCE_NAMES.BLS}, average prices.
            </span>
          ) : null}
        </caption>
        <thead>
          <tr>
            <th scope="col">Item</th>
            <th scope="col">Price</th>
            <th scope="col">Over the year</th>
            <th scope="col">Trend</th>
          </tr>
        </thead>
        <tbody>
          {PRICE_ITEMS.map((spec) => {
            const result = answers[spec.item];
            const other = result?.reading && common && result.reading.area.geoId !== common.geoId;
            return (
              <tr key={spec.item} data-testid={`place-cost-${spec.item}`} data-area={result?.reading?.area.geoId || ""}>
                <th scope="row">{spec.title}</th>
                <td>
                  {!result ? (
                    "Reading…"
                  ) : result.reading ? (
                    <>
                      <strong>{result.text}</strong>
                      <span className="subtle block">
                        {result.period}
                        {other ? ` · ${areaLabel(result.reading.area, place.geo_id)}` : ""}
                      </span>
                    </>
                  ) : (
                    <span>Not loaded into this site yet.</span>
                  )}
                </td>
                <td>{result?.change || ""}</td>
                <td>{result?.rows?.length ? <PlaceSparkline rows={result.rows} testId={`place-cost-${spec.item}-spark`} name={spec.title} /> : null}</td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <dl className="place-cost-lines">
        {LINES.map((measure) => {
          const result = answers[measure];
          return (
            <div key={measure} data-testid={`place-cost-${measure}`} data-area={result?.reading?.area.geoId || ""}>
              <dt>{TITLES[measure]}</dt>
              <dd>
                {!result ? (
                  "Reading…"
                ) : result.reading ? (
                  <>
                    <strong>{result.text}</strong>
                    <span className="subtle">
                      {" "}· {areaLabel(result.reading.area, place.geo_id)}, {result.period}. Source: {SOURCE_NAMES[result.reading.source]}.
                    </span>
                  </>
                ) : measure === "parity" && level === "NATIONAL" ? (
                  <span>100 by definition: BEA measures every area&apos;s price level against the nation&apos;s.</span>
                ) : (
                  <span>Not published for this place or any area containing it.</span>
                )}
                {result?.reading && measure === "food" ? (
                  <span className="subtle block"> Each area&apos;s index has its own base, so only its change over a year is shown.</span>
                ) : null}
                {result?.reading && measure === "rent" ? (
                  <span className="subtle block"> Rent plus the utilities the renter pays, from the five-year survey.</span>
                ) : null}
                {result ? <Notes notes={result.notes} /> : null}
              </dd>
            </div>
          );
        })}
      </dl>
    </section>
  );
}
