"use client";

// The "Groceries and gas" card on a county, state or nation page
// (groceries-and-gas-cards). Every number names the area it describes and
// its source; the rules for choosing the area are `lib/groceriesAndGas.ts`.

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
  SOURCE_NAMES,
  areaLabel,
  changeOverYear,
  containingAreas,
  formatChange,
  formatGas,
  formatParity,
  readingsFor,
  withheldReason,
} from "../lib/groceriesAndGas";
import type { CostMeasure, Reading } from "../lib/groceriesAndGas";

interface Answer {
  reading: Reading | null;
  text: string;
  period: string;
  /** Why no more local area answered, or why nothing did. */
  notes: string[];
}

const TITLES: Record<CostMeasure, string> = {
  gas: "Regular gasoline",
  food: "Groceries (food at home)",
  parity: "Overall price level",
};

const ORDER: CostMeasure[] = ["gas", "food", "parity"];

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
      if (reading.measure === "food") {
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
        if (change.percent === null) {
          notes.push(`${where}: ${change.reason}`);
          continue;
        }
        return { reading, text: formatChange(change.percent), period: change.period, notes };
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
      return {
        reading,
        text: reading.measure === "gas" ? formatGas(value) : formatParity(value),
        period: observationPeriodLabel(row),
        notes,
      };
    } catch (error) {
      if (signal.aborted) throw error;
      notes.push(error instanceof ApiError && error.status === 404 ? `${where}: not published.` : `${where}: ${apiErrorMessage(error)}`);
    }
  }
  return { reading: null, text: "", period: "", notes };
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
    const readings = readingsFor(containingAreas(level, place, related));
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

  return (
    <section className="analysis-panel place-cost" aria-labelledby="place-cost-heading" data-testid="place-groceries-gas">
      <h2 id="place-cost-heading">Groceries and gas</h2>
      {level !== "NATIONAL" ? <p className="subtle" data-testid="place-cost-no-local">{NO_LOCAL_PRICES}</p> : null}
      <dl className="place-cost-lines">
        {ORDER.map((measure) => {
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
                {result?.notes.length ? (
                  <ul className="subtle place-cost-notes">
                    {result.notes.map((note) => <li key={note}>{note}</li>)}
                  </ul>
                ) : null}
              </dd>
            </div>
          );
        })}
      </dl>
    </section>
  );
}
