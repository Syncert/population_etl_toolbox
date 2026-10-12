"use client";

// An explainer's worked example, read live: the explainer's example measure
// for the nation and, when the reader has looked at a place in this tab, for
// that place too (explainer-pages). The place comes from session state and is
// never written to the URL.

import { useEffect, useState } from "react";
import StatusPill from "./StatusPill";
import { apiErrorMessage, apiFetch, fetchAllPages, getCapabilities, getMetric } from "../lib/api/client";
import type { CollectionResponse, GeographySummary, Observation } from "../lib/api/types";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName } from "../lib/format";
import { lastPlaceRows } from "../lib/explainerExample";
import type { ExampleRow } from "../lib/explainerExample";
import { readLastPlace } from "../lib/lastPlace";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildNewestValueRequest,
  normalizeObservationRows,
  observationPeriodLabel,
} from "../lib/observationAccess";

export default function ExplainerExample({ metricCode }: { metricCode: string }) {
  const [rows, setRows] = useState<ExampleRow[]>([]);
  const [measureName, setMeasureName] = useState(metricCode);
  const [note, setNote] = useState("");
  const [status, setStatus] = useState({ state: "loading", message: "reading the published value" });

  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    (async () => {
      try {
        const [capabilities, metric, nations] = await Promise.all([
          getCapabilities({ signal }),
          getMetric(metricCode, { signal }),
          fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "NATIONAL" },
            signal,
          }),
        ]);
        const source = buildExplorerSources(capabilities.items).find((item) => item.sourceCode === metric.source_code) || null;
        setMeasureName(displayMetricName(metric));
        const plan = lastPlaceRows(readLastPlace(window.sessionStorage), nations[0] || null, metric.valid_geo_grains);
        setNote(plan.note);
        const answered: ExampleRow[] = [];
        for (const target of plan.targets) {
          if (!source || !target.geoId) {
            answered.push({ ...target, row: null, message: target.message || "No declared route for this source" });
            continue;
          }
          const { resource, params } = buildNewestValueRequest(source, { metricCode, geoId: target.geoId });
          const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params, signal });
          const normalized = normalizeObservationRows(source, payload.items || []).filter(
            (row) => row.metric_code === metricCode && row.geo_id === target.geoId,
          );
          const row: ObservationRow | null = normalized.at(-1) ?? null;
          answered.push({ ...target, row, message: row ? "" : "Not published for this place" });
        }
        if (signal.aborted) return;
        setRows(answered);
        setStatus({ state: "ok", message: "newest published values" });
      } catch (error) {
        if (!signal.aborted) setStatus({ state: "bad", message: apiErrorMessage(error) });
      }
    })();
    return () => controller.abort();
  }, [metricCode]);

  return (
    <div className="explainer-example" data-testid="explainer-example">
      <StatusPill state={status.state} label="Live example" message={status.message} testId="explainer-example-status" />
      {note ? <p className="subtle" data-testid="explainer-example-note">{note}</p> : null}
      {rows.length ? (
        <table className="place-card-table">
          <caption>{measureName}, newest published value</caption>
          <tbody>
            {rows.map((entry) => (
              <tr key={entry.key} data-testid={`explainer-example-${entry.key}`}>
                <th scope="row">{entry.name}</th>
                <td>
                  {entry.row && entry.row.value !== null && entry.row.value !== undefined
                    ? `${formatObservationValue(entry.row.value)} ${observationUnit(entry.row) === "value" ? "" : observationUnit(entry.row)}`.trim()
                    : entry.message || "Not published"}
                </td>
                <td className="subtle">{entry.row ? observationPeriodLabel(entry.row) : ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </div>
  );
}
