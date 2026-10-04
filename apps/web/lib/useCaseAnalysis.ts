import type { MetricSummary } from "./api/types";
import type { ExplorerSource } from "./explorerSources";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import {
  buildHistoryObservationRequest, buildSettledHistoryRequest,
  describeStratification, seriesDimensionNames, observationPeriodLabel,
} from "./observationAccess";
import type { ObservationRequest, ObservationScope } from "./observationAccess";

/** Refuse a widened or mislabeled response before it can answer a card. */
export function verifyUseCaseRows(rows: ObservationRow[], metricCode: string, geoId: string) {
  if (rows.some((row) => row.metric_code !== metricCode)) throw new Error("The source returned a different metric from the one requested; this answer was refused.");
  if (rows.some((row) => row.geo_id !== geoId)) throw new Error("The source returned a different geography from the one requested; this answer was refused.");
}

export function buildUseCaseHistory(
  source: ExplorerSource | null, metric: MetricSummary | null,
  geoId: string, geoLevel: string,
): { request: ObservationRequest | null; reason: string } {
  if (!geoId) return { request: null, reason: "Choose a place to read its published history." };
  if (!source || !metric) return { request: null, reason: "Select a published measure with a declared observation route." };
  const grains = metric.valid_geo_grains;
  if (Array.isArray(grains) && !grains.includes(geoLevel)) {
    return { request: null, reason: `This measure is not published at ${geoLevel}. Choose one of its published grains: ${grains.join(", ") || "none (numeric values withheld)"}.` };
  }
  if (source.accessShape === "neutral" && !source.neutralFilters.includes("geo_id")) {
    return { request: null, reason: "This source does not declare the geo_id filter; a place request cannot be sent safely." };
  }
  const query = { metricCode: metric.metric_code || "", geoId, limit: 500 };
  const request = buildSettledHistoryRequest(source, query) || buildHistoryObservationRequest(source, query);
  return { request, reason: "" };
}

export function useCaseChartState(source: ExplorerSource | null, rows: ObservationRow[], scope: ObservationScope) {
  const stratification = describeStratification(rows, seriesDimensionNames(source, scope));
  const periods = rows.map((row) => observationPeriodLabel(row));
  const duplicatePeriods = new Set(periods).size !== periods.length;
  const numericRows = rows.filter((row) => publishedNumber(row.value) !== null);
  const reason = stratification.stratified || duplicatePeriods
    ? "These rows describe separate series or releases. They remain in the table; open the source explorer to pin dimensions before drawing a single trend."
    : numericRows.length === 0 ? "No numeric observations were published for this selection." : "";
  return { drawable: !reason, reason, numericRows };
}
