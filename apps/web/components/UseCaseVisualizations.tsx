"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { Download, ExternalLink, LineChart, Table2 } from "lucide-react";
import TimeSeriesChart from "./TimeSeriesChart";
import UseCasePeers from "./UseCasePeers";
import StatusPill from "./StatusPill";
import { apiErrorMessage, buildApiPath, fetchCollectionPages } from "../lib/api/client";
import type { GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName } from "../lib/format";
import { buildUseCaseHistory, useCaseChartState, verifyUseCaseRows } from "../lib/useCaseAnalysis";
import {
  describeHistoryLoad, normalizeObservationRows, observationPeriodLabel,
  observationUncertaintyLabel, observationCoverageValue, OBSERVATION_COVERAGE_FIELDS,
} from "../lib/observationAccess";
import { observationExport } from "../lib/observationExport";
import { explorerHref, workbenchHref, comparisonHref } from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";
import type { UseCasePage } from "../lib/useCasePages";
import type { ResolvedMeasure } from "../lib/productTemplates";

export default function UseCaseVisualizations({ entry, measures, sources, geoId, geoLevel, stateFips, places }: {
  entry: UseCasePage; measures: ResolvedMeasure[]; sources: ExplorerSource[];
  geoId: string; geoLevel: GeoLevel; stateFips: string;
  places: GeographySummary[];
}) {
  const [chosen, setChosen] = useState("");
  const [view, setView] = useState("trend");
  const [loadedRows, setRows] = useState<ObservationRow[]>([]);
  const [status, setStatus] = useState({ state: "idle", message: "Choose a place and measure." });
  const measure = measures.find((item) => item.metricCode === chosen) || measures.find((item) => !Array.isArray(item.metric?.valid_geo_grains) || item.metric.valid_geo_grains.includes(geoLevel)) || measures[0];
  const metric: MetricSummary | null = measure?.metric || null;
  const rows = loadedRows.filter((row) => row.metric_code === metric?.metric_code && row.geo_id === geoId);
  const source = sources.find((item) => item.sourceCode === metric?.source_code) || null;
  const reading = useMemo(() => buildUseCaseHistory(source, metric, geoId, geoLevel), [source, metric, geoId, geoLevel]);
  const scope = reading.request?.params.scope === "as_released" ? "as_released" : "latest";
  const chart = useCaseChartState(source, rows, scope);
  const requestUrl = reading.request ? buildApiPath(reading.request.resource, reading.request.params) : "";

  useEffect(() => {
    const controller = new AbortController();
    setRows([]);
    if (!reading.request) {
      setStatus({ state: "idle", message: reading.reason });
      return () => controller.abort();
    }
    setStatus({ state: "loading", message: "Reading published history…" });
    const { resource, params } = reading.request;
    fetchCollectionPages<Observation>(resource, { params, pageSize: 500, maxPages: 4, signal: controller.signal })
      .then((result) => {
        if (controller.signal.aborted || !source) return;
        const normalized = normalizeObservationRows(source, result.items);
        verifyUseCaseRows(normalized, metric?.metric_code || "", geoId);
        setRows(normalized);
        setStatus({ state: result.complete ? "ok" : "warn", message: describeHistoryLoad(normalized.length, result.total, result.complete, scope === "as_released") });
      }).catch((error) => {
        if (!controller.signal.aborted) setStatus({ state: "bad", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, [reading, source, scope, metric, geoId]);

  function exportHistory() {
    const exported = observationExport(rows, { scope, dimensions: source?.publishedDimensions || [] });
    exported.headings.push("api_query", "read_status");
    const csv = [exported.headings, ...exported.rows.map((row) => [...row, requestUrl, status.message])]
      .map((row) => row.map((cell) => `"${String(cell ?? "").replaceAll('"', '""')}"`).join(",")).join("\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url; link.download = `${entry.id}-history.csv`; link.click(); URL.revokeObjectURL(url);
  }

  const explore = explorerHref({ source: source?.key, metric: metric?.metric_code, geoId: geoId || undefined, geoLevel, stateFips: stateFips || undefined });
  const compose = workbenchHref({ series: metric && source && geoId ? [{ sourceKey: source.key, metricCode: metric.metric_code || "", geoId, geoLevel, scope: "latest" }] : [] });
  const secondMetric = measures.find((item) => item.metricCode !== metric?.metric_code);
  const compare = comparisonHref({ metricA: metric?.metric_code, metricB: secondMetric?.metricCode, geoLevel, stateFips: stateFips || undefined });

  return (
    <section className="analysis-panel use-case-viz" aria-labelledby="use-case-viz-title" data-testid="use-case-visualizations">
      <div className="use-case-viz-heading"><div><div className="section-kicker">From indicators to evidence</div><h2 id="use-case-viz-title">Explore a published history</h2><p className="subtle">One measure, one place, its own units. Use the source explorer for additional metrics, strata, and release choices.</p></div><span className="use-case-live">Catalog-backed</span></div>
      <div className="use-case-viz-controls">
        <label>Measure<select value={metric?.metric_code || ""} onChange={(event) => setChosen(event.target.value)} data-testid="use-case-measure">
          {!measures.length ? <option value="">No published candidates</option> : null}
          {measures.map((item) => <option value={item.metricCode} key={item.slot.id}>{item.slot.label} · {item.metric?.source_code}</option>)}
        </select></label>
        <div className="use-case-view-switch" role="group" aria-label="Evidence view">
          <button type="button" aria-pressed={view === "trend"} onClick={() => setView("trend")}><LineChart size={15} />Trend</button>
          <button type="button" aria-pressed={view === "table"} onClick={() => setView("table")}><Table2 size={15} />Table</button>
          {entry.tool === "comparison" ? <button type="button" aria-pressed={view === "peers"} onClick={() => setView("peers")}>Peers</button> : null}
        </div>
        <button className="button secondary" type="button" onClick={exportHistory} disabled={!rows.length}><Download size={15} />Export history</button>
      </div>
      <div role="status"><StatusPill state={status.state} label="History" message={status.message} testId="use-case-history-status" /></div>
      {metric ? <p className="use-case-viz-context">{displayMetricName(metric)} · {metric.metric_code}<br />Source: {metric.source_code} · Unit: {[...new Set(rows.map(observationUnit).filter(Boolean))].join(", ") || metric.units || "Not published"} · Geography: {geoId || "not selected"} ({geoLevel}) · {scope === "as_released" ? "Newest published release per period; revised history" : "Latest source publication"}</p> : null}
      {view === "peers" ? <UseCasePeers source={source} metric={metric} geoId={geoId} places={places} unavailableReason={reading.reason} /> : null}
      {view === "trend" && chart.drawable ? <TimeSeriesChart items={rows} publishesValueStatus={source?.publishesValueStatus !== false} /> : null}
      {rows.length > 0 && !chart.drawable ? <p className="coverage-note partial">{chart.reason}</p> : null}
      {view === "table" || (view === "trend" && !chart.drawable) ? <div className="table-wrap use-case-table"><table>
        <caption>Published observations, including unavailable values. {rows.length > 200 ? "First 200 loaded rows shown; export includes every loaded row." : ""}</caption>
        <thead><tr><th scope="col">Period</th><th scope="col">Value / unit</th><th scope="col">Status</th><th scope="col">Uncertainty / coverage</th><th scope="col">Release / dimensions</th></tr></thead>
        <tbody>{rows.slice(0, 200).map((row, index) => <tr key={index}>
          <td>{observationPeriodLabel(row) || "Not published"}</td>
          <td>{row.value == null ? "Not published" : formatObservationValue(row.value)} {observationUnit(row)}</td>
          <td>{String(row.value_status || "Not published")}</td>
          <td>{observationUncertaintyLabel(row) || "Uncertainty not published"}<br />{OBSERVATION_COVERAGE_FIELDS.map((field) => { const value = observationCoverageValue(row, field); return value ? `${field}: ${value}` : ""; }).filter(Boolean).join(" · ") || "Coverage not published"}</td>
          <td>{String(row.release || row.as_of || "Not published")}<br />{JSON.stringify(row.dimensions || {})}</td>
        </tr>)}</tbody>
      </table>{!rows.length ? <p className="empty-state">{status.state === "bad" ? "The source could not be read. See the error above." : reading.reason || "No observations loaded for this selection."}</p> : null}</div> : null}
      <div className="use-case-tool-row">
        <Link className="button secondary" href={explore} target="_blank" rel="noopener noreferrer">Open map & source explorer <ExternalLink size={14} /></Link>
        <Link className="button secondary" href={compose}>Compose & save chart</Link>
        {entry.tool === "comparison" ? <Link className="button secondary" href={compare}>Compare measures & peers</Link> : null}
      </div>
      {requestUrl ? <details className="use-case-query"><summary>Reproduce this read</summary><a href={requestUrl}>{requestUrl}</a><p>Rows are bounded to four pages of 500. A partial read is labeled above and in the exported file.</p></details> : null}
    </section>
  );
}
