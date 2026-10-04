"use client";

import { useEffect, useMemo, useState } from "react";
import { Download } from "lucide-react";
import BarChart from "./BarChart";
import type { BarDatum } from "./BarChart";
import { apiFetch, apiErrorMessage, buildApiPath } from "../lib/api/client";
import type { CollectionResponse, GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { publishedNumber, formatObservationValue, observationUnit } from "../lib/explorerViewModel";
import { buildNewestValueRequest, normalizeObservationRows, observationPeriodLabel, observationUncertaintyLabel } from "../lib/observationAccess";
import { observationExport } from "../lib/observationExport";
import { verifyUseCaseRows } from "../lib/useCaseAnalysis";

export default function UseCasePeers({ source, metric, places, geoId, unavailableReason = "" }: {
  source: ExplorerSource | null; metric: MetricSummary | null; places: GeographySummary[]; geoId: string;
  unavailableReason?: string;
}) {
  const [chosen, setChosen] = useState<string[]>([]);
  const [rationale, setRationale] = useState("");
  const [answers, setAnswers] = useState<{ geoId: string; row: ObservationRow | null; message: string; query: string }[]>([]);
  const [status, setStatus] = useState("");
  const selected = useMemo(() => [...new Set([geoId, ...chosen].filter(Boolean))].slice(0, 6), [geoId, chosen]);
  const reason = unavailableReason || (!source?.supportsNewestPerGeography
    ? "This source does not declare an aligned value per geography. Its strata, domains, or reporting subjects stay in the source explorer; they are not combined into peer bars."
    : !source.neutralFilters.includes("geo_id") ? "This source does not declare the geo_id filter for a peer request." : "");

  useEffect(() => {
    setChosen([]);
    setRationale("");
  }, [places, geoId]);

  useEffect(() => {
    setAnswers([]);
    if (reason || !source || !metric || !selected.length) return;
    const controller = new AbortController();
    setStatus("Reading the selected peers…");
    Promise.all(selected.map(async (peer) => {
      const request = buildNewestValueRequest(source, { metricCode: metric.metric_code || "", geoId: peer });
      try {
        const result = await apiFetch<CollectionResponse<Observation>>(request.resource, { params: request.params, signal: controller.signal });
        const rows = normalizeObservationRows(source, result.items);
        verifyUseCaseRows(rows, metric.metric_code || "", peer);
        return { geoId: peer, row: rows.length === 1 ? rows[0]! : null, query: buildApiPath(request.resource, request.params), message: rows.length > 1 ? "The API returned several rows; no peer value was chosen." : rows.length ? "" : "Not published for this peer." };
      } catch (error) { return { geoId: peer, row: null, query: buildApiPath(request.resource, request.params), message: apiErrorMessage(error) }; }
    })).then((items) => { if (!controller.signal.aborted) { setAnswers(items); setStatus(`${items.length} selected peers read independently; periods may differ.`); } });
    return () => controller.abort();
  }, [source, metric, selected, reason]);

  const bars: BarDatum[] = answers.flatMap((answer) => {
    const value = publishedNumber(answer.row?.value);
    if (value === null || !answer.row) return [];
    const place = places.find((item) => item.geo_id === answer.geoId);
    return [{ key: answer.geoId, category: String(place?.geo_name || place?.county_name || place?.state_name || answer.geoId), value, unit: observationUnit(answer.row), groupKey: "peers", groupLabel: metric?.metric_display_name || metric?.metric_code || "Measure", period: observationPeriodLabel(answer.row), release: String(answer.row.release || "") }];
  });

  function exportPeers() {
    const exported = observationExport(answers.map((answer) => answer.row || { geo_id: answer.geoId, metric_code: metric?.metric_code }), { scope: "latest", dimensions: source?.publishedDimensions || [] });
    const csv = [ [...exported.headings, "peer_criteria", "availability", "api_query"], ...exported.rows.map((row, index) => [...row, rationale || "Criteria not yet recorded", answers[index]?.message || "Published row", answers[index]?.query || ""]) ].map((row) => row.map((cell) => `"${String(cell ?? "").replaceAll('"', '""')}"`).join(",")).join("\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a"); link.href = url; link.download = "selected-peer-evidence.csv"; link.click(); URL.revokeObjectURL(url);
  }

  if (reason) return <p className="coverage-note partial">{reason}</p>;
  return <div className="use-case-peers" data-testid="use-case-peers">
    <div className="use-case-peer-controls">
      <label>Choose peers at this grain (up to five additional places)<select aria-label="Add a peer" value="" disabled={selected.length >= 6} onChange={(event) => { if (event.target.value) setChosen((items) => [...items, event.target.value]); }}><option value="">Select a peer…</option>{places.filter((place) => !selected.includes(place.geo_id || "")).map((place) => <option key={place.geo_id} value={place.geo_id}>{String(place.geo_name || place.county_name || place.state_name || place.geo_id)}</option>)}</select></label>
      <label>Why these peers?<input value={rationale} onChange={(event) => setRationale(event.target.value)} placeholder="State your comparison criteria" /></label>
    </div>
    <div className="use-case-peer-selection">{selected.map((peer) => <span key={peer}>{String(places.find((place) => place.geo_id === peer)?.geo_name || places.find((place) => place.geo_id === peer)?.county_name || places.find((place) => place.geo_id === peer)?.state_name || peer)}{peer !== geoId ? <button type="button" aria-label={`Remove peer ${peer}`} onClick={() => setChosen((items) => items.filter((item) => item !== peer))}>×</button> : null}</span>)}</div>
    <p className="subtle" role="status">{status}</p>
    {!rationale ? <p className="subtle">Record the peer-selection rationale before using this comparison as planning evidence.</p> : null}
    <BarChart bars={bars} orientation="geography" colorOf={() => "#0b6b57"} label={`${bars.length} explicitly selected peer values; ${metric?.metric_code}`} testId="use-case-peer-chart" unpublished={answers.length - bars.length} />
    <p className="subtle">Values retain their own periods and uncertainty. This comparison contains one published measure; no composite score or statistical significance is computed.</p>
    <div className="table-wrap use-case-table"><table><caption>Selected peer evidence</caption><thead><tr><th scope="col">Geography</th><th scope="col">Value / unit</th><th scope="col">Period</th><th scope="col">Uncertainty / availability</th><th scope="col">Request</th></tr></thead><tbody>{answers.map((answer) => <tr key={answer.geoId}><td>{answer.geoId}</td><td>{answer.row?.value == null ? "Not published" : formatObservationValue(answer.row.value)} {observationUnit(answer.row)}</td><td>{observationPeriodLabel(answer.row) || "Not published"}</td><td>{answer.message || observationUncertaintyLabel(answer.row) || String(answer.row?.value_status || "Not published")}</td><td><a href={answer.query}>Reproduce</a></td></tr>)}</tbody></table></div>
    <button type="button" className="button secondary" onClick={exportPeers} disabled={!answers.length}><Download size={15} />Export selected peers</button>
  </div>;
}
