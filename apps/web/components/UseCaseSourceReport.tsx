"use client";

import { useEffect, useMemo, useState } from "react";
import { apiErrorMessage, buildApiPath, fetchCollectionPages } from "../lib/api/client";
import type { GeographySummary, Observation } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ResolvedMeasure } from "../lib/productTemplates";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, observationUnit } from "../lib/explorerViewModel";
import { buildUseCaseHistory, verifyUseCaseRows } from "../lib/useCaseAnalysis";
import { normalizeObservationRows, observationPeriodLabel, observationUncertaintyLabel, OBSERVATION_COVERAGE_FIELDS, observationCoverageValue } from "../lib/observationAccess";
import { explorerHref } from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";

/** Read each health/safety stratum as published, without choosing a winner. */
export default function UseCaseSourceReport({ sectionId, measures, sources, geoId, geoLevel, placeName, state }: {
  sectionId: string; measures: ResolvedMeasure[]; sources: ExplorerSource[];
  geoId: string; geoLevel: GeoLevel; placeName: string; state?: GeographySummary;
}) {
  const [scope, setScope] = useState("local");
  const [chosen, setChosen] = useState("");
  const [loaded, setLoaded] = useState<ObservationRow[]>([]);
  const [status, setStatus] = useState("Choose a place to read reports.");
  const reportGeo = scope === "state" ? state?.geo_id || "" : geoId;
  const reportGrain = scope === "state" ? "STATE" : geoLevel;
  const metric = measures.find((item) => item.metricCode === chosen)?.metric || measures.find((item) => item.metric?.valid_geo_grains?.includes(reportGrain))?.metric || measures[0]?.metric || null;
  const source = sources.find((item) => item.sourceCode === metric?.source_code) || null;
  const reading = useMemo(() => buildUseCaseHistory(source, metric, reportGeo, reportGrain), [source, metric, reportGeo, reportGrain]);
  const rows = loaded.filter((row) => row.metric_code === metric?.metric_code && row.geo_id === reportGeo);
  useEffect(() => { setScope("local"); setChosen(""); }, [geoId]);
  useEffect(() => {
    setLoaded([]);
    if (!reading.request) { setStatus(reading.reason); return; }
    const controller = new AbortController();
    setStatus("Reading published reports…");
    fetchCollectionPages<Observation>(reading.request.resource, { params: reading.request.params, pageSize: 500, maxPages: 4, signal: controller.signal }).then((result) => {
      if (controller.signal.aborted || !source) return;
      const normalized = normalizeObservationRows(source, result.items);
      verifyUseCaseRows(normalized, metric?.metric_code || "", reportGeo);
      setLoaded(normalized);
      setStatus(`${normalized.length} published report rows${result.complete ? "" : `; partial read of ${result.total ?? "unknown"} rows`}. Every stratum remains separate.`);
    }).catch((error) => { if (!controller.signal.aborted) setStatus(apiErrorMessage(error)); });
    return () => controller.abort();
  }, [reading, source, metric, reportGeo]);

  return <div className="use-case-source-report" data-testid={`source-report-${sectionId}`}>
    <h3>Published {metric?.source_code === "CDC" ? "CDC health" : "FBI safety"} reports</h3>
    <div className="use-case-tool-row">
      <button type="button" className="button secondary" aria-pressed={scope === "local"} onClick={() => { setScope("local"); setChosen(""); }}>Selected place</button>
      {state && geoLevel !== "STATE" ? <button type="button" className="button secondary" aria-pressed={scope === "state"} onClick={() => { setScope("state"); setChosen(""); }}>Load {String(state.state_name || state.geo_name || state.geo_id)} state report</button> : null}
      <label>Report measure<select aria-label={`Report measure for ${sectionId}`} value={metric?.metric_code || ""} onChange={(event) => setChosen(event.target.value)}><option value="" disabled>No published measure</option>{measures.map((item) => <option key={item.slot.id} value={item.metricCode}>{item.slot.label}</option>)}</select></label>
    </div>
    <p><strong>{scope === "state" ? `State context: ${state?.state_name || reportGeo}; these are not ${placeName || "local"} figures.` : `Selected place: ${placeName || reportGeo || "none"}.`}</strong><br />{metric?.metric_code}</p>
    <p role="status">{status}</p>
    {rows.length ? <div className="table-wrap use-case-table"><table><caption>{metric?.source_code} report for {reportGeo}. First 30 rows shown; the source explorer exposes the complete publication.</caption><thead><tr><th>Period</th><th>Value / unit</th><th>Published status</th><th>Stratum / reporting subject</th><th>Uncertainty / participation</th></tr></thead><tbody>{rows.slice(0, 30).map((row, index) => <tr key={index}><td>{observationPeriodLabel(row)}</td><td>{row.value == null ? "Value not published" : formatObservationValue(row.value)} {observationUnit(row)}</td><td>{String(row.value_status || "Not published")}</td><td>{JSON.stringify(row.dimensions || {})}</td><td>{observationUncertaintyLabel(row) || "Uncertainty not published"}<br />{OBSERVATION_COVERAGE_FIELDS.map((field) => { const value = observationCoverageValue(row, field); return value ? `${field}: ${value}` : ""; }).filter(Boolean).join(" · ") || "Coverage not published"}</td></tr>)}</tbody></table></div> : null}
    {reading.request ? <a className="text-link" href={buildApiPath(reading.request.resource, reading.request.params)}>Reproduce this report read</a> : null}
    <a className="text-link" href={explorerHref({ source: source?.key, metric: metric?.metric_code, geoId: reportGeo, geoLevel: reportGrain, stateFips: state?.state_fips || undefined })} target="_blank" rel="noopener noreferrer">Open report in source explorer</a>
  </div>;
}
