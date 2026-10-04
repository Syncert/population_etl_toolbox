"use client";

import { useEffect, useRef, useState } from "react";
import TimeSeriesChart from "./TimeSeriesChart";
import { apiFetch, apiErrorMessage, buildApiPath } from "../lib/api/client";
import type { ObservationRow } from "../lib/explorerViewModel";
import type { ResolvedMeasure } from "../lib/productTemplates";
import { formatNumber } from "../lib/format";

type Scenario = { derived: true; model: string; base: ObservationRow; annual_change_percent: number; horizon_years: number; formula: string; caveats: string[]; items: { year: number; value: number }[] };

export default function PopulationScenario({ measures, geoId }: { measures: ResolvedMeasure[]; geoId: string }) {
  const metricCode = measures.find((item) => item.metricCode === "CENSUS_PEP:POPESTIMATE")?.metricCode || measures.find((item) => /CENSUS_ACS:acs[15]:B01003_001$/.test(item.metricCode))?.metricCode || "";
  const [rate, setRate] = useState("1");
  const [horizon, setHorizon] = useState("10");
  const [result, setResult] = useState<Scenario | null>(null);
  const [status, setStatus] = useState("Enter an annual-change assumption and run a scenario.");
  const [busy, setBusy] = useState(false);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    controller.current?.abort(); setResult(null); setBusy(false);
    setStatus("Enter an annual-change assumption and run a scenario.");
    return () => controller.current?.abort();
  }, [geoId, metricCode, rate, horizon]);
  const params = { metric_code: metricCode, geo_id: geoId, annual_change_percent: Number(rate), horizon_years: Number(horizon) };
  const query = buildApiPath("/population/scenario", params);
  async function run() {
    controller.current?.abort();
    const request = new AbortController(); controller.current = request;
    setBusy(true); setResult(null); setStatus("Reading the published baseline and calculating the API scenario…");
    try {
      const response = await apiFetch<Scenario>("/population/scenario", { params, signal: request.signal });
      if (request.signal.aborted) return;
      if (response.derived !== true || response.base.metric_code !== metricCode || response.base.geo_id !== geoId || response.annual_change_percent !== Number(rate) || response.horizon_years !== Number(horizon)) throw new Error("The scenario does not match the requested baseline and assumptions; the answer was refused.");
      setResult(response); setStatus(`${response.items.length} derived years after the published baseline. These are scenario values.`);
    } catch (error) { if (!request.signal.aborted) setStatus(apiErrorMessage(error)); }
    finally { if (!request.signal.aborted) setBusy(false); }
  }
  function exportScenario() {
    if (!result) return;
    const rows = [["year", "derived_population", "unit", "metric_code", "geo_id", "baseline_period", "baseline_release", "annual_change_percent", "horizon_years", "model", "api_query"], ...result.items.map((item) => [item.year, item.value, result.base.unit, result.base.metric_code, result.base.geo_id, result.base.period_end || result.base.period_start, result.base.release, result.annual_change_percent, result.horizon_years, result.model, query])];
    const csv = rows.map((row) => row.map((value) => `"${String(value ?? "").replaceAll('"', '""')}"`).join(",")).join("\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a"); link.href = url; link.download = "derived-population-scenario.csv"; link.click();
    // Revoked later, not synchronously: the download reads the blob after
    // click() returns, and an immediate revoke races it into an empty file.
    setTimeout(() => URL.revokeObjectURL(url), 10_000);
  }
  const drawable = result?.items.map((item) => ({ metric_code: metricCode, geo_id: geoId, value: String(item.value), unit: String(result.base.unit || "people"), period_start: String(item.year), period_end: String(item.year), value_status: "derived", release: result.model })) || [];
  return <section className="analysis-panel population-scenario" data-testid="population-scenario" aria-labelledby="population-scenario-title">
    <div className="section-kicker">Derived planning scenario</div><h2 id="population-scenario-title">Look ahead under an explicit assumption</h2>
    <p>The API projects from this place’s newest published population using a constant annual change you choose. This is a planning scenario, not an official forecast.</p>
    <div className="use-case-viz-controls"><label>Assumed annual population change (%)<input aria-label="Assumed annual population change (%)" type="number" min="-10" max="10" step="0.1" value={rate} onChange={(event) => setRate(event.target.value)} /></label><label>Years after baseline<input aria-label="Years after baseline" type="number" min="1" max="30" step="1" value={horizon} onChange={(event) => setHorizon(event.target.value)} /></label><button type="button" className="button primary" onClick={run} disabled={busy || !geoId || !metricCode || rate.trim() === "" || horizon.trim() === "" || !Number.isFinite(Number(rate)) || Math.abs(Number(rate)) > 10 || !Number.isInteger(Number(horizon)) || Number(horizon) < 1 || Number(horizon) > 30}>Run population scenario</button></div>
    <p role="status">{status}</p>
    {result ? <>
      <p><strong>Published baseline: {formatNumber(result.base.value)} {String(result.base.unit || "people")}</strong> · {String(result.base.period_start)}–{String(result.base.period_end || result.base.period_start)}<br />{result.base.metric_code} · {result.base.geo_id} · Release {String(result.base.release || "not published")}<br />Baseline uncertainty: {JSON.stringify(result.base.uncertainty || "Not published")}</p>
      <p><strong>Scenario assumption: {result.annual_change_percent}% per year for {result.horizon_years} years.</strong> Population × (1 + annual change / 100)<sup>years after baseline</sup>.</p>
      <TimeSeriesChart items={drawable} />
      <div className="table-wrap use-case-table"><table><caption>API-derived population scenario; every row is modeled, not observed.</caption><thead><tr><th>Year</th><th>Scenario population</th><th>Evidence type</th></tr></thead><tbody>{result.items.map((item) => <tr key={item.year}><td>{item.year}</td><td>{formatNumber(item.value)}</td><td>Derived scenario</td></tr>)}</tbody></table></div>
      <ul>{result.caveats.map((caveat) => <li key={caveat}>{caveat}</li>)}</ul>
      <div className="use-case-tool-row"><button type="button" className="button secondary" onClick={exportScenario}>Export derived scenario</button><a className="text-link" href={query}>Reproduce scenario in API</a></div>
    </> : null}
  </section>;
}
