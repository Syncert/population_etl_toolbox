"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ArrowRight, MapPin } from "lucide-react";
import MiniLineChart from "../../components/MiniLineChart";
import SourceNote from "../../components/SourceNote";
import { getGeographies, getTimeseries } from "../../lib/api/client";
import { explorerHref } from "../../lib/urlState";

const populationMetric = "ACS:acs5:B01003_001";

async function getItems(request) {
  const payload = await request;
  return payload.items || [];
}

export default function ProfilesPage() {
  const [states, setStates] = useState([]);
  const [counties, setCounties] = useState([]);
  const [stateFips, setStateFips] = useState("55");
  const [geoId, setGeoId] = useState("");
  const [history, setHistory] = useState([]);
  const [status, setStatus] = useState("loading");

  useEffect(() => {
    getItems(getGeographies({ geo_level: "STATE", limit: "100" })).then(setStates).catch(() => setStatus("error"));
  }, []);

  useEffect(() => {
    setStatus("loading");
    getItems(getGeographies({ geo_level: "COUNTY", state_fips: stateFips, limit: "1000" })).then((items) => {
      setCounties(items);
      setGeoId((current) => items.some((item) => item.geo_id === current) ? current : items[0]?.geo_id || "");
    }).catch(() => setStatus("error"));
  }, [stateFips]);

  useEffect(() => {
    if (!geoId) return;
    setStatus("loading");
    getItems(getTimeseries({ metric_code: populationMetric, geo_id: geoId, limit: "1000" })).then((items) => {
      setHistory(items);
      setStatus("ready");
    }).catch(() => { setHistory([]); setStatus("error"); });
  }, [geoId]);

  const county = useMemo(() => counties.find((item) => item.geo_id === geoId), [counties, geoId]);
  const latest = history.at(-1);
  const first = history[0];
  const change = latest && first && Number(first.value) ? ((Number(latest.value) / Number(first.value)) - 1) * 100 : null;

  return (
    <main className="page-shell compact-page">
      <header className="page-heading"><div className="section-kicker">Geography profile</div><h1>{county?.county_name || "County profile"}{county?.state_name ? `, ${county.state_name}` : ""}</h1><p>A reusable local profile with a traceable population series and direct path back to the explorer.</p></header>
      <section className="profile-controls">
        <label>State<select value={stateFips} onChange={(event) => setStateFips(event.target.value)}>{states.map((state) => <option value={state.state_fips} key={state.geo_id}>{state.state_name}</option>)}</select></label>
        <label>County<select value={geoId} onChange={(event) => setGeoId(event.target.value)}>{counties.map((item) => <option value={item.geo_id} key={item.geo_id}>{item.county_name}</option>)}</select></label>
        <Link className="button secondary" href={explorerHref({ metric: populationMetric, geoId, stateFips })}>Open in Explorer <ArrowRight size={15} /></Link>
      </section>
      {status === "error" ? <div className="notice error">This profile could not load its observations.</div> : null}
      <section className="profile-stats">
        <div><MapPin size={18} /><span>Latest population</span><strong>{latest?.value == null ? "-" : Number(latest.value).toLocaleString()}</strong><small>{latest?.period || latest?.observation_date || "Latest available"}</small></div>
        <div><span>Change across series</span><strong>{change == null ? "-" : `${change >= 0 ? "+" : ""}${change.toFixed(1)}%`}</strong><small>{first?.period || first?.observation_date || "-"} to {latest?.period || latest?.observation_date || "-"}</small></div>
        <div><span>Margin of error</span><strong>{latest?.margin_of_error == null ? "Not reported" : `+/- ${Number(latest.margin_of_error).toLocaleString()}`}</strong><small>ACS uncertainty context</small></div>
      </section>
      <section className="analysis-panel"><div className="panel-heading"><div><div className="section-kicker">Trend</div><h2>Total population</h2></div><span>{status === "loading" ? "Loading..." : `${history.length} observations`}</span></div><MiniLineChart items={history} label={`${county?.county_name || "County"} population`} /></section>
      <SourceNote source="U.S. Census Bureau" dataset="ACS 5-year" metric="B01003_001 - Total population" geography={county ? `${county.county_name}, ${county.state_name}` : "County"} period={first && latest ? `${first.period || first.observation_date} to ${latest.period || latest.observation_date}` : "Latest available"} updatedAt={latest?.updated_at} caveats="ACS 5-year estimates combine five years of survey responses. Use the reported margin of error when comparing small differences." />
    </main>
  );
}
