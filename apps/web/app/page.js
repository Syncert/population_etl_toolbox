"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ArrowRight, BarChart3, BookOpen, Database, Map } from "lucide-react";
import { getSources, searchMetrics } from "../lib/api/client";
import { displayMetricName } from "../lib/format";
import { connectedSourcesBand } from "../lib/catalog";
import { explorerHref } from "../lib/urlState";

export default function HomePage() {
  const [sources, setSources] = useState([]);
  const [metrics, setMetrics] = useState({ total: 0, items: [] });
  const [status, setStatus] = useState("loading");

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      getSources(),
      searchMetrics({ active_only: "true", q: "population", limit: "6" }),
    ]).then(([sourceItems, metricPayload]) => {
      if (!cancelled) {
        setSources(sourceItems);
        setMetrics(metricPayload);
        setStatus("ready");
      }
    }).catch(() => {
      if (!cancelled) setStatus("error");
    });
    return () => { cancelled = true; };
  }, []);

  const featuredMetric = useMemo(
    () => metrics.items.find((item) => item.metric_code === "CENSUS_ACS:acs5:B01003_001") || metrics.items[0],
    [metrics],
  );
  const sourceBand = useMemo(() => connectedSourcesBand(status, sources), [status, sources]);

  return (
    <main className="page-shell home-page">
      <section className="home-intro">
        <div className="section-kicker">Public economic intelligence</div>
        <h1>Economic Data Studio</h1>
        {/* Not an enumeration: the published list is the band below,
            and this sentence named three sources while the API served
            seven (WEB-080). */}
        <p>Explore published federal statistics with every metric, map, and chart tied back to its source.</p>
        <div className="command-row">
          <Link className="button primary" href="/explore">Open the explorer <ArrowRight size={16} /></Link>
          <Link className="button secondary" href="/catalog">Browse the catalog</Link>
        </div>
      </section>

      <section className="signal-strip" aria-label="Platform signals">
        <div><strong>{status === "ready" ? sources.length : "-"}</strong><span>connected sources</span></div>
        <div><strong>{status === "ready" ? metrics.total.toLocaleString() : "-"}</strong><span>population matches</span></div>
        <div><strong>County</strong><span>national map coverage</span></div>
        <div><strong>Live</strong><span>API-backed observations</span></div>
      </section>

      {/* Always present so the first render is the baseline; a failure that
          arrives afterwards is announced rather than only shown (WEB-037). */}
      <div className="status-row" role="status" data-testid="home-status">
        {status === "error" ? (
          <div className="notice error">Live catalog data is temporarily unavailable.</div>
        ) : null}
      </div>

      <section className="home-grid">
        <article className="feature-story">
          <div className="section-kicker">Featured analysis</div>
          <h2>Population concentration is best read county by county</h2>
          <p>Use the national county explorer to see the latest estimate, inspect uncertainty, and pin any county for its historical series.</p>
          <Link className="text-link" href="/articles">Read the analysis <BookOpen size={15} /></Link>
        </article>
        <article className="snapshot-panel">
          <div className="snapshot-icon"><Map aria-hidden="true" /></div>
          <div>
            <div className="section-kicker">National snapshot</div>
            <h2>{featuredMetric ? displayMetricName(featuredMetric) : "County population estimates"}</h2>
            <p>Latest source-backed metric metadata and observation coverage.</p>
            <Link className="text-link" href={explorerHref({
              metric: featuredMetric?.metric_code || "CENSUS_ACS:acs5:B01003_001",
              source: featuredMetric?.source_code || "CENSUS_ACS",
            })}>Open map <ArrowRight size={15} /></Link>
          </div>
        </article>
      </section>

      <section className="path-grid" aria-label="Primary workflows">
        <Link href="/catalog"><Database /><strong>Catalog</strong><span>Find metrics and inspect provenance.</span></Link>
        <Link href="/explore"><BarChart3 /><strong>Explore</strong><span>Map, compare, and save a view.</span></Link>
        <Link href="/builder"><BookOpen /><strong>Compose</strong><span>Build a page from reusable analysis.</span></Link>
      </section>

      <section className="source-band">
        <div><div className="section-kicker">Connected sources</div><h2>Public data with its identity intact</h2></div>
        <div className="source-list" data-testid="home-source-list">
          {/* Named only where the API named them: a list from this page
              would read as a fact about the warehouse (WEB-080). */}
          {sourceBand.names.length > 0 ? (
            sourceBand.names.map((name) => <span key={name}>{name}</span>)
          ) : (
            <span className="subtle">{sourceBand.message}</span>
          )}
        </div>
      </section>
    </main>
  );
}
