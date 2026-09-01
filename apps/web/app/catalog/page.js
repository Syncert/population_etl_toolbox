"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { ArrowRight, Database, Search } from "lucide-react";
import { getSources, searchMetrics } from "../../lib/api/client";
import { sourceFilterOptions } from "../../lib/catalog";
import { displayMetricName } from "../../lib/format";
import { explorerHref } from "../../lib/urlState";

export default function CatalogPage() {
  const [query, setQuery] = useState("");
  const [source, setSource] = useState("");
  const [sourceItems, setSourceItems] = useState([]);
  const [payload, setPayload] = useState({ total: 0, items: [] });
  const [status, setStatus] = useState("loading");

  useEffect(() => {
    const controller = new AbortController();
    getSources({ signal: controller.signal })
      .then((items) => setSourceItems(Array.isArray(items) ? items : []))
      .catch(() => {
        // The metric list still works without the source filter row.
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      setStatus("loading");
      try {
        setPayload(await searchMetrics(
          {
            active_only: "true",
            limit: "100",
            q: query.trim() || undefined,
            source_code: source || undefined,
          },
          { signal: controller.signal },
        ));
        setStatus("ready");
      } catch (error) {
        if (error.name !== "AbortError") setStatus("error");
      }
    }, 180);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [query, source]);

  const sources = useMemo(() => sourceFilterOptions(sourceItems), [sourceItems]);

  const groups = useMemo(() => {
    const values = new Map();
    for (const metric of payload.items) {
      const parts = metric.metric_code.split(":");
      const key = `${metric.source_code}:${parts[1] || metric.source_object_type}`;
      if (!values.has(key)) values.set(key, { key, source: metric.source_code, dataset: parts[1] || metric.source_object_type, metrics: [] });
      values.get(key).metrics.push(metric);
    }
    return [...values.values()];
  }, [payload.items]);

  return (
    <main className="page-shell compact-page">
      <header className="page-heading">
        <div className="section-kicker">Trust layer</div>
        <h1>Data Catalog</h1>
        <p>Discover public metrics, understand their analytical shape, and launch directly into a reproducible view.</p>
      </header>

      <section className="catalog-toolbar" aria-label="Catalog filters">
        <label className="search-field"><Search size={17} /><span className="sr-only">Search metrics</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search metrics, variables, or definitions" /></label>
        <div className="segmented-control" aria-label="Source filter">
          {sources.map((item) => <button className={source === item.value ? "selected" : ""} type="button" onClick={() => setSource(item.value)} key={item.value}>{item.label}</button>)}
        </div>
      </section>

      <div className="catalog-summary"><strong>{status === "ready" ? payload.total.toLocaleString() : "-"}</strong> matching metrics <span>{payload.items.length} shown</span></div>
      {status === "loading" ? <div className="loading-state">Loading catalog...</div> : null}
      {status === "error" ? <div className="notice error">The catalog could not be loaded.</div> : null}
      {status === "ready" && groups.length === 0 ? <div className="empty-state">No metrics match these filters.</div> : null}

      <section className="dataset-list">
        {groups.map((group) => (
          <article className="dataset-card" key={group.key}>
            <div className="dataset-mark"><Database aria-hidden="true" size={19} /></div>
            <div className="dataset-body">
              <div className="dataset-title-row"><div><span className="source-badge">{group.source}</span><h2>{group.dataset.toUpperCase()}</h2></div><span>{group.metrics.length} shown</span></div>
              <p>Source-backed metric identity, grains, units, and publication lineage.</p>
              <dl className="inline-metadata"><div><dt>Geographies</dt><dd>{[...new Set(group.metrics.flatMap((metric) => metric.valid_geo_grains))].join(", ") || "Source-defined"}</dd></div><div><dt>Time grain</dt><dd>{[...new Set(group.metrics.flatMap((metric) => metric.valid_time_grains))].join(", ") || "Source-defined"}</dd></div><div><dt>Harvested</dt><dd>{group.metrics[0].harvested_at ? new Date(group.metrics[0].harvested_at).toLocaleDateString() : "Pending"}</dd></div></dl>
              <div className="metric-preview-list">
                {group.metrics.slice(0, 4).map((metric) => (
                  <Link href={explorerHref({ metric: metric.metric_code })} key={metric.metric_code}>
                    <span><strong>{displayMetricName(metric)}</strong><small>{metric.metric_code}</small></span><ArrowRight size={15} />
                  </Link>
                ))}
              </div>
            </div>
          </article>
        ))}
      </section>
    </main>
  );
}
