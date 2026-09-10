"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowRight, Database, Search } from "lucide-react";
import { apiErrorMessage, getSources, searchMetrics } from "../../lib/api/client";
import { createRequestTracker } from "../../lib/api/requestState";
import {
  CATALOG_PAGE_SIZE,
  DEFAULT_CATALOG_STATE,
  catalogPageModel,
  catalogRequestParams,
  metricProvenance,
  metricQualityState,
  parseCatalogState,
  serializeCatalogState,
  sourceFilterOptions,
} from "../../lib/catalog";
import { displayMetricName } from "../../lib/format";
import { explorerHref } from "../../lib/urlState";
import StatusPill from "../../components/StatusPill";

export default function CatalogPage() {
  const searchTracker = useRef(createRequestTracker()).current;
  const [state, setState] = useState(DEFAULT_CATALOG_STATE);
  const [sourceItems, setSourceItems] = useState([]);
  const [payload, setPayload] = useState({ total: 0, items: [] });
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState("");

  // The catalog opens on the state its URL names, so a shared catalog link
  // reproduces the same query, filter, and page.
  useEffect(() => {
    setState(parseCatalogState(window.location.search));
  }, []);

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
    const request = searchTracker.begin();
    const timer = window.setTimeout(async () => {
      setStatus("loading");
      try {
        const result = await searchMetrics(catalogRequestParams(state), {
          signal: controller.signal,
        });
        if (request.isCurrent()) {
          setPayload(result);
          setError("");
          setStatus("ready");
        }
      } catch (caught) {
        if (caught.name !== "AbortError" && request.isCurrent()) {
          setError(apiErrorMessage(caught));
          setStatus("error");
        }
      }
    }, 180);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
      searchTracker.invalidate();
    };
  }, [state, searchTracker]);

  // Keep the URL a shareable reproduction of the current catalog state.
  useEffect(() => {
    const query = serializeCatalogState(state);
    const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [state]);

  // Any filter change returns to the first page; keeping the offset would
  // show an empty page of a differently sized result.
  const updateFilters = useCallback((changes) => {
    setState((current) => ({ ...current, ...changes, page: 0 }));
  }, []);

  const sources = useMemo(() => sourceFilterOptions(sourceItems), [sourceItems]);
  const page = useMemo(
    () => catalogPageModel(payload, state, CATALOG_PAGE_SIZE),
    [payload, state],
  );

  const groups = useMemo(() => {
    const values = new Map();
    for (const metric of payload.items) {
      const parts = metric.metric_code.split(":");
      const key = `${metric.source_code}:${parts[1] || metric.source_object_type}`;
      if (!values.has(key)) {
        values.set(key, {
          key,
          source: metric.source_code,
          dataset: parts[1] || metric.source_object_type,
          metrics: [],
        });
      }
      values.get(key).metrics.push(metric);
    }
    return [...values.values()];
  }, [payload.items]);

  return (
    <main
      className="page-shell compact-page"
      data-testid="catalog"
      data-total={page.total === null ? "" : String(page.total)}
      data-page={String(page.pageIndex)}
      data-shown={String(page.shown)}
    >
      <header className="page-heading">
        <div className="section-kicker">Trust layer</div>
        <h1>Data Catalog</h1>
        <p>Discover public metrics, understand their analytical shape, and launch directly into a reproducible view.</p>
      </header>

      <section className="catalog-toolbar" aria-label="Catalog filters">
        <label className="search-field">
          <Search size={17} />
          <span className="sr-only">Search metrics</span>
          <input
            data-testid="catalog-search"
            value={state.query}
            onChange={(event) => updateFilters({ query: event.target.value })}
            placeholder="Search metrics, variables, or definitions"
          />
        </label>
        <div className="segmented-control" aria-label="Source filter">
          {sources.map((item) => (
            <button
              className={state.sourceCode === item.value ? "selected" : ""}
              type="button"
              data-testid={`catalog-source-${item.value || "all"}`}
              onClick={() => updateFilters({ sourceCode: item.value })}
              key={item.value}
            >
              {item.label}
            </button>
          ))}
        </div>
        <label className="catalog-toggle">
          <input
            type="checkbox"
            data-testid="catalog-include-retired"
            checked={!state.activeOnly}
            onChange={(event) => updateFilters({ activeOnly: !event.target.checked })}
          />
          Include retired metrics
        </label>
      </section>

      <div className="catalog-summary">
        {/* The count is the API's own published total for these filters; a
            page of results is never presented as the total. */}
        <strong data-testid="catalog-total">
          {status === "ready" && page.total !== null ? page.total.toLocaleString() : "-"}
        </strong>{" "}
        matching metrics
        <span data-testid="catalog-range">
          {page.firstRow === null
            ? "none shown"
            : `showing ${page.firstRow.toLocaleString()}-${page.lastRow.toLocaleString()}`}
          {page.pageCount > 0 ? ` · page ${page.pageIndex + 1} of ${page.pageCount}` : ""}
        </span>
      </div>

      {status === "loading" ? <div className="loading-state">Loading catalog...</div> : null}
      {status === "error" ? (
        <div className="notice error" data-testid="catalog-error">
          The catalog could not be loaded: {error}
        </div>
      ) : null}
      {status === "ready" && groups.length === 0 ? (
        <div className="empty-state">No metrics match these filters.</div>
      ) : null}

      {/* While a new page or filter is in flight the loaded rows are the
          previous answer; marking the list busy keeps them from reading as
          the current one. */}
      <section
        className="dataset-list"
        data-testid="catalog-results"
        data-stale={status === "loading" ? "true" : "false"}
        aria-busy={status === "loading"}
      >
        {groups.map((group) => (
          <article className="dataset-card" key={group.key}>
            <div className="dataset-mark"><Database aria-hidden="true" size={19} /></div>
            <div className="dataset-body">
              <div className="dataset-title-row">
                <div>
                  <span className="source-badge">{group.source}</span>
                  <h2>{String(group.dataset).toUpperCase()}</h2>
                </div>
                <span>{group.metrics.length} shown</span>
              </div>
              <p>Source-backed metric identity, grains, units, and publication lineage.</p>
              <div className="metric-preview-list">
                {group.metrics.map((metric) => {
                  const quality = metricQualityState(metric);
                  const provenance = metricProvenance(metric);
                  return (
                    <div className="metric-row" key={metric.metric_code}>
                      <Link
                        href={explorerHref({ metric: metric.metric_code })}
                        data-testid={`catalog-metric-link-${metric.metric_code}`}
                      >
                        <span>
                          <strong>{displayMetricName(metric)}</strong>
                          <small>{metric.metric_code}</small>
                        </span>
                        <ArrowRight size={15} />
                      </Link>
                      <StatusPill
                        state={quality.state}
                        label="Freshness"
                        message={quality.label}
                        testId={`catalog-freshness-${metric.metric_code}`}
                      />
                      {provenance.length > 0 ? (
                        <dl className="inline-metadata">
                          {provenance.map((entry) => (
                            <div key={entry.label}>
                              <dt>{entry.label}</dt>
                              <dd>{entry.value}</dd>
                            </div>
                          ))}
                        </dl>
                      ) : (
                        <p className="subtle">This metric publishes no provenance fields.</p>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </article>
        ))}
      </section>

      <nav className="catalog-pagination" aria-label="Catalog pages">
        <button
          className="button secondary"
          type="button"
          data-testid="catalog-previous"
          disabled={!page.hasPrevious}
          onClick={() => setState((current) => ({ ...current, page: current.page - 1 }))}
        >
          Previous
        </button>
        <span aria-live="polite">
          {page.pageCount > 0 ? `Page ${page.pageIndex + 1} of ${page.pageCount}` : "Page 1"}
        </span>
        <button
          className="button secondary"
          type="button"
          data-testid="catalog-next"
          disabled={!page.hasNext}
          onClick={() => setState((current) => ({ ...current, page: current.page + 1 }))}
        >
          Next
        </button>
      </nav>
    </main>
  );
}
