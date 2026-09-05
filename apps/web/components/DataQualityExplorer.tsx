"use client";

// The source coverage and data-quality explorer.
//
// Everything here is the warehouse's own published quality signal, served by
// the API. Nothing is recomputed, and there is deliberately no score, index,
// or grade: one number over unlike measures would be a client-authored
// judgement wearing the appearance of a published fact.
//
// Where the API publishes no dedicated quality resource — suppression,
// non-reporting, revisions — the screen says where that evidence actually
// lives and links to it, rather than inventing a surface for it here.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { RefreshCw } from "lucide-react";
import StatusPill from "./StatusPill";
import { apiErrorMessage, apiFetch, fetchAllPages } from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type { CollectionResponse, MetricSummary } from "../lib/api/types";
import {
  EVIDENCE_LOCATIONS,
  UNPUBLISHED_EVIDENCE,
  coverageSegments,
  freshnessRows,
  metricQualityRows,
} from "../lib/dataQuality";
import type { SourceFreshness } from "../lib/dataQuality";
import { explorerHref } from "../lib/urlState";

const CATALOG_PAGE_SIZE = 1000;
const METRIC_SAMPLE = 40;

interface RequestStatus {
  state: string;
  message: string;
}

export default function DataQualityExplorer() {
  const freshnessTracker = useRef(createRequestTracker()).current;
  const metricsTracker = useRef(createRequestTracker()).current;

  const [rows, setRows] = useState<ReturnType<typeof freshnessRows>>([]);
  const [freshnessStatus, setFreshnessStatus] = useState<RequestStatus>({
    state: "loading",
    message: "loading published freshness",
  });
  const [selectedSource, setSelectedSource] = useState("");
  const [metrics, setMetrics] = useState<MetricSummary[]>([]);
  const [metricsStatus, setMetricsStatus] = useState<RequestStatus>({
    state: "idle",
    message: "select a source",
  });

  const loadFreshness = useCallback(async () => {
    const request = freshnessTracker.begin();
    setFreshnessStatus({ state: "loading", message: "loading published freshness" });
    try {
      const payload = await apiFetch<CollectionResponse<SourceFreshness>>("/catalog/freshness");
      if (!request.isCurrent()) {
        return;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      setRows(freshnessRows(items));
      setFreshnessStatus({
        state: items.length > 0 ? "ok" : "idle",
        message:
          items.length > 0
            ? `${items.length} sources report a published freshness rollup`
            : "no source published a freshness rollup",
      });
    } catch (error) {
      if (request.isCurrent()) {
        setRows([]);
        setFreshnessStatus({ state: "bad", message: apiErrorMessage(error) });
      }
    }
  }, [freshnessTracker]);

  useEffect(() => {
    loadFreshness();
    return () => {
      freshnessTracker.invalidate();
    };
  }, [loadFreshness, freshnessTracker]);

  useEffect(() => {
    if (!selectedSource) {
      setMetrics([]);
      setMetricsStatus({ state: "idle", message: "select a source" });
      return;
    }
    const request = metricsTracker.begin();
    setMetricsStatus({ state: "loading", message: `loading ${selectedSource} metrics` });
    (async () => {
      try {
        const items = await fetchAllPages<MetricSummary>("/catalog/metrics", {
          params: { source_code: selectedSource, active_only: "false" },
          pageSize: CATALOG_PAGE_SIZE,
        });
        if (!request.isCurrent()) {
          return;
        }
        setMetrics(items);
        setMetricsStatus({
          state: "ok",
          message: `${items.length} metrics published for ${selectedSource}`,
        });
      } catch (error) {
        if (request.isCurrent()) {
          setMetrics([]);
          setMetricsStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    })();
    return () => {
      metricsTracker.invalidate();
    };
  }, [metricsTracker, selectedSource]);

  const metricRows = useMemo(() => metricQualityRows(metrics), [metrics]);
  const shown = metricRows.slice(0, METRIC_SAMPLE);

  return (
    <main
      className="page-shell"
      data-testid="quality-explorer"
      data-source-count={rows.length}
      data-selected-source={selectedSource}
    >
      <header className="page-heading">
        <div className="section-kicker">Source coverage</div>
        <h1>Data quality</h1>
        <p>
          The warehouse publishes a quality signal and the API serves it. Everything below is
          that published signal. There is no quality score here: one number over unlike
          measures would be a judgement this application invented, not a fact any source
          published.
        </p>
      </header>

      <section className="status-row">
        <StatusPill
          state={freshnessStatus.state}
          label="Freshness"
          message={freshnessStatus.message}
          testId="quality-freshness-status"
        />
        <StatusPill
          state={metricsStatus.state}
          label="Metrics"
          message={metricsStatus.message}
          testId="quality-metrics-status"
        />
        <button className="button secondary" type="button" onClick={loadFreshness}>
          <RefreshCw size={15} /> Refresh
        </button>
      </section>

      <section className="analysis-panel">
        <div className="panel-heading">
          <div>
            <div className="section-kicker">Per source</div>
            <h2>Published freshness rollup</h2>
            <p className="subtle">
              Current, stale, and retired are the warehouse&apos;s own published counts. A metric
              carrying none of those states is counted separately as unknown — it is not
              thereby current.
            </p>
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Source</th>
                <th>State</th>
                <th>Metrics</th>
                <th>Current</th>
                <th>Stale</th>
                <th>Retired</th>
                <th>No published state</th>
                <th>Last published</th>
                <th>Last harvested</th>
                <th>Composition</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.sourceCode} data-testid={`quality-row-${row.sourceCode}`}>
                  <td>
                    <button
                      className="text-link"
                      type="button"
                      onClick={() => setSelectedSource(row.sourceCode)}
                      data-testid={`quality-select-${row.sourceCode}`}
                    >
                      {row.sourceCode}
                    </button>
                  </td>
                  <td>
                    <StatusPill
                      state={row.state}
                      label="Freshness"
                      message={row.summary}
                      testId={`quality-state-${row.sourceCode}`}
                    />
                  </td>
                  <td>{row.metricCount}</td>
                  <td>{row.currentCount}</td>
                  <td>{row.staleCount}</td>
                  <td>{row.retiredCount}</td>
                  <td>{row.unclassifiedCount}</td>
                  {/* An unpublished time reads as unknown, never as recent. */}
                  <td>{row.publishedAt || "Not published"}</td>
                  <td>{row.harvestedAt || "Not published"}</td>
                  <td>
                    <div
                      className="coverage-bar"
                      role="img"
                      aria-label={coverageSegments(row)
                        .map((segment) => `${segment.count} ${segment.label}`)
                        .join(", ")}
                    >
                      {coverageSegments(row).map((segment) => (
                        <span
                          key={segment.label}
                          data-segment={segment.label}
                          style={{ width: `${(segment.share * 100).toFixed(1)}%` }}
                          title={`${segment.count} ${segment.label}`}
                        />
                      ))}
                    </div>
                  </td>
                </tr>
              ))}
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={10} className="subtle">
                    No source published a freshness rollup.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      {selectedSource ? (
        <section className="analysis-panel" data-testid="quality-metrics-panel">
          <div className="panel-heading">
            <div>
              <div className="section-kicker">Per measure</div>
              <h2>{selectedSource} metrics</h2>
              <p className="subtle">
                Showing {shown.length} of {metricRows.length}. A field the publisher did not
                publish reads as not published, never as a placeholder.
              </p>
            </div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Freshness</th>
                  <th>Published</th>
                  <th>Harvested</th>
                  <th>Source watermark</th>
                  <th>Publisher contract</th>
                  <th>Inspect</th>
                </tr>
              </thead>
              <tbody>
                {shown.map((row) => (
                  <tr key={row.metricCode} data-testid={`quality-metric-${row.metricCode}`}>
                    <td>{row.displayName || row.metricCode}</td>
                    <td>{row.freshness || "Not published"}</td>
                    <td>{row.publishedAt || "Not published"}</td>
                    <td>{row.harvestedAt || "Not published"}</td>
                    <td>{row.watermark || "Not published"}</td>
                    <td>{row.contractVersion || "Not published"}</td>
                    <td>
                      {/* Quality evidence links back to the context it affects. */}
                      <Link
                        className="text-link"
                        href={explorerHref({ metric: row.metricCode })}
                        data-testid={`quality-explore-${row.metricCode}`}
                      >
                        Explore
                      </Link>
                    </td>
                  </tr>
                ))}
                {shown.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="subtle">
                      No metrics published for this source.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      <section className="analysis-panel" data-testid="evidence-locations">
        <div className="panel-heading">
          <div>
            <div className="section-kicker">Where the evidence lives</div>
            <h2>Finding quality evidence the rollup does not carry</h2>
            <p className="subtle">
              Suppression, non-reporting, and revision evidence is published on the observation
              rows themselves rather than in a separate quality resource. Rather than invent a
              surface for it here, this names where it really is.
            </p>
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Evidence</th>
                <th>Published by</th>
                <th>Inspect here</th>
                <th>What it means</th>
              </tr>
            </thead>
            <tbody>
              {EVIDENCE_LOCATIONS.map((entry) => (
                <tr key={entry.kind} data-testid={`evidence-${entry.kind.split(" ")[0]}`}>
                  <td>{entry.kind}</td>
                  <td>
                    <code>{entry.publishedBy}</code>
                  </td>
                  <td>{entry.inspectHere}</td>
                  <td>{entry.meaning}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="coverage-note partial" data-testid="unpublished-evidence">
        <strong>Not published, and not invented here:</strong>
        <ul>
          {UNPUBLISHED_EVIDENCE.map((entry) => (
            <li key={entry}>{entry}</li>
          ))}
        </ul>
      </section>
    </main>
  );
}
