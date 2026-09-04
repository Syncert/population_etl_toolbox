"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Download, Save } from "lucide-react";
import ChoroplethMap from "./ChoroplethMap";
import ScatterChart from "./ScatterChart";
import StatusPill from "./StatusPill";
import {
  apiErrorMessage,
  buildApiPath,
  fetchAllPages,
  getCapabilities,
  getComparison,
  getComparisonPreflight,
} from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type {
  ComparisonPreflight,
  ComparisonResponse,
  GeographySummary,
  MetricSummary,
} from "../lib/api/types";
import { buildExplorerSources, findExplorerSource } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  DEFAULT_COMPARISON_SELECTION,
  comparisonCells,
  comparisonColumns,
  comparisonExport,
  comparisonMapRows,
  comparisonMetricOptions,
  comparisonRequestParams,
  comparisonScatterModel,
  compatibilityState,
  defaultDerivedField,
  describePreflight,
  incompatibleAlternatives,
  mayRequestComparison,
  periodsDiffer,
  preflightRequestParams,
  selectionIsComplete,
} from "../lib/comparison";
import type { ComparisonSelection, ComparisonSide } from "../lib/comparison";
import { saveChart } from "../lib/savedCharts";
import { discoverTileMetadata } from "../lib/tiles";
import {
  describeComparisonViewModes,
  supportedComparisonModes,
  unsupportedComparisonModes,
} from "../lib/viewModes";
import {
  comparisonHref,
  explorerHref,
  parseComparisonState,
  serializeComparisonState,
} from "../lib/urlState";
import type { GeoLevel } from "../lib/urlState";

const DEFAULT_GEO_LEVEL = "COUNTY";
const CATALOG_PAGE_SIZE = 1000;
const COMPARISON_PAGE_SIZE = 1000;
const SIDES = ["a", "b"] as const;

type SideKey = (typeof SIDES)[number];

interface RequestStatus {
  state: string;
  message: string;
}

const SIDE_LABEL: Record<SideKey, string> = { a: "Measure A", b: "Measure B" };

export default function ComparisonWorkspace() {
  const capabilitiesTracker = useRef(createRequestTracker()).current;
  const metricsTrackers = {
    a: useRef(createRequestTracker()).current,
    b: useRef(createRequestTracker()).current,
  };
  const preflightTracker = useRef(createRequestTracker()).current;
  const comparisonTracker = useRef(createRequestTracker()).current;
  const geographyTracker = useRef(createRequestTracker()).current;
  const tileTracker = useRef(createRequestTracker()).current;
  // The requested link state, applied once each side's catalog arrives so a
  // shared link reopens the same pair rather than a default one.
  const requestedRef = useRef<ReturnType<typeof parseComparisonState> | null>(null);

  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [sourcesError, setSourcesError] = useState("");
  const [selection, setSelection] = useState<ComparisonSelection>(
    DEFAULT_COMPARISON_SELECTION,
  );
  const [metrics, setMetrics] = useState<Record<SideKey, MetricSummary[]>>({ a: [], b: [] });
  const [metricsError, setMetricsError] = useState<Record<SideKey, string>>({ a: "", b: "" });
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [tileMetadata, setTileMetadata] = useState<Awaited<
    ReturnType<typeof discoverTileMetadata>
  > | null>(null);

  const [preflight, setPreflight] = useState<ComparisonPreflight | null>(null);
  const [preflightStatus, setPreflightStatus] = useState<RequestStatus>({
    state: "idle",
    message: "select two measures",
  });
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);
  const [comparisonStatus, setComparisonStatus] = useState<RequestStatus>({
    state: "idle",
    message: "waiting for a compatibility verdict",
  });
  const [saveStatus, setSaveStatus] = useState("");

  const sourceOf = useCallback(
    (side: SideKey) => findExplorerSource(sources, selection[side].sourceCode),
    [sources, selection],
  );

  // Capability discovery decides which sources can be named at all; nothing
  // here carries a client-side source list.
  useEffect(() => {
    const request = capabilitiesTracker.begin();
    requestedRef.current = parseComparisonState(window.location.search);

    async function loadCapabilities() {
      try {
        const payload = await getCapabilities();
        const discovered = buildExplorerSources(payload.items);
        if (!request.isCurrent()) {
          return;
        }
        setSources(discovered);
        const requested = requestedRef.current;
        const first = discovered[0]?.key || "";
        const second = discovered[1]?.key || first;
        setSelection((current) => ({
          ...current,
          a: { ...current.a, sourceCode: requested?.sourceA || first },
          b: { ...current.b, sourceCode: requested?.sourceB || second },
          geoLevel: requested?.geoLevel || current.geoLevel,
          stateFips: requested?.stateFips || current.stateFips,
        }));
      } catch (error) {
        if (request.isCurrent()) {
          setSourcesError(apiErrorMessage(error));
        }
      }
    }

    loadCapabilities();
    return () => {
      capabilitiesTracker.invalidate();
    };
  }, [capabilitiesTracker]);

  // One metric catalog per side, keyed by that side's chosen source.
  const sourceCodeA = sourceOf("a")?.sourceCode || "";
  const sourceCodeB = sourceOf("b")?.sourceCode || "";
  const sourceCodes: Record<SideKey, string> = useMemo(
    () => ({ a: sourceCodeA, b: sourceCodeB }),
    [sourceCodeA, sourceCodeB],
  );

  useEffect(() => {
    for (const side of SIDES) {
      const sourceCode = sourceCodes[side];
      if (!sourceCode) {
        continue;
      }
      const tracker = metricsTrackers[side];
      const request = tracker.begin();

      (async () => {
        try {
          const items = await fetchAllPages<MetricSummary>("/catalog/metrics", {
            params: { source_code: sourceCode, active_only: "true" },
            pageSize: CATALOG_PAGE_SIZE,
          });
          if (!request.isCurrent()) {
            return;
          }
          setMetrics((current) => ({ ...current, [side]: items }));
          setMetricsError((current) => ({ ...current, [side]: "" }));

          const requested = requestedRef.current;
          const wanted = side === "a" ? requested?.metricA : requested?.metricB;
          setSelection((current) => {
            if (current[side].sourceCode && current[side].metricCode) {
              // Keep an already valid choice; only fill an empty side.
              const stillListed = items.some(
                (item) => item.metric_code === current[side].metricCode,
              );
              if (stillListed) {
                return current;
              }
            }
            const chosen =
              wanted && items.some((item) => item.metric_code === wanted)
                ? wanted
                : items[0]?.metric_code || "";
            return { ...current, [side]: { ...current[side], metricCode: chosen } };
          });
        } catch (error) {
          if (request.isCurrent()) {
            setMetrics((current) => ({ ...current, [side]: [] }));
            setMetricsError((current) => ({
              ...current,
              [side]: apiErrorMessage(error) || "Unable to load measures.",
            }));
          }
        }
      })();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceCodes.a, sourceCodes.b]);

  useEffect(() => {
    const request = geographyTracker.begin();
    (async () => {
      try {
        const items = await fetchAllPages<GeographySummary>("/catalog/geographies", {
          params: { geo_level: "STATE" },
          pageSize: CATALOG_PAGE_SIZE,
        });
        if (request.isCurrent()) {
          setStates(
            items.sort((left, right) =>
              String(left.state_name).localeCompare(String(right.state_name)),
            ),
          );
        }
      } catch {
        // The state scope is optional; its absence is visible as an empty
        // selector rather than a failure of the comparison itself.
      }
    })();
    return () => {
      geographyTracker.invalidate();
    };
  }, [geographyTracker]);

  // The vector boundary decides whether this comparison is spatial at all;
  // its absence is a stated reason, not a blank map.
  useEffect(() => {
    const request = tileTracker.begin();
    (async () => {
      try {
        const discovered = await discoverTileMetadata();
        if (request.isCurrent()) {
          setTileMetadata(discovered);
        }
      } catch {
        // Leaving this null makes the map mode unsupported with the
        // published reason, which the mode notes render.
      }
    })();
    return () => {
      tileTracker.invalidate();
    };
  }, [tileTracker]);

  const complete = selectionIsComplete(selection);
  const metricCodeA = selection.a.metricCode;
  const metricCodeB = selection.b.metricCode;

  // Preflight first, always. The verdict decides whether any comparison data
  // may be requested at all, so it is asked before the pair is queried and
  // re-asked whenever the pair changes.
  useEffect(() => {
    if (!metricCodeA || !metricCodeB) {
      setPreflight(null);
      setPreflightStatus({ state: "idle", message: "select two measures" });
      return;
    }

    const request = preflightTracker.begin();
    setPreflight(null);
    setPreflightStatus({ state: "loading", message: "evaluating declared rules" });

    (async () => {
      try {
        const payload = await getComparisonPreflight(
          preflightRequestParams({
            ...selection,
            a: { ...selection.a, metricCode: metricCodeA },
            b: { ...selection.b, metricCode: metricCodeB },
          }),
        );
        if (!request.isCurrent()) {
          return;
        }
        setPreflight(payload);
        setPreflightStatus(compatibilityState(payload));
      } catch (error) {
        if (request.isCurrent()) {
          setPreflight(null);
          setPreflightStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    })();

    return () => {
      preflightTracker.invalidate();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [preflightTracker, metricCodeA, metricCodeB]);

  const comparable = mayRequestComparison(preflight);

  useEffect(() => {
    setComparison(null);
    if (!preflight) {
      setComparisonStatus({
        state: "idle",
        message: "waiting for a compatibility verdict",
      });
      return;
    }
    if (!comparable) {
      // The pair is blocked. Requesting anyway would turn a stated
      // explanation into a 422 and move data the policy rejected.
      setComparisonStatus({
        state: "incompatible",
        message: "not requested: the declared policy blocks this pair",
      });
      return;
    }

    const request = comparisonTracker.begin();
    setComparisonStatus({ state: "loading", message: "loading aligned comparison" });

    (async () => {
      try {
        const payload = await getComparison(
          comparisonRequestParams(selection, COMPARISON_PAGE_SIZE),
        );
        if (!request.isCurrent()) {
          return;
        }
        const items = Array.isArray(payload.items) ? payload.items : [];
        setComparison(payload);
        setComparisonStatus({
          state: "ok",
          message: `${items.length} of ${payload.total ?? items.length} aligned geographies`,
        });
      } catch (error) {
        if (request.isCurrent()) {
          setComparison(null);
          setComparisonStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    })();

    return () => {
      comparisonTracker.invalidate();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [comparisonTracker, preflight, comparable, selection.geoLevel, selection.stateFips]);

  // The link reproduces the selection, never the verdict.
  useEffect(() => {
    if (!complete) {
      return;
    }
    const query = serializeComparisonState(
      {
        metricA: selection.a.metricCode,
        metricB: selection.b.metricCode,
        sourceA: selection.a.sourceCode,
        sourceB: selection.b.sourceCode,
        geoLevel: selection.geoLevel as GeoLevel,
        stateFips: selection.stateFips,
      },
      { geoLevel: DEFAULT_GEO_LEVEL as GeoLevel },
    );
    const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [complete, selection]);

  const model = useMemo(() => describePreflight(preflight), [preflight]);
  const alternatives = useMemo(() => incompatibleAlternatives(preflight), [preflight]);
  const columns = useMemo(() => comparisonColumns(comparison), [comparison]);
  const rows = useMemo(
    () => (Array.isArray(comparison?.items) ? comparison.items : []),
    [comparison],
  );
  const scatter = useMemo(() => comparisonScatterModel(comparison), [comparison]);
  const derivedField = useMemo(() => defaultDerivedField(comparison), [comparison]);
  const mapRows = useMemo(
    () => comparisonMapRows(comparison, derivedField),
    [comparison, derivedField],
  );

  // Which aligned presentations this comparison can answer, from the same
  // published evidence the explorer's modes read: the verdict, the rows the
  // response carried, the pairs that are actually plottable, the fields the
  // API named as derived, and the vector layer's published geography fields.
  const viewModes = useMemo(
    () => describeComparisonViewModes({
      comparable,
      rowCount: rows.length,
      plottablePoints: scatter.points.length,
      derivations: comparison?.derivations,
      geoLevel: selection.geoLevel,
      tileFields: tileMetadata?.fields,
    }),
    [comparable, rows.length, scatter.points.length, comparison, selection.geoLevel, tileMetadata],
  );
  const unavailableModes = useMemo(
    () => unsupportedComparisonModes(viewModes),
    [viewModes],
  );

  const options: Record<SideKey, { value: string; label: string }[]> = useMemo(
    () => ({
      a: comparisonMetricOptions(metrics.a),
      b: comparisonMetricOptions(metrics.b),
    }),
    [metrics],
  );

  // The exact request the comparison effect issued, so the result is
  // reproducible outside the application.
  const apiQuery = comparable
    ? buildApiPath("/comparison", comparisonRequestParams(selection, COMPARISON_PAGE_SIZE))
    : buildApiPath("/comparison/preflight", preflightRequestParams(selection));

  function updateSide(side: SideKey, patch: Partial<ComparisonSide>) {
    requestedRef.current = null;
    setSelection((current) => ({ ...current, [side]: { ...current[side], ...patch } }));
  }

  function exportCsv() {
    const { headings, rows: exportRows, filename } = comparisonExport(comparison, preflight);
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const content = [headings, ...exportRows]
      .map((row) => row.map(escape).join(","))
      .join("\n");
    const blob = new Blob([content], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  function handleSave() {
    if (!comparable) {
      return;
    }
    saveChart({
      id: `comparison:${selection.a.metricCode}:${selection.b.metricCode}:${selection.geoLevel}:${selection.stateFips || "US"}`,
      version: 1,
      title: `${selection.a.metricCode} vs ${selection.b.metricCode}`,
      chartType: "comparison",
      metricCode: selection.a.metricCode,
      metricCodeB: selection.b.metricCode,
      source: comparison?.source_code_a || sourceOf("a")?.sourceCode || null,
      sourceB: comparison?.source_code_b || sourceOf("b")?.sourceCode || null,
      geoLevel: selection.geoLevel,
      stateFips: selection.stateFips || null,
      transformation: "api-derived",
      derivations: comparison?.derivations || [],
      caveats: comparison?.caveats || [],
      apiQuery,
      savedAt: new Date().toISOString(),
    });
    setSaveStatus("Saved for Builder");
    window.setTimeout(() => setSaveStatus(""), 2400);
  }

  return (
    <main
      className="dashboard"
      data-testid="comparison-workspace"
      data-metric-a={selection.a.metricCode}
      data-metric-b={selection.b.metricCode}
      data-comparable={preflight ? String(comparable) : ""}
      data-blocking-rules={model.blocking.map((rule) => rule.rule).join(",")}
      data-unverified-rules={model.unverified.map((rule) => rule.rule).join(",")}
      data-row-count={rows.length}
      data-view-modes={supportedComparisonModes(viewModes).join(",")}
      data-plottable-points={scatter.points.length}
    >
      <header className="explorer-heading">
        <div>
          <div className="section-kicker">Analytical workbench</div>
          <h1>Comparison workspace</h1>
          <p>
            Two published measures, checked against the API&apos;s declared compatibility
            rules before any data moves.
          </p>
        </div>
        <div className="command-row">
          <button
            className="button secondary"
            type="button"
            onClick={exportCsv}
            disabled={!viewModes.export.supported}
            title={viewModes.export.reason}
            data-testid="comparison-export"
          >
            <Download size={15} /> Export CSV
          </button>
          <button
            className="button primary"
            type="button"
            onClick={handleSave}
            disabled={!comparable}
            title={comparable ? "" : "a blocked pair is not saved as an analysis"}
            data-testid="comparison-save"
          >
            <Save size={15} /> Save comparison
          </button>
        </div>
      </header>
      {saveStatus ? <div className="save-toast" role="status">{saveStatus}</div> : null}

      <section className="status-row">
        <StatusPill
          state={preflightStatus.state}
          label="Compatibility"
          message={preflightStatus.message}
          testId="preflight-status"
        />
        <StatusPill
          state={comparisonStatus.state}
          label="Comparison"
          message={comparisonStatus.message}
          testId="comparison-status"
        />
      </section>

      <section className="grid">
        <article className="card span-2">
          <h2>Measures</h2>
          <div className="selector-grid">
            {SIDES.map((side) => (
              <div className="control-group span-controls" key={side}>
                <label htmlFor={`source-${side}`}>{SIDE_LABEL[side]} source</label>
                <select
                  id={`source-${side}`}
                  className="select"
                  data-testid={`comparison-source-${side}`}
                  value={selection[side].sourceCode}
                  onChange={(event) =>
                    updateSide(side, { sourceCode: event.target.value, metricCode: "" })
                  }
                  disabled={sources.length === 0}
                >
                  {sources.map((source) => (
                    <option value={source.key} key={source.key}>
                      {source.title}
                      {source.servesComparison ? "" : " — analysis routes not declared"}
                    </option>
                  ))}
                </select>
                <label htmlFor={`metric-${side}`}>
                  {SIDE_LABEL[side]} ({options[side].length.toLocaleString()} available)
                </label>
                <select
                  id={`metric-${side}`}
                  className="select"
                  data-testid={`comparison-metric-${side}`}
                  value={selection[side].metricCode}
                  onChange={(event) => updateSide(side, { metricCode: event.target.value })}
                  disabled={options[side].length === 0}
                >
                  {options[side].map((option) => (
                    <option value={option.value} key={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
                {metricsError[side] ? (
                  <p className="subtle">Measures error: {metricsError[side]}</p>
                ) : null}
              </div>
            ))}

            <div className="control-group">
              <label htmlFor="comparison-geo-level">View level</label>
              <select
                id="comparison-geo-level"
                className="select"
                data-testid="comparison-geo-level"
                value={selection.geoLevel}
                onChange={(event) =>
                  setSelection((current) => ({ ...current, geoLevel: event.target.value }))
                }
              >
                <option value="NATIONAL">National</option>
                <option value="STATE">State</option>
                <option value="COUNTY">County</option>
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="comparison-state">State</label>
              <select
                id="comparison-state"
                className="select"
                data-testid="comparison-state"
                value={selection.stateFips}
                onChange={(event) =>
                  setSelection((current) => ({ ...current, stateFips: event.target.value }))
                }
                disabled={selection.geoLevel === "NATIONAL"}
              >
                <option value="">All states</option>
                {states.map((state) => (
                  <option value={state.state_fips || ""} key={state.geo_id}>
                    {state.state_name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {sourcesError ? (
            <p className="subtle">Sources error: {sourcesError}</p>
          ) : null}
        </article>

        <article className="card span-2" data-testid="verdict-panel">
          <div className="section-kicker">Checked before any data moves</div>
          <h2>Compatibility verdict</h2>
          {!complete ? (
            <p className="subtle">Select a measure on each side to evaluate the declared rules.</p>
          ) : (
            <>
              <p className="subtle">
                Every rule below is evaluated by the API over the two measures&apos; published
                semantics. A rule it could not verify is stated as a caveat, not treated as a
                pass; only a failed rule blocks the pair.
              </p>
              <table data-testid="rule-table">
                <thead>
                  <tr>
                    <th>Rule</th>
                    <th>Status</th>
                    <th>Published reason</th>
                  </tr>
                </thead>
                <tbody>
                  {[...model.blocking, ...model.unverified, ...model.passed].map((rule) => (
                    <tr key={rule.rule} data-testid={`rule-${rule.rule}`}>
                      <td>{rule.rule}</td>
                      <td>{rule.status}</td>
                      <td>{rule.reason}</td>
                    </tr>
                  ))}
                  {model.blocking.length + model.unverified.length + model.passed.length === 0 ? (
                    <tr>
                      <td colSpan={3} className="subtle">
                        No rule verdicts have been published for this pair yet.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>

              {model.caveats.length > 0 ? (
                <ul className="coverage-note partial" data-testid="verdict-caveats">
                  {model.caveats.map((caveat) => (
                    <li key={caveat}>{caveat}</li>
                  ))}
                </ul>
              ) : null}

              {!comparable && preflight ? (
                <div data-testid="incompatible-explanation">
                  <p className="coverage-note partial">
                    These measures are not comparable, so no comparison was requested. The
                    declared rules that failed are listed above.
                  </p>
                  <ul>
                    {alternatives.map((alternative) => (
                      <li key={alternative}>{alternative}</li>
                    ))}
                  </ul>
                  <p className="subtle">
                    {SIDES.map((side) => (
                      <Link
                        className="nav-link"
                        href={explorerHref({
                          source: selection[side].sourceCode,
                          metric: selection[side].metricCode,
                        })}
                        key={side}
                        data-testid={`explore-${side}`}
                      >
                        Explore {SIDE_LABEL[side].toLowerCase()} on its own
                      </Link>
                    ))}
                  </p>
                </div>
              ) : null}
            </>
          )}
        </article>

        {comparable && comparison && unavailableModes.length > 0 ? (
          <article className="card span-2">
            <p className="subtle" data-testid="comparison-unsupported-modes">
              Not available for this comparison:{" "}
              {unavailableModes.map((entry) => `${entry.mode} — ${entry.reason}`).join("; ")}.
            </p>
          </article>
        ) : null}

        {viewModes.chart.supported && comparison ? (
          <article className="card span-2" data-testid="comparison-chart-panel">
            <h2>Aligned scatter</h2>
            <p className="subtle">
              Both axes are published values, each on its own scale; the plot asserts no
              shared unit and no relationship beyond what the two publishers stated. Every
              value it shows is also in the table below.
            </p>
            <ScatterChart
              model={scatter}
              labelX={String(comparison.metric_code_a || "measure A")}
              labelY={String(comparison.metric_code_b || "measure B")}
            />
          </article>
        ) : null}

        {viewModes.map.supported && comparison ? (
          <article className="card span-2" data-testid="comparison-map-panel">
            <h2>Comparison map</h2>
            <p className="subtle" data-testid="map-derived-note">
              Coloured by <strong>{derivedField}</strong>, which the API derived from the two
              published inputs — it is not a value either source published. A geography
              where one side published nothing stays uncoloured rather than being coloured
              as zero, and every value remains in the table below.
            </p>
            <ChoroplethMap
              rows={mapRows}
              tileMetadata={tileMetadata}
              geoLevel={selection.geoLevel}
              legendTitle={`${derivedField} · API-derived`}
            />
          </article>
        ) : null}

        {comparable && comparison ? (
          <article className="card span-2" data-testid="comparison-table-panel">
            <h2>Aligned comparison</h2>
            <p className="subtle">
              Each side keeps its own published value and the period that value describes;
              the API combines each side&apos;s newest value per geography rather than
              aligning them to a shared period.
              {comparison.derivations && comparison.derivations.length > 0 ? (
                <>
                  {" "}
                  <strong data-testid="derived-note">
                    {comparison.derivations.join(" and ")}{" "}
                    {comparison.derivations.length === 1 ? "is" : "are"} API-derived, not
                    published by either source.
                  </strong>
                </>
              ) : null}
            </p>
            {comparison.caveats && comparison.caveats.length > 0 ? (
              <ul className="coverage-note partial" data-testid="comparison-caveats">
                {comparison.caveats.map((caveat) => (
                  <li key={caveat}>{caveat}</li>
                ))}
              </ul>
            ) : null}
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    {columns.map((column) => (
                      <th key={column.key} data-derived={column.derived ? "true" : "false"}>
                        {column.label}
                        {column.derived ? " (API-derived)" : ""}
                      </th>
                    ))}
                    <th>Period basis</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.slice(0, 25).map((row, index) => {
                    const cells = comparisonCells(comparison, row);
                    return (
                      <tr key={`${row.geo_id}-${index}`}>
                        {columns.map((column) => (
                          <td key={column.key}>{cells[column.key]}</td>
                        ))}
                        <td data-testid={periodsDiffer(row) ? "periods-differ" : undefined}>
                          {periodsDiffer(row) ? "Different periods" : "Same period"}
                        </td>
                      </tr>
                    );
                  })}
                  {rows.length === 0 ? (
                    <tr>
                      <td colSpan={columns.length + 1} className="subtle">
                        No aligned geographies were published for this selection.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </article>
        ) : null}

        <article className="card span-2">
          <div className="section-kicker">Reproducible request</div>
          <h2>API Query</h2>
          <p className="subtle">
            {comparable
              ? "This endpoint reproduces the comparison above."
              : "Only the preflight was requested; the comparison was not."}
          </p>
          <code className="api-query" data-testid="comparison-api-query">
            GET {apiQuery}
          </code>
          <p className="subtle">
            <Link className="nav-link" href={comparisonHref({}, {})}>
              Reset the workspace
            </Link>
          </p>
        </article>
      </section>
    </main>
  );
}
