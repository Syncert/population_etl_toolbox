"use client";

// The workbench: mix any capability-reachable measures onto one chart.
//
// The measure picker is the catalog, filtered by capability — sources come
// from `lib/explorerSources`, measures from `/catalog/metrics` paged
// deterministically, and a source with no declared observation route is
// absent with its reason. The chart never names a route: which resource
// answers a series is `lib/observationAccess`'s decision, from the
// capability entry.
//
// Every rule this screen enforces lives in `lib/workbench.ts` so it can be
// read and tested as a rule rather than inferred from JSX. What is here is
// the wiring: discovery, the request per series, the URL state, and the
// presentation of decisions those modules made.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import BarChart from "./BarChart";
import LineChart, { seriesStroke } from "./LineChart";
import StatusPill from "./StatusPill";
import {
  apiErrorMessage,
  fetchAllMetrics,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getMetric,
} from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type {
  GeographySummary,
  MetricSummary,
  Observation,
} from "../lib/api/types";
import { buildExplorerSources, findExplorerSource } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildHistoryObservationRequest,
  buildSettledHistoryRequest,
  normalizeObservationRows,
  observationPeriodLabel,
} from "../lib/observationAccess";
import { GEO_GRAIN_LABELS, geographyPickerState } from "../lib/geographyPicker";
import { GEO_GRAIN_ORDER } from "../lib/geographyPicker";
import { metricSupportedGeoLevels } from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import {
  MAX_WORKBENCH_SERIES,
  PRESENTATION_LABELS,
  admitSeries,
  assignValueAxes,
  availablePresentations,
  buildPlottedSeries,
  describeChart,
  describeSeries,
  presentationOffer,
  sameSeries,
  seriesKey,
  unavailablePresentations,
  unpinnedDimensions,
} from "../lib/workbench";
import type {
  PlottedSeries,
  WorkbenchPresentation,
  WorkbenchSeries,
} from "../lib/workbench";
import {
  parseWorkbenchState,
  serializeWorkbenchState,
  workbenchHref,
  workbenchLinkCeiling,
} from "../lib/urlState";
import type { WorkbenchUrlState } from "../lib/urlState";

const CATALOG_PAGE_SIZE = 1000;
/** Pages of history per series. Ten pages of 1,000 covers any published run. */
const HISTORY_PAGE_SIZE = 1000;
const HISTORY_PAGE_LIMIT = 10;
const DEFAULT_PRESENTATION: WorkbenchPresentation = "line";

interface LoadedSeries {
  rows: ObservationRow[];
  complete: boolean;
  error: string;
}

function metricLabel(metric: MetricSummary | undefined): string {
  if (!metric) {
    return "";
  }
  return String(metric.metric_display_name || metric.metric_code).replaceAll(
    "!!",
    " › ",
  );
}

function metricUnit(metric: MetricSummary | undefined): string | null {
  const units = metric?.units;
  return typeof units === "string" ? units : null;
}

export default function WorkbenchPage() {
  const capabilitiesTracker = useRef(createRequestTracker()).current;
  const metricsTracker = useRef(createRequestTracker()).current;
  const geographyTracker = useRef(createRequestTracker()).current;
  const requestedRef = useRef<WorkbenchUrlState | null>(null);

  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [sourcesError, setSourcesError] = useState("");
  const [series, setSeries] = useState<WorkbenchSeries[]>([]);
  const [presentation, setPresentation] =
    useState<WorkbenchPresentation>(DEFAULT_PRESENTATION);
  const [loaded, setLoaded] = useState<Record<string, LoadedSeries>>({});
  const [loading, setLoading] = useState(false);

  // The picker's own working selection, before a series is admitted.
  const [draftSourceKey, setDraftSourceKey] = useState("");
  const [draftMetricCode, setDraftMetricCode] = useState("");
  const [draftGeoLevel, setDraftGeoLevel] = useState("");
  const [draftStateFips, setDraftStateFips] = useState("");
  const [draftGeoId, setDraftGeoId] = useState("");
  const [draftFilters, setDraftFilters] = useState<Record<string, string>>({});
  const [refusal, setRefusal] = useState("");

  const [metrics, setMetrics] = useState<MetricSummary[]>([]);
  const [metricsError, setMetricsError] = useState("");
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [geographies, setGeographies] = useState<GeographySummary[]>([]);
  const [geographiesRead, setGeographiesRead] = useState(false);

  // Every measure any series names, so a legend can label a series whose
  // source is not the one currently in the picker.
  const [metricIndex, setMetricIndex] = useState<Record<string, MetricSummary>>(
    {},
  );

  // --- The requested composition, read once ---------------------------------

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    requestedRef.current = parseWorkbenchState(window.location.search);
    const requested = requestedRef.current;
    if (requested.presentation) {
      setPresentation(requested.presentation as WorkbenchPresentation);
    }
  }, []);

  // --- Discovery ------------------------------------------------------------

  useEffect(() => {
    const request = capabilitiesTracker.begin();
    (async () => {
      try {
        const payload = await getCapabilities();
        if (!request.isCurrent()) {
          return;
        }
        const discovered = buildExplorerSources(payload.items || []);
        setSources(discovered);
        setSourcesError("");
        if (discovered.length > 0) {
          setDraftSourceKey((current) => current || discovered[0]!.key);
        }
      } catch (error) {
        if (request.isCurrent()) {
          setSourcesError(apiErrorMessage(error));
        }
      }
    })();
    return () => {
      capabilitiesTracker.invalidate();
    };
  }, [capabilitiesTracker]);

  const draftSource = useMemo(
    () => findExplorerSource(sources, draftSourceKey),
    [sources, draftSourceKey],
  );

  /**
   * Restore the composition a link named, once discovery can resolve it.
   *
   * A series in a link carries a source *key*, and a key means nothing until
   * the capability list says which source it is and how that source is
   * reached — so the restore waits for discovery rather than building a
   * series against a source that may not be published. A series naming a key
   * discovery does not answer is dropped with the rest kept: a link that
   * reopens three of four series and says so is better than one that opens
   * blank because the fourth source was retired.
   *
   * `requestedRef` is cleared on the first successful restore, so a later
   * discovery refresh cannot overwrite what the reader has since composed.
   */
  useEffect(() => {
    const requested = requestedRef.current;
    if (!requested || sources.length === 0) {
      return;
    }
    requestedRef.current = null;
    const restored: WorkbenchSeries[] = [];
    for (const entry of requested.series || []) {
      const source = findExplorerSource(sources, entry.sourceKey);
      if (!source) {
        continue;
      }
      restored.push({
        sourceKey: source.key,
        sourceCode: source.sourceCode,
        metricCode: entry.metricCode,
        scope: entry.scope || "latest",
        release: entry.release,
        geoLevel: entry.geoLevel || "",
        geoId: entry.geoId || "",
        filters: entry.filters || {},
      });
    }
    if (restored.length > 0) {
      setSeries(restored);
    }
  }, [sources]);

  /**
   * The catalog row for every measure any series names.
   *
   * The picker's own read only covers the source currently selected, so a
   * restored composition — or one whose picker has since moved on — would
   * have no display name and no unit for its other series, and a series with
   * no unit is a series this page would put on the "unit not published" axis.
   * That would be this application inventing a fact about the publication,
   * which is the one thing the axis rule exists to prevent, so each named
   * measure is read individually.
   */
  useEffect(() => {
    const missing = [
      ...new Set(
        series
          .map((entry) => entry.metricCode)
          .filter((code) => code && !metricIndex[code]),
      ),
    ];
    if (missing.length === 0) {
      return;
    }
    let cancelled = false;
    (async () => {
      const found: Record<string, MetricSummary> = {};
      for (const code of missing) {
        try {
          found[code] = await getMetric(code);
        } catch {
          // A measure the catalog no longer publishes keeps its code as its
          // label; the series still draws what the observations resource
          // answers, and the legend says the code rather than nothing.
        }
      }
      if (!cancelled && Object.keys(found).length > 0) {
        setMetricIndex((current) => ({ ...found, ...current }));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [series, metricIndex]);

  useEffect(() => {
    if (!draftSource) {
      return;
    }
    const request = metricsTracker.begin();
    (async () => {
      try {
        const items = await fetchAllMetrics(
          { source_code: draftSource.sourceCode },
          { pageSize: CATALOG_PAGE_SIZE },
        );
        if (!request.isCurrent()) {
          return;
        }
        const ordered = [...items].sort((left, right) =>
          String(left.metric_code).localeCompare(String(right.metric_code)),
        );
        setMetrics(ordered);
        setMetricsError("");
        setMetricIndex((current) => {
          const next = { ...current };
          for (const metric of ordered) {
            next[String(metric.metric_code)] = metric;
          }
          return next;
        });
      } catch (error) {
        if (request.isCurrent()) {
          setMetrics([]);
          setMetricsError(apiErrorMessage(error));
        }
      }
    })();
    return () => {
      metricsTracker.invalidate();
    };
  }, [draftSource, metricsTracker]);

  useEffect(() => {
    (async () => {
      try {
        const items = await fetchAllPages<GeographySummary>(
          "/catalog/geographies",
          {
            params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" },
            pageSize: CATALOG_PAGE_SIZE,
          },
        );
        setStates(
          items.sort((left, right) =>
            String(left.state_name).localeCompare(String(right.state_name)),
          ),
        );
      } catch {
        // The state scope narrows the geography picker; its absence shows as
        // an empty selector rather than as a failure of the workbench.
      }
    })();
  }, []);

  // The geographies offered for the drafted grain, read from the projection.
  useEffect(() => {
    const grain = draftGeoLevel;
    if (!grain || grain === "NATIONAL") {
      setGeographies([]);
      setGeographiesRead(true);
      return;
    }
    const needsState = grain === "COUNTY" || grain === "PLACE";
    if (needsState && !draftStateFips) {
      setGeographies([]);
      setGeographiesRead(true);
      return;
    }
    const request = geographyTracker.begin();
    setGeographiesRead(false);
    (async () => {
      try {
        const items = await fetchAllPages<GeographySummary>(
          "/catalog/geographies",
          {
            params: {
              ...ACTIVE_GEOGRAPHIES_ONLY,
              geo_level: grain,
              ...(needsState ? { state_fips: draftStateFips } : {}),
            },
            pageSize: CATALOG_PAGE_SIZE,
          },
        );
        if (request.isCurrent()) {
          setGeographies(items);
          setGeographiesRead(true);
        }
      } catch {
        if (request.isCurrent()) {
          setGeographies([]);
          setGeographiesRead(true);
        }
      }
    })();
    return () => {
      geographyTracker.invalidate();
    };
  }, [draftGeoLevel, draftStateFips, geographyTracker]);

  // --- The picker's offers --------------------------------------------------

  const draftMetric = useMemo(
    () => metrics.find((metric) => metric.metric_code === draftMetricCode),
    [metrics, draftMetricCode],
  );

  /**
   * The grains this measure publishes, in the vocabulary's order.
   *
   * A measure declaring none publishes at unknown grains, not at none — the
   * explorer's own rule (WEB-038) — so the whole vocabulary stays offered
   * rather than the control going empty.
   */
  const draftGrains = useMemo(() => {
    const declared = metricSupportedGeoLevels(draftMetric);
    return declared.length > 0
      ? GEO_GRAIN_ORDER.filter((grain) => declared.includes(grain))
      : [...GEO_GRAIN_ORDER];
  }, [draftMetric]);

  useEffect(() => {
    setDraftGeoId("");
    setDraftGeoLevel((current) =>
      current && draftGrains.includes(current) ? current : (draftGrains[0] ?? ""),
    );
  }, [draftGrains]);

  const draftDimensions = useMemo(
    () =>
      draftSource
        ? draftSource.accessShape === "neutral"
          ? draftSource.neutralDimensionFilters
          : draftSource.dimensionFilters
        : [],
    [draftSource],
  );

  const picker = useMemo(
    () =>
      geographyPickerState(draftGeoLevel, {
        geographies,
        stateSelected: Boolean(draftStateFips),
        read: geographiesRead,
      }),
    [draftGeoLevel, geographies, draftStateFips, geographiesRead],
  );

  const candidate = useMemo<WorkbenchSeries | null>(() => {
    if (!draftSource || !draftMetricCode) {
      return null;
    }
    return {
      sourceKey: draftSource.key,
      sourceCode: draftSource.sourceCode,
      metricCode: draftMetricCode,
      scope: "latest",
      geoLevel: draftGeoLevel,
      // A national series has one geography and no picker; its identity comes
      // from the row rather than from a choice, so the grain stands in until
      // the answer names it.
      geoId: draftGeoLevel === "NATIONAL" ? "NATIONAL" : draftGeoId,
      filters: draftFilters,
    };
  }, [
    draftSource,
    draftMetricCode,
    draftGeoLevel,
    draftGeoId,
    draftFilters,
  ]);

  const admission = useMemo(
    () =>
      candidate
        ? admitSeries({ source: draftSource, candidate, existing: series })
        : { admitted: false, reason: "Choose a source and a measure." },
    [candidate, draftSource, series],
  );

  const addSeries = useCallback(() => {
    if (!candidate) {
      return;
    }
    const verdict = admitSeries({
      source: draftSource,
      candidate,
      existing: series,
    });
    if (!verdict.admitted) {
      setRefusal(verdict.reason);
      return;
    }
    setRefusal("");
    setSeries((current) => [...current, candidate]);
  }, [candidate, draftSource, series]);

  const removeSeries = useCallback((target: WorkbenchSeries) => {
    setSeries((current) =>
      current.filter((entry) => !sameSeries(entry, target)),
    );
  }, []);

  // --- Reading each series --------------------------------------------------

  useEffect(() => {
    let cancelled = false;
    if (series.length === 0) {
      setLoaded({});
      return;
    }
    setLoading(true);
    (async () => {
      const next: Record<string, LoadedSeries> = {};
      for (const entry of series) {
        const key = seriesKey(entry);
        const source = findExplorerSource(sources, entry.sourceKey);
        if (!source) {
          next[key] = {
            rows: [],
            complete: true,
            error:
              `The API published no observation route for ${entry.sourceCode}, ` +
              "so this series cannot be read.",
          };
          continue;
        }
        // A settled history where the resource serves one: each period as its
        // newest release left it, which is the `explorer-settled-history`
        // rule. Where it does not, the source's own latest history.
        const request =
          buildSettledHistoryRequest(source, {
            metricCode: entry.metricCode,
            geoId: entry.geoId,
            dimensions: entry.filters,
          }) ||
          buildHistoryObservationRequest(source, {
            metricCode: entry.metricCode,
            geoId: entry.geoId,
            dimensions: entry.filters,
          });
        if (!request) {
          next[key] = {
            rows: [],
            complete: true,
            error:
              `${source.title} publishes no history route this series can be ` +
              "read through.",
          };
          continue;
        }
        try {
          const page = await fetchCollectionPages<Observation>(
            request.resource,
            {
              params: request.params,
              pageSize: HISTORY_PAGE_SIZE,
              maxPages: HISTORY_PAGE_LIMIT,
            },
          );
          next[key] = {
            rows: normalizeObservationRows(source, page.items) as ObservationRow[],
            complete: page.complete,
            error: "",
          };
        } catch (error) {
          next[key] = { rows: [], complete: true, error: apiErrorMessage(error) };
        }
      }
      if (!cancelled) {
        setLoaded(next);
        setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [series, sources]);

  // --- The chart model ------------------------------------------------------

  const plotted = useMemo<PlottedSeries[]>(
    () =>
      series.map((entry) => {
        const key = seriesKey(entry);
        const load = loaded[key];
        const metric = metricIndex[entry.metricCode];
        return buildPlottedSeries({
          series: entry,
          rows: load?.rows || [],
          label: metricLabel(metric),
          unit: metricUnit(metric),
          truncated: load ? !load.complete : false,
        });
      }),
    [series, loaded, metricIndex],
  );

  const assignment = useMemo(
    () =>
      assignValueAxes(
        plotted.map((entry) => ({
          key: entry.key,
          unit: entry.unitUnpublished ? null : entry.unit,
        })),
      ),
    [plotted],
  );

  const offer = useMemo(
    () =>
      presentationOffer({
        series,
        facts: plotted.map((entry) => ({
          key: entry.key,
          metricCode: entry.series.metricCode,
          periodCount: new Set(entry.points.map((point) => point.period)).size,
        })),
        // WB-2 lands the cross-sectional presentations and WB-2/WB-5 the
        // heatmap; until then they are listed with the reason rather than
        // rendered as controls that do nothing.
        crossSectionalReason:
          "A cross-sectional reading — scatter, ranking, correlation — needs " +
          "one shared grain and the aligned analysis routes. It arrives with " +
          "the next phase of this page.",
        heatmapReason:
          "The geography × period heatmap arrives with the next phase of " +
          "this page.",
      }),
    [series, plotted],
  );

  const offered = useMemo(() => availablePresentations(offer), [offer]);
  const withheld = useMemo(() => unavailablePresentations(offer), [offer]);

  // The reader's choice is kept across a transient unavailability rather than
  // silently reassigned: a selection that snapped back while a request was in
  // flight would read as the screen overruling them (the WEB-017 pattern).
  const effectivePresentation =
    offer[presentation].available || offered.length === 0
      ? presentation
      : (offered[0] as WorkbenchPresentation);

  const geographyNames = useMemo(() => {
    const names: Record<string, string> = {};
    for (const row of [...states, ...geographies]) {
      if (row?.geo_id) {
        names[String(row.geo_id)] =
          String(row.county_name || row.place_name || row.state_name || row.geo_id);
      }
    }
    return names;
  }, [states, geographies]);

  const colorOf = useCallback(
    (key: string) => {
      const index = plotted.findIndex((entry) => entry.key === key);
      return seriesStroke(index < 0 ? 0 : index);
    },
    [plotted],
  );

  const bars = useMemo(
    () =>
      plotted.flatMap((entry) =>
        entry.points.map((point) => ({
          key: `${entry.key}-${point.period}`,
          category: point.period,
          value: point.value,
          unit: entry.unit,
          groupKey: entry.key,
          groupLabel: describeSeries(entry, geographyNames[entry.series.geoId]),
          period: point.period,
          release: String(point.row?.release || "") || undefined,
        })),
      ),
    [plotted, geographyNames],
  );

  // --- The shareable link ---------------------------------------------------

  const urlState = useMemo<WorkbenchUrlState>(
    () => ({
      series: series.map((entry) => ({
        sourceKey: entry.sourceKey,
        metricCode: entry.metricCode,
        scope: entry.scope,
        release: entry.release,
        geoLevel: entry.geoLevel as never,
        geoId: entry.geoId,
        filters: entry.filters,
      })),
      presentation: effectivePresentation,
    }),
    [series, effectivePresentation],
  );

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const query = serializeWorkbenchState(urlState, {
      presentation: DEFAULT_PRESENTATION,
    });
    const next = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== next) {
      window.history.replaceState(null, "", next);
    }
  }, [urlState]);

  const ceiling = workbenchLinkCeiling(series.length);

  const errors = plotted
    .map((entry) => ({ entry, error: loaded[entry.key]?.error || "" }))
    .filter((row) => row.error !== "");

  return (
    <main
      className="dashboard"
      data-testid="workbench"
      data-series-count={series.length}
      data-presentation={effectivePresentation}
      data-presentations={offered.join(",")}
    >
      <header className="explorer-heading">
        <div>
          <div className="section-kicker">Build your own</div>
          <h1>Workbench</h1>
          <p>
            Put any published measures on one chart. Each series is one measure
            at one geography; nothing here is rolled up from a finer grain,
            normalised, or rescaled to share an axis, because a line that was
            would not be a published measure.
          </p>
        </div>
      </header>

      <section className="status-row" role="status">
        <StatusPill
          state={sourcesError ? "bad" : sources.length > 0 ? "ok" : "loading"}
          label="Sources"
          message={
            sourcesError
              ? sourcesError
              : sources.length > 0
                ? `${sources.length} published`
                : "reading the capability list"
          }
          testId="workbench-sources-status"
        />
        <StatusPill
          state={loading ? "loading" : series.length === 0 ? "idle" : "ok"}
          label="Series"
          message={
            loading
              ? "reading each published history"
              : `${series.length} of ${MAX_WORKBENCH_SERIES}`
          }
          testId="workbench-series-status"
        />
      </section>

      <section className="grid">
        <div className="card">
          <h2>Add a series</h2>
          <div className="selector-grid">
            <div className="control-group">
              <label htmlFor="workbench-source">Source</label>
              <select
                id="workbench-source"
                className="select"
                value={draftSourceKey}
                onChange={(event) => {
                  setDraftSourceKey(event.target.value);
                  setDraftMetricCode("");
                  setDraftFilters({});
                  setRefusal("");
                }}
                data-testid="workbench-source"
              >
                {sources.map((source) => (
                  <option key={source.key} value={source.key}>
                    {source.title}
                  </option>
                ))}
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="workbench-metric">Measure</label>
              <select
                id="workbench-metric"
                className="select"
                value={draftMetricCode}
                onChange={(event) => {
                  setDraftMetricCode(event.target.value);
                  setRefusal("");
                }}
                data-testid="workbench-metric"
              >
                <option value="">Choose a measure</option>
                {metrics.map((metric) => (
                  <option
                    key={String(metric.metric_code)}
                    value={String(metric.metric_code)}
                  >
                    {metricLabel(metric)} ({String(metric.metric_code)})
                  </option>
                ))}
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="workbench-grain">Grain</label>
              <select
                id="workbench-grain"
                className="select"
                value={draftGeoLevel}
                onChange={(event) => {
                  setDraftGeoLevel(event.target.value);
                  setDraftGeoId("");
                  setRefusal("");
                }}
                data-testid="workbench-grain"
              >
                {draftGrains.map((grain) => (
                  <option key={grain} value={grain}>
                    {GEO_GRAIN_LABELS[grain]?.one || grain}
                  </option>
                ))}
              </select>
            </div>

            {draftGeoLevel === "COUNTY" || draftGeoLevel === "PLACE" ? (
              <div className="control-group">
                <label htmlFor="workbench-state">State</label>
                <select
                  id="workbench-state"
                  className="select"
                  value={draftStateFips}
                  onChange={(event) => {
                    setDraftStateFips(event.target.value);
                    setDraftGeoId("");
                  }}
                  data-testid="workbench-state"
                >
                  <option value="">Select a state</option>
                  {states.map((row) => (
                    <option key={String(row.geo_id)} value={String(row.state_fips)}>
                      {String(row.state_name)}
                    </option>
                  ))}
                </select>
              </div>
            ) : null}

            <div className="control-group">
              <label htmlFor="workbench-geography">{picker.label}</label>
              <select
                id="workbench-geography"
                className="select"
                value={draftGeoId}
                disabled={picker.disabled}
                onChange={(event) => {
                  setDraftGeoId(event.target.value);
                  setRefusal("");
                }}
                data-testid="workbench-geography"
              >
                <option value="">{picker.placeholder}</option>
                {picker.options.map((option) => (
                  <option key={option.geoId} value={option.geoId}>
                    {option.name}
                  </option>
                ))}
              </select>
            </div>

            {draftDimensions.map((name) => (
              <div className="control-group" key={name}>
                <label htmlFor={`workbench-dimension-${name}`}>{name}</label>
                <input
                  id={`workbench-dimension-${name}`}
                  className="select"
                  type="text"
                  value={draftFilters[name] || ""}
                  onChange={(event) => {
                    const value = event.target.value;
                    setDraftFilters((current) => ({ ...current, [name]: value }));
                    setRefusal("");
                  }}
                  data-testid={`workbench-dimension-${name}`}
                />
              </div>
            ))}
          </div>

          {draftDimensions.length > 0 ? (
            <p className="subtle" data-testid="workbench-dimension-note">
              {draftSource?.title} publishes several series per geography,
              separated by {draftDimensions.join(", ")}. Each must be pinned to
              one value before the series can be added: an unpinned answer is
              many published lines, and drawing it as one would combine
              measures the source publishes separately.
            </p>
          ) : null}

          <div className="command-row">
            <button
              type="button"
              className="button primary"
              onClick={addSeries}
              disabled={!admission.admitted}
              data-testid="workbench-add-series"
            >
              Add to the chart
            </button>
          </div>

          {!admission.admitted && candidate ? (
            <p className="subtle" data-testid="workbench-admission-reason">
              {admission.reason}
            </p>
          ) : null}
          {refusal ? (
            <p className="notice error" data-testid="workbench-refusal">
              {refusal}
            </p>
          ) : null}
          {metricsError ? (
            <p className="notice error" data-testid="workbench-metrics-error">
              {metricsError}
            </p>
          ) : null}
        </div>

        <div className="card">
          <h2>
            Series ({series.length} of {MAX_WORKBENCH_SERIES})
          </h2>
          {series.length === 0 ? (
            <p className="subtle">
              No series yet. Add one above; the chart is drawn from what you put
              on it, and nothing is drawn that the publication does not support.
            </p>
          ) : (
            <ul className="chart-legend" data-testid="workbench-series-list">
              {plotted.map((entry, index) => (
                <li key={entry.key} data-series-key={entry.key}>
                  <span
                    aria-hidden="true"
                    className="legend-swatch"
                    style={{ backgroundColor: seriesStroke(index) }}
                  />{" "}
                  <span>
                    {describeSeries(entry, geographyNames[entry.series.geoId])} —{" "}
                    {entry.unit}
                  </span>{" "}
                  <button
                    type="button"
                    className="button ghost"
                    onClick={() => removeSeries(entry.series)}
                    data-testid="workbench-remove-series"
                  >
                    Remove
                  </button>
                </li>
              ))}
            </ul>
          )}
          {errors.length > 0 ? (
            <ul className="notice error" data-testid="workbench-series-errors">
              {errors.map((row) => (
                <li key={row.entry.key}>
                  {row.entry.series.metricCode}: {row.error}
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      </section>

      <section className="grid">
        <div className="card span-2">
          <h2>Chart</h2>

          <div className="command-row" role="group" aria-label="Presentation">
            {offered.map((option) => (
              <button
                key={option}
                type="button"
                className={
                  option === effectivePresentation
                    ? "button primary"
                    : "button secondary"
                }
                aria-pressed={option === effectivePresentation}
                onClick={() => setPresentation(option)}
                data-testid={`workbench-presentation-${option}`}
              >
                {PRESENTATION_LABELS[option]}
              </button>
            ))}
          </div>

          {withheld.length > 0 ? (
            <ul
              className="subtle"
              data-testid="workbench-unavailable-presentations"
            >
              {withheld.map((entry) => (
                <li key={entry.presentation} data-presentation={entry.presentation}>
                  <strong>{PRESENTATION_LABELS[entry.presentation]}</strong>:{" "}
                  {entry.reason}
                </li>
              ))}
            </ul>
          ) : null}

          {loading ? (
            <p className="subtle" data-testid="workbench-loading">
              Reading each series&apos; published history…
            </p>
          ) : null}

          {assignment.smallMultiples &&
          plotted.some((entry) => entry.points.length > 0) ? (
            <p className="notice" data-testid="workbench-small-multiples">
              {assignment.note}
            </p>
          ) : null}

          {offer[effectivePresentation].available &&
          effectivePresentation === "line" ? (
            <LineChart
              plotted={plotted}
              assignment={assignment}
              geographyNames={geographyNames}
            />
          ) : null}

          {offer[effectivePresentation].available &&
          effectivePresentation === "bar" ? (
            <BarChart
              bars={bars}
              assignment={assignment}
              orientation="time"
              colorOf={colorOf}
              unpublished={plotted.reduce(
                (total, entry) => total + entry.droppedPeriods,
                0,
              )}
              label={describeChart("bar", plotted)}
            />
          ) : null}
        </div>
      </section>

      {plotted.some((entry) => entry.points.length > 0) ? (
        <section className="grid">
          <div className="card span-2">
            <h2>Every plotted value</h2>
            <div className="table-wrap">
              <table data-testid="workbench-table">
                <caption className="subtle">
                  One row per plotted value. A period that published no value is
                  not a row here and is not a zero on the chart.
                </caption>
                <thead>
                  <tr>
                    <th scope="col">Series</th>
                    <th scope="col">Period</th>
                    <th scope="col">Value</th>
                    <th scope="col">Unit</th>
                    <th scope="col">Release</th>
                  </tr>
                </thead>
                <tbody>
                  {plotted.flatMap((entry) =>
                    entry.points.map((point) => (
                      <tr key={`${entry.key}-${point.period}`}>
                        <td>
                          {describeSeries(
                            entry,
                            geographyNames[entry.series.geoId],
                          )}
                        </td>
                        <td>{point.period}</td>
                        <td>{point.value}</td>
                        <td>{entry.unit}</td>
                        <td>{String(point.row?.release || "")}</td>
                      </tr>
                    )),
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      ) : null}

      <section className="grid">
        <div className="card span-2">
          <h2>Share</h2>
          {ceiling.fits ? (
            <p className="subtle" data-testid="workbench-share">
              This page&apos;s address carries the composition: every
              series&apos; source, measure, grain, geography and pins, and the
              presentation. It carries no value and no account — reopening it
              re-asks the API, so a shared link can never show what the
              warehouse said when the link was made while presenting it as
              current.{" "}
              <Link
                href={workbenchHref(urlState, {
                  presentation: DEFAULT_PRESENTATION,
                })}
              >
                Copy this composition&apos;s link
              </Link>
              .
            </p>
          ) : (
            <p className="notice" data-testid="workbench-share-ceiling">
              {ceiling.reason}
            </p>
          )}
        </div>
      </section>
    </main>
  );
}
