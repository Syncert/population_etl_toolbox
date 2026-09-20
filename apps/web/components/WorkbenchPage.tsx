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
import { Download, Save } from "lucide-react";
import BarChart from "./BarChart";
import CorrelationMatrixChart from "./CorrelationMatrixChart";
import CorrelationPanel from "./CorrelationPanel";
import HeatmapChart from "./HeatmapChart";
import LineChart, { seriesStroke } from "./LineChart";
import ScatterChart from "./ScatterChart";
import StatusPill from "./StatusPill";
import {
  apiErrorMessage,
  fetchAllMetrics,
  fetchAllPages,
  fetchCollectionPages,
  createSavedAnalysis,
  fetchComparisonPages,
  getCapabilities,
  getComparisonCorrelation,
  getComparisonMatrix,
  getComparisonPreflight,
  getMetric,
} from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type {
  ComparisonCorrelation,
  ComparisonMatrix,
  ComparisonPreflight,
  ComparisonResponse,
  ComparisonRow,
  GeographySummary,
  MetricSummary,
  Observation,
} from "../lib/api/types";
import {
  comparisonRowName,
  comparisonScatterModel,
  compatibilityState,
  describeComparisonCoverage,
  describePreflight,
  incompatibleAlternatives,
  mayRequestComparison,
  sharedGrainOffer,
} from "../lib/comparison";
import { buildExplorerSources, findExplorerSource } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildHistoryObservationRequest,
  buildSettledHistoryRequest,
  buildSettledSurfaceRequest,
  normalizeObservationRows,
  observationPeriodLabel,
} from "../lib/observationAccess";
import { GEO_GRAIN_LABELS, geographyPickerState } from "../lib/geographyPicker";
import { GEO_GRAIN_ORDER } from "../lib/geographyPicker";
import {
  metricSupportedGeoLevels,
  publishedNumber,
} from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import {
  MAX_WORKBENCH_SERIES,
  PRESENTATION_LABELS,
  UNPUBLISHED_UNIT_LABEL,
  admitSeries,
  correlationEligibility,
  correlationMatrixModel,
  correlationReadings,
  crossSectionalPair,
  crossSectionalRefusal,
  pairedGeographiesText,
  selectablePairs,
  heatmapModel,
  isCrossSectional,
  referenceLineOffer,
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
  workbenchExport,
  workbenchExportFilename,
} from "../lib/observationExport";
import { SAVED_CHART_LIMIT, saveChart } from "../lib/savedCharts";
import { useStoredToken } from "../lib/apiToken";
import {
  describeLocalSave,
  describeSaveFailure,
  describeSaveSuccess,
  saveDestination,
  workbenchDocument,
} from "../lib/savedAnalysis";
import type { SaveOutcome } from "../lib/savedAnalysis";
import {
  parseWorkbenchState,
  serializeWorkbenchState,
  workbenchHref,
  workbenchLinkCeiling,
} from "../lib/urlState";
import type { WorkbenchUrlState } from "../lib/urlState";
import { formatNumber } from "../lib/format";

const CATALOG_PAGE_SIZE = 1000;
/** Pages of history per series. Ten pages of 1,000 covers any published run. */
const HISTORY_PAGE_SIZE = 1000;
const HISTORY_PAGE_LIMIT = 10;
/** The comparison route's own declared page limit. */
const COMPARISON_PAGE_SIZE = 1000;
/** Eight pages reach 8,000 aligned geographies; a national county grain is 3,144. */
const COMPARISON_PAGE_LIMIT = 8;
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

  // The cross-sectional reading: one shared grain, the API's verdict on the
  // pair, and the aligned rows it serves where the verdict allows them.
  const [alignmentGeoLevel, setAlignmentGeoLevel] = useState("");
  const [alignmentStateFips, setAlignmentStateFips] = useState("");
  const [preflight, setPreflight] = useState<ComparisonPreflight | null>(null);
  const [preflightError, setPreflightError] = useState("");
  const [comparison, setComparison] = useState<ComparisonResponse | null>(null);
  const [comparisonRows, setComparisonRows] = useState<ComparisonRow[]>([]);
  const [comparisonComplete, setComparisonComplete] = useState(true);
  const [comparisonError, setComparisonError] = useState("");
  const [comparisonLoading, setComparisonLoading] = useState(false);
  /** Which of the pair the ranking sorts by: the first measure or the second. */
  const [rankBy, setRankBy] = useState<"a" | "b">("a");
  const preflightTracker = useRef(createRequestTracker()).current;
  const comparisonTracker = useRef(createRequestTracker()).current;

  // The correlation: which coefficient is shown, the optional same-year pin,
  // the chosen pair once more than two measures are selected, and the answer.
  const [coefficient, setCoefficient] = useState<"pearson_r" | "spearman_rho">(
    "pearson_r",
  );
  const [yearPin, setYearPin] = useState<number | null>(null);
  const [chosenPair, setChosenPair] = useState<[string, string] | null>(null);
  const [correlation, setCorrelation] = useState<ComparisonCorrelation | null>(
    null,
  );
  const [matrix, setMatrix] = useState<ComparisonMatrix | null>(null);
  const [correlationError, setCorrelationError] = useState("");
  const [correlationLoading, setCorrelationLoading] = useState(false);
  const correlationTracker = useRef(createRequestTracker()).current;

  // The heatmap reads every geography at one grain, so it has its own request
  // and its own optional state scope -- not the series' pinned geography.
  const [heatmapRows, setHeatmapRows] = useState<ObservationRow[]>([]);
  const [heatmapStateFips, setHeatmapStateFips] = useState("");
  const [heatmapError, setHeatmapError] = useState("");
  const [heatmapLoading, setHeatmapLoading] = useState(false);
  const heatmapTracker = useRef(createRequestTracker()).current;

  const { token: accountToken } = useStoredToken();
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<SaveOutcome | null>(null);

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
    if (requested.alignmentGeoLevel) {
      setAlignmentGeoLevel(requested.alignmentGeoLevel);
    }
    if (requested.stateFips) {
      setAlignmentStateFips(requested.stateFips);
    }
    if (typeof requested.year === "number") {
      setYearPin(requested.year);
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

  // --- The cross-sectional reading (WB-2) -----------------------------------

  /** The two measures a cross-sectional presentation is about, if there are two. */
  const pair = useMemo(
    () => crossSectionalPair(series, chosenPair),
    [series, chosenPair],
  );

  /** Every pair the reader may draw, once more than two measures are on. */
  const pairChoices = useMemo(() => selectablePairs(series), [series]);

  /**
   * The grains the whole selection can be read at together.
   *
   * `sharedGrainOffer` is `lib/comparison`'s own function, generalised from
   * the pair the workspace asks about to the set this page composes, so both
   * screens reach the same conclusion from the same publication (WEB-074).
   */
  const grainOffer = useMemo(
    () =>
      sharedGrainOffer({
        metrics: [...new Set(series.map((entry) => entry.metricCode))].map(
          (code) => metricIndex[code],
        ),
        requested: alignmentGeoLevel,
      }),
    [series, metricIndex, alignmentGeoLevel],
  );

  useEffect(() => {
    setAlignmentGeoLevel((current) =>
      current && grainOffer.levels.includes(current)
        ? current
        : (grainOffer.levels.includes("COUNTY")
            ? "COUNTY"
            : (grainOffer.levels[0] ?? "")),
    );
  }, [grainOffer]);

  const crossSectionReason = useMemo(
    () =>
      crossSectionalRefusal({
        series,
        sharedGrains: grainOffer.levels,
        chosenPair: pair,
      }),
    [series, grainOffer, pair],
  );

  /**
   * The preflight for the pair, asked before any aligned data moves.
   *
   * Asked whenever there is a pair, not only when a cross-sectional
   * presentation is on screen: the verdict is what decides whether those
   * presentations may be offered at all, so asking it lazily would leave the
   * control enabled until a request came back and refused.
   */
  useEffect(() => {
    if (!pair) {
      setPreflight(null);
      setPreflightError("");
      return;
    }
    const request = preflightTracker.begin();
    (async () => {
      try {
        const verdict = await getComparisonPreflight({
          metric_code_a: pair[0],
          metric_code_b: pair[1],
        });
        if (request.isCurrent()) {
          setPreflight(verdict);
          setPreflightError("");
        }
      } catch (error) {
        if (request.isCurrent()) {
          setPreflight(null);
          setPreflightError(apiErrorMessage(error));
        }
      }
    })();
    return () => {
      preflightTracker.invalidate();
    };
  }, [pair, preflightTracker]);

  const preflightModel = useMemo(() => describePreflight(preflight), [preflight]);
  const comparable = mayRequestComparison(preflight);

  /**
   * The aligned rows, requested only where the preflight says they may be.
   *
   * `mayRequestComparison` is the same gate the comparison workspace uses,
   * called rather than reimplemented: the route enforces exactly the
   * preflight's verdict, so a screen that decided for itself when to ask
   * would be a second compatibility policy that could disagree with the one
   * the API publishes.
   *
   * Paged with the bounded reader at the route's declared 1,000-row limit, so
   * a full county grain arrives whole; `comparisonComplete` carries whether
   * the page bound cut it short, which the coverage note then states.
   */
  useEffect(() => {
    // The same rule as the correlation below: the rows on screen are dropped
    // before a new read is decided on, so a scatter or a ranking cannot go on
    // drawing geographies from a grain or a state scope the reader has left.
    setComparison(null);
    setComparisonRows([]);
    setComparisonComplete(true);
    setComparisonError("");
    if (!pair || !comparable || !alignmentGeoLevel || !isCrossSectional(presentation)) {
      setComparisonLoading(false);
      return;
    }
    const request = comparisonTracker.begin();
    setComparisonLoading(true);
    (async () => {
      try {
        const pages = await fetchComparisonPages(
          {
            metric_code_a: pair[0],
            metric_code_b: pair[1],
            geo_level: alignmentGeoLevel,
            ...(alignmentStateFips && alignmentGeoLevel !== "NATIONAL"
              ? { state_fips: alignmentStateFips }
              : {}),
          },
          { pageSize: COMPARISON_PAGE_SIZE, maxPages: COMPARISON_PAGE_LIMIT },
        );
        if (request.isCurrent()) {
          setComparison(pages.payload);
          setComparisonRows(pages.items);
          setComparisonComplete(pages.complete);
          setComparisonError("");
          setComparisonLoading(false);
        }
      } catch (error) {
        if (request.isCurrent()) {
          setComparison(null);
          setComparisonRows([]);
          setComparisonComplete(true);
          setComparisonError(apiErrorMessage(error));
          setComparisonLoading(false);
        }
      }
    })();
    return () => {
      comparisonTracker.invalidate();
    };
  }, [
    pair,
    comparable,
    alignmentGeoLevel,
    alignmentStateFips,
    presentation,
    comparisonTracker,
  ]);

  /** The whole envelope the scatter and ranking read: every paged row. */
  const alignedResponse = useMemo<ComparisonResponse | null>(
    () => (comparison ? { ...comparison, items: comparisonRows } : null),
    [comparison, comparisonRows],
  );

  const scatter = useMemo(
    () => comparisonScatterModel(alignedResponse),
    [alignedResponse],
  );

  const coverageNote = useMemo(
    () => describeComparisonCoverage(alignedResponse),
    [alignedResponse],
  );

  /**
   * The ranking: one bar per geography, sorted by the chosen side.
   *
   * A geography whose chosen side published no number is not a bar and is
   * counted instead — the same rule the scatter applies to a pair with a
   * missing side, and the same reason: zero is a published value a county can
   * really have.
   */
  const rankingBars = useMemo(() => {
    if (!pair) {
      return { bars: [], unpublished: 0 };
    }
    const field = rankBy === "a" ? "value_a" : "value_b";
    const periodField = rankBy === "a" ? "period_a" : "period_b";
    const otherPeriod = rankBy === "a" ? "period_b" : "period_a";
    const code = rankBy === "a" ? pair[0] : pair[1];
    const unit = metricUnit(metricIndex[code]) || UNPUBLISHED_UNIT_LABEL;
    const rows = [];
    let unpublished = 0;
    for (const row of comparisonRows) {
      const raw = (row as Record<string, unknown>)[field];
      const value = publishedNumber(raw);
      if (value === null) {
        unpublished += 1;
        continue;
      }
      rows.push({
        key: String(row.geo_id ?? ""),
        category: comparisonRowName(row),
        value,
        unit,
        groupKey: code,
        groupLabel: `${metricLabel(metricIndex[code]) || code}`,
        // Both periods ride the tooltip: the route combines each side's own
        // newest value, so a bar sorted by one side can be paired with a
        // different year on the other (WEB-049's rule, in a bar's terms).
        period: `${String((row as Record<string, unknown>)[periodField] ?? "")}${
          String((row as Record<string, unknown>)[otherPeriod] ?? "") &&
          String((row as Record<string, unknown>)[otherPeriod]) !==
            String((row as Record<string, unknown>)[periodField])
            ? ` against ${String((row as Record<string, unknown>)[otherPeriod])}`
            : ""
        }`,
      });
    }
    rows.sort((left, right) => right.value - left.value);
    return { bars: rows, unpublished };
  }, [comparisonRows, pair, rankBy, metricIndex]);

  /**
   * The national measures the reader could add to the ranking as a line.
   *
   * Offered from the selection itself rather than from a search: a reference
   * line is a measure already on the composition, drawn differently because
   * the cross-sectional axis cannot hold it as a geography.
   */
  const referenceLines = useMemo(() => {
    const axisUnit = pair
      ? metricUnit(metricIndex[rankBy === "a" ? pair[0] : pair[1]])
      : null;
    return plotted
      .map((entry) => {
        const metric = metricIndex[entry.series.metricCode];
        const offer = referenceLineOffer({
          grains: metricSupportedGeoLevels(metric),
          unit: metricUnit(metric),
          axisUnit,
          presentation,
        });
        const newest = entry.points[entry.points.length - 1];
        return { entry, offer, newest };
      })
      .filter((row) => row.offer.eligible && row.newest);
  }, [plotted, metricIndex, pair, rankBy, presentation]);

  // --- The geography by period heatmap (WB-2) -------------------------------

  /**
   * The measure the heatmap lays out.
   *
   * The first selected series' measure: the heatmap is one measure by
   * definition, and picking the first is the composition's own order rather
   * than a choice this page makes for the reader. A selection of several
   * measures still draws the first, and the caption names it.
   */
  const heatmapSeries = plotted[0] || null;

  const heatmap = useMemo(
    () => heatmapModel({ rows: heatmapRows, geographyNames }),
    [heatmapRows, geographyNames],
  );

  // --- The correlation (WB-5) -----------------------------------------------

  /**
   * Why each source's analysis routes decline it, from the capability entry.
   *
   * A source that declares no comparison routes is one the analysis surface
   * has already declined, so the correlation control names it before a
   * request is made rather than after a 422. Derived from the discovery
   * answer, never from a list of source codes here.
   */
  const analysisRefusals = useMemo(() => {
    const refusals: Record<string, string> = {};
    for (const source of sources) {
      if (!source.servesComparison) {
        refusals[source.sourceCode] =
          `${source.title} is not served by the aligned analysis routes: it ` +
          "publishes stratified, multi-dimensional or agency-grain " +
          "observations that a one-value-per-geography analysis would " +
          "silently collapse. Read it on the explorer with its own filters.";
      }
    }
    return refusals;
  }, [sources]);

  /**
   * Which correlation route each source's capability entry declares.
   *
   * Separate from `analysisRefusals` above because they are separate
   * declarations: the refusal says the aligned analysis surface declines the
   * source, and this says which of the two correlation routes it publishes
   * for the ones it does not decline. Inferring the second from the first is
   * what API-138 ended.
   */
  const declaredCorrelationRoutes = useMemo(() => {
    const declared: Record<string, { correlation: boolean; matrix: boolean }> = {};
    for (const source of sources) {
      declared[source.sourceCode] = {
        correlation: source.servesCorrelation,
        matrix: source.servesMatrix,
      };
    }
    return declared;
  }, [sources]);

  const correlationOffer = useMemo(
    () =>
      correlationEligibility({
        series,
        analysisRefusals,
        declaredRoutes: declaredCorrelationRoutes,
        preflightBlocking: preflightModel.blocking,
        preflightRead: Boolean(preflight) || !pair,
      }),
    [
      series,
      analysisRefusals,
      declaredCorrelationRoutes,
      preflightModel,
      preflight,
      pair,
    ],
  );

  /**
   * Why the heatmap cannot be drawn, or "".
   *
   * It is one measure at one grain over its own settled history, so the only
   * things that stop it are having nothing selected and the selected measure
   * having published nothing. A selection of several measures still draws the
   * first; the caption names which.
   */
  const heatmapReason = useMemo(() => {
    if (series.length === 0) {
      return "Add a measure to lay out.";
    }
    const first = plotted[0];
    if (!first || first.points.length === 0) {
      return (
        `${first?.label || series[0]?.metricCode} published no values over ` +
        "this read, so there is nothing to lay out. An empty answer is not a " +
        "grid of zeroes."
      );
    }
    return "";
  }, [series, plotted]);

  const offer = useMemo(
    () =>
      presentationOffer({
        series,
        facts: plotted.map((entry) => ({
          key: entry.key,
          metricCode: entry.series.metricCode,
          periodCount: new Set(entry.points.map((point) => point.period)).size,
        })),
        // The selection's own refusals first; then the API's verdict on the
        // pair, presented as the preflight worded it rather than paraphrased.
        crossSectionalReason:
          crossSectionReason ||
          (preflightError
            ? `The compatibility verdict could not be read: ${preflightError}`
            : !preflight
              ? "Checking whether these two measures may be read together…"
              : !comparable
                ? `Not comparable: ${preflightModel.blocking
                    .map((rule) => rule.reason)
                    .join("; ")}`
                : ""),
        // The correlation's own eligibility, which is a different route's
        // question than the scatter's and the ranking's.
        correlationReason: correlationOffer.reason,
        heatmapReason: heatmapReason,
      }),
    [
      series,
      plotted,
      crossSectionReason,
      preflight,
      preflightError,
      preflightModel,
      comparable,
      correlationOffer,
      heatmapReason,
    ],
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

  useEffect(() => {
    // Drop the answer already on screen *before* deciding whether to ask for
    // a new one, so it can never outlive the selection it describes.
    //
    // The guard below returns early for an ineligible selection, which used
    // to leave a previous answer in state. It was invisible while the
    // presentation fell back — and then rendered again, unchanged, the moment
    // the selection became eligible once more, or while a narrowed read was
    // in flight. A coefficient measured over every state, shown against a
    // selection narrowed to one, is the worst kind of wrong: a plausible
    // number, correctly formatted, about something else. `createRequestTracker`
    // stops a *stale response* being committed; nothing stopped a stale
    // *answer* being kept, and these are different failures.
    setCorrelation(null);
    setMatrix(null);
    setCorrelationError("");
    if (
      !correlationOffer.eligible ||
      !alignmentGeoLevel ||
      effectivePresentation !== "correlation"
    ) {
      setCorrelationLoading(false);
      return;
    }
    const request = correlationTracker.begin();
    setCorrelationLoading(true);
    (async () => {
      const filters = {
        geo_level: alignmentGeoLevel,
        ...(alignmentStateFips && alignmentGeoLevel !== "NATIONAL"
          ? { state_fips: alignmentStateFips }
          : {}),
        ...(yearPin ? { year: String(yearPin) } : {}),
      };
      try {
        if (correlationOffer.route === "correlation" && pair) {
          const answer = await getComparisonCorrelation({
            metric_code_a: pair[0],
            metric_code_b: pair[1],
            ...filters,
          });
          if (request.isCurrent()) {
            setCorrelation(answer);
            setMatrix(null);
            setCorrelationError("");
          }
        } else {
          const answer = await getComparisonMatrix({
            metric_codes: [
              ...new Set(series.map((entry) => entry.metricCode)),
            ].join(","),
            ...filters,
          });
          if (request.isCurrent()) {
            setMatrix(answer);
            setCorrelation(null);
            setCorrelationError("");
          }
        }
      } catch (error) {
        if (request.isCurrent()) {
          setCorrelation(null);
          setMatrix(null);
          setCorrelationError(apiErrorMessage(error));
        }
      } finally {
        if (request.isCurrent()) {
          setCorrelationLoading(false);
        }
      }
    })();
    return () => {
      correlationTracker.invalidate();
    };
  }, [
    correlationOffer,
    alignmentGeoLevel,
    alignmentStateFips,
    yearPin,
    effectivePresentation,
    pair,
    series,
    correlationTracker,
  ]);

  /**
   * The reason a coefficient is null, taken from the answer's own caveats.
   *
   * The API states it ("no coefficient is reported: 2 paired geographies is
   * fewer than…"), so the panel presents that sentence rather than composing
   * its own from `n` — which would be this screen re-deriving a rule the API
   * owns and could change.
   */
  const nullCoefficientReason = useMemo(
    () =>
      (correlation?.caveats || []).find((caveat) =>
        caveat.startsWith("no coefficient is reported"),
      ) || "",
    [correlation],
  );

  const readings = useMemo(
    () =>
      correlationReadings(correlation, {
        geographiesA: correlation?.geographies_a,
        geographiesB: correlation?.geographies_b,
        nullReason: nullCoefficientReason,
      }),
    [correlation, nullCoefficientReason],
  );

  /**
   * The years the same-year pin offers.
   *
   * Read from the periods the selected measures actually published, so the
   * control cannot ask for a year no side has rows in — which would answer an
   * empty correlation and report it as a coverage problem. Newest first,
   * because a reader pinning a year is almost always pinning a recent one.
   */
  const pinnableYears = useMemo(() => {
    const years = new Set<number>();
    for (const entry of plotted) {
      for (const point of entry.points) {
        const year = Number(String(point.period).slice(0, 4));
        if (Number.isInteger(year) && year > 1000) {
          years.add(year);
        }
      }
    }
    return [...years].sort((left, right) => right - left);
  }, [plotted]);

  const matrixModel = useMemo(
    () =>
      matrix
        ? correlationMatrixModel({
            codes: (matrix.metrics || []).map((entry) =>
              String(entry.metric_code ?? ""),
            ),
            pairs: matrix.pairs || [],
            which: coefficient,
          })
        : null,
    [matrix, coefficient],
  );

  // --- Saving the composition (WB-6) ----------------------------------------

  /**
   * The document this composition saves as.
   *
   * Built from the same state the chart is drawn from, so what is stored and
   * what is on screen cannot disagree — the WEB-081 rule: a saved view records
   * the request it issued, not the selection it was built out of. The
   * alignment rides only a cross-sectional presentation, because a
   * longitudinal composition has no shared grain and storing one would invent
   * the roll-up this surface refuses.
   */
  const document = useMemo(
    () =>
      workbenchDocument({
        series: series.map((entry) => ({
          metricCode: entry.metricCode,
          scope: entry.scope,
          release: entry.release,
          geoLevel: entry.geoLevel,
          geoId: entry.geoId,
          filters: entry.filters,
        })),
        presentation: effectivePresentation,
        alignment: isCrossSectional(effectivePresentation)
          ? {
              geoLevel: alignmentGeoLevel,
              stateFips: alignmentStateFips,
              year: yearPin,
            }
          : null,
      }),
    [
      series,
      effectivePresentation,
      alignmentGeoLevel,
      alignmentStateFips,
      yearPin,
    ],
  );

  const saveTitle = useMemo(
    () =>
      plotted.length > 0
        ? `${PRESENTATION_LABELS[effectivePresentation]}: ${plotted
            .map((entry) => entry.label)
            .join(", ")}`
        : "Workbench composition",
    [plotted, effectivePresentation],
  );

  const onSave = useCallback(async () => {
    if (series.length === 0) {
      return;
    }
    const destination = saveDestination(accountToken);
    if (destination === "account") {
      setSaving(true);
      setSaveStatus({
        state: "loading",
        message: "Saving to your account",
        destination: null,
      });
      try {
        await createSavedAnalysis(accountToken, {
          name: saveTitle,
          document,
        });
        setSaveStatus(describeSaveSuccess("account", saveTitle));
      } catch (error) {
        // Reported where the reader asked for it, and never redirected to the
        // browser store: a save they were told went to their account and
        // silently did not is worse than a save that failed.
        setSaveStatus(describeSaveFailure(error));
      } finally {
        setSaving(false);
      }
      window.setTimeout(() => setSaveStatus(null), 4000);
      return;
    }

    const localSave = saveChart({
      id: `workbench:${series.map((entry) => seriesKey(entry)).join("~")}`,
      version: 1,
      title: saveTitle,
      chartType: "workbench",
      presentation: effectivePresentation,
      // One envelope per series, so the evidence packet's completeness rule
      // sees every series' source, measure, grain, geography, period, release
      // and caveats rather than one envelope for a composition of eight.
      series: plotted.map((entry) => ({
        metricCode: entry.series.metricCode,
        source: entry.series.sourceCode,
        geoLevel: entry.series.geoLevel,
        geoId: entry.series.geoId,
        unit: entry.unitUnpublished ? null : entry.unit,
        period: entry.points.length > 0
          ? entry.points[entry.points.length - 1]!.period
          : null,
        release: entry.points.length > 0
          ? String(entry.points[entry.points.length - 1]!.row?.release || "") || null
          : null,
        droppedPeriods: entry.droppedPeriods,
        truncated: entry.truncated,
        filters: entry.series.filters,
      })),
      caveats: [
        ...(correlation?.caveats || []),
        ...(matrix?.caveats || []),
        ...preflightModel.caveats,
      ],
      transformation: isCrossSectional(effectivePresentation)
        ? "api-derived"
        : "published",
      document,
      savedAt: new Date().toISOString(),
    });
    setSaveStatus(describeLocalSave(localSave, saveTitle, SAVED_CHART_LIMIT));
    window.setTimeout(() => setSaveStatus(null), 4000);
  }, [
    series,
    accountToken,
    saveTitle,
    document,
    plotted,
    effectivePresentation,
    correlation,
    matrix,
    preflightModel,
  ]);

  /**
   * The heatmap's own read: every geography at one grain, not one geography.
   *
   * It cannot reuse the series' loaded rows. Each series is read with its
   * geography pinned — that is what a series *is* — so laying those rows out
   * as geographies × periods produces a grid one row tall, which looks like a
   * heatmap and is a single line. This asks for the same settled history at
   * the grain instead, with no geography pinned.
   */
  useEffect(() => {
    setHeatmapRows([]);
    setHeatmapError("");
    if (!heatmapSeries || effectivePresentation !== "heatmap") {
      setHeatmapLoading(false);
      return;
    }
    const source = findExplorerSource(sources, heatmapSeries.series.sourceKey);
    const request = buildSettledSurfaceRequest(source, {
      metricCode: heatmapSeries.series.metricCode,
      geoLevel: heatmapSeries.series.geoLevel,
      stateFips: heatmapStateFips || undefined,
      dimensions: heatmapSeries.series.filters,
    });
    if (!request) {
      setHeatmapError(
        `${source?.title || heatmapSeries.series.sourceCode} publishes no ` +
          "settled-history route, so a geography × period layout cannot be " +
          "read for it without mixing releases.",
      );
      return;
    }
    const tracked = heatmapTracker.begin();
    setHeatmapLoading(true);
    (async () => {
      try {
        const page = await fetchCollectionPages<Observation>(request.resource, {
          params: request.params,
          pageSize: HISTORY_PAGE_SIZE,
          maxPages: HISTORY_PAGE_LIMIT,
        });
        if (tracked.isCurrent()) {
          setHeatmapRows(
            normalizeObservationRows(source, page.items) as ObservationRow[],
          );
          setHeatmapError("");
          setHeatmapLoading(false);
        }
      } catch (error) {
        if (tracked.isCurrent()) {
          setHeatmapRows([]);
          setHeatmapError(apiErrorMessage(error));
          setHeatmapLoading(false);
        }
      }
    })();
    return () => {
      heatmapTracker.invalidate();
    };
  }, [
    heatmapSeries,
    effectivePresentation,
    heatmapStateFips,
    sources,
    heatmapTracker,
  ]);

  // --- Export (WB-7) --------------------------------------------------------

  /**
   * Every dimension the selected sources declare, one column each.
   *
   * The declared set rather than the filterable subset, and the union across
   * the composition's sources rather than one source's: a file carrying a
   * subset would be this client deciding which part of a source's published
   * description a reader may have (WEB-061), and a composition has several
   * descriptions in it.
   */
  const exportDimensions = useMemo(() => {
    const names = new Set<string>();
    for (const entry of series) {
      const source = findExplorerSource(sources, entry.sourceKey);
      for (const name of source?.publishedDimensions || []) {
        names.add(name);
      }
    }
    return [...names].sort();
  }, [series, sources]);

  const onExport = useCallback(() => {
    const { headings, rows } = workbenchExport(plotted, {
      dimensions: exportDimensions,
      geographyNames,
      // The coefficients travel only where one was asked for and answered,
      // and they are the only rows the file marks `derived`.
      correlation:
        correlation && pair
          ? {
              metricCodeA: pair[0],
              metricCodeB: pair[1],
              readings,
            }
          : null,
    });
    const escape = (value: unknown) =>
      `"${String(value ?? "").replaceAll('"', '""')}"`;
    const blob = new Blob(
      [[headings, ...rows].map((row) => row.map(escape).join(",")).join("\n")],
      { type: "text/csv;charset=utf-8" },
    );
    const link = window.document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = workbenchExportFilename({
      presentation: effectivePresentation,
      plotted,
    });
    link.click();
    URL.revokeObjectURL(link.href);
  }, [
    plotted,
    exportDimensions,
    geographyNames,
    correlation,
    pair,
    readings,
    effectivePresentation,
  ]);

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
      // Carried only where it means something: a shared grain and a state
      // scope describe a cross-sectional reading, and putting them in a link
      // to a line chart would reopen controls that are not on screen.
      ...(isCrossSectional(effectivePresentation)
        ? {
            alignmentGeoLevel: (alignmentGeoLevel || undefined) as never,
            stateFips: alignmentStateFips || undefined,
          }
        : {}),
      // The year pin and the correlation toggle are carried only where they
      // describe the answer on screen, for the same reason the grain is: a
      // link to a line chart should not reopen controls that are not on it.
      ...(effectivePresentation === "correlation"
        ? { correlation: true, year: yearPin ?? undefined }
        : {}),
    }),
    [
      series,
      effectivePresentation,
      alignmentGeoLevel,
      alignmentStateFips,
      yearPin,
    ],
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

          {isCrossSectional(effectivePresentation) && pair ? (
            <div className="selector-grid" data-testid="workbench-alignment">
              <div className="control-group">
                <label htmlFor="workbench-alignment-grain">Shared grain</label>
                <select
                  id="workbench-alignment-grain"
                  className="select"
                  value={alignmentGeoLevel}
                  onChange={(event) => setAlignmentGeoLevel(event.target.value)}
                  data-testid="workbench-alignment-grain"
                >
                  {grainOffer.levels.map((level) => (
                    <option key={level} value={level}>
                      {GEO_GRAIN_LABELS[level]?.one || level}
                    </option>
                  ))}
                </select>
              </div>
              {alignmentGeoLevel !== "NATIONAL" ? (
                <div className="control-group">
                  <label htmlFor="workbench-alignment-state">State scope</label>
                  <select
                    id="workbench-alignment-state"
                    className="select"
                    value={alignmentStateFips}
                    onChange={(event) =>
                      setAlignmentStateFips(event.target.value)
                    }
                    data-testid="workbench-alignment-state"
                  >
                    <option value="">Every state</option>
                    {states.map((row) => (
                      <option key={String(row.geo_id)} value={String(row.state_fips)}>
                        {String(row.state_name)}
                      </option>
                    ))}
                  </select>
                </div>
              ) : null}
              {effectivePresentation === "ranking" ? (
                <div className="control-group">
                  <label htmlFor="workbench-rank-by">Sort by</label>
                  <select
                    id="workbench-rank-by"
                    className="select"
                    value={rankBy}
                    onChange={(event) =>
                      setRankBy(event.target.value === "b" ? "b" : "a")
                    }
                    data-testid="workbench-rank-by"
                  >
                    <option value="a">
                      {metricLabel(metricIndex[pair[0]]) || pair[0]}
                    </option>
                    <option value="b">
                      {metricLabel(metricIndex[pair[1]]) || pair[1]}
                    </option>
                  </select>
                </div>
              ) : null}
            </div>
          ) : null}

          {pairChoices.length > 1 && isCrossSectional(effectivePresentation) ? (
            <div className="control-group" data-testid="workbench-pair-chooser">
              <label htmlFor="workbench-pair">Pair to draw</label>
              <select
                id="workbench-pair"
                className="select"
                value={pair ? `${pair[0]}|${pair[1]}` : ""}
                onChange={(event) => {
                  const [left, right] = event.target.value.split("|");
                  setChosenPair(left && right ? [left, right] : null);
                }}
                data-testid="workbench-pair"
              >
                <option value="">Choose a pair</option>
                {pairChoices.map(([left, right]) => (
                  <option key={`${left}|${right}`} value={`${left}|${right}`}>
                    {`${metricLabel(metricIndex[left]) || left} against ${
                      metricLabel(metricIndex[right]) || right
                    }`}
                  </option>
                ))}
              </select>
            </div>
          ) : null}

          {effectivePresentation === "correlation" ? (
            <div className="selector-grid" data-testid="workbench-correlation-controls">
              <div className="control-group">
                <label htmlFor="workbench-coefficient">Coefficient</label>
                <select
                  id="workbench-coefficient"
                  className="select"
                  value={coefficient}
                  onChange={(event) =>
                    setCoefficient(
                      event.target.value === "spearman_rho"
                        ? "spearman_rho"
                        : "pearson_r",
                    )
                  }
                  data-testid="workbench-coefficient"
                >
                  <option value="pearson_r">Pearson r</option>
                  <option value="spearman_rho">Spearman ρ</option>
                </select>
              </div>
              <div className="control-group">
                <label htmlFor="workbench-year-pin">Same-year pin</label>
                <select
                  id="workbench-year-pin"
                  className="select"
                  value={yearPin === null ? "" : String(yearPin)}
                  onChange={(event) =>
                    setYearPin(
                      event.target.value ? Number(event.target.value) : null,
                    )
                  }
                  data-testid="workbench-year-pin"
                >
                  <option value="">
                    Off — each side&apos;s own newest value
                  </option>
                  {pinnableYears.map((year) => (
                    <option key={year} value={String(year)}>
                      {year}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          ) : null}

          {correlationLoading ? (
            <p className="subtle" data-testid="workbench-correlation-loading">
              Asking the API for the coefficient…
            </p>
          ) : null}

          {correlationError ? (
            <p className="notice error" data-testid="workbench-correlation-error">
              {correlationError}
            </p>
          ) : null}

          {effectivePresentation === "correlation" && correlation ? (
            <CorrelationPanel
              readings={readings}
              caveats={correlation.caveats || []}
              year={correlation.year ?? null}
              periodA={correlation.period_a ?? null}
              periodB={correlation.period_b ?? null}
            />
          ) : null}

          {effectivePresentation === "correlation" && matrixModel && matrix ? (
            <>
              <CorrelationMatrixChart
                model={matrixModel}
                which={coefficient}
                labelFor={(code) => metricLabel(metricIndex[code]) || code}
              />
              {(matrix.caveats || []).length > 0 ? (
                <p className="notice" data-testid="workbench-matrix-causation">
                  <strong>{(matrix.caveats || [])[0]}</strong>
                </p>
              ) : null}
              <ul className="subtle" data-testid="workbench-matrix-pairs">
                {(matrix.pairs || []).map((entry) => {
                  const code = `${entry.metric_code_a}|${entry.metric_code_b}`;
                  const label = `${
                    metricLabel(metricIndex[String(entry.metric_code_a)]) ||
                    entry.metric_code_a
                  } against ${
                    metricLabel(metricIndex[String(entry.metric_code_b)]) ||
                    entry.metric_code_b
                  }`;
                  return (
                    <li key={code} data-pair={code}>
                      <strong>{label}</strong>:{" "}
                      {entry.comparable === false
                        ? (entry.rules || [])
                            .filter((rule) => rule.status === "fail")
                            .map((rule) => rule.reason)
                            .join("; ")
                        : (entry.caveats || []).join(" ") ||
                          pairedGeographiesText(entry.statistic)}
                    </li>
                  );
                })}
              </ul>
            </>
          ) : null}


          {isCrossSectional(effectivePresentation) && grainOffer.note ? (
            <p className="subtle" data-testid="workbench-grain-note">
              {grainOffer.note}
              {grainOffer.absent
                .filter((entry) => entry.withoutIt.length > 0)
                .map((entry) => (
                  <span key={entry.level} data-absent-grain={entry.level}>
                    {` ${GEO_GRAIN_LABELS[entry.level]?.one || entry.level} is ` +
                      `not offered because ${entry.withoutIt.join(", ")} ` +
                      `${entry.withoutIt.length === 1 ? "does" : "do"} not publish it.`}
                  </span>
                ))}
            </p>
          ) : null}

          {pair && preflight ? (
            <div className="status-row" role="status">
              <StatusPill
                {...compatibilityState(preflight)}
                label="Compatibility"
                testId="workbench-preflight-status"
              />
            </div>
          ) : null}

          {pair && preflight && !comparable ? (
            <div className="notice error" data-testid="workbench-incomparable">
              <ul>
                {preflightModel.blocking.map((rule) => (
                  <li key={rule.rule} data-rule={rule.rule}>
                    {rule.reason}
                  </li>
                ))}
              </ul>
              <ul>
                {incompatibleAlternatives(preflight).map((alternative) => (
                  <li key={alternative}>{alternative}</li>
                ))}
              </ul>
            </div>
          ) : null}

          {pair && preflightModel.unverified.length > 0 ? (
            <ul className="subtle" data-testid="workbench-preflight-caveats">
              {preflightModel.unverified.map((rule) => (
                <li key={rule.rule}>{rule.reason}</li>
              ))}
            </ul>
          ) : null}

          {comparisonLoading ? (
            <p className="subtle" data-testid="workbench-comparison-loading">
              Reading the aligned rows…
            </p>
          ) : null}

          {comparisonError ? (
            <p className="notice error" data-testid="workbench-comparison-error">
              {comparisonError}
            </p>
          ) : null}

          {offer[effectivePresentation].available &&
          effectivePresentation === "scatter" &&
          pair ? (
            <ScatterChart
              model={scatter}
              labelX={metricLabel(metricIndex[pair[0]]) || pair[0]}
              labelY={metricLabel(metricIndex[pair[1]]) || pair[1]}
              testId="workbench-scatter"
            />
          ) : null}

          {offer[effectivePresentation].available &&
          effectivePresentation === "ranking" &&
          pair ? (
            <>
              <BarChart
                bars={rankingBars.bars}
                orientation="geography"
                colorOf={() => seriesStroke(rankBy === "a" ? 0 : 1)}
                unpublished={rankingBars.unpublished}
                label={
                  `Ranking of ${rankingBars.bars.length} geographies by ` +
                  `${metricLabel(metricIndex[rankBy === "a" ? pair[0] : pair[1]]) || ""}` +
                  `, highest first. ${rankingBars.unpublished} published no value.`
                }
                testId="workbench-ranking"
              />
              {referenceLines.length > 0 ? (
                <ul className="subtle" data-testid="workbench-reference-lines">
                  {referenceLines.map((row) => (
                    <li key={row.entry.key} data-series-key={row.entry.key}>
                      <strong>{row.entry.label}</strong>:{" "}
                      {formatNumber(row.newest!.value)} {row.entry.unit} (
                      {row.newest!.period}). {row.offer.reason}
                    </li>
                  ))}
                </ul>
              ) : null}
            </>
          ) : null}

          {isCrossSectional(effectivePresentation) && coverageNote ? (
            <p className="subtle" data-testid="workbench-coverage">
              {coverageNote}
            </p>
          ) : null}

          {isCrossSectional(effectivePresentation) && !comparisonComplete ? (
            <p className="notice" data-testid="workbench-comparison-partial">
              The page bound cut this read short, so the geographies shown are a
              prefix of the ones the route paired. Narrow to a state to see the
              rest.
            </p>
          ) : null}

          {effectivePresentation === "heatmap" && heatmapSeries ? (
            <div className="selector-grid" data-testid="workbench-heatmap-controls">
              <div className="control-group">
                <label htmlFor="workbench-heatmap-state">State scope</label>
                <select
                  id="workbench-heatmap-state"
                  className="select"
                  value={heatmapStateFips}
                  onChange={(event) => setHeatmapStateFips(event.target.value)}
                  data-testid="workbench-heatmap-state"
                >
                  <option value="">Every geography at this grain</option>
                  {states.map((row) => (
                    <option key={String(row.geo_id)} value={String(row.state_fips)}>
                      {String(row.state_name)}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          ) : null}

          {heatmapLoading ? (
            <p className="subtle" data-testid="workbench-heatmap-loading">
              Reading every geography&apos;s settled history at this grain…
            </p>
          ) : null}

          {effectivePresentation === "heatmap" && heatmapError ? (
            <p className="notice error" data-testid="workbench-heatmap-error">
              {heatmapError}
            </p>
          ) : null}

          {offer[effectivePresentation].available &&
          effectivePresentation === "heatmap" &&
          heatmap &&
          heatmapSeries ? (
            <HeatmapChart
              model={heatmap}
              measureLabel={describeSeries(
                heatmapSeries,
                geographyNames[heatmapSeries.series.geoId],
              )}
              unit={heatmapSeries.unit}
            />
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
                        <td>{formatNumber(point.value)}</td>
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

      {saveStatus ? (
        <div
          className="save-toast"
          data-state={saveStatus.state}
          data-destination={saveStatus.destination || ""}
          data-testid="workbench-save-toast"
          role="status"
        >
          {saveStatus.message}
        </div>
      ) : null}

      <section className="grid">
        <div className="card span-2">
          <h2>Save</h2>
          <p className="subtle">
            A saved workbench stores the composition, not the values: which
            measures, at which geographies, read how, drawn as what. It reopens
            against the live publication, so it follows the warehouse rather
            than freezing a copy of it.
          </p>
          <div className="command-row">
            <button
              type="button"
              className="button primary"
              onClick={onSave}
              disabled={saving || series.length === 0}
              title={
                saveDestination(accountToken) === "account"
                  ? "Saves to your account"
                  : "Saves in this browser only; sign in on Saved analyses to keep it"
              }
              data-testid="workbench-save"
              data-destination={saveDestination(accountToken)}
            >
              <Save size={15} />{" "}
              {saveDestination(accountToken) === "account"
                ? "Save to account"
                : "Save in browser"}
            </button>
            <button
              type="button"
              className="button secondary"
              onClick={onExport}
              disabled={plotted.every((entry) => entry.points.length === 0)}
              data-testid="workbench-export"
            >
              <Download size={15} /> Export CSV
            </button>
          </div>
          <p className="subtle" data-testid="workbench-export-note">
            The file carries one row per plotted value with its full envelope —
            every published uncertainty and coverage field, the period, the
            release and the scope — and a <code>derived</code> column that is
            true only for the API-computed coefficients. A read the page bound
            cut short is named a prefix in the file name, because the screen
            says so and the file has to say it too.
          </p>
        </div>
      </section>

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
