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
  createSavedAnalysis,
  fetchAllPages,
  getCapabilities,
  fetchComparisonPages,
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
import { requestedMetricState } from "../lib/requestedMetric";
import { ACTIVE_GEOGRAPHIES_ONLY } from "../lib/observationAccess";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  DEFAULT_COMPARISON_SELECTION,
  comparisonCells,
  comparisonColumns,
  comparisonExport,
  comparisonGrainOffer,
  comparisonMapRows,
  describeComparisonCoverage,
  mapPeriodMismatchNote,
  comparisonMetricOptions,
  comparisonRequestParams,
  comparisonScatterModel,
  compatibilityState,
  defaultDerivedField,
  describePreflight,
  incompatibleAlternatives,
  mayRequestComparison,
  periodsDiffer,
  preferredComparisonGrain,
  preflightRequestParams,
  selectionIsComplete,
} from "../lib/comparison";
import { GEO_GRAIN_LABELS } from "../lib/geographyPicker";
import type {
  ComparisonLoad,
  ComparisonSelection,
  ComparisonSide,
} from "../lib/comparison";
import { SAVED_CHART_LIMIT, saveChart } from "../lib/savedCharts";
import { useStoredToken } from "../lib/apiToken";
import {
  comparisonDocument,
  describeLocalSave,
  describeSaveFailure,
  describeSaveSuccess,
  saveDestination,
} from "../lib/savedAnalysis";
import type { SaveOutcome } from "../lib/savedAnalysis";
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
import { formatNumber } from "../lib/format";
import { tableCaption, tablePageModel, tablePageRows } from "../lib/tablePage";

const DEFAULT_GEO_LEVEL = "COUNTY";
const CATALOG_PAGE_SIZE = 1000;
const COMPARISON_PAGE_SIZE = 1000;
// Eight pages reach 8,000 aligned geographies: a national county
// comparison is 3,144, with room for a grain that grows.
const COMPARISON_PAGE_LIMIT = 8;
const SIDES = ["a", "b"] as const;

type SideKey = (typeof SIDES)[number];

interface RequestStatus {
  state: string;
  message: string;
}

const SIDE_LABEL: Record<SideKey, string> = { a: "Measure A", b: "Measure B" };

export default function ComparisonWorkspace() {
  const capabilitiesTracker = useRef(createRequestTracker()).current;
  const metricsTrackerA = useRef(createRequestTracker()).current;
  const metricsTrackerB = useRef(createRequestTracker()).current;
  // Each tracker is stable; the object holding them was not, so an effect
  // depending on it re-ran every render and the dependency had to be
  // suppressed. Memoised, it can be a real dependency.
  const metricsTrackers: Record<SideKey, ReturnType<typeof createRequestTracker>> =
    useMemo(
      () => ({ a: metricsTrackerA, b: metricsTrackerB }),
      [metricsTrackerA, metricsTrackerB],
    );
  const preflightTracker = useRef(createRequestTracker()).current;
  const comparisonTracker = useRef(createRequestTracker()).current;
  const geographyTracker = useRef(createRequestTracker()).current;
  const tileTracker = useRef(createRequestTracker()).current;
  // The requested link state, applied once each side's catalog arrives so a
  // shared link reopens the same pair rather than a default one.
  const requestedRef = useRef<ReturnType<typeof parseComparisonState> | null>(null);
  // The discovered sources, mirrored for the one place that needs the current
  // list without wanting to re-run when it changes: the catalog effect below
  // names a source in a notice, and re-fetching both catalogs because a source
  // title arrived would be a request nothing asked for. Naming it a ref says
  // that out loud, where suppressing the dependency rule said nothing.
  const sourcesRef = useRef<ExplorerSource[]>([]);
  const [tablePage, setTablePage] = useState(0);

  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [sourcesError, setSourcesError] = useState("");
  const [selection, setSelection] = useState<ComparisonSelection>(
    DEFAULT_COMPARISON_SELECTION,
  );
  const [metrics, setMetrics] = useState<Record<SideKey, MetricSummary[]>>({ a: [], b: [] });
  const [metricsError, setMetricsError] = useState<Record<SideKey, string>>({ a: "", b: "" });
  // What a link asked for on each side that the side's source does not
  // publish (WEB-072).
  const [requestedMetricNotice, setRequestedMetricNotice] = useState<
    Record<SideKey, string>
  >({ a: "", b: "" });
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
  // How much of the aligned answer is loaded. The pill states it; the export
  // needs it too, because the file outlives the pill (WEB-067).
  const [comparisonLoad, setComparisonLoad] = useState<ComparisonLoad>({
    loaded: 0,
    total: null,
    complete: true,
  });
  const [comparisonStatus, setComparisonStatus] = useState<RequestStatus>({
    state: "idle",
    message: "waiting for a compatibility verdict",
  });
  const [saveStatus, setSaveStatus] = useState<SaveOutcome | null>(null);
  const [saving, setSaving] = useState(false);
  // Decides where a save goes. Never rendered, never put in a URL.
  const { token: accountToken } = useStoredToken();

  const sourceOf = useCallback(
    (side: SideKey) => findExplorerSource(sources, selection[side].sourceCode),
    [sources, selection],
  );

  // Capability discovery decides which sources can be named at all; nothing
  // here carries a client-side source list.
  useEffect(() => {
    const request = capabilitiesTracker.begin();
    requestedRef.current = parseComparisonState(window.location.search);
    if (typeof requestedRef.current.tablePage === "number") {
      setTablePage(requestedRef.current.tablePage);
    }

    async function loadCapabilities() {
      try {
        const payload = await getCapabilities();
        const discovered = buildExplorerSources(payload.items);
        if (!request.isCurrent()) {
          return;
        }
        setSources(discovered);
        sourcesRef.current = discovered;
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
          // A link that names a measure this side's source does not publish
          // is said out loud rather than answered with `items[0]`. Reopening
          // a saved BLS-versus-FRED comparison on the first two discovered
          // sources ran a real preflight, and a real comparison, on a pair
          // the reader never saved (WEB-072).
          const resolved = requestedMetricState({
            requested: wanted,
            items,
            sourceTitle: findExplorerSource(sourcesRef.current, sourceCode)?.title,
          });
          setRequestedMetricNotice((current) => ({
            ...current,
            [side]: resolved.notice,
          }));
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
            const chosen = resolved.metricCode
              ? resolved.metricCode
              : resolved.chooseDefault
                ? items[0]?.metric_code || ""
                : "";
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
  }, [metricsTrackers, sourceCodes]);

  useEffect(() => {
    const request = geographyTracker.begin();
    (async () => {
      try {
        const items = await fetchAllPages<GeographySummary>("/catalog/geographies", {
          params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "STATE" },
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
  // The four fields a comparison request is built from, named individually so
  // the effects below depend on the values they actually send rather than on
  // the whole selection object.
  const metricCodeA = selection.a.metricCode;
  const metricCodeB = selection.b.metricCode;
  const selectionGeoLevel = selection.geoLevel;
  const selectionStateFips = selection.stateFips;

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
            a: { metricCode: metricCodeA },
            b: { metricCode: metricCodeB },
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
  }, [preflightTracker, metricCodeA, metricCodeB]);

  const comparable = mayRequestComparison(preflight);

  useEffect(() => {
    setComparison(null);
    // The pair this effect would send, not the pair the verdict was given
    // for. Both measures are now dependencies -- the request is built from
    // them -- so the effect runs on the intermediate state where one side has
    // been cleared and the previous pair's `comparable` verdict is still
    // held. Requesting there sends a comparison naming one measure.
    if (!metricCodeA || !metricCodeB) {
      setComparisonStatus({ state: "idle", message: "select two measures" });
      return;
    }
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
        // Paged: the route caps `limit` at 1000 and a national county
        // comparison aligns 3,144 geographies, so one request drew the
        // scatter, the map, and the export from the first thousand rows by
        // geo_id (WEB-039).
        const pages = await fetchComparisonPages(
          comparisonRequestParams(
            {
              a: { metricCode: metricCodeA },
              b: { metricCode: metricCodeB },
              geoLevel: selectionGeoLevel,
              stateFips: selectionStateFips,
            },
            COMPARISON_PAGE_SIZE,
          ),
          { pageSize: COMPARISON_PAGE_SIZE, maxPages: COMPARISON_PAGE_LIMIT },
        );
        if (!request.isCurrent()) {
          return;
        }
        setComparison(pages.payload);
        // Kept, not only rendered: the file the export writes outlives the
        // pill that states the shortfall (WEB-067).
        setComparisonLoad({
          loaded: pages.items.length,
          total: pages.total,
          complete: pages.complete,
        });
        setComparisonStatus({
          state: pages.complete ? "ok" : "bad",
          message: pages.complete
            ? `${pages.items.length} aligned geographies`
            : `loaded ${pages.items.length} of ${pages.total} aligned geographies; ` +
              "the page bound cut the answer short, so this comparison is incomplete",
        });
      } catch (error) {
        if (request.isCurrent()) {
          setComparison(null);
          setComparisonLoad({ loaded: 0, total: null, complete: true });
          setComparisonStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    })();

    return () => {
      comparisonTracker.invalidate();
    };
  }, [
    comparisonTracker,
    preflight,
    comparable,
    metricCodeA,
    metricCodeB,
    selectionGeoLevel,
    selectionStateFips,
  ]);

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
        tablePage,
      },
      { geoLevel: DEFAULT_GEO_LEVEL as GeoLevel },
    );
    const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [complete, selection, tablePage]);

  const model = useMemo(() => describePreflight(preflight), [preflight]);
  const alternatives = useMemo(() => incompatibleAlternatives(preflight), [preflight]);
  const columns = useMemo(() => comparisonColumns(comparison), [comparison]);
  const rows = useMemo(
    () => (Array.isArray(comparison?.items) ? comparison.items : []),
    [comparison],
  );
  // The aligned table's page. Client-side over rows already fetched: the read
  // is paged upstream and says when it was cut short, this pages what arrived.
  const tableModel = tablePageModel(rows.length, tablePage);
  const tableRows = tablePageRows(rows, tablePage);
  const tableCaptionText = tableCaption(tableModel, {
    noun: { one: "aligned geography", many: "aligned geographies" },
    order: "in the order `/comparison` declares",
  });

  const scatter = useMemo(() => comparisonScatterModel(comparison), [comparison]);
  const derivedField = useMemo(() => defaultDerivedField(comparison), [comparison]);
  const mapRows = useMemo(
    () => comparisonMapRows(comparison, derivedField),
    [comparison, derivedField],
  );
  // The map colours one API-derived number per polygon, and that number can
  // be a subtraction between two publications years apart. Empty unless some
  // coloured geography is actually in that state (WEB-049).
  const mapPeriodNote = useMemo(
    () => mapPeriodMismatchNote(comparison, derivedField),
    [comparison, derivedField],
  );
  // The join is an inner one, so the geographies here are an intersection.
  // Empty unless one of the two measures published more than was paired
  // (WEB-050).
  const coverageNote = useMemo(() => describeComparisonCoverage(comparison), [comparison]);

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

  // The grains the *pair* can be compared at, from each side's published
  // `valid_geo_grains`. The control offered a hard-coded NATIONAL/STATE/COUNTY
  // and ignored both, while the link parser accepts all five published words:
  // a `?geo_level=PLACE` link put a value in the select that no option
  // carried, so the control showed one grain and the request sent another
  // (WEB-074).
  const selectedMetricRow = useCallback(
    (side: SideKey) =>
      metrics[side].find(
        (metric) => metric.metric_code === selection[side].metricCode,
      ) || null,
    [metrics, selection],
  );
  const grainOffer = useMemo(
    () =>
      comparisonGrainOffer({
        metricA: selectedMetricRow("a"),
        metricB: selectedMetricRow("b"),
        requested: selection.geoLevel,
      }),
    [selectedMetricRow, selection.geoLevel],
  );
  // A grain neither side publishes is reported and replaced, not held: the
  // selection would otherwise build a request for a grain the pair cannot be
  // read at. The report survives the replacement, which is the point of it.
  const [grainNotice, setGrainNotice] = useState("");
  useEffect(() => {
    if (!grainOffer.unavailable) {
      return;
    }
    const replacement = preferredComparisonGrain(grainOffer.levels);
    setGrainNotice(grainOffer.unavailable);
    if (replacement && replacement !== selection.geoLevel) {
      setSelection((current) => ({ ...current, geoLevel: replacement }));
    }
  }, [grainOffer, selection.geoLevel]);

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
    const { headings, rows: exportRows, filename } = comparisonExport(
      comparison,
      preflight,
      comparisonLoad,
    );
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

  async function handleSave() {
    if (!comparable || saving) {
      return;
    }
    const title = `${selection.a.metricCode} vs ${selection.b.metricCode}`;

    // The configuration stores the pair and the geography, not the comparison
    // response: the verdict, the derived fields, and the caveats are the
    // API's to publish, and replaying the intent asks for them again rather
    // than preserving a copy that could outlive its own compatibility rules.
    if (saveDestination(accountToken) === "account") {
      setSaving(true);
      setSaveStatus({ state: "loading", message: "Saving to your account", destination: null });
      try {
        await createSavedAnalysis(accountToken, {
          name: title,
          document: comparisonDocument({
            metricCodeA: selection.a.metricCode,
            metricCodeB: selection.b.metricCode,
            geoLevel: selection.geoLevel,
            stateFips: selection.stateFips,
          }),
        });
        setSaveStatus(describeSaveSuccess("account", title));
      } catch (error) {
        setSaveStatus(describeSaveFailure(error));
      } finally {
        setSaving(false);
      }
      window.setTimeout(() => setSaveStatus(null), 4000);
      return;
    }

    const localSave = saveChart({
      id: `comparison:${selection.a.metricCode}:${selection.b.metricCode}:${selection.geoLevel}:${selection.stateFips || "US"}`,
      version: 1,
      title,
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
    setSaveStatus(describeLocalSave(localSave, title, SAVED_CHART_LIMIT));
    window.setTimeout(() => setSaveStatus(null), 4000);
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
            disabled={!comparable || saving}
            title={
              comparable
                ? saveDestination(accountToken) === "account"
                  ? "Saves to your account"
                  : "Saves in this browser only; sign in on Saved analyses to keep it"
                : "a blocked pair is not saved as an analysis"
            }
            data-testid="comparison-save"
            data-destination={saveDestination(accountToken)}
          >
            <Save size={15} />{" "}
            {saveDestination(accountToken) === "account"
              ? "Save to account"
              : "Save in browser"}
          </button>
        </div>
      </header>
      {saveStatus ? (
        <div
          className="save-toast"
          data-state={saveStatus.state}
          data-destination={saveStatus.destination || ""}
          data-testid="save-toast"
          role="status"
        >
          {saveStatus.message}
        </div>
      ) : null}

      <section className="status-row" role="status">
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

      {coverageNote ? (
        <section className="grid">
          <article className="card span-2">
            <p className="subtle" data-testid="comparison-coverage-note">
              {coverageNote}
            </p>
          </article>
        </section>
      ) : null}

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
                  {SIDE_LABEL[side]} ({formatNumber(options[side].length)} available)
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
                {requestedMetricNotice[side] ? (
                  <p className="subtle" data-testid={`requested-metric-note-${side}`}>
                    {requestedMetricNotice[side]}
                  </p>
                ) : null}
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
                onChange={(event) => {
                  // The reader has chosen; the link's report no longer holds.
                  setGrainNotice("");
                  setSelection((current) => ({
                    ...current,
                    geoLevel: event.target.value,
                  }));
                }}
                disabled={grainOffer.levels.length === 0}
              >
                {grainOffer.levels.map((level) => (
                  <option value={level} key={level}>
                    {GEO_GRAIN_LABELS[level]?.one || level}
                  </option>
                ))}
              </select>
              {grainOffer.note ? (
                <p className="subtle" data-testid="comparison-grain-note">
                  {grainOffer.note}
                </p>
              ) : null}
              {grainNotice ? (
                <p className="subtle" data-testid="comparison-grain-unavailable">
                  {grainNotice}
                </p>
              ) : null}
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
            {mapPeriodNote ? (
              <p className="subtle">
                <strong data-testid="map-period-note">{mapPeriodNote}</strong>
              </p>
            ) : null}
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
                <caption data-testid="comparison-table-caption">
                  {tableCaptionText}{" "}
                  <span className="subtle">The CSV export carries every loaded row.</span>
                </caption>
                <tbody>
                  {tableRows.map((row, index) => {
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
            {tableModel.pageCount > 1 ? (
              <nav className="catalog-pagination" aria-label="Aligned comparison pages">
                <button
                  className="button secondary"
                  type="button"
                  data-testid="comparison-table-previous"
                  disabled={!tableModel.hasPrevious}
                  onClick={() => setTablePage((current) => Math.max(0, current - 1))}
                >
                  Previous
                </button>
                <span aria-live="polite" data-testid="comparison-table-page">
                  {`Page ${tableModel.pageIndex + 1} of ${tableModel.pageCount}`}
                </span>
                <button
                  className="button secondary"
                  type="button"
                  data-testid="comparison-table-next"
                  disabled={!tableModel.hasNext}
                  onClick={() => setTablePage((current) => current + 1)}
                >
                  Next
                </button>
              </nav>
            ) : null}
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
