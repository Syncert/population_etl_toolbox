"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";
import { Download, Save } from "lucide-react";
import * as maplibregl from "maplibre-gl";
import type {
  ExpressionSpecification,
  FilterSpecification,
  MapLayerMouseEvent,
} from "maplibre-gl";
import type { FeatureCollection } from "geojson";
import ChoroplethLegend from "./ChoroplethLegend";
import SourceNote from "./SourceNote";
import StatusPill from "./StatusPill";
import TimeSeriesChart from "./TimeSeriesChart";
import { useMapLibre } from "./useMapLibre";
import {
  apiErrorMessage,
  apiFetch,
  buildApiPath,
  createSavedAnalysis,
  fetchAllPages,
  fetchCollectionPages,
  getCapabilities,
  getDistributionBins,
  getHealth,
} from "../lib/api/client";
import type { QueryParams } from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import { observationExport } from "../lib/observationExport";
import type {
  CollectionResponse,
  DistributionResponse,
  GeographySummary,
  MetricRelease,
  MetricSummary,
  Observation,
} from "../lib/api/types";
import {
  CHOROPLETH_PALETTE,
  boundsOfFeatures,
  buildChoroplethMatchExpression,
  buildChoroplethModel,
  buildExtrusionHeightExpression,
  buildObservationIndex,
  buildSelectionFilter,
  datasetFacetOptions,
  distributionBins,
  distributionCaveats,
  distributionPeriodNote,
  formatObservationValue,
  marginOfErrorText,
  metricDataset,
  metricOptions,
  metricSupportedGeoLevels,
  metricVariable,
  normalizeGeoLevel,
  observationExportFilename,
  observationJoinValue,
  observationName,
  observationToFeature,
  observationUnit,
  pickPreferredMetric,
  preferredDatasetFacet,
  preferredGeoLevelForMetric,
  tileFilterForGeoLevel,
  tileFilterForSelection,
} from "../lib/explorerViewModel";
import type { FeatureLike, ObservationRow } from "../lib/explorerViewModel";
import {
  FALLBACK_EXPLORER_SOURCES,
  buildExplorerSources,
  findExplorerSource,
  sourceSupportsParameter,
} from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  RELEASE_DIMENSION,
  SCOPE_AS_RELEASED,
  SCOPE_LATEST,
  ACTIVE_GEOGRAPHIES_ONLY,
  buildHistoryObservationRequest,
  describeHistoryLoad,
  buildLatestObservationRequest,
  buildReleaseListRequest,
  buildSettledHistoryRequest,
  collapseToNewestRelease,
  countObservationPeriods,
  describeStratification,
  newestPerGeography,
  normalizeObservationRows,
  observationDimensionLabel,
  observationDimensionOptions,
  observationDimensionValue,
  OBSERVATION_COVERAGE_FIELDS,
  OBSERVATION_UNCERTAINTY_FIELDS,
  observationCoverageValue,
  observationPeriodLabel,
  observationUncertaintyLabel,
  sharedObservationPeriod,
  observationUncertaintyValue,
  publishesCoverage,
  publishesUncertainty,
  scopedDimensionFilters,
  servesAsReleased,
  stateScopeNote,
  stratificationDimensions,
} from "../lib/observationAccess";
import type { ObservationScope } from "../lib/observationAccess";
import { metricProvenance, metricQualityState } from "../lib/catalog";
import { requestedMetricState } from "../lib/requestedMetric";
import {
  describeViewModes,
  servesHistory,
  supportedViewModes,
  unsupportedViewModes,
} from "../lib/viewModes";
import { displayMetricName } from "../lib/format";
import {
  GEO_GRAIN_LABELS,
  GEO_GRAIN_ORDER,
  GRAINS_WITHIN_A_STATE,
  geographyPickerState,
} from "../lib/geographyPicker";
import { saveChart } from "../lib/savedCharts";
import { useStoredToken } from "../lib/apiToken";
import {
  describeLibraryLoad,
  describeSaveFailure,
  describeSaveSuccess,
  explorerDocument,
  saveDestination,
} from "../lib/savedAnalysis";
import type { SaveOutcome } from "../lib/savedAnalysis";
import { discoverTileMetadata, loadPreviewTileFeatures } from "../lib/tiles";
import {
  EXPLORER_CHOROPLETH_LAYERS,
  US_OVERVIEW_VIEW,
  pitchForMapMode,
  syncExplorerMapMode,
  syncLayerFilter,
  syncLayerPaint,
} from "../lib/mapWiring";
import { parseExplorerState, serializeExplorerState } from "../lib/urlState";
import type { ExplorerState, ValueScale } from "../lib/urlState";

// The pure view models moved to ../lib/explorerViewModel; existing consumers
// (tests included) keep importing them from here.
export {
  buildChoroplethMatchExpression,
  buildChoroplethModel,
  buildObservationIndex,
  buildSelectionFilter,
  distributionBins,
  metricOptions,
  observationExportFilename,
  pickPreferredMetric,
  preferredGeoLevelForMetric,
} from "../lib/explorerViewModel";

const CATALOG_PAGE_SIZE = 1000;
// The observation resources cap `limit` at 5000. A source whose latest
// publication is a series (Census PEP: six estimated years per county, some
// 19,000 county rows) does not fit one page, so the explorer pages by offset
// until the reported total, bounded so a runaway answer stops and says so.
const OBSERVATION_PAGE_SIZE = 5000;
const OBSERVATION_PAGE_LIMIT = 8;
const DEFAULT_GEO_LEVEL = "COUNTY";
/**
 * Geography grains in presentation order, broadest first — the published
 * vocabulary in full. PLACE is Census PEP's and AGENCY is FBI UCR's; with
 * only the spatial three here, a measure declaring either offered no levels
 * at all and was queried at a grain it does not publish (WEB-038). The map
 * still declines any grain the tile boundary has no geometry for, with the
 * reason it already gives.
 *
 * Read from `GEO_LEVELS` through the picker module rather than spelled here:
 * the declared order *is* broadest-first, and two copies of a vocabulary is
 * how the picker came to offer states as places (WEB-064).
 */
const GEO_LEVEL_ORDER = GEO_GRAIN_ORDER;
const DEFAULT_MAP_MODE = "choropleth";
const DEFAULT_VALUE_SCALE: ValueScale = "linear";
// Presentation panels that are not measure-dependent: they describe the
// request and how to read it, and answer for every selection.
const PRESENTATION_TABS = ["api query", "notes"] as const;
const DEFAULT_SCOPE: ObservationScope = SCOPE_LATEST;
// The release listing is a bounded, deterministic page; a metric with more
// published releases than this is reported as such rather than truncated
// into a silently partial option list.
const RELEASE_PAGE_SIZE = 200;
// The release control is a picker: selecting a release is the only way this
// screen sends `scope=as_released&release=…` or builds the link that
// reproduces it, so a release it did not list is unreachable and
// unshareable. Paged like every other collection read (WEB-045).
const RELEASE_PAGE_LIMIT = 10;
// One geography's history. Paged like every other collection read, so a
// publication longer than a single page is loaded rather than truncated --
// and when the bound is reached the panel says so instead of labelling a
// prefix as the history (WEB-036).
const HISTORY_PAGE_SIZE = 1000;
const HISTORY_PAGE_LIMIT = 5;

/**
 * The observations status line: how many rows the publication answered, how
 * many geographies that is, and, when the page bound cut the answer short,
 * that the map is incomplete rather than silently sparse.
 */
function describeObservationLoad(
  items: ObservationRow[],
  total: number | null,
  complete: boolean,
  geoLevelLabel: string,
  scopeNote = "",
): string {
  // "this selection" and "these rows" both have to be true. Where a state
  // narrows the map and the geography list and not the rows, the note says
  // so, rather than letting a national answer read as one state's
  // (WEB-075).
  const qualifier = scopeNote ? ` — ${scopeNote}` : "";
  if (items.length === 0) {
    return `0 ${geoLevelLabel} records published for this selection${qualifier}`;
  }
  const geographies = newestPerGeography(items).length;
  const periods = countObservationPeriods(items);
  const loaded = complete || total === null
    ? `loaded ${items.length} ${geoLevelLabel} records`
    : `loaded ${items.length} of ${total} ${geoLevelLabel} records; the page bound cut the answer short, so the map is incomplete`;
  const shape = periods > 1
    ? ` (${geographies} geographies across ${periods} periods)`
    : "";
  return `${loaded}${shape}${qualifier}`;
}

type TileMetadata = Awaited<ReturnType<typeof discoverTileMetadata>>;

interface RequestStatus {
  state: string;
  message: string;
}

interface HoveredCounty {
  observation: ObservationRow;
  hasObservation: boolean;
  x: number;
  y: number;
  alignRight: boolean;
}

function fetchAllCatalogItems<T>(resource: string, params: QueryParams = {}): Promise<T[]> {
  return fetchAllPages<T>(resource, { params, pageSize: CATALOG_PAGE_SIZE });
}

export default function SourceExplorerPage({ sourceKey = "census" }: { sourceKey?: string }) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const metricsTracker = useRef(createRequestTracker()).current;
  const observationTracker = useRef(createRequestTracker()).current;
  const distributionTracker = useRef(createRequestTracker()).current;
  const timeseriesTracker = useRef(createRequestTracker()).current;
  const releasesTracker = useRef(createRequestTracker()).current;
  const grainGeographyTracker = useRef(createRequestTracker()).current;
  // The metric a pinned release was chosen for. A release identity belongs
  // to one metric, so the pin is dropped when the metric changes — but not
  // when a shared link selects the metric and its pin together.
  const releaseMetricRef = useRef("");
  // The initially requested URL state, applied once when the first metric
  // catalog for the resolved source arrives.
  const initialStateRef = useRef<ExplorerState | null>(null);

  const [apiHealth, setApiHealth] = useState<RequestStatus>({
    state: "loading",
    message: "checking /api/v1/health",
  });
  const [tilesHealth, setTilesHealth] = useState<RequestStatus>({
    state: "loading",
    message: "checking /tiles/catalog",
  });
  // Explorer sources come from /api/v1/catalog/capabilities: the sources
  // whose declared routes carry the explorer's latest + timeseries
  // workflow. There is no client-side source enumeration; the fallback is
  // the labeled offline entry for the mounted default source.
  const [explorerSources, setExplorerSources] = useState<ExplorerSource[]>([]);
  const [sourcesError, setSourcesError] = useState("");
  const [activeSourceKey, setActiveSourceKey] = useState(sourceKey);
  const [metrics, setMetrics] = useState<MetricSummary[]>([]);
  const [metricsError, setMetricsError] = useState("");
  // What a link asked for and this source does not publish (WEB-072).
  const [requestedMetricNotice, setRequestedMetricNotice] = useState("");
  const [selectedDataset, setSelectedDataset] = useState("");
  const [selectedGeoLevel, setSelectedGeoLevel] = useState(DEFAULT_GEO_LEVEL);
  const [mapMode, setMapMode] = useState(DEFAULT_MAP_MODE);
  const [valueScale, setValueScale] = useState<ValueScale>(DEFAULT_VALUE_SCALE);
  const [selectedMetric, setSelectedMetric] = useState("");
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [countyGeographies, setCountyGeographies] = useState<GeographySummary[]>([]);
  // Geographies for a grain the eager pair does not cover -- PLACE, and
  // whatever the vocabulary grows. Read for the selected grain rather than
  // all at once: the projection carries some 32k places (WEB-064).
  const [grainGeographies, setGrainGeographies] = useState<GeographySummary[]>([]);
  const [grainGeographiesRead, setGrainGeographiesRead] = useState(false);
  const [geographiesError, setGeographiesError] = useState("");
  // Whether the eager state/county read has answered. "None published"
  // and "none has arrived" are different statements, and only the first
  // is about the warehouse.
  const [geographiesRead, setGeographiesRead] = useState(false);
  const [selectedStateFips, setSelectedStateFips] = useState("");
  // Selected values for the active source's own declared dimension filters
  // (CDC strata/adjustment, FBI UCR subject, USDA NASS domain). Keyed by the
  // filter name the capability declares; nothing here enumerates sources.
  const [dimensionSelections, setDimensionSelections] = useState<Record<string, string>>({});
  // Which publication the explorer is reading: the source's own latest, or
  // the published releases with one optionally pinned. Both come from the
  // API's declared vocabulary, never from a client-authored list.
  const [observationScope, setObservationScope] = useState<ObservationScope>(DEFAULT_SCOPE);
  const [selectedRelease, setSelectedRelease] = useState("");
  const [releases, setReleases] = useState<MetricRelease[]>([]);
  const [releasesStatus, setReleasesStatus] = useState<RequestStatus>({
    state: "idle",
    message: "waiting for metric",
  });

  const [observationStatus, setObservationStatus] = useState<RequestStatus>({
    state: "idle",
    message: "selecting metric",
  });
  const [observations, setObservations] = useState<ObservationRow[]>([]);
  // What the last observation read was, beyond its rows: the loader computed
  // `complete` and the API's `total` for the status line and then dropped
  // them, so an export of a prefix could not say it was one (WEB-059).
  const [observationLoad, setObservationLoad] = useState<{
    total: number | null;
    complete: boolean;
  }>({ total: null, complete: true });
  const [distribution, setDistribution] = useState<DistributionResponse | null>(null);
  // The polygons currently drawn, kept so a state selection can fit their
  // extent; set once the layers exist so the fit never runs ahead of them.
  const [choroplethFeatures, setChoroplethFeatures] = useState<FeatureLike[]>([]);
  const fittedStateRef = useRef("");
  const [distributionStatus, setDistributionStatus] = useState<RequestStatus>({
    state: "idle",
    message: "waiting for metric",
  });
  const [tileMetadata, setTileMetadata] = useState<TileMetadata | null>(null);
  const [activeSourceLayer, setActiveSourceLayer] = useState<string | null>(null);
  const [hoveredCounty, setHoveredCounty] = useState<HoveredCounty | null>(null);
  const [selectedGeoId, setSelectedGeoId] = useState("");
  const [timeseries, setTimeseries] = useState<ObservationRow[]>([]);
  const [timeseriesStatus, setTimeseriesStatus] = useState<RequestStatus>({
    state: "idle",
    message: "Click a geography to load its history.",
  });
  const [activeTab, setActiveTab] = useState("map");
  const [saveStatus, setSaveStatus] = useState<SaveOutcome | null>(null);
  const [saving, setSaving] = useState(false);
  // The token the saved-analysis screen remembered for this tab, if any. It
  // decides where a save goes; it is never rendered and never put in a URL.
  const { token: accountToken } = useStoredToken();

  // The active source resolves against discovery; an unknown requested
  // segment degrades to the mounted default, then to the first discovered
  // source, and is reflected back into the shareable URL below.
  const activeSource = useMemo<ExplorerSource | null>(() => {
    if (explorerSources.length === 0) {
      return null;
    }
    return (
      findExplorerSource(explorerSources, activeSourceKey) ||
      findExplorerSource(explorerSources, sourceKey) ||
      explorerSources[0] ||
      null
    );
  }, [explorerSources, activeSourceKey, sourceKey]);

  // Only the filters the active source's capability entry declares reach a
  // request; the builders drop the rest rather than sending one the resource
  // would reject, or silently widening the answer by omitting it.
  const supportsStateFilter = sourceSupportsParameter(activeSource, "state_fips");
  const supportsGeoLevelFilter = sourceSupportsParameter(activeSource, "geo_level");
  // As-released reads answer on the neutral resource, so the dimension
  // controls under that scope are the neutral ones the capability declares.
  // Declared by `/catalog/capabilities`, so a dimension a page happens not to
  // publish is still shown rather than vanishing with the page (WEB-061).
  const publishedDimensions = useMemo(
    () => [...(activeSource?.publishedDimensions || [])],
    [activeSource],
  );
  const dimensionFilters = useMemo(
    () => scopedDimensionFilters(activeSource, observationScope),
    [activeSource, observationScope],
  );
  // The file carries every declared dimension, plus any filterable name the
  // declaration does not list, so neither list can drop a column the other
  // would have written.
  const exportDimensions = useMemo(() => {
    const names = [...publishedDimensions];
    for (const name of dimensionFilters) {
      if (!names.includes(name)) {
        names.push(name);
      }
    }
    return names;
  }, [publishedDimensions, dimensionFilters]);
  // The table gives a column to what the reader is filtering on and carries
  // the rest of the declared set in one cell, which is the presentation this
  // repository already chose for the seven uncertainty fields rather than
  // seven columns (WEB-061).
  const tableDimensions = useMemo(
    () => publishedDimensions.filter((name) => !dimensionFilters.includes(name)),
    [publishedDimensions, dimensionFilters],
  );
  const releasesDeclared = servesAsReleased(activeSource);
  const asReleased = observationScope === SCOPE_AS_RELEASED && releasesDeclared;
  // Keyed by value so the observation effect re-runs on a real selection
  // change rather than on every render.
  const dimensionKey = JSON.stringify(
    dimensionFilters.map((name) => [name, dimensionSelections[name] || ""]),
  );
  const latestQuery = useMemo(
    () => ({
      metricCode: selectedMetric,
      geoLevel: selectedGeoLevel,
      stateFips: selectedGeoLevel === "NATIONAL" ? "" : selectedStateFips,
      limit: String(OBSERVATION_PAGE_SIZE),
      // The map colours one value per geography. Where the resource can
      // answer that directly, ask it to: a source publishing a long series
      // otherwise sends every period of it across the wire to be reduced here.
      newestPerGeography: true,
      scope: observationScope,
      release: selectedRelease,
      dimensions: Object.fromEntries(
        JSON.parse(dimensionKey) as [string, string][],
      ) as Record<string, string>,
    }),
    [
      selectedMetric,
      selectedGeoLevel,
      selectedStateFips,
      dimensionKey,
      observationScope,
      selectedRelease,
    ],
  );

  // A stratified source publishes several declared-dimension series per
  // geography. Joining them to one polygon or one line would keep whichever
  // row arrived last, so the map declines and says so instead.
  // Under an unpinned as-released read the release is one more axis: every
  // published release answers, and colouring the join would show whichever
  // release sorted last as the value.
  const seriesDimensions = useMemo(
    () => stratificationDimensions(dimensionFilters, observationScope),
    [dimensionFilters, observationScope],
  );
  const stratification = useMemo(
    () => describeStratification(observations, seriesDimensions),
    [observations, seriesDimensions],
  );
  // The map colours one value per polygon. A latest publication that is a
  // series (several periods per geography) is reduced to each geography's
  // newest period, the same ranking the API's distribution bins apply, so
  // the legend counts and the coloured polygons describe the same rows.
  const mappableObservations = useMemo(
    () => (stratification.stratified ? [] : newestPerGeography(observations)),
    [observations, stratification.stratified],
  );
  // A source that publishes a participation basis is shown it; one that does
  // not grows no empty column. Read from the loaded rows, not from a list of
  // sources (WEB-051).
  const showsCoverage = useMemo(() => publishesCoverage(observations), [observations]);
  // Read from the answer, like the participation column beside it: a source
  // that publishes an interval or a coefficient of variation is shown it
  // without an edit here, and one that publishes none grows no empty column
  // (WEB-053).
  const showsUncertainty = useMemo(
    () => publishesUncertainty(observations),
    [observations],
  );
  const historyStratification = useMemo(
    () => describeStratification(timeseries, seriesDimensions),
    [timeseries, seriesDimensions],
  );

  const facetOptions = useMemo(() => datasetFacetOptions(metrics), [metrics]);
  const showDatasetSelector = facetOptions.length >= 2;
  const datasetMetrics = useMemo(() => {
    if (!showDatasetSelector || !selectedDataset) {
      return metrics;
    }
    return metrics.filter((metric) => metricDataset(metric.metric_code) === selectedDataset);
  }, [metrics, selectedDataset, showDatasetSelector]);
  const options = useMemo(
    () => metricOptions(datasetMetrics),
    [datasetMetrics],
  );
  const counties = useMemo(
    () => selectedStateFips
      ? countyGeographies.filter((county) => county.state_fips === selectedStateFips)
      : [],
    [countyGeographies, selectedStateFips],
  );
  const allGeographies = useMemo(
    () => {
      if (selectedGeoLevel === "STATE") {
        return states;
      }
      if (selectedGeoLevel === "COUNTY") {
        return countyGeographies;
      }
      // Any other grain the vocabulary declares. Empty here is what made the
      // picker unable to hold a choice it had just offered (WEB-064).
      return grainGeographies;
    },
    [selectedGeoLevel, states, countyGeographies, grainGeographies],
  );
  // Which geographies the picker offers, what its empty option says, and
  // whether it can be used at all -- decided in one place, over the selected
  // grain, so no grain is ever answered with another grain's list.
  const geographyPicker = useMemo(
    () => geographyPickerState(selectedGeoLevel, {
      geographies: selectedGeoLevel === "COUNTY" ? counties : allGeographies,
      stateSelected: Boolean(selectedStateFips),
      read:
        selectedGeoLevel === "STATE" || selectedGeoLevel === "COUNTY"
          ? geographiesRead
          : grainGeographiesRead,
    }),
    [
      selectedGeoLevel,
      counties,
      allGeographies,
      selectedStateFips,
      geographiesRead,
      grainGeographiesRead,
    ],
  );
  const observationIndex = useMemo(
    () => buildObservationIndex(mappableObservations, tileMetadata?.joinKey || "geo_id"),
    [mappableObservations, tileMetadata],
  );
  const selectedObservation = useMemo(
    () => observations.find((item) => item.geo_id === selectedGeoId) || null,
    [observations, selectedGeoId],
  );
  const selectedCountyGeography = useMemo(
    () => allGeographies.find((item) => item.geo_id === selectedGeoId) || null,
    [allGeographies, selectedGeoId],
  );
  const selectedCounty: ObservationRow | null =
    selectedObservation || timeseries[timeseries.length - 1] || selectedCountyGeography || null;
  const selectedCountyHasObservation = Boolean(selectedObservation || timeseries.length > 0);
  const geographyIndex = useMemo(
    () => buildObservationIndex(allGeographies, tileMetadata?.joinKey || "geo_id"),
    [allGeographies, tileMetadata],
  );
  // Observation rows publish no names; the geography catalog does.
  const geographyById = useMemo(
    () => new Map(allGeographies.map((item) => [String(item.geo_id), item])),
    [allGeographies],
  );
  const missingValueLabel = selectedDataset === "acs1"
    ? "Not published in ACS1"
    : "No observation";

  const selectedMetricMeta = metrics.find((metric) => metric.metric_code === selectedMetric);

  // The grains this measure can actually be viewed at, from the publisher's
  // own `valid_geo_grains`.
  //
  // The control used to offer all three unconditionally. Choosing one the
  // measure does not publish sent a request the API answered with zero rows,
  // and a corrective effect then snapped the selection back — so the choice
  // was offered, accepted, and silently discarded, which reads as the app
  // losing the click rather than as the measure not being published there.
  //
  // A metric that declares no grains is a metric whose grains are unknown,
  // which is not the same as a metric published at none; that case keeps the
  // full set rather than narrowing to nothing.
  const offeredGeoLevels = useMemo(() => {
    const declared = metricSupportedGeoLevels(selectedMetricMeta);
    return declared.length > 0
      ? GEO_LEVEL_ORDER.filter((level) => declared.includes(level))
      : [...GEO_LEVEL_ORDER];
  }, [selectedMetricMeta]);
  const geoLevelsNarrowed =
    Boolean(selectedMetricMeta) && offeredGeoLevels.length < GEO_LEVEL_ORDER.length;

  // Which presentations this selection can actually answer, read from the
  // measure's catalog row, the source's declared routes, and the vector
  // layer's published fields. A mode is rendered only where it is supported;
  // the rest are named with their published reason rather than going missing.
  const viewModes = useMemo(
    () => describeViewModes({
      metric: selectedMetricMeta,
      source: activeSource,
      geoLevel: selectedGeoLevel,
      tileFields: tileMetadata?.fields,
      rowCount: observations.length,
    }),
    [selectedMetricMeta, activeSource, selectedGeoLevel, tileMetadata, observations.length],
  );
  const mapSupported = viewModes.map.supported;
  const trendSupported = viewModes.trend.supported;
  const unavailableModes = useMemo(() => unsupportedViewModes(viewModes), [viewModes]);
  const workspaceTabs = useMemo(
    () => [
      ...supportedViewModes(viewModes).filter((mode) => mode !== "trend" && mode !== "export"),
      ...PRESENTATION_TABS,
    ],
    [viewModes],
  );

  // `activeTab` is the tab the user asked for; the one actually rendered is
  // that tab when the selection can answer it, and the first available one
  // otherwise. Deriving rather than rewriting the request means a mode that
  // is briefly unavailable — while discovery or the first page is in flight —
  // does not permanently move the user off it.
  const effectiveTab = workspaceTabs.includes(activeTab as (typeof workspaceTabs)[number])
    ? activeTab
    : workspaceTabs[0] || "";

  // Keep the selected metric consistent with the selected dataset facet.
  useEffect(() => {
    // Except when a link asked for a measure this source does not publish.
    // Filling the empty selection here is the same substitution the notice
    // exists to refuse, one effect later (WEB-072); the reader's own choice
    // below clears the notice and this resumes.
    if (requestedMetricNotice) {
      return;
    }
    if (!showDatasetSelector || !selectedDataset) {
      if (!selectedMetric && metrics.length > 0) {
        setSelectedMetric(pickPreferredMetric(metrics, selectedDataset));
      }
      return;
    }

    if (datasetMetrics.length === 0) {
      return;
    }

    if (
      metricDataset(selectedMetric) === selectedDataset &&
      datasetMetrics.some((metric) => metric.metric_code === selectedMetric)
    ) {
      return;
    }

    setSelectedMetric(
      pickPreferredMetric(metrics, selectedDataset, metricVariable(selectedMetric)),
    );
  }, [
    datasetMetrics,
    metrics,
    requestedMetricNotice,
    selectedDataset,
    selectedMetric,
    showDatasetSelector,
  ]);

  // One-time bootstrap: health, capability discovery, URL state, tiles.
  useEffect(() => {
    let cancelled = false;
    const requested = parseExplorerState(window.location.search);
    initialStateRef.current = requested;
    if (requested.source) {
      setActiveSourceKey(requested.source);
    }

    async function bootstrap() {
      try {
        const payload = await getHealth();
        if (!cancelled) {
          setApiHealth({ state: "ok", message: String(payload.status || "ok") });
        }
      } catch (error) {
        if (!cancelled) {
          setApiHealth({ state: "bad", message: apiErrorMessage(error) });
        }
      }

      try {
        const payload = await getCapabilities();
        const sources = buildExplorerSources(payload.items);
        if (!cancelled) {
          if (sources.length > 0) {
            setExplorerSources(sources);
          } else {
            setSourcesError("capability discovery returned no explorable sources");
            setExplorerSources(FALLBACK_EXPLORER_SOURCES);
          }
        }
      } catch (error) {
        if (!cancelled) {
          setSourcesError(apiErrorMessage(error));
          setExplorerSources(FALLBACK_EXPLORER_SOURCES);
        }
      }

      try {
        const discoveredTileMetadata = await discoverTileMetadata();
        if (!cancelled) {
          setTileMetadata(discoveredTileMetadata);
          setActiveSourceLayer(discoveredTileMetadata.sourceLayer);
        }
      } catch (error) {
        if (!cancelled) {
          setTilesHealth({
            state: "warn",
            message: error instanceof Error ? error.message : "catalog unavailable",
          });
        }
      }
    }

    bootstrap();

    return () => {
      cancelled = true;
    };
  }, []);

  // Per-source metric catalog; re-runs when the active source changes.
  useEffect(() => {
    if (!activeSource) {
      return;
    }

    const request = metricsTracker.begin();
    setMetrics([]);
    setMetricsError("");
    setSelectedMetric("");
    setSelectedDataset("");

    async function loadMetrics(source: ExplorerSource) {
      try {
        const items = await fetchAllCatalogItems<MetricSummary>("/catalog/metrics", {
          source_code: source.sourceCode,
          active_only: "true",
        });

        if (!request.isCurrent()) {
          return;
        }

        setMetrics(items);
        const requested = initialStateRef.current;
        initialStateRef.current = null;
        // A link that names a measure this source does not publish is
        // answered, not quietly rewritten: the explorer used to select
        // `pickPreferredMetric` instead and said nothing, so "Explore" on
        // `BLS:LAU:UNEMP_RATE` opened Census ACS total population (WEB-072).
        const wanted = requestedMetricState({
          requested: requested?.metric,
          items,
          sourceTitle: source.title,
        });
        setRequestedMetricNotice(wanted.notice);
        if (wanted.metricCode) {
          setSelectedDataset(metricDataset(wanted.metricCode));
          setSelectedMetric(wanted.metricCode);
        } else if (wanted.chooseDefault && items.length > 0) {
          const facet = preferredDatasetFacet(items);
          setSelectedDataset(facet);
          setSelectedMetric(pickPreferredMetric(items, facet));
        }
        // Every grain the published vocabulary names. WEB-038 widened the
        // vocabulary, the control and the serializer to five words and left
        // this branch at two, so a link carrying NATIONAL, PLACE or AGENCY
        // was parsed, validated, and then discarded: the selection fell to
        // COUNTY and a measure publishing both kept the wrong grain
        // (WEB-073). The parser has already refused anything outside
        // `GEO_LEVELS`, and the grain a measure does not publish is narrowed
        // by `offeredGeoLevels` below, which is where that rule lives.
        if (requested?.geoLevel) {
          setSelectedGeoLevel(requested.geoLevel);
        }
        // The dimension narrowing the copied view was reading under, applied
        // only for the names this source declares -- the same rule every
        // request builder applies, so a link cannot introduce a filter the
        // resource would reject.
        if (requested?.dimensions) {
          const declared = new Set([
            ...source.dimensionFilters,
            ...source.neutralDimensionFilters,
          ]);
          const carried = Object.entries(requested.dimensions).filter(
            ([name, value]) => declared.has(name) && value,
          );
          if (carried.length > 0) {
            setDimensionSelections(Object.fromEntries(carried));
          }
        }
        if (requested?.mapMode) {
          setMapMode(requested.mapMode);
        }
        if (requested?.valueScale) {
          setValueScale(requested.valueScale);
        }
        // A requested state is applied wherever the control can hold one,
        // which is now every source: a state narrows the map and the
        // geography picker regardless of what the observation routes accept
        // (WEB-075). WEB-066 gated this on the source declaring `state_fips`
        // for three reasons, two of which were the disabled control itself —
        // a state the reader could not see or clear, and a map narrowed while
        // the rows stayed national with nothing saying so. The third stands
        // and is kept below: the saved document records the state only where
        // the request carried it, so a save is never a 422 over a filter the
        // source does not declare.
        if (requested?.stateFips) {
          setSelectedStateFips(requested.stateFips);
        }
        if (requested?.geoId) setSelectedGeoId(requested.geoId);
        // The requested scope is applied only where the source declares it;
        // a link asking for an as-released read of a source that publishes
        // none resolves to the latest publication rather than a 422.
        if (requested?.scope === SCOPE_AS_RELEASED && servesAsReleased(source)) {
          setObservationScope(SCOPE_AS_RELEASED);
          if (requested.release && source.supportsReleasePin) {
            releaseMetricRef.current = requested.metric || "";
            setSelectedRelease(requested.release);
          }
        }
      } catch (error) {
        if (request.isCurrent()) {
          setMetricsError(apiErrorMessage(error) || "Unable to load metrics.");
        }
      }
    }

    loadMetrics(activeSource);

    return () => {
      metricsTracker.invalidate();
    };
  }, [metricsTracker, activeSource]);

  useEffect(() => {
    setSelectedGeoId("");
  }, [selectedGeoLevel]);

  useEffect(() => {
    if (selectedGeoLevel === "NATIONAL" && selectedStateFips) {
      setSelectedStateFips("");
    }
  }, [selectedGeoLevel, selectedStateFips]);

  useEffect(() => {
    if (!tileMetadata) {
      return;
    }

    setTilesHealth({
      state: "ok",
      message: `layer=${tileMetadata.layerId}; chosen_layer=${activeSourceLayer || tileMetadata.sourceLayer}; configured_source=${tileMetadata.sourceLayer}; active_source=${activeSourceLayer || tileMetadata.sourceLayer}; join=${tileMetadata.joinKey}; healthy_tile=true`,
    });
  }, [tileMetadata, activeSourceLayer]);

  // The releases a metric published, from /observations/releases. This is
  // the only source of a release identity; nothing here may infer one from a
  // period, a vintage, or an observation row.
  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }

    const listRequest = buildReleaseListRequest(activeSource, {
      metricCode: selectedMetric,
      limit: String(RELEASE_PAGE_SIZE),
    });
    setReleases([]);
    if (!listRequest || !releasesDeclared) {
      setReleasesStatus({
        state: "warn",
        message: "as-released reads are not declared for this source",
      });
      return;
    }

    const request = releasesTracker.begin();
    setReleasesStatus({ state: "loading", message: "loading published releases" });

    async function loadReleases() {
      try {
        const pages = await fetchCollectionPages<MetricRelease>(listRequest!.resource, {
          params: listRequest!.params,
          pageSize: RELEASE_PAGE_SIZE,
          maxPages: RELEASE_PAGE_LIMIT,
        });
        if (!request.isCurrent()) {
          return;
        }
        setReleases(pages.items);
        setReleasesStatus({
          state: pages.complete ? "ok" : "bad",
          message: describeLibraryLoad(
            pages.items.length,
            pages.total,
            pages.complete,
            "published release",
            "published releases",
          ),
        });
      } catch (error) {
        if (request.isCurrent()) {
          setReleases([]);
          setReleasesStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    }

    loadReleases();

    return () => {
      releasesTracker.invalidate();
    };
  }, [releasesTracker, selectedMetric, activeSource, releasesDeclared]);

  // A release identity belongs to one metric; carrying a pin across a metric
  // change would send an identity that metric never published.
  useEffect(() => {
    if (!selectedRelease || releaseMetricRef.current === selectedMetric) {
      return;
    }
    setSelectedRelease("");
  }, [selectedMetric, selectedRelease]);

  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }
    // Never ask for a grain the measure does not declare (WEB-038). The
    // selection settles one render later -- the correction effect below
    // moves it to a declared grain -- and firing here first spent a request
    // on a grain that answers nothing and flashed "0 records published" for
    // a measure that publishes plenty.
    const declaredGrains = metricSupportedGeoLevels(selectedMetricMeta);
    if (
      declaredGrains.length > 0 &&
      !declaredGrains.includes(normalizeGeoLevel(selectedGeoLevel))
    ) {
      return;
    }

    const request = observationTracker.begin();
    setObservationStatus({ state: "loading", message: `loading ${selectedMetric}` });

    async function loadObservations(source: ExplorerSource) {
      try {
        const { resource, params } = buildLatestObservationRequest(source, latestQuery);
        const pages = await fetchCollectionPages<Observation>(resource, {
          params,
          pageSize: OBSERVATION_PAGE_SIZE,
          maxPages: OBSERVATION_PAGE_LIMIT,
        });
        const items = normalizeObservationRows(source, pages.items);

        if (request.isCurrent()) {
          setObservations(items);
          setObservationLoad({ total: pages.total, complete: pages.complete });
          setObservationStatus({
            state: pages.complete ? "ok" : "bad",
            message: describeObservationLoad(
              items,
              pages.total,
              pages.complete,
              selectedGeoLevel.toLowerCase(),
              // Read back from the request the effect issued, not from the
              // intent above it: `buildLatestObservationRequest` drops a
              // filter the source does not declare, so `params` is the only
              // place that knows whether the state reached the rows.
              stateScopeNote({
                stateSelected: Boolean(latestQuery.stateFips),
                narrowsRows: Boolean(params.state_fips),
                sourceTitle: source.title,
              }),
            ),
          });
        }
      } catch (error) {
        if (request.isCurrent()) {
          setObservations([]);
          setObservationLoad({ total: null, complete: true });
          setObservationStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    }

    loadObservations(activeSource);

    return () => {
      observationTracker.invalidate();
    };
  }, [
    observationTracker,
    selectedMetric,
    selectedMetricMeta,
    latestQuery,
    selectedGeoLevel,
    activeSource,
  ]);

  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }

    const request = distributionTracker.begin();
    setDistribution(null);
    // /distribution/bins answers only for the sources whose capability
    // entry declares it; for the rest the honest state is "the API does not
    // serve this here", not a failed request retried as a fallback.
    //
    // The route also declares no `scope`: its bins are computed over the
    // metric's latest values. Under an as-released read they would describe
    // a different answer than the one on screen, so the request is not made
    // and the legend says the bins are local to the loaded rows.
    const servesDistribution = activeSource.servesDistribution && !asReleased;
    setDistributionStatus(
      activeSource.servesDistribution && asReleased
        ? {
            state: "warn",
            message:
              "API bins describe the latest publication only; local bins over the released rows",
          }
        : servesDistribution
          ? { state: "loading", message: "loading API bins" }
          : {
              state: "warn",
              message: "not declared for this source; using local fallback bins",
            },
    );

    async function loadDistribution() {
      if (!servesDistribution) {
        await loadGeographies();
        return;
      }
      try {
        const payload = await getDistributionBins({
          metric_code: selectedMetric,
          geo_level: selectedGeoLevel,
          bin_count: String(CHOROPLETH_PALETTE.length),
          state_fips:
            selectedStateFips && selectedGeoLevel !== "NATIONAL" ? selectedStateFips : undefined,
        });
        if (Number(payload.total) === 0) {
          if (request.isCurrent()) {
            setDistribution(null);
            setDistributionStatus({
              state: "ok",
              message: "no published values for selection",
            });
          }
          return;
        }
        if (distributionBins(payload).length === 0) {
          throw new Error("no distribution values");
        }

        if (request.isCurrent()) {
          setDistribution(payload);
          // The legend's scale is built from these bins, and the map is
          // painted from that scale. A scale over a mix of periods is a
          // legitimate map of each geography's newest value and a misleading
          // one to read as a snapshot, so the answer's own statement of which
          // it is travels with the count (WEB-054).
          const periodNote = distributionPeriodNote(payload);
          setDistributionStatus({
            state: payload.periods_differ === true ? "warn" : "ok",
            message: [
              `${payload.bin_count} API bins across ${payload.total} records`,
              periodNote,
            ]
              .filter(Boolean)
              .join(" "),
          });
        }
      } catch (error) {
        if (request.isCurrent()) {
          setDistribution(null);
          setDistributionStatus({
            state: "warn",
            message: `${apiErrorMessage(error)}; using local fallback`,
          });
        }
      }

      await loadGeographies();
    }

    async function loadGeographies() {
      try {
        const [stateItems, countyItems] = await Promise.all([
          fetchAllCatalogItems<GeographySummary>("/catalog/geographies", {
            ...ACTIVE_GEOGRAPHIES_ONLY,
            geo_level: "STATE",
          }),
          fetchAllCatalogItems<GeographySummary>("/catalog/geographies", {
            ...ACTIVE_GEOGRAPHIES_ONLY,
            geo_level: "COUNTY",
          }),
        ]);

        if (request.isCurrent()) {
          setStates(
            stateItems.sort((left, right) =>
              String(left.state_name).localeCompare(String(right.state_name))),
          );
          setCountyGeographies(
            countyItems.sort((left, right) =>
              String(left.county_name).localeCompare(String(right.county_name))),
          );
          setGeographiesRead(true);
        }
      } catch (error) {
        if (request.isCurrent()) {
          setGeographiesError(apiErrorMessage(error) || "Unable to load geography selectors.");
        }
      }
    }

    loadDistribution();

    return () => {
      distributionTracker.invalidate();
    };
  }, [
    distributionTracker,
    selectedMetric,
    selectedStateFips,
    selectedGeoLevel,
    activeSource,
    asReleased,
  ]);

  // Geographies for a grain the eager state/county read does not cover.
  //
  // Read for the grain actually selected, and bounded the way counties are:
  // a grain that sits inside a state waits for one, because the projection
  // carries some 32k places and a picker is not the place to load them. A
  // grain the projection publishes nothing for -- AGENCY, whose identities
  // the geography dimension does not carry -- answers empty, and the picker
  // says so instead of offering another grain's list (WEB-064).
  useEffect(() => {
    const grain = normalizeGeoLevel(selectedGeoLevel);
    if (!grain || grain === "NATIONAL" || grain === "STATE" || grain === "COUNTY") {
      setGrainGeographies([]);
      setGrainGeographiesRead(false);
      return;
    }
    if (GRAINS_WITHIN_A_STATE.includes(grain) && !selectedStateFips) {
      setGrainGeographies([]);
      setGrainGeographiesRead(false);
      return;
    }

    const request = grainGeographyTracker.begin();
    setGrainGeographies([]);
    setGrainGeographiesRead(false);

    async function loadGrainGeographies() {
      try {
        const items = await fetchAllCatalogItems<GeographySummary>(
          "/catalog/geographies",
          selectedStateFips
            ? {
                ...ACTIVE_GEOGRAPHIES_ONLY,
                geo_level: grain,
                state_fips: selectedStateFips,
              }
            : { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: grain },
        );
        if (!request.isCurrent()) {
          return;
        }
        setGrainGeographies(items);
        setGrainGeographiesRead(true);
      } catch (error) {
        if (request.isCurrent()) {
          setGeographiesError(
            apiErrorMessage(error) || "Unable to load geography selectors.",
          );
        }
      }
    }

    loadGrainGeographies();

    return () => {
      grainGeographyTracker.invalidate();
    };
  }, [grainGeographyTracker, selectedGeoLevel, selectedStateFips]);

  useEffect(() => {
    // No declared history route means no trend to request. Asking anyway
    // would present a route error where the honest answer is that this
    // source publishes no per-geography history here.
    if (!servesHistory(activeSource)) {
      setTimeseries([]);
      setTimeseriesStatus({
        state: "warn",
        message: `${activeSource?.title || "This source"} declares no history route`,
      });
      return;
    }

    if (!selectedMetric || !selectedGeoId || !activeSource) {
      setTimeseries([]);
      setTimeseriesStatus({
        state: "idle",
        message: "Click a geography to load its history.",
      });
      return;
    }

    const request = timeseriesTracker.begin();
    setTimeseries([]);
    setTimeseriesStatus({ state: "loading", message: "Loading history..." });

    async function loadTimeseries(source: ExplorerSource) {
      try {
        const { resource, params } = buildHistoryObservationRequest(source, {
          metricCode: selectedMetric,
          geoId: selectedGeoId,
          limit: String(HISTORY_PAGE_SIZE),
          scope: observationScope,
          release: selectedRelease,
          dimensions: dimensionSelections,
        });
        const pages = await fetchCollectionPages<Observation>(resource, {
          params,
          pageSize: HISTORY_PAGE_SIZE,
          maxPages: HISTORY_PAGE_LIMIT,
        });
        let items = normalizeObservationRows(source, pages.items);
        let total = pages.total;
        let complete = pages.complete;
        let acrossReleases = false;
        // A source's latest relation can keep one row per geography -- ACS
        // holds only the newest vintage -- so under the latest scope a
        // history comes back as a single point. Every published release is
        // that geography's history; read it and keep the newest release of
        // each period, which is what "latest" means period by period.
        if (items.length <= 1 && observationScope === SCOPE_LATEST && servesAsReleased(source)) {
          // The settled history is the resource's answer where it declares
          // one (API-081): each period as its newest release left it, ranked
          // by the source's own declared release order. Where it does not,
          // the releases are read and reduced here as before, so a
          // deployment on an older API keeps its trend (WEB-046).
          const settled = buildSettledHistoryRequest(source, {
            metricCode: selectedMetric,
            geoId: selectedGeoId,
            limit: String(HISTORY_PAGE_SIZE),
            dimensions: dimensionSelections,
          });
          const released = settled
            ? settled
            : buildHistoryObservationRequest(source, {
                metricCode: selectedMetric,
                geoId: selectedGeoId,
                limit: String(HISTORY_PAGE_SIZE),
                scope: SCOPE_AS_RELEASED,
                dimensions: dimensionSelections,
              });
          const releasedPages = await fetchCollectionPages<Observation>(released.resource, {
            params: released.params,
            pageSize: HISTORY_PAGE_SIZE,
            maxPages: HISTORY_PAGE_LIMIT,
          });
          const releasedRows = normalizeObservationRows(source, releasedPages.items);
          const releasedItems = settled
            ? releasedRows
            : collapseToNewestRelease(releasedRows);
          if (releasedItems.length > items.length) {
            items = releasedItems;
            // The reported total counts released rows, which collapse to
            // fewer periods; carrying it forward would read as a shortfall
            // that is not one. Completeness is what travels.
            total = null;
            complete = releasedPages.complete;
            acrossReleases = true;
          }
        }
        if (request.isCurrent()) {
          setTimeseries(items);
          setTimeseriesStatus({
            state: complete ? "ok" : "bad",
            message: describeHistoryLoad(items.length, total, complete, acrossReleases),
          });
        }
      } catch (error) {
        if (request.isCurrent()) {
          setTimeseries([]);
          setTimeseriesStatus({
            state: "bad",
            message: apiErrorMessage(error) || "Unable to load history.",
          });
        }
      }
    }

    loadTimeseries(activeSource);

    return () => {
      timeseriesTracker.invalidate();
    };
  }, [
    timeseriesTracker,
    selectedMetric,
    selectedGeoId,
    activeSource,
    dimensionSelections,
    observationScope,
    selectedRelease,
  ]);

  // The canvas exists only while the boundary can draw the selection, so the
  // map is removed rather than hidden when that changes. The observation
  // points are this screen's own layer, added once the style has loaded.
  const { mapRef, ready: mapReady } = useMapLibre(mapContainerRef, mapSupported, (map) => {
    map.addSource("obs", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: [],
      },
    });
    map.addLayer({
      id: "obs-points",
      type: "circle",
      source: "obs",
      paint: {
        "circle-color": "#0a7a6d",
        "circle-radius": 1.4,
        "circle-opacity": 0.08,
        "circle-stroke-color": "#ffffff",
        "circle-stroke-width": 0.25,
      },
    });
  });

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady || !tileMetadata) {
      return;
    }

    let cancelled = false;
    let interactionHandlersAttached = false;
    const currentSourceLayer = activeSourceLayer || tileMetadata.sourceLayer;

    const handleCountyMove = (event: MapLayerMouseEvent) => {
      const feature = event.features?.[0];
      const rawJoinValue = feature?.properties?.[tileMetadata.joinKey];
      const observation = rawJoinValue === null || rawJoinValue === undefined
        ? null
        : observationIndex.get(String(rawJoinValue));
      const geography = rawJoinValue === null || rawJoinValue === undefined
        ? null
        : geographyIndex.get(String(rawJoinValue));
      const county = observation || geography;

      if (!county) {
        setHoveredCounty(null);
        map.getCanvas().style.cursor = "";
        return;
      }

      const container = map.getContainer();
      setHoveredCounty({
        observation: county,
        hasObservation: Boolean(observation),
        x: event.point.x,
        y: Math.min(event.point.y, Math.max(8, container.clientHeight - 170)),
        alignRight: event.point.x > container.clientWidth - 250,
      });
      map.getCanvas().style.cursor = "pointer";
    };

    const handleCountyLeave = () => {
      setHoveredCounty(null);
      map.getCanvas().style.cursor = "";
    };

    const handleCountyClick = (event: MapLayerMouseEvent) => {
      const feature = event.features?.[0];
      const rawJoinValue = feature?.properties?.[tileMetadata.joinKey];
      const observation = rawJoinValue === null || rawJoinValue === undefined
        ? null
        : observationIndex.get(String(rawJoinValue));
      const geography = rawJoinValue === null || rawJoinValue === undefined
        ? null
        : geographyIndex.get(String(rawJoinValue));
      const county = observation || geography;

      if (county?.geo_id) {
        setSelectedGeoId(String(county.geo_id));
      }
    };

    const removeChoropleth = () => {
      if (map.getLayer("choropleth-selected")) {
        map.removeLayer("choropleth-selected");
      }
      if (map.getLayer("choropleth-extrusion")) {
        map.removeLayer("choropleth-extrusion");
      }
      if (map.getLayer("choropleth-outline")) {
        map.removeLayer("choropleth-outline");
      }
      if (map.getLayer("choropleth-fill")) {
        map.removeLayer("choropleth-fill");
      }
      if (map.getSource("choropleth")) {
        map.removeSource("choropleth");
      }
    };

    const addChoropleth = async (sourceLayer: string) => {
      if (!sourceLayer) {
        return;
      }

      const featureCollection = await loadPreviewTileFeatures(
        tileMetadata.tileTemplate,
        sourceLayer,
        selectedGeoLevel,
      );
      if (cancelled) {
        return;
      }

      removeChoropleth();
      const geoFilter = tileFilterForGeoLevel(selectedGeoLevel) as FilterSpecification;

      map.addSource("choropleth", {
        type: "geojson",
        data: featureCollection as FeatureCollection,
      });

      map.addLayer(
        {
          id: "choropleth-fill",
          type: "fill",
          source: "choropleth",
          filter: geoFilter,
          paint: {
            "fill-color": buildChoroplethMatchExpression(
              mappableObservations,
              tileMetadata.joinKey,
              distribution,
              missingValueLabel,
              valueScale,
            ) as unknown as ExpressionSpecification,
            "fill-opacity": mapMode === "choropleth" ? 0.95 : 0.08,
          },
        },
        "obs-points",
      );

      map.addLayer(
        {
          id: "choropleth-extrusion",
          type: "fill-extrusion",
          source: "choropleth",
          filter: geoFilter,
          layout: {
            visibility: mapMode === "extrusion" ? "visible" : "none",
          },
          paint: {
            "fill-extrusion-color": buildChoroplethMatchExpression(
              mappableObservations,
              tileMetadata.joinKey,
              distribution,
              missingValueLabel,
              valueScale,
            ) as unknown as ExpressionSpecification,
            "fill-extrusion-height": buildExtrusionHeightExpression(
              mappableObservations,
              tileMetadata.joinKey,
              valueScale,
            ) as unknown as ExpressionSpecification,
            "fill-extrusion-opacity": 0.92,
          },
        },
        "obs-points",
      );

      map.addLayer(
        {
          id: "choropleth-outline",
          type: "line",
          source: "choropleth",
          filter: geoFilter,
          paint: {
            "line-color": "#22384d",
            "line-width": 0.7,
            "line-opacity": 0.85,
          },
        },
        "obs-points",
      );

      const selectedItem = mappableObservations.find((item) => item.geo_id === selectedGeoId);
      const selectedJoinValue = selectedItem
        ? observationJoinValue(selectedItem, tileMetadata.joinKey)
        : null;

      map.addLayer(
        {
          id: "choropleth-selected",
          type: "line",
          source: "choropleth",
          filter: buildSelectionFilter(
            selectedJoinValue,
            tileMetadata.joinKey,
          ) as unknown as FilterSpecification,
          paint: {
            "line-color": "#d96b2b",
            "line-width": 3,
            "line-opacity": 1,
          },
        },
        "obs-points",
      );

      map.on("mousemove", "choropleth-fill", handleCountyMove);
      map.on("mouseleave", "choropleth-fill", handleCountyLeave);
      map.on("click", "choropleth-fill", handleCountyClick);
      interactionHandlersAttached = true;
      setChoroplethFeatures(
        Array.isArray(featureCollection.features) ? featureCollection.features : [],
      );
    };

    addChoropleth(currentSourceLayer).catch((error: unknown) => {
      if (!cancelled) {
        setTilesHealth({
          state: "warn",
          message: error instanceof Error ? error.message : "tile preview render failed",
        });
      }
    });

    return () => {
      cancelled = true;
      setHoveredCounty(null);
      map.getCanvas().style.cursor = "";
      if (interactionHandlersAttached) {
        map.off("mousemove", "choropleth-fill", handleCountyMove);
        map.off("mouseleave", "choropleth-fill", handleCountyLeave);
        map.off("click", "choropleth-fill", handleCountyClick);
      }
    };
  }, [
    mapRef,
    mapReady,
    tileMetadata,
    activeSourceLayer,
    selectedGeoLevel,
    mapMode,
    mappableObservations,
    observationIndex,
    geographyIndex,
    distribution,
    missingValueLabel,
    valueScale,
    selectedGeoId,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) {
      return;
    }

    const source = map.getSource("obs") as maplibregl.GeoJSONSource | undefined;
    if (!source) {
      return;
    }

    const features = mappableObservations
      .map((item) => observationToFeature(item))
      .filter((feature) => feature !== null);

    source.setData({
      type: "FeatureCollection",
      features,
    } as FeatureCollection);

    if (tileMetadata?.joinKey) {
      // One colouring expression feeds both the flat fill and the columns,
      // so the two modes can never disagree about a geography's colour.
      const colour = buildChoroplethMatchExpression(
        mappableObservations,
        tileMetadata.joinKey,
        distribution,
        missingValueLabel,
        valueScale,
      ) as unknown as ExpressionSpecification;
      syncLayerPaint(map, ["choropleth-fill"], "fill-color", colour);
      syncLayerPaint(map, ["choropleth-extrusion"], "fill-extrusion-color", colour);
      syncLayerPaint(
        map,
        ["choropleth-extrusion"],
        "fill-extrusion-height",
        buildExtrusionHeightExpression(
          mappableObservations,
          tileMetadata.joinKey,
          valueScale,
        ) as unknown as ExpressionSpecification,
      );
    }

    if (features.length > 0 && selectedStateFips) {
      const bounds = new maplibregl.LngLatBounds();
      for (const feature of features) {
        const [lng, lat] = feature.geometry.coordinates;
        bounds.extend([lng, lat]);
      }
      map.fitBounds(bounds, { padding: 30, maxZoom: 7, duration: 800 });
    } else if (!selectedStateFips) {
      map.easeTo({ ...US_OVERVIEW_VIEW, duration: 800 });
    }
  }, [mapRef, mapReady, mappableObservations, tileMetadata, distribution, missingValueLabel, valueScale, selectedStateFips]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) {
      return;
    }

    const mode = mapMode === "extrusion" ? "extrusion" : "choropleth";
    syncExplorerMapMode(map, mode);

    // From straight overhead an extrusion shows only its top, in the same
    // colour the choropleth uses, so the two modes are indistinguishable.
    // Tilt the camera with the mode and level it again on the way back.
    map.easeTo({ pitch: pitchForMapMode(mode), duration: 600 });
  }, [mapRef, mapMode, mapReady]);

  // A selected state is the whole map: every other state's geometry is
  // filtered out of the choropleth layers and the view fits the state's
  // extent. The layers are rebuilt on each selection, so the filter is
  // re-applied whenever the drawn features change, but the view is fitted
  // once per state -- a click inside the state must not move the camera.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) {
      return;
    }

    const filter = tileFilterForSelection(
      selectedGeoLevel,
      selectedStateFips,
    ) as FilterSpecification;
    syncLayerFilter(map, EXPLORER_CHOROPLETH_LAYERS, filter);

    if (!selectedStateFips) {
      fittedStateRef.current = "";
      return;
    }
    if (fittedStateRef.current === selectedStateFips) {
      return;
    }
    const bounds = boundsOfFeatures(choroplethFeatures, selectedStateFips);
    if (bounds) {
      fittedStateRef.current = selectedStateFips;
      map.fitBounds(bounds, { padding: 45, maxZoom: 7, duration: 700 });
    }
  }, [mapRef, mapReady, selectedGeoLevel, selectedStateFips, choroplethFeatures]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady || !tileMetadata?.joinKey || !map.getLayer("choropleth-selected")) {
      return;
    }

    const selectedItem =
      mappableObservations.find((item) => item.geo_id === selectedGeoId) || selectedCountyGeography;
    const selectedJoinValue = selectedItem
      ? observationJoinValue(selectedItem, tileMetadata.joinKey)
      : null;
    map.setFilter(
      "choropleth-selected",
      buildSelectionFilter(
        selectedJoinValue,
        tileMetadata.joinKey,
      ) as unknown as FilterSpecification,
    );
  }, [mapRef, mapReady, mappableObservations, selectedCountyGeography, selectedGeoId, tileMetadata]);


  useEffect(() => {
    if (!selectedMetricMeta) {
      return;
    }

    const preferredGeoLevel = preferredGeoLevelForMetric(selectedMetricMeta, DEFAULT_GEO_LEVEL);
    const supported = metricSupportedGeoLevels(selectedMetricMeta);
    const currentLevel = normalizeGeoLevel(selectedGeoLevel);

    if (supported.length > 0 && !supported.includes(currentLevel)) {
      setSelectedGeoLevel(preferredGeoLevel);
      return;
    }

    if (!currentLevel) {
      setSelectedGeoLevel(preferredGeoLevel);
    }
  }, [selectedMetricMeta, selectedGeoLevel]);

  const choroplethModel = useMemo(
    () => buildChoroplethModel(
      mappableObservations,
      tileMetadata?.joinKey || "geo_id",
      distribution,
      missingValueLabel,
      valueScale,
    ),
    [mappableObservations, tileMetadata, distribution, missingValueLabel, valueScale],
  );

  // The exact request the observation effect issues, built by the same
  // capability-bounded builder, so the displayed path reproduces the set.
  const latestRequest = useMemo(
    () => (selectedMetric && activeSource
      ? buildLatestObservationRequest(activeSource, latestQuery)
      : null),
    [selectedMetric, activeSource, latestQuery],
  );
  const apiQuery = latestRequest
    ? buildApiPath(latestRequest.resource, latestRequest.params)
    : "Select a metric to generate an API query.";
  // What the map actually asked the resource for, read back from the request
  // it issues rather than from the intent above it. The builder drops
  // `newest_per_geography` where the source's capability entry does not
  // declare it, so a view saved from such a source must not claim a
  // reduction it never asked for (WEB-047).
  const viewedNewestPerGeography = latestRequest?.params.newest_per_geography === "true";
  // The state the *request* carried, which is "" for a source that declares
  // no `state_fips`: a document may only hold filters its own route accepts
  // (API-117), and the reader can now select a state on such a source to
  // narrow the map and the picker (WEB-075). Read back from the request for
  // the same reason the reduction above is.
  const viewedStateFips = String(latestRequest?.params.state_fips || "");

  // Keep the URL a shareable reproduction of the current exploration state.
  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }

    const query = serializeExplorerState(
      {
        source: activeSource.key,
        metric: selectedMetric,
        geoLevel: selectedGeoLevel as ExplorerState["geoLevel"],
        mapMode: mapMode as ExplorerState["mapMode"],
        valueScale,
        stateFips: selectedStateFips,
        geoId: selectedGeoId,
        scope: observationScope,
        release: selectedRelease,
        // Under the source's own declared filter names, which is what the
        // saved document records too, so the two records of one view agree.
        dimensions: dimensionSelections,
      },
      {
        source: sourceKey,
        geoLevel: DEFAULT_GEO_LEVEL,
        mapMode: DEFAULT_MAP_MODE,
        valueScale: DEFAULT_VALUE_SCALE,
        scope: DEFAULT_SCOPE,
      },
    );
    const nextUrl = query
      ? `${window.location.pathname}?${query}`
      : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [
    selectedMetric,
    selectedGeoLevel,
    mapMode,
    valueScale,
    selectedStateFips,
    selectedGeoId,
    activeSource,
    sourceKey,
    observationScope,
    selectedRelease,
    // Keyed by value, like the observation effect above: the link has to
    // change when the narrowing does, or it reproduces a different view.
    dimensionKey,
    dimensionSelections,
  ]);

  function handleSourceChange(key: string) {
    if (!key || key === activeSource?.key) {
      return;
    }
    initialStateRef.current = null;
    setActiveSourceKey(key);
    setDimensionSelections({});
    setObservationScope(DEFAULT_SCOPE);
    setSelectedRelease("");
    setReleases([]);
    setObservations([]);
    setDistribution(null);
    setTimeseries([]);
    setSelectedGeoId("");
    setObservationStatus({ state: "idle", message: "selecting metric" });
    setDistributionStatus({ state: "idle", message: "waiting for metric" });
  }

  async function handleSaveChart() {
    if (!selectedMetric || !selectedMetricMeta || saving) return;
    const title = `${displayMetricName(selectedMetricMeta)} by county`;

    // Signed in, the view is saved as a configuration: intent replayed
    // against live publications, which is what makes it survive the tab and
    // follow the warehouse. The document carries the selection only — never
    // an observation value, because a saved analysis that froze values would
    // drift silently from the data it claims to describe.
    if (saveDestination(accountToken) === "account") {
      setSaving(true);
      setSaveStatus({ state: "loading", message: "Saving to your account", destination: null });
      try {
        await createSavedAnalysis(accountToken, {
          name: title,
          document: explorerDocument({
            metricCode: selectedMetric,
            scope: observationScope === SCOPE_AS_RELEASED ? "as_released" : "latest",
            release: selectedRelease,
            geoLevel: selectedGeoLevel,
            stateFips: viewedStateFips,
            geoId: selectedGeoId,
            dimensions: dimensionSelections,
            // A map saved without the reduction reopens as the whole latest
            // publication -- for a source publishing a series per geography
            // that is every period of it, and the map would colour whichever
            // row arrived last rather than the newest one.
            newestPerGeography: viewedNewestPerGeography,
          }),
        });
        setSaveStatus(describeSaveSuccess("account", title));
      } catch (error) {
        // Reported, not retried into the browser store: silently writing
        // somewhere else would tell the user their work is safe in a place
        // they did not choose and cannot see from their account.
        setSaveStatus(describeSaveFailure(error));
      } finally {
        setSaving(false);
      }
      window.setTimeout(() => setSaveStatus(null), 4000);
      return;
    }

    const chart = {
      id: `${selectedMetric}:${selectedStateFips || "US"}:${selectedGeoId || "all"}`,
      version: 1,
      title,
      chartType: "choropleth",
      metricCode: selectedMetric,
      metricName: displayMetricName(selectedMetricMeta),
      source: selectedMetricMeta.source_code,
      dataset: selectedDataset || null,
      geoLevel: selectedGeoLevel,
      stateFips: selectedStateFips || null,
      geoId: selectedGeoId || null,
      transformation: "raw",
      // What the request asked, beside the request itself. `apiQuery` records
      // the URL, but a consumer rebuilding the query from this chart -- the
      // packet builder, the account migration -- had only the filters, so a
      // map reopened as the source's whole latest publication (WEB-048).
      scope: observationScope,
      release: selectedRelease || null,
      newestPerGeography: viewedNewestPerGeography,
      // The one period every loaded row describes, or empty where they
      // differ: a packet block composed from this view states a period the
      // source published, and states none when the publication spans
      // several (WEB-069).
      period: sharedObservationPeriod(observations),
      apiQuery,
      savedAt: new Date().toISOString(),
    };
    saveChart(chart);
    setSaveStatus(describeSaveSuccess("browser", title));
    window.setTimeout(() => setSaveStatus(null), 4000);
  }

  function exportCsv() {
    // The columns and rows are `observationExport`'s, so what the file
    // carries is asserted at the unit tier rather than only reviewed: every
    // published uncertainty field (WEB-053) and every published coverage
    // field (WEB-051) travels whether or not this source publishes one.
    const { headings, rows } = observationExport(observations, {
      scope: observationScope,
      // The declared set, not the filterable subset: a file carrying a
      // subset would be this client deciding which part of a source's
      // published description a reader may have (WEB-061).
      dimensions: exportDimensions,
    });
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const blob = new Blob([[headings, ...rows].map((row) => row.map(escape).join(",")).join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    // A prefix names itself: the screen said the page bound cut the answer
    // short, and the file has to say it too (WEB-059).
    link.download = observationExportFilename({
      metricCode: selectedMetric,
      geoLevel: selectedGeoLevel,
      // The same `asReleased` the export's `scope` column carries: a
      // release-pinned name only where releases are actually declared.
      scope: asReleased ? "as_released" : "latest",
      release: selectedRelease,
      loaded: observations.length,
      total: observationLoad.total,
      complete: observationLoad.complete,
    });
    link.click();
    URL.revokeObjectURL(link.href);
  }

  function handleMapKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    const selectable = (observations.length > 0 ? observations : allGeographies)
      .filter((item) => item?.geo_id);
    if (event.key === "Escape") {
      setSelectedGeoId("");
      return;
    }
    if (selectable.length === 0) {
      return;
    }

    const current = selectable.findIndex((item) => item.geo_id === selectedGeoId);
    let next = current;
    if (event.key === "Enter" || event.key === " ") {
      next = current >= 0 ? current : 0;
    } else if (event.key === "ArrowRight" || event.key === "ArrowDown") {
      next = (current + 1 + selectable.length) % selectable.length;
    } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
      next = (current - 1 + selectable.length) % selectable.length;
    } else {
      return;
    }
    event.preventDefault();
    setSelectedGeoId(String(selectable[next]!.geo_id));
  }

  return (
    <main
      className="dashboard dashboard-wide"
      data-testid="dashboard"
      data-selected-dataset={selectedDataset}
      data-selected-metric={selectedMetric}
      data-metric-count={metrics.length}
      data-county-count={countyGeographies.length}
      data-selected-geo-id={selectedGeoId}
      data-selected-state={selectedStateFips}
      data-observation-count={observations.length}
      data-source-key={activeSource?.key || ""}
      data-source-count={explorerSources.length}
      data-access-shape={activeSource?.accessShape || ""}
      data-dimension-filters={dimensionFilters.join(",")}
      data-series-count={stratification.seriesCount}
      data-stratified={stratification.stratified ? "true" : "false"}
      data-scope={observationScope}
      data-release={selectedRelease}
      data-release-count={releases.length}
      data-view-modes={supportedViewModes(viewModes).join(",")}
      data-map-supported={mapSupported ? "true" : "false"}
    >
      <header className="explorer-heading">
        <div>
          <div className="section-kicker">Analytical workbench</div>
          <h1>{activeSource ? activeSource.title : "Source"} Explorer</h1>
          <p>Build a source-visible geography view, inspect observations, and validate data availability for this MVP.</p>
        </div>
        <div className="command-row"><button className="button secondary" type="button" onClick={exportCsv} disabled={!viewModes.export.supported} title={viewModes.export.reason} data-testid="export-csv"><Download size={15} /> Export CSV</button><button className="button primary" type="button" onClick={handleSaveChart} disabled={!selectedMetric || saving} data-testid="save-view" data-destination={saveDestination(accountToken)} title={saveDestination(accountToken) === "account" ? "Saves to your account" : "Saves in this browser only; sign in on Saved analyses to keep it"}><Save size={15} /> {saveDestination(accountToken) === "account" ? "Save to account" : "Save in browser"}</button></div>
      </header>
      <div className="segmented-control source-page-tabs" role="tablist" aria-label="Explorable sources">
        {explorerSources.map((source) => (
          <button
            key={source.key}
            type="button"
            role="tab"
            aria-selected={activeSource?.key === source.key}
            className={activeSource?.key === source.key ? "source-tab selected" : "source-tab"}
            data-testid={`source-tab-${source.key}`}
            data-access-shape={source.accessShape}
            title={`${source.title} (${source.accessShape === "neutral" ? "neutral /observations resource" : "source-scoped routes"})`}
            onClick={() => handleSourceChange(source.key)}
          >
            {source.tabLabel}
          </button>
        ))}
        {explorerSources.length === 0 ? (
          <span className="source-tab" aria-live="polite">Discovering sources…</span>
        ) : null}
      </div>
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
        <StatusPill state={apiHealth.state} label="API" message={apiHealth.message} testId="api-status" />
        <StatusPill state={tilesHealth.state} label="Tiles" message={tilesHealth.message} testId="tiles-status" />
        <StatusPill
          state={observationStatus.state}
          label="Observations"
          message={observationStatus.message}
          testId="observations-status"
        />
        <StatusPill
          state={distributionStatus.state}
          label="Distribution"
          message={distributionStatus.message}
          testId="distribution-status"
        />
        <StatusPill
          state={releasesStatus.state}
          label="Releases"
          message={releasesStatus.message}
          testId="releases-status"
        />
      </section>

      <div className="workspace-tabs" role="tablist" aria-label="Explorer views">
        {workspaceTabs.map((tab) => <button role="tab" aria-selected={effectiveTab === tab} className={effectiveTab === tab ? "active" : ""} type="button" onClick={() => setActiveTab(tab)} key={tab}>{tab}</button>)}
      </div>
      {unavailableModes.length > 0 ? (
        <p className="subtle" data-testid="unsupported-modes">
          Not available for this selection:{" "}
          {unavailableModes
            .map((entry) => `${entry.mode} — ${entry.reason}`)
            .join("; ")}
          .
        </p>
      ) : null}

      <section className="grid workspace-grid">
        <article className="card">
          <h2>Data &amp; Geography</h2>
          <div className="selector-grid">
            <div className="control-group">
              <label htmlFor="geo-level-select">View level</label>
              <select
                id="geo-level-select"
                className="select"
                data-testid="geo-level-select"
                value={selectedGeoLevel}
                onChange={(event) => setSelectedGeoLevel(event.target.value)}
                disabled={!supportsGeoLevelFilter}
              >
                {offeredGeoLevels.map((level) => (
                  <option key={level} value={level}>
                    {GEO_GRAIN_LABELS[level]?.one || level}
                  </option>
                ))}
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="map-mode-select">Map mode</label>
              <select
                id="map-mode-select"
                className="select"
                data-testid="map-mode-select"
                value={mapMode}
                onChange={(event) => setMapMode(event.target.value)}
              >
                <option value="choropleth">Choropleth</option>
                <option value="extrusion">Extruded polygons</option>
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="value-scale-select">Value scale</label>
              <select
                id="value-scale-select"
                className="select"
                data-testid="value-scale-select"
                value={valueScale}
                onChange={(event) => setValueScale(event.target.value as ValueScale)}
              >
                <option value="linear">Linear (API bins)</option>
                <option value="log">Logarithmic</option>
              </select>
            </div>

            {showDatasetSelector ? (
              <div className="control-group">
                <label htmlFor="dataset-select">Dataset</label>
                <select
                  id="dataset-select"
                  className="select"
                  data-testid="dataset-select"
                  value={selectedDataset}
                  onChange={(event) => {
                    setRequestedMetricNotice("");
                    setSelectedDataset(event.target.value);
                  }}
                >
                  {facetOptions.map((facet) => (
                    <option value={facet.value} key={facet.value}>
                      {facet.label}
                    </option>
                  ))}
                </select>
              </div>
            ) : null}

            <div className="control-group span-controls">
              <label htmlFor="metric-select">Metric ({options.length.toLocaleString()} available)</label>
              <select
                id="metric-select"
                className="select"
                data-testid="metric-select"
                value={selectedMetric}
                onChange={(event) => {
                  // The reader has answered the notice; it is no longer true.
                  setRequestedMetricNotice("");
                  setSelectedMetric(event.target.value);
                }}
                disabled={options.length === 0}
              >
                {options.map((option) => (
                  <option value={option.value} key={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>

            {releasesDeclared ? (
              <div className="control-group">
                <label htmlFor="publication-select">Publication</label>
                <select
                  id="publication-select"
                  className="select"
                  data-testid="publication-select"
                  value={
                    observationScope === SCOPE_AS_RELEASED && selectedRelease
                      ? `release:${selectedRelease}`
                      : observationScope
                  }
                  onChange={(event) => {
                    const choice = event.target.value;
                    if (choice.startsWith("release:")) {
                      setObservationScope(SCOPE_AS_RELEASED);
                      releaseMetricRef.current = selectedMetric;
                      setSelectedRelease(choice.slice("release:".length));
                      return;
                    }
                    setObservationScope(choice as ObservationScope);
                    setSelectedRelease("");
                  }}
                >
                  <option value={SCOPE_LATEST}>Latest published</option>
                  <option value={SCOPE_AS_RELEASED}>All published releases</option>
                  {/* Release identities as /observations/releases published
                      them, with the counts it published; a pin is offered
                      only where the resource declares `release`. */}
                  {activeSource?.supportsReleasePin
                    ? releases.map((release) => (
                        <option value={`release:${release.release}`} key={release.release}>
                          {`As released: ${release.release}`}
                          {release.as_of ? ` (as of ${release.as_of})` : ""}
                          {typeof release.observation_count === "number"
                            ? ` — ${release.observation_count.toLocaleString()} observations`
                            : ""}
                        </option>
                      ))
                    : null}
                </select>
              </div>
            ) : null}

            {dimensionFilters.map((name) => {
              const options = observationDimensionOptions(observations, name);
              return (
                <div className="control-group" key={name}>
                  <label htmlFor={`dimension-${name}`}>
                    {name.replaceAll("_", " ")}
                  </label>
                  <select
                    id={`dimension-${name}`}
                    className="select"
                    data-testid={`dimension-select-${name}`}
                    value={dimensionSelections[name] || ""}
                    onChange={(event) =>
                      setDimensionSelections((current) => ({
                        ...current,
                        [name]: event.target.value,
                      }))
                    }
                    disabled={options.length === 0}
                  >
                    <option value="">
                      {options.length === 0
                        ? `No ${name.replaceAll("_", " ")} values loaded`
                        : `All published ${name.replaceAll("_", " ")}`}
                    </option>
                    {options.map((option) => (
                      <option value={option} key={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </div>
              );
            })}

            <div className="control-group">
              <label htmlFor="state-select">State</label>
              <select
                id="state-select"
                className="select"
                data-testid="state-select"
                value={selectedStateFips}
                onChange={(event) => {
                  setSelectedStateFips(event.target.value);
                  setSelectedGeoId("");
                }}
                // A state narrows the map and the geography picker on every
                // source; it narrows the rows only where the source declares
                // the filter. Gating the control on the filter left Census
                // PEP's county and place pickers saying "select a state
                // first" with no way to give them one (WEB-075).
                disabled={selectedGeoLevel === "NATIONAL"}
              >
                <option value="">All states</option>
                {states.map((state) => (
                  <option value={state.state_fips || ""} key={state.geo_id}>
                    {state.state_name}
                  </option>
                ))}
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="county-select">{geographyPicker.label}</label>
              <select
                id="county-select"
                className="select"
                data-testid="county-select"
                value={
                  geographyPicker.options.some((option) => option.geoId === selectedGeoId)
                    ? selectedGeoId
                    : ""
                }
                onChange={(event) => setSelectedGeoId(event.target.value)}
                disabled={geographyPicker.disabled}
              >
                <option value="">{geographyPicker.placeholder}</option>
                {geographyPicker.options.map((option) => (
                  <option value={option.geoId} key={option.geoId}>
                    {option.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {sourcesError ? (
            <p className="subtle">
              Sources error: {sourcesError} (offline fallback source in use)
            </p>
          ) : null}
          {stratification.stratified ? (
            <p className="coverage-note partial" data-testid="stratification-note">
              {activeSource?.title} publishes {stratification.seriesCount} series per
              geography for this selection
              {stratification.varyingDimensions.length > 0
                ? ` (${stratification.varyingDimensions.join(", ")})`
                : ""}
              . The map and history chart stay blank rather than showing one of them
              as the value; narrow the{" "}
              {stratification.varyingDimensions.join(", ") || "source"} filter to chart
              a single series.
            </p>
          ) : null}
          {!mapSupported ? (
            <p className="coverage-note partial" data-testid="non-spatial-note">
              No map for this selection: {viewModes.map.reason}. Every loaded value
              stays available in the observation table and the CSV export, which
              carry the same geography, period, unit, and status context.
            </p>
          ) : null}
          {asReleased ? (
            <p className="coverage-note partial" data-testid="as-released-note">
              Reading {selectedRelease
                ? `release ${selectedRelease} as it was published`
                : "every published release"}
              . Values are as that publication stated them, not the source&apos;s current
              latest{selectedRelease ? "" : ", so a geography carries one row per release"}.
              API-derived distribution bins are not requested for an as-released read;
              the legend&apos;s bins are local to the loaded rows.
            </p>
          ) : null}
          {distributionCaveats(distribution).length > 0 ? (
            <p className="coverage-note partial" data-testid="distribution-caveats">
              {distributionCaveats(distribution).join(" ")}
            </p>
          ) : null}
          {!releasesDeclared && activeSource ? (
            <p className="subtle" data-testid="releases-note">
              {activeSource.title} declares no as-released surface, so this source is
              explored at its latest publication only.
            </p>
          ) : null}
          {!supportsGeoLevelFilter ? (
            <p className="subtle" data-testid="geo-level-note">
              {activeSource?.title} declares no geography-level filter, so this source
              is explored at the grain it publishes.
            </p>
          ) : null}
          {geoLevelsNarrowed ? (
            <p className="subtle" data-testid="geo-grain-note">
              {displayMetricName(selectedMetricMeta)} is published at{" "}
              {offeredGeoLevels.map((level) => GEO_GRAIN_LABELS[level]?.one || level).join(", ")} only,
              so the other view levels are not offered for it. A narrower list is the
              publisher&apos;s declaration, not a limit of this screen.
            </p>
          ) : null}
          {!supportsStateFilter && supportsGeoLevelFilter ? (
            <p className="subtle" data-testid="state-filter-note">
              {activeSource?.title} declares no state filter for its observations, so
              the state selector narrows the map and the geography list and not the
              rows. The observations line says so whenever a state is selected.
            </p>
          ) : null}
          {requestedMetricNotice ? (
            <p className="subtle" data-testid="requested-metric-note">
              {requestedMetricNotice}
            </p>
          ) : null}
          {metricsError ? <p className="subtle">Metrics error: {metricsError}</p> : null}
          {geographiesError ? <p className="subtle">Geographies error: {geographiesError}</p> : null}
          {selectedMetricMeta ? (
            <p className="metric-meta">
              Source: {selectedMetricMeta.source_code}
              {selectedDataset ? ` | Dataset: ${selectedDataset.toUpperCase()}` : ""}
              {` | Loaded catalog: ${metrics.length.toLocaleString()} metrics`}
            </p>
          ) : null}
          {selectedDataset === "acs1" || selectedDataset === "acs5" ? (
            <p className={`coverage-note ${selectedDataset === "acs1" ? "partial" : "complete"}`}>
              {selectedDataset === "acs1"
                ? "ACS 1-year county coverage is partial: Census publishes counties with populations of 65,000 or more. Uncolored counties are not published in ACS1."
                : "ACS 5-year estimates provide complete county coverage and are the default for nationwide county maps."}
            </p>
          ) : null}

          <section className="county-panel" aria-live="polite">
            <div className="county-panel-header">
              <div>
                <div className="eyebrow">Selected geography</div>
                <h3>
                  {selectedCounty
                    ? observationName(selectedCountyGeography || selectedCounty)
                    : "Choose a geography on the map"}
                </h3>
              </div>
              {selectedGeoId ? (
                <button className="clear-button" type="button" onClick={() => setSelectedGeoId("")}>
                  Clear
                </button>
              ) : null}
            </div>

            {selectedCounty ? (
              <>
                <dl className="county-details">
                  <div>
                    <dt>Latest value</dt>
                    <dd>
                      {selectedCountyHasObservation
                        ? `${formatObservationValue(selectedCounty.value)} ${observationUnit(selectedCounty)}`
                        : missingValueLabel}
                    </dd>
                  </div>
                  <div>
                    <dt>Period</dt>
                    <dd>
                      {selectedCountyHasObservation
                        ? observationPeriodLabel(selectedCounty) || "-"
                        : "Not published"}
                    </dd>
                  </div>
                  <div>
                    <dt>Source</dt>
                    <dd>
                      {selectedCountyHasObservation
                        ? String(selectedCounty.source || selectedCounty.source_code || "-")
                        : activeSource?.sourceCode || "-"}
                    </dd>
                  </div>
                  <div>
                    <dt>Dataset</dt>
                    <dd>{String(selectedCounty.dataset || selectedCounty.dataset_code || selectedDataset || "Source default")}</dd>
                  </div>
                  <div>
                    <dt>Margin of error</dt>
                    <dd>{selectedCountyHasObservation ? marginOfErrorText(selectedCounty) : "Not published"}</dd>
                  </div>
                  <div>
                    <dt>Geography ID</dt>
                    <dd>{String(selectedCounty.geo_id ?? "-")}</dd>
                  </div>
                </dl>
                <div className="timeseries-heading">
                  <strong>History</strong>
                  <span
                    className={`inline-status ${timeseriesStatus.state}`}
                    data-testid="history-status"
                  >
                    {timeseriesStatus.message}
                  </span>
                </div>
                {!trendSupported ? (
                  <p className="subtle" data-testid="trend-unsupported-note">
                    {viewModes.trend.reason}, so no trend is shown for this geography.
                  </p>
                ) : historyStratification.stratified ? (
                  <p className="subtle" data-testid="history-stratification-note">
                    {historyStratification.seriesCount} published series for this
                    geography
                    {historyStratification.varyingDimensions.length > 0
                      ? ` (${historyStratification.varyingDimensions.join(", ")})`
                      : ""}
                    . Narrow the filter above to chart one; the table below lists every
                    row as published.
                  </p>
                ) : (
                  <TimeSeriesChart
                    items={timeseries}
                    publishesValueStatus={
                      activeSource?.publishesValueStatus !== false
                    }
                  />
                )}
              </>
            ) : (
              <p className="subtle county-prompt">
                Hover for a quick read; click a geography to pin details and fetch its time series.
              </p>
            )}
          </section>
        </article>

        {/* Every workspace view renders in this grid slot, beside the controls:
            a full-width panel would wrap to the row below the (tall) controls
            card and land under the fold, which reads as the tab doing nothing. */}
        {mapSupported ? (
        <article className="card workspace-panel" data-active={effectiveTab === "map"}>
          <h2>{selectedMetricMeta ? displayMetricName(selectedMetricMeta) : `${selectedGeoLevel.toLowerCase()} map`}</h2>
          <p className="subtle">
            Latest {selectedGeoLevel.toLowerCase()} estimates, joined to Martin vector geometry by the discovered geography key.
            {countObservationPeriods(observations) > 1
              ? ` The publication spans ${countObservationPeriods(observations)} periods; each geography is coloured by its newest one.`
              : ""}
          </p>
          <div className="map-shell">
            <div
              className="map-canvas"
              data-testid="map-canvas"
              data-map-ready={mapReady ? "true" : "false"}
              data-colored-values={choroplethModel.valueCount}
              ref={mapContainerRef}
              role="region"
              tabIndex={0}
              aria-label="Interactive geography map; use arrow keys to move, Enter to select, and Escape to clear"
              onKeyDown={handleMapKeyDown}
            />
            {hoveredCounty ? (
              <div
                className="county-tooltip"
                role="tooltip"
                style={{
                  left: hoveredCounty.x,
                  top: hoveredCounty.y,
                  transform: hoveredCounty.alignRight
                    ? "translate(calc(-100% - 12px), 12px)"
                    : "translate(12px, 12px)",
                }}
              >
                <strong>
                  {observationName(
                    geographyById.get(String(hoveredCounty.observation.geo_id ?? "")) ||
                      hoveredCounty.observation,
                  )}
                </strong>
                {hoveredCounty.hasObservation ? (
                  <>
                    <span>
                      {formatObservationValue(hoveredCounty.observation.value)} {observationUnit(hoveredCounty.observation)}
                    </span>
                    <small>
                      {String(hoveredCounty.observation.period || hoveredCounty.observation.observation_date || "")} · {String(hoveredCounty.observation.source || hoveredCounty.observation.source_code || "")}
                    </small>
                    <small>
                      MOE: {marginOfErrorText(hoveredCounty.observation)}
                    </small>
                  </>
                ) : (
                  <>
                    <span>{missingValueLabel}</span>
                    <small>
                      {selectedDataset === "acs1"
                        ? "ACS1 publishes county estimates only for areas meeting its population threshold."
                        : "No value was returned for the selected metric and vintage."}
                    </small>
                  </>
                )}
              </div>
            ) : null}
            <ChoroplethLegend
              title={`Value · ${
                choroplethModel.scale === "log"
                  ? "logarithmic bins"
                  : choroplethModel.usesDistribution
                    ? "API distribution"
                    : "local fallback"
              }`}
              items={choroplethModel.legendItems}
              ariaLabel="Choropleth value legend"
              showCounts
            />
          </div>
        </article>
        ) : null}

        <article className="card workspace-panel" data-active={effectiveTab === "table"}>
          <h2>Observation Sample</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Geo</th>
                  <th>Level</th>
                  <th>Period</th>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Status</th>
                  <th>Units</th>
                  {showsUncertainty ? <th>Uncertainty</th> : null}
                  {showsCoverage ? <th>Participation</th> : null}
                  {asReleased ? <th>Release</th> : null}
                  {dimensionFilters.map((name) => (
                    <th key={name}>{name.replaceAll("_", " ")}</th>
                  ))}
                  {/* Every other field the source declares its rows carry:
                      one cell, as the seven uncertainty fields are one cell
                      rather than seven columns (WEB-061). */}
                  {tableDimensions.length > 0 ? <th>Dimensions</th> : null}
                </tr>
              </thead>
              <tbody>
                {observations.slice(0, 12).map((item, index) => (
                  <tr
                    key={`${item.geo_id}-${observationPeriodLabel(item)}-${item.metric_code}-${String(item.release ?? "")}-${index}`}
                  >
                    <td>{observationName(geographyById.get(String(item.geo_id ?? "")) || item)}</td>
                    <td>{String(item.geo_level || "-")}</td>
                    <td>{observationPeriodLabel(item) || "-"}</td>
                    <td>{item.metric_code}</td>
                    {/* A missing or suppressed value is never rendered as a
                        number; the source's own status says why. */}
                    <td>{item.value ?? "-"}</td>
                    <td>{String(item.value_status || (item.value === null ? "not published" : "-"))}</td>
                    <td>{observationUnit(item)}</td>
                    {showsUncertainty ? (
                      <td data-testid={`uncertainty-${item.geo_id}`}>
                        {observationUncertaintyLabel(item) || "-"}
                      </td>
                    ) : null}
                    {showsCoverage ? (
                      <td data-testid={`coverage-${item.geo_id}`}>
                        {observationCoverageValue(item, "participation_status") || "-"}
                        {observationCoverageValue(item, "coverage_percent")
                          ? ` (${observationCoverageValue(item, "coverage_percent")}% covered)`
                          : ""}
                      </td>
                    ) : null}
                    {asReleased ? (
                      <td>{observationDimensionValue(item, RELEASE_DIMENSION) || "-"}</td>
                    ) : null}
                    {dimensionFilters.map((name) => (
                      <td key={name}>{observationDimensionValue(item, name) || "-"}</td>
                    ))}
                    {tableDimensions.length > 0 ? (
                      <td data-testid={`dimensions-${item.geo_id}`}>
                        {observationDimensionLabel(item, tableDimensions) || "-"}
                      </td>
                    ) : null}
                  </tr>
                ))}
                {observations.length === 0 ? (
                  <tr>
                    <td
                      colSpan={
                        7 +
                        (showsUncertainty ? 1 : 0) +
                        (showsCoverage ? 1 : 0) +
                        (asReleased ? 1 : 0) +
                        dimensionFilters.length +
                        (tableDimensions.length > 0 ? 1 : 0)
                      }
                      className="subtle"
                    >
                      No observations available for selected metric.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </article>
        <article className="card workspace-panel" data-active={effectiveTab === "metadata"}>
          <SourceNote source={selectedMetricMeta?.source_code} dataset={selectedDataset ? selectedDataset.toUpperCase() : activeSource?.tabLabel} metric={selectedMetricMeta ? `${displayMetricName(selectedMetricMeta)} (${selectedMetricMeta.metric_code})` : null} geography={selectedStateFips ? `${selectedGeoLevel.toLowerCase()}s in selected state` : `United States ${selectedGeoLevel.toLowerCase()}s`} period={observations[0]?.period || observations[0]?.observation_date} updatedAt={selectedMetricMeta?.harvested_at} caveats={selectedDataset === "acs1" ? "ACS 1-year county estimates are available only for counties meeting the Census population threshold." : "Validate geographies and coverage before drawing conclusions from sparse source-series values."} />
        </article>
        {viewModes.quality.supported ? (
          <article className="card workspace-panel" data-active={effectiveTab === "quality"}>
            <div className="section-kicker">Published quality context</div>
            <h2>Freshness and provenance</h2>
            <p className="subtle">
              Everything below is published by the measure&apos;s own catalog entry. A
              field the publisher did not publish is omitted rather than filled in,
              and an unpublished freshness reads as unknown — never as healthy.
            </p>
            <StatusPill
              state={metricQualityState(selectedMetricMeta).state}
              label="Freshness"
              message={metricQualityState(selectedMetricMeta).label}
              testId="explorer-freshness"
            />
            <dl className="county-details" data-testid="explorer-provenance">
              {metricProvenance(selectedMetricMeta).map((entry) => (
                <div key={entry.label}>
                  <dt>{entry.label}</dt>
                  <dd>{entry.value}</dd>
                </div>
              ))}
            </dl>
          </article>
        ) : null}
        <article className="card workspace-panel" data-active={effectiveTab === "api query"}>
          <div className="section-kicker">Reproducible request</div><h2>API Query</h2><p className="subtle">This endpoint reproduces the observation set currently used by the map, paged by <code>offset</code> until its reported total.</p><code className="api-query">GET {apiQuery}</code>
        </article>
        <article className="card workspace-panel" data-active={effectiveTab === "notes"}>
          <div className="section-kicker">Interpretation notes</div><h2>Use this view carefully</h2><p>The map uses API-calculated distribution bins, reports missing observations separately, and preserves context in the selected geography details.</p><p className="subtle">Transformation: raw value. Geography: {selectedGeoLevel.toLowerCase()}. Dataset: {selectedDataset ? selectedDataset.toUpperCase() : activeSource?.tabLabel || "Source default"}. Color treatment: {valueScale === "log" ? "five logarithmic intervals over the published values, so a long-tailed measure such as population is not one colour" : "five distribution-backed intervals with a local fallback only when the distribution endpoint is unavailable"}.</p>
        </article>
      </section>
    </main>
  );
}
