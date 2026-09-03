"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";
import { Download, Save } from "lucide-react";
import maplibregl from "maplibre-gl";
import type {
  ExpressionSpecification,
  FilterSpecification,
  MapLayerMouseEvent,
} from "maplibre-gl";
import SourceNote from "./SourceNote";
import StatusPill from "./StatusPill";
import TimeSeriesChart from "./TimeSeriesChart";
import {
  apiErrorMessage,
  apiFetch,
  buildApiPath,
  fetchAllPages,
  getCapabilities,
  getDistributionBins,
  getHealth,
} from "../lib/api/client";
import type { QueryParams } from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type {
  CollectionResponse,
  DistributionResponse,
  GeographySummary,
  MetricRelease,
  MetricReleaseListResponse,
  MetricSummary,
  Observation,
} from "../lib/api/types";
import {
  CHOROPLETH_PALETTE,
  buildChoroplethMatchExpression,
  buildChoroplethModel,
  buildExtrusionHeightExpression,
  buildObservationIndex,
  buildSelectionFilter,
  datasetFacetOptions,
  distributionBins,
  formatObservationValue,
  marginOfErrorText,
  metricDataset,
  metricOptions,
  metricSupportedGeoLevels,
  metricVariable,
  normalizeGeoLevel,
  observationJoinValue,
  observationName,
  observationToFeature,
  observationUnit,
  pickPreferredMetric,
  preferredDatasetFacet,
  preferredGeoLevelForMetric,
  tileFilterForGeoLevel,
} from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
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
  buildHistoryObservationRequest,
  buildLatestObservationRequest,
  buildReleaseListRequest,
  describeStratification,
  normalizeObservationRows,
  observationDimensionOptions,
  observationDimensionValue,
  observationPeriodLabel,
  scopedDimensionFilters,
  servesAsReleased,
  stratificationDimensions,
} from "../lib/observationAccess";
import type { ObservationScope } from "../lib/observationAccess";
import { displayMetricName } from "../lib/format";
import { saveChart } from "../lib/savedCharts";
import { discoverTileMetadata, loadPreviewTileFeatures } from "../lib/tiles";
import { parseExplorerState, serializeExplorerState } from "../lib/urlState";
import type { ExplorerState } from "../lib/urlState";

// The pure view models moved to ../lib/explorerViewModel; existing consumers
// (tests included) keep importing them from here.
export {
  buildChoroplethMatchExpression,
  buildChoroplethModel,
  buildObservationIndex,
  buildSelectionFilter,
  distributionBins,
  metricOptions,
  pickPreferredMetric,
  preferredGeoLevelForMetric,
} from "../lib/explorerViewModel";

const CATALOG_PAGE_SIZE = 1000;
const DEFAULT_GEO_LEVEL = "COUNTY";
const DEFAULT_MAP_MODE = "choropleth";
const DEFAULT_SCOPE: ObservationScope = SCOPE_LATEST;
// The release listing is a bounded, deterministic page; a metric with more
// published releases than this is reported as such rather than truncated
// into a silently partial option list.
const RELEASE_PAGE_SIZE = 200;

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
  const mapRef = useRef<maplibregl.Map | null>(null);
  const metricsTracker = useRef(createRequestTracker()).current;
  const observationTracker = useRef(createRequestTracker()).current;
  const distributionTracker = useRef(createRequestTracker()).current;
  const timeseriesTracker = useRef(createRequestTracker()).current;
  const releasesTracker = useRef(createRequestTracker()).current;
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
  const [selectedDataset, setSelectedDataset] = useState("");
  const [selectedGeoLevel, setSelectedGeoLevel] = useState(DEFAULT_GEO_LEVEL);
  const [mapMode, setMapMode] = useState(DEFAULT_MAP_MODE);
  const [selectedMetric, setSelectedMetric] = useState("");
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [countyGeographies, setCountyGeographies] = useState<GeographySummary[]>([]);
  const [geographiesError, setGeographiesError] = useState("");
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
  const [distribution, setDistribution] = useState<DistributionResponse | null>(null);
  const [distributionStatus, setDistributionStatus] = useState<RequestStatus>({
    state: "idle",
    message: "waiting for metric",
  });
  const [tileMetadata, setTileMetadata] = useState<TileMetadata | null>(null);
  const [activeSourceLayer, setActiveSourceLayer] = useState<string | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const [hoveredCounty, setHoveredCounty] = useState<HoveredCounty | null>(null);
  const [selectedGeoId, setSelectedGeoId] = useState("");
  const [timeseries, setTimeseries] = useState<ObservationRow[]>([]);
  const [timeseriesStatus, setTimeseriesStatus] = useState<RequestStatus>({
    state: "idle",
    message: "Click a geography to load its history.",
  });
  const [activeTab, setActiveTab] = useState("chart");
  const [saveStatus, setSaveStatus] = useState("");

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
  const dimensionFilters = useMemo(
    () => scopedDimensionFilters(activeSource, observationScope),
    [activeSource, observationScope],
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
      limit: "4000",
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
  const mappableObservations = useMemo(
    () => (stratification.stratified ? [] : observations),
    [observations, stratification.stratified],
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
      return [];
    },
    [selectedGeoLevel, states, countyGeographies],
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
  const missingValueLabel = selectedDataset === "acs1"
    ? "Not published in ACS1"
    : "No observation";

  // Keep the selected metric consistent with the selected dataset facet.
  useEffect(() => {
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
  }, [datasetMetrics, metrics, selectedDataset, selectedMetric, showDatasetSelector]);

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
        if (
          requested?.metric &&
          items.some((item) => item.metric_code === requested.metric)
        ) {
          setSelectedDataset(metricDataset(requested.metric));
          setSelectedMetric(requested.metric);
        } else if (items.length > 0) {
          const facet = preferredDatasetFacet(items);
          setSelectedDataset(facet);
          setSelectedMetric(pickPreferredMetric(items, facet));
        }
        if (requested?.geoLevel === "STATE" || requested?.geoLevel === "COUNTY") {
          setSelectedGeoLevel(requested.geoLevel);
        }
        if (requested?.mapMode) {
          setMapMode(requested.mapMode);
        }
        if (requested?.stateFips) setSelectedStateFips(requested.stateFips);
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
        const payload = await apiFetch<MetricReleaseListResponse>(listRequest!.resource, {
          params: listRequest!.params,
        });
        const items = Array.isArray(payload.items) ? payload.items : [];
        if (!request.isCurrent()) {
          return;
        }
        setReleases(items);
        const total = typeof payload.total === "number" ? payload.total : items.length;
        setReleasesStatus({
          state: "ok",
          message:
            items.length < total
              ? `${items.length} of ${total} published releases listed`
              : `${items.length} published release${items.length === 1 ? "" : "s"}`,
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

    const request = observationTracker.begin();
    setObservationStatus({ state: "loading", message: `loading ${selectedMetric}` });

    async function loadObservations(source: ExplorerSource) {
      try {
        const { resource, params } = buildLatestObservationRequest(source, latestQuery);
        const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params });
        const items = normalizeObservationRows(
          source,
          Array.isArray(payload.items) ? payload.items : [],
        );

        if (request.isCurrent()) {
          setObservations(items);
          setObservationStatus({
            state: "ok",
            message: items.length > 0
              ? `loaded ${items.length} ${selectedGeoLevel.toLowerCase()} records`
              : `0 ${selectedGeoLevel.toLowerCase()} records published for this selection`,
          });
        }
      } catch (error) {
        if (request.isCurrent()) {
          setObservations([]);
          setObservationStatus({ state: "bad", message: apiErrorMessage(error) });
        }
      }
    }

    loadObservations(activeSource);

    return () => {
      observationTracker.invalidate();
    };
  }, [observationTracker, selectedMetric, latestQuery, selectedGeoLevel, activeSource]);

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
          setDistributionStatus({
            state: "ok",
            message: `${payload.bin_count} API bins across ${payload.total} records`,
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
          fetchAllCatalogItems<GeographySummary>("/catalog/geographies", { geo_level: "STATE" }),
          fetchAllCatalogItems<GeographySummary>("/catalog/geographies", { geo_level: "COUNTY" }),
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

  useEffect(() => {
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
          limit: "1000",
          scope: observationScope,
          release: selectedRelease,
          dimensions: dimensionSelections,
        });
        const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params });
        const items = normalizeObservationRows(
          source,
          Array.isArray(payload.items) ? payload.items : [],
        );
        if (request.isCurrent()) {
          setTimeseries(items);
          setTimeseriesStatus({
            state: "ok",
            message: `${items.length} historical observation${items.length === 1 ? "" : "s"}`,
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

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: {
        version: 8,
        sources: {},
        layers: [
          {
            id: "background",
            type: "background",
            paint: {
              "background-color": "#dfe8ed",
            },
          },
        ],
      },
      center: [-98.5795, 39.8283],
      zoom: 3,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", () => {
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

      setMapReady(true);
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

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
        data: featureCollection as GeoJSON.FeatureCollection,
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
            ) as unknown as ExpressionSpecification,
            "fill-extrusion-height": buildExtrusionHeightExpression(
              mappableObservations,
              tileMetadata.joinKey,
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
    } as GeoJSON.FeatureCollection);

    if (map.getLayer("choropleth-fill") && tileMetadata?.joinKey) {
      map.setPaintProperty(
        "choropleth-fill",
        "fill-color",
        buildChoroplethMatchExpression(
          mappableObservations,
          tileMetadata.joinKey,
          distribution,
          missingValueLabel,
        ) as unknown as ExpressionSpecification,
      );
    }

    if (map.getLayer("choropleth-extrusion") && tileMetadata?.joinKey) {
      map.setPaintProperty(
        "choropleth-extrusion",
        "fill-extrusion-color",
        buildChoroplethMatchExpression(
          mappableObservations,
          tileMetadata.joinKey,
          distribution,
          missingValueLabel,
        ) as unknown as ExpressionSpecification,
      );
      map.setPaintProperty(
        "choropleth-extrusion",
        "fill-extrusion-height",
        buildExtrusionHeightExpression(
          mappableObservations,
          tileMetadata.joinKey,
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
      map.easeTo({ center: [-98.5, 38.5], zoom: 3.05, duration: 800 });
    }
  }, [mapReady, mappableObservations, tileMetadata, distribution, missingValueLabel, selectedStateFips]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) {
      return;
    }

    if (map.getLayer("choropleth-fill")) {
      map.setPaintProperty(
        "choropleth-fill",
        "fill-opacity",
        mapMode === "choropleth" ? 0.95 : 0.08,
      );
    }
    if (map.getLayer("choropleth-extrusion")) {
      map.setLayoutProperty(
        "choropleth-extrusion",
        "visibility",
        mapMode === "extrusion" ? "visible" : "none",
      );
    }
  }, [mapMode, mapReady]);

  useEffect(() => {
    const map = mapRef.current;
    const scopedGeographies = selectedGeoLevel === "STATE" ? states : counties;
    if (!map || !mapReady || !selectedStateFips || scopedGeographies.length === 0) {
      return;
    }

    const bounds = new maplibregl.LngLatBounds();
    for (const geography of scopedGeographies) {
      const longitude = Number(geography.longitude);
      const latitude = Number(geography.latitude);
      if (Number.isFinite(longitude) && Number.isFinite(latitude)) {
        bounds.extend([longitude, latitude]);
      }
    }

    if (!bounds.isEmpty()) {
      map.fitBounds(bounds, { padding: 45, maxZoom: 7, duration: 700 });
    }
  }, [counties, states, selectedGeoLevel, mapReady, selectedStateFips]);

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
  }, [mapReady, mappableObservations, selectedCountyGeography, selectedGeoId, tileMetadata]);

  const selectedMetricMeta = metrics.find((metric) => metric.metric_code === selectedMetric);

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
    ),
    [mappableObservations, tileMetadata, distribution, missingValueLabel],
  );

  // The exact request the observation effect issues, built by the same
  // capability-bounded builder, so the displayed path reproduces the set.
  const apiQuery = selectedMetric && activeSource
    ? (() => {
        const { resource, params } = buildLatestObservationRequest(activeSource, latestQuery);
        return buildApiPath(resource, params);
      })()
    : "Select a metric to generate an API query.";

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
        stateFips: selectedStateFips,
        geoId: selectedGeoId,
        scope: observationScope,
        release: selectedRelease,
      },
      {
        source: sourceKey,
        geoLevel: DEFAULT_GEO_LEVEL,
        mapMode: DEFAULT_MAP_MODE,
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
    selectedStateFips,
    selectedGeoId,
    activeSource,
    sourceKey,
    observationScope,
    selectedRelease,
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

  function handleSaveChart() {
    if (!selectedMetric || !selectedMetricMeta) return;
    const chart = {
      id: `${selectedMetric}:${selectedStateFips || "US"}:${selectedGeoId || "all"}`,
      version: 1,
      title: `${displayMetricName(selectedMetricMeta)} by county`,
      chartType: "choropleth",
      metricCode: selectedMetric,
      metricName: displayMetricName(selectedMetricMeta),
      source: selectedMetricMeta.source_code,
      dataset: selectedDataset || null,
      geoLevel: selectedGeoLevel,
      stateFips: selectedStateFips || null,
      geoId: selectedGeoId || null,
      transformation: "raw",
      apiQuery,
      savedAt: new Date().toISOString(),
    };
    saveChart(chart);
    setSaveStatus("Saved for Builder");
    window.setTimeout(() => setSaveStatus(""), 2400);
  }

  function exportCsv() {
    // The export carries its own reproducibility envelope: which scope and
    // release answered, and each row's own published release identity.
    const headings = ["geo_id", "geo_name", "period", "metric_code", "value", "value_status", "unit", "source", "dataset", "margin_of_error", "scope", "release", "as_of", ...dimensionFilters];
    const rows = observations.map((item) => [
      item.geo_id,
      observationName(item),
      observationPeriodLabel(item),
      item.metric_code,
      item.value,
      item.value_status,
      observationUnit(item),
      item.source || item.source_code,
      item.dataset || item.dataset_code,
      item.margin_of_error,
      observationScope,
      item.release,
      item.as_of,
      ...dimensionFilters.map((name) => observationDimensionValue(item, name)),
    ]);
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const blob = new Blob([[headings, ...rows].map((row) => row.map(escape).join(",")).join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    const scopeSuffix = asReleased
      ? `as-released${selectedRelease ? `-${selectedRelease.replaceAll(":", "-")}` : ""}`
      : "latest";
    link.download = `${selectedMetric.replaceAll(":", "-")}-${selectedGeoLevel.toLowerCase()}-${scopeSuffix}.csv`;
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
      className="dashboard"
      data-testid="dashboard"
      data-selected-dataset={selectedDataset}
      data-selected-metric={selectedMetric}
      data-metric-count={metrics.length}
      data-county-count={countyGeographies.length}
      data-selected-geo-id={selectedGeoId}
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
    >
      <header className="explorer-heading">
        <div>
          <div className="section-kicker">Analytical workbench</div>
          <h1>{activeSource ? activeSource.title : "Source"} Explorer</h1>
          <p>Build a source-visible geography view, inspect observations, and validate data availability for this MVP.</p>
        </div>
        <div className="command-row"><button className="button secondary" type="button" onClick={exportCsv} disabled={observations.length === 0}><Download size={15} /> Export CSV</button><button className="button primary" type="button" onClick={handleSaveChart} disabled={!selectedMetric}><Save size={15} /> Save view</button></div>
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
      {saveStatus ? <div className="save-toast" role="status">{saveStatus}</div> : null}

      <section className="status-row">
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
        {["chart", "table", "metadata", "api query", "notes"].map((tab) => <button role="tab" aria-selected={activeTab === tab} className={activeTab === tab ? "active" : ""} type="button" onClick={() => setActiveTab(tab)} key={tab}>{tab}</button>)}
      </div>

      <section className="grid">
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
                <option value="NATIONAL">National</option>
                <option value="STATE">State</option>
                <option value="COUNTY">County</option>
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

            {showDatasetSelector ? (
              <div className="control-group">
                <label htmlFor="dataset-select">Dataset</label>
                <select
                  id="dataset-select"
                  className="select"
                  data-testid="dataset-select"
                  value={selectedDataset}
                  onChange={(event) => setSelectedDataset(event.target.value)}
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
                onChange={(event) => setSelectedMetric(event.target.value)}
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
                disabled={selectedGeoLevel === "NATIONAL" || !supportsStateFilter}
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
              <label htmlFor="county-select">{selectedGeoLevel === "COUNTY" ? "County" : "State geography"}</label>
              <select
                id="county-select"
                className="select"
                data-testid="county-select"
                value={allGeographies.some((item) => item.geo_id === selectedGeoId) ? selectedGeoId : ""}
                onChange={(event) => setSelectedGeoId(event.target.value)}
                disabled={
                  selectedGeoLevel === "NATIONAL"
                    ? true
                    : selectedGeoLevel === "COUNTY"
                      ? (!selectedStateFips || counties.length === 0)
                      : states.length === 0
                }
              >
                <option value="">
                  {selectedGeoLevel === "NATIONAL"
                    ? "Not applicable for national view"
                    : selectedGeoLevel === "COUNTY"
                    ? (selectedStateFips ? "All counties" : "Select a state first")
                    : "All states"}
                </option>
                {(selectedGeoLevel === "COUNTY" ? counties : states).map((county) => (
                  <option value={county.geo_id} key={county.geo_id}>
                    {selectedGeoLevel === "COUNTY" ? county.county_name : county.state_name}
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
          {!supportsStateFilter && supportsGeoLevelFilter ? (
            <p className="subtle" data-testid="state-filter-note">
              {activeSource?.title} declares no state filter; the state selector scopes
              the geography list only, not the request.
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
                <h3>{selectedCounty ? observationName(selectedCounty) : "Choose a geography on the map"}</h3>
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
                  <span className={`inline-status ${timeseriesStatus.state}`}>{timeseriesStatus.message}</span>
                </div>
                {historyStratification.stratified ? (
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
                  <TimeSeriesChart items={timeseries} />
                )}
              </>
            ) : (
              <p className="subtle county-prompt">
                Hover for a quick read; click a geography to pin details and fetch its time series.
              </p>
            )}
          </section>
        </article>

        <article className="card workspace-panel" data-active={activeTab === "chart"}>
          <h2>{selectedMetricMeta ? displayMetricName(selectedMetricMeta) : `${selectedGeoLevel.toLowerCase()} map`}</h2>
          <p className="subtle">Latest {selectedGeoLevel.toLowerCase()} estimates, joined to Martin vector geometry by the discovered geography key.</p>
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
                <strong>{observationName(hoveredCounty.observation)}</strong>
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
            {choroplethModel.legendItems.length > 0 ? (
              <div className="map-legend" aria-label="Choropleth value legend">
                <div className="legend-title">
                  Value · {choroplethModel.usesDistribution ? "API distribution" : "local fallback"}
                </div>
                {choroplethModel.legendItems.map((item) => (
                  <div className="legend-row" key={`${item.color}-${item.label}`}>
                    <span className="legend-swatch" style={{ backgroundColor: item.color }} />
                    <span>
                      {item.label}
                      {Number.isFinite(item.count) ? ` (${item.count})` : ""}
                    </span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </article>

        <article className="card span-2 workspace-panel" data-active={activeTab === "table"}>
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
                  {asReleased ? <th>Release</th> : null}
                  {dimensionFilters.map((name) => (
                    <th key={name}>{name.replaceAll("_", " ")}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {observations.slice(0, 12).map((item, index) => (
                  <tr
                    key={`${item.geo_id}-${observationPeriodLabel(item)}-${item.metric_code}-${String(item.release ?? "")}-${index}`}
                  >
                    <td>{String(item.county_name || item.state_name || item.geo_id || "-")}</td>
                    <td>{String(item.geo_level || "-")}</td>
                    <td>{observationPeriodLabel(item) || "-"}</td>
                    <td>{item.metric_code}</td>
                    {/* A missing or suppressed value is never rendered as a
                        number; the source's own status says why. */}
                    <td>{item.value ?? "-"}</td>
                    <td>{String(item.value_status || (item.value === null ? "not published" : "-"))}</td>
                    <td>{observationUnit(item)}</td>
                    {asReleased ? (
                      <td>{observationDimensionValue(item, RELEASE_DIMENSION) || "-"}</td>
                    ) : null}
                    {dimensionFilters.map((name) => (
                      <td key={name}>{observationDimensionValue(item, name) || "-"}</td>
                    ))}
                  </tr>
                ))}
                {observations.length === 0 ? (
                  <tr>
                    <td
                      colSpan={7 + (asReleased ? 1 : 0) + dimensionFilters.length}
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
        <article className="card span-2 workspace-panel" data-active={activeTab === "metadata"}>
          <SourceNote source={selectedMetricMeta?.source_code} dataset={selectedDataset ? selectedDataset.toUpperCase() : activeSource?.tabLabel} metric={selectedMetricMeta ? `${displayMetricName(selectedMetricMeta)} (${selectedMetricMeta.metric_code})` : null} geography={selectedStateFips ? `${selectedGeoLevel.toLowerCase()}s in selected state` : `United States ${selectedGeoLevel.toLowerCase()}s`} period={observations[0]?.period || observations[0]?.observation_date} updatedAt={selectedMetricMeta?.harvested_at} caveats={selectedDataset === "acs1" ? "ACS 1-year county estimates are available only for counties meeting the Census population threshold." : "Validate geographies and coverage before drawing conclusions from sparse source-series values."} />
        </article>
        <article className="card span-2 workspace-panel" data-active={activeTab === "api query"}>
          <div className="section-kicker">Reproducible request</div><h2>API Query</h2><p className="subtle">This endpoint reproduces the observation set currently used by the map.</p><code className="api-query">GET {apiQuery}</code>
        </article>
        <article className="card span-2 workspace-panel" data-active={activeTab === "notes"}>
          <div className="section-kicker">Interpretation notes</div><h2>Use this view carefully</h2><p>The map uses API-calculated distribution bins, reports missing observations separately, and preserves context in the selected geography details.</p><p className="subtle">Transformation: raw value. Geography: {selectedGeoLevel.toLowerCase()}. Dataset: {selectedDataset ? selectedDataset.toUpperCase() : activeSource?.tabLabel || "Source default"}. Color treatment: five distribution-backed intervals with a local fallback only when the distribution endpoint is unavailable.</p>
        </article>
      </section>
    </main>
  );
}
