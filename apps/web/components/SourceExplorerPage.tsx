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
  buildApiPath,
  fetchAllPages,
  getCapabilities,
  getDistributionBins,
  getHealth,
  getSourceLatestObservations,
  getSourceTimeseries,
} from "../lib/api/client";
import type { QueryParams } from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type {
  DistributionResponse,
  GeographySummary,
  MetricSummary,
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
    () => buildObservationIndex(observations, tileMetadata?.joinKey || "geo_id"),
    [observations, tileMetadata],
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

  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }

    const request = observationTracker.begin();
    setObservationStatus({ state: "loading", message: `loading ${selectedMetric}` });

    async function loadObservations(source: ExplorerSource) {
      try {
        const payload = await getSourceLatestObservations(source.segment, {
          metric_code: selectedMetric,
          geo_level: selectedGeoLevel,
          limit: "4000",
          state_fips:
            selectedStateFips &&
            selectedGeoLevel !== "NATIONAL" &&
            sourceSupportsParameter(source, "state_fips")
              ? selectedStateFips
              : undefined,
        });
        const items = Array.isArray(payload.items) ? payload.items : [];

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
  }, [observationTracker, selectedMetric, selectedStateFips, selectedGeoLevel, activeSource]);

  useEffect(() => {
    if (!selectedMetric) {
      return;
    }

    const request = distributionTracker.begin();
    setDistribution(null);
    setDistributionStatus({ state: "loading", message: "loading API bins" });

    async function loadDistribution() {
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
  }, [distributionTracker, selectedMetric, selectedStateFips, selectedGeoLevel]);

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
        const payload = await getSourceTimeseries(source.segment, {
          metric_code: selectedMetric,
          geo_id: selectedGeoId,
          limit: "1000",
        });
        const items = Array.isArray(payload.items) ? payload.items : [];
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
  }, [timeseriesTracker, selectedMetric, selectedGeoId, activeSource]);

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
              observations,
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
              observations,
              tileMetadata.joinKey,
              distribution,
              missingValueLabel,
            ) as unknown as ExpressionSpecification,
            "fill-extrusion-height": buildExtrusionHeightExpression(
              observations,
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

      const selectedItem = observations.find((item) => item.geo_id === selectedGeoId);
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
    observations,
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

    const features = observations
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
          observations,
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
          observations,
          tileMetadata.joinKey,
          distribution,
          missingValueLabel,
        ) as unknown as ExpressionSpecification,
      );
      map.setPaintProperty(
        "choropleth-extrusion",
        "fill-extrusion-height",
        buildExtrusionHeightExpression(
          observations,
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
  }, [mapReady, observations, tileMetadata, distribution, missingValueLabel, selectedStateFips]);

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
      observations.find((item) => item.geo_id === selectedGeoId) || selectedCountyGeography;
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
  }, [mapReady, observations, selectedCountyGeography, selectedGeoId, tileMetadata]);

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
      observations,
      tileMetadata?.joinKey || "geo_id",
      distribution,
      missingValueLabel,
    ),
    [observations, tileMetadata, distribution, missingValueLabel],
  );

  const apiQuery = selectedMetric && activeSource
    ? buildApiPath(`/${activeSource.segment}/observations/latest`, {
        metric_code: selectedMetric,
        geo_level: selectedGeoLevel,
        state_fips:
          selectedStateFips && selectedGeoLevel !== "NATIONAL" ? selectedStateFips : undefined,
        limit: "4000",
      })
    : "Select a metric to generate an API query.";

  // Keep the URL a shareable reproduction of the current exploration state.
  useEffect(() => {
    if (!selectedMetric || !activeSource) {
      return;
    }

    const query = serializeExplorerState(
      {
        source: activeSource.segment,
        metric: selectedMetric,
        geoLevel: selectedGeoLevel as ExplorerState["geoLevel"],
        mapMode: mapMode as ExplorerState["mapMode"],
        stateFips: selectedStateFips,
        geoId: selectedGeoId,
      },
      {
        source: sourceKey,
        geoLevel: DEFAULT_GEO_LEVEL,
        mapMode: DEFAULT_MAP_MODE,
      },
    );
    const nextUrl = query
      ? `${window.location.pathname}?${query}`
      : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [selectedMetric, selectedGeoLevel, mapMode, selectedStateFips, selectedGeoId, activeSource, sourceKey]);

  function handleSourceChange(segment: string) {
    if (!segment || segment === activeSource?.segment) {
      return;
    }
    initialStateRef.current = null;
    setActiveSourceKey(segment);
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
    const headings = ["geo_id", "geo_name", "period", "metric_code", "value", "unit", "source", "dataset", "margin_of_error"];
    const rows = observations.map((item) => [item.geo_id, observationName(item), item.period || item.observation_date, item.metric_code, item.value, observationUnit(item), item.source || item.source_code, item.dataset || item.dataset_code, item.margin_of_error]);
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const blob = new Blob([[headings, ...rows].map((row) => row.map(escape).join(",")).join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${selectedMetric.replaceAll(":", "-")}-${selectedGeoLevel.toLowerCase()}-latest.csv`;
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
      data-source-key={activeSource?.segment || ""}
      data-source-count={explorerSources.length}
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
            aria-selected={activeSource?.segment === source.segment}
            className={activeSource?.segment === source.segment ? "source-tab selected" : "source-tab"}
            data-testid={`source-tab-${source.segment}`}
            title={source.title}
            onClick={() => handleSourceChange(source.segment)}
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
                        ? String(selectedCounty.period || selectedCounty.observation_date || "-")
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
                <TimeSeriesChart items={timeseries} />
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
                  <th>Date</th>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Units</th>
                </tr>
              </thead>
              <tbody>
                {observations.slice(0, 12).map((item) => (
                  <tr key={`${item.geo_id}-${item.observation_date}-${item.metric_code}`}>
                    <td>{String(item.county_name || item.state_name || item.geo_id || "-")}</td>
                    <td>{String(item.geo_level || "-")}</td>
                    <td>{String(item.observation_date ?? "-")}</td>
                    <td>{item.metric_code}</td>
                    <td>{item.value ?? "-"}</td>
                    <td>{String(item.units || "-")}</td>
                  </tr>
                ))}
                {observations.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="subtle">
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
