"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { VectorTile } from "@mapbox/vector-tile";
import { Download, Save } from "lucide-react";
import maplibregl from "maplibre-gl";
import Protobuf from "pbf";
import SourceNote from "../../components/SourceNote";
import { displayMetricName } from "../../lib/format";
import { saveChart } from "../../lib/savedCharts";

const COUNTY_TILE_FILTER = ["has", "county_fips"];
const CHOROPLETH_FALLBACK_COLOR = "#9fb0ba";
const CHOROPLETH_PALETTE = ["#edcf63", "#9dc57d", "#419261", "#2f7fa6", "#594a9b"];
const CATALOG_PAGE_SIZE = 1000;
const DEFAULT_ACS_DATASET = "acs5";
const DEFAULT_POPULATION_VARIABLE = "B01003_001";

function metricDataset(metricCode) {
  const parts = typeof metricCode === "string" ? metricCode.split(":") : [];
  return parts.length >= 3 ? parts[1].toLowerCase() : "";
}

function metricVariable(metricCode) {
  const parts = typeof metricCode === "string" ? metricCode.split(":") : [];
  return parts.length >= 3 ? parts.slice(2).join(":") : "";
}

function pickPreferredMetric(metrics, dataset, preferredVariable = DEFAULT_POPULATION_VARIABLE) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return "";
  }

  const datasetMetrics = metrics.filter(
    (item) => metricDataset(item.metric_code) === dataset,
  );
  const candidates = datasetMetrics.length > 0 ? datasetMetrics : metrics;
  const matchingVariable = candidates.find(
    (item) => metricVariable(item.metric_code) === preferredVariable,
  );
  const canonicalPopulation = candidates.find(
    (item) => metricVariable(item.metric_code) === DEFAULT_POPULATION_VARIABLE,
  );

  return (matchingVariable || canonicalPopulation || candidates[0]).metric_code;
}

function metricOptions(metrics) {
  return (metrics || []).map((metric) => ({
    value: metric.metric_code,
    label: `${metric.metric_display_name.replaceAll("!!", " › ")} (${metric.metric_code})`,
    source: metric.source_code,
  }));
}

async function fetchAllCatalogItems(path, query = {}) {
  const items = [];
  let offset = 0;
  let total = null;

  do {
    const params = new URLSearchParams({
      ...query,
      limit: String(CATALOG_PAGE_SIZE),
      offset: String(offset),
    });
    const response = await fetch(`${path}?${params.toString()}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`status ${response.status}`);
    }

    const payload = await response.json();
    const pageItems = Array.isArray(payload.items) ? payload.items : [];
    total = Number.isFinite(Number(payload.total)) ? Number(payload.total) : null;
    items.push(...pageItems);
    offset += pageItems.length;

    if (pageItems.length === 0) {
      break;
    }
  } while (total === null || items.length < total);

  return items;
}

function observationToFeature(item) {
  const longitude = Number(item?.geo_longitude);
  const latitude = Number(item?.geo_latitude);

  if (!item || !Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    return null;
  }

  return {
    type: "Feature",
    properties: {
      geo_id: item.geo_id,
      geo_level: item.geo_level,
      metric_code: item.metric_code,
      value: item.value,
      name: item.county_name || item.state_name || item.geo_id,
    },
    geometry: {
      type: "Point",
      coordinates: [longitude, latitude],
    },
  };
}

function isCountyObservation(item) {
  if (!item) {
    return false;
  }

  if (typeof item.geo_level === "string" && item.geo_level.toUpperCase() === "COUNTY") {
    return true;
  }

  if (item.county_fips) {
    return true;
  }

  return typeof item.geo_id === "string" && item.geo_id.toLowerCase().includes("|county:");
}

function collectTileCandidates(catalogPayload) {
  const candidates = [];

  if (Array.isArray(catalogPayload)) {
    for (const entry of catalogPayload) {
      if (typeof entry === "string") {
        candidates.push(entry);
      } else if (entry && typeof entry.id === "string") {
        candidates.push(entry.id);
      }
    }
  } else if (catalogPayload && typeof catalogPayload === "object") {
    if (Array.isArray(catalogPayload.collections)) {
      for (const entry of catalogPayload.collections) {
        if (entry && typeof entry.id === "string") {
          candidates.push(entry.id);
        }
      }
    }

    for (const key of Object.keys(catalogPayload)) {
      if (key !== "collections") {
        candidates.push(key);
      }
    }
  }

  return [...new Set(candidates)].filter(Boolean);
}

function prioritizeTileCandidates(candidates) {
  const preferredOrder = ["dim_geo", "dim_geo_latest", "counties"];
  const remaining = [];
  const seen = new Set();

  for (const candidate of Array.isArray(candidates) ? candidates : []) {
    if (typeof candidate !== "string" || !candidate || seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    remaining.push(candidate);
  }

  const prioritized = [];
  for (const preferred of preferredOrder) {
    if (seen.has(preferred)) {
      prioritized.push(preferred);
    }
  }

  for (const candidate of remaining) {
    if (!prioritized.includes(candidate)) {
      prioritized.push(candidate);
    }
  }

  return prioritized;
}

function pickJoinKey(fields = {}) {
  const fieldKeys = Array.isArray(fields)
    ? fields
    : Object.keys(fields || {});
  const preferred = ["geo_id", "geoid", "GEOID", "county_fips", "state_fips"];

  for (const preferredKey of preferred) {
    const matched = fieldKeys.find(
      (key) => typeof key === "string" && key.toLowerCase() === preferredKey.toLowerCase(),
    );
    if (matched) {
      return matched;
    }
  }

  return "geo_id";
}

function normalizeTileTemplate(layerId) {
  return `/tiles/${layerId}/{z}/{x}/{y}`;
}

function normalizeTileJsonUrl(layerId) {
  return `/tiles/${layerId}`;
}

function isVectorTileContentType(contentType) {
  const normalized = (contentType || "").toLowerCase();
  return (
    normalized.includes("application/x-protobuf") ||
    normalized.includes("application/vnd.mapbox-vector-tile") ||
    normalized.includes("application/octet-stream")
  );
}

function normalizeTileTemplateFromTileJson(rawTemplate) {
  if (typeof rawTemplate !== "string" || !rawTemplate) {
    return "";
  }

  let path = rawTemplate;

  if (rawTemplate.startsWith("http://") || rawTemplate.startsWith("https://")) {
    try {
      const parsed = new URL(rawTemplate);
      path = `${parsed.pathname}${parsed.search}`;
    } catch {
      return "";
    }
  }

  if (!path.startsWith("/")) {
    path = `/${path}`;
  }

  if (path.startsWith("/tiles/")) {
    return path;
  }

  return `/tiles${path}`;
}

function buildSampleUrlFromTemplate(tileTemplate) {
  return tileTemplate
    .replaceAll("{z}", "0")
    .replaceAll("{x}", "0")
    .replaceAll("{y}", "0")
    .replaceAll(
      "{bbox-epsg-3857}",
      "-20037508.342789244,-20037508.342789244,20037508.342789244,20037508.342789244",
    );
}

async function discoverTileMetadata() {
  const discoveryPaths = ["/tiles/catalog", "/tiles/"];
  let prioritizedCandidates = [];

  for (const path of discoveryPaths) {
    try {
      const response = await fetch(path, { cache: "no-store" });
      if (!response.ok) {
        continue;
      }

      const payload = await response.json();
      const candidates = collectTileCandidates(payload);
      if (candidates.length > 0) {
        prioritizedCandidates = prioritizeTileCandidates(candidates);
        break;
      }
    } catch {
      // Continue to fallback discovery endpoint.
    }
  }

  if (prioritizedCandidates.length === 0) {
    throw new Error("No tile layer ids discovered from /tiles/catalog or /tiles/");
  }

  for (const id of prioritizedCandidates) {
    try {
      const tileJsonResponse = await fetch(`/tiles/${id}`, { cache: "no-store" });
      if (!tileJsonResponse.ok) {
        continue;
      }

      const tileJson = await tileJsonResponse.json();
      const vectorLayer =
        Array.isArray(tileJson.vector_layers) && tileJson.vector_layers.length > 0
          ? tileJson.vector_layers[0]
          : null;
      const sourceLayerCandidates = [];

      if (Array.isArray(tileJson.vector_layers)) {
        for (const item of tileJson.vector_layers) {
          if (item && typeof item.id === "string") {
            sourceLayerCandidates.push(item.id);
          }
        }
      }

      if (typeof tileJson.name === "string") {
        sourceLayerCandidates.push(tileJson.name);
      }

      sourceLayerCandidates.push(id);

      const dedupedSourceLayerCandidates = [];
      const seenCandidates = new Set();
      for (const candidate of sourceLayerCandidates) {
        if (!candidate || seenCandidates.has(candidate)) {
          continue;
        }
        seenCandidates.add(candidate);
        dedupedSourceLayerCandidates.push(candidate);
      }

      const tileTemplateCandidates = [];

      if (Array.isArray(tileJson.tiles)) {
        for (const rawTemplate of tileJson.tiles) {
          const normalizedTemplate = normalizeTileTemplateFromTileJson(rawTemplate);
          if (normalizedTemplate) {
            tileTemplateCandidates.push(normalizedTemplate);
          }
        }
      }

      tileTemplateCandidates.push(normalizeTileTemplate(id));
      tileTemplateCandidates.push(`/${id}/{z}/{x}/{y}`);
      tileTemplateCandidates.push(`/${id}/{z}/{x}/{y}.pbf`);
      tileTemplateCandidates.push(`/tiles/${id}/{z}/{x}/{y}`);
      tileTemplateCandidates.push(`/tiles/${id}/{z}/{x}/{y}.pbf`);

      const dedupedTileTemplateCandidates = [];
      const seenTileTemplates = new Set();
      for (const candidateTemplate of tileTemplateCandidates) {
        if (!candidateTemplate || seenTileTemplates.has(candidateTemplate)) {
          continue;
        }
        seenTileTemplates.add(candidateTemplate);
        dedupedTileTemplateCandidates.push(candidateTemplate);
      }

      let selectedTileTemplate = null;
      for (const candidateTemplate of dedupedTileTemplateCandidates) {
        const sampleUrl = buildSampleUrlFromTemplate(candidateTemplate);
        const sampleTileResponse = await fetch(sampleUrl, { cache: "no-store" });
        const sampleContentType = sampleTileResponse.headers.get("content-type") || "";

        if (sampleTileResponse.ok && isVectorTileContentType(sampleContentType)) {
          selectedTileTemplate = candidateTemplate;
          break;
        }
      }

      if (!selectedTileTemplate) {
        continue;
      }

      const sourceLayerId = dedupedSourceLayerCandidates[0] || id;
      const joinKey = pickJoinKey(vectorLayer?.fields || {});

      return {
        layerId: id,
        sourceLayer: sourceLayerId,
        sourceLayerCandidates: dedupedSourceLayerCandidates,
        joinKey,
        tileJsonUrl: normalizeTileJsonUrl(id),
        tileTemplate: selectedTileTemplate,
      };
    } catch {
      // Try next layer id.
    }
  }

  throw new Error("No healthy vector tile endpoint found from discovered /tiles/{id} candidates");
}

async function loadPreviewTileFeatures(tileTemplate, sourceLayer) {
  const sampleUrl = buildSampleUrlFromTemplate(tileTemplate);
  const response = await fetch(sampleUrl, { cache: "no-store" });

  if (!response.ok) {
    throw new Error(`tile sample status ${response.status}`);
  }

  const tile = new VectorTile(new Protobuf(new Uint8Array(await response.arrayBuffer())));
  const layer = tile.layers[sourceLayer] || tile.layers[Object.keys(tile.layers)[0]];

  if (!layer) {
    throw new Error("tile sample contained no vector layers");
  }

  const features = [];
  for (let index = 0; index < layer.length; index += 1) {
    const feature = layer.feature(index).toGeoJSON(0, 0, 0);
    if (isCountyObservation(feature.properties)) {
      features.push(feature);
    }
  }

  return {
    type: "FeatureCollection",
    features,
  };
}

function observationJoinValue(item, joinKey) {
  const normalizedJoinKey = typeof joinKey === "string" ? joinKey.toLowerCase() : "geo_id";

  if (normalizedJoinKey === "geo_id") {
    return item.geo_id || null;
  }

  if (normalizedJoinKey === "geoid") {
    if (item.state_fips && item.county_fips) {
      return `${item.state_fips}${item.county_fips}`;
    }
    if (item.state_fips) {
      return item.state_fips;
    }
    if (typeof item.geo_id === "string") {
      const countyMatch = item.geo_id.match(/^state:(\d{2})\|county:(\d{3})$/i);
      if (countyMatch) {
        return `${countyMatch[1]}${countyMatch[2]}`;
      }

      const stateMatch = item.geo_id.match(/^state:(\d{2})$/i);
      if (stateMatch) {
        return stateMatch[1];
      }
    }
  }

  if (normalizedJoinKey === "county_fips") {
    return item.county_fips || null;
  }

  if (normalizedJoinKey === "state_fips") {
    return item.state_fips || null;
  }

  return item[joinKey] || item.geo_id || null;
}

function colorForValue(value, minValue, maxValue) {
  if (!Number.isFinite(value) || !Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return CHOROPLETH_FALLBACK_COLOR;
  }

  const span = maxValue - minValue;
  const ratio = span <= 0 ? 0 : (value - minValue) / span;
  const index = Math.max(0, Math.min(CHOROPLETH_PALETTE.length - 1, Math.floor(ratio * CHOROPLETH_PALETTE.length)));
  return CHOROPLETH_PALETTE[index];
}

function distributionBins(payload) {
  const minValue = Number(payload?.min_value);
  const maxValue = Number(payload?.max_value);
  const binCount = Number(payload?.bin_count);

  if (
    !Number.isFinite(minValue) ||
    !Number.isFinite(maxValue) ||
    !Number.isInteger(binCount) ||
    binCount < 1 ||
    binCount > CHOROPLETH_PALETTE.length ||
    Number(payload?.total) < 1
  ) {
    return [];
  }

  const counts = new Map(
    (payload.items || []).map((item) => [Number(item.bin_index), Number(item.count) || 0]),
  );
  const width = (maxValue - minValue) / binCount;

  return CHOROPLETH_PALETTE.slice(0, binCount).map((color, index) => ({
    binIndex: index + 1,
    color,
    lowerBound: minValue + index * width,
    upperBound: index === binCount - 1 ? maxValue : minValue + (index + 1) * width,
    count: counts.get(index + 1) || 0,
  }));
}

function colorForDistributionValue(value, bins) {
  if (!Number.isFinite(value) || bins.length === 0) {
    return CHOROPLETH_FALLBACK_COLOR;
  }

  const matched = bins.find(
    (bin, index) => index === bins.length - 1 || value < bin.upperBound,
  );
  return matched?.color || CHOROPLETH_FALLBACK_COLOR;
}

function formatLegendValue(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    notation: Math.abs(value) >= 10000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 10000 ? 1 : 0,
  }).format(value);
}

function formatObservationValue(value, maximumFractionDigits = 1) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(numericValue);
}

function observationName(item) {
  if (!item) {
    return "Unknown county";
  }

  const county = item.geo_name || item.county_name || item.geo_id || "Unknown county";
  return item.state_name ? `${county}, ${item.state_name}` : county;
}

function observationUnit(item) {
  return item?.unit || item?.units || "value";
}

function marginOfErrorText(item) {
  const marginOfError = Number(item?.margin_of_error);
  if (Number.isFinite(marginOfError) && marginOfError >= 0) {
    const marginPct = Number(item?.margin_of_error_pct);
    const pctText = Number.isFinite(marginPct) && marginPct >= 0
      ? ` (${formatObservationValue(marginPct, 2)}%)`
      : "";
    return `±${formatObservationValue(marginOfError)}${pctText}`;
  }

  if (marginOfError === -555555555) {
    return "0 (Census-controlled estimate)";
  }
  if (marginOfError === -222222222) {
    return "Not computed (insufficient sample)";
  }
  if (marginOfError === -333333333) {
    return "Not computed (open-ended median)";
  }
  if (marginOfError === -666666666 || marginOfError === -888888888) {
    return "Not applicable";
  }
  if (marginOfError === -999999999) {
    return "Suppressed (sample too small)";
  }

  return "Not provided";
}

function buildObservationIndex(observations, joinKey) {
  const index = new Map();

  for (const item of observations || []) {
    const joinValue = observationJoinValue(item, joinKey);
    if (joinValue !== null && joinValue !== undefined && joinValue !== "") {
      index.set(String(joinValue), item);
    }
  }

  return index;
}

function buildSelectionFilter(joinValue, joinKey) {
  return [
    "==",
    ["to-string", ["get", joinKey]],
    joinValue === null || joinValue === undefined ? "__no_selected_county__" : String(joinValue),
  ];
}

function TimeSeriesChart({ items }) {
  const series = (items || [])
    .map((item) => ({ ...item, numericValue: Number(item.value) }))
    .filter((item) => Number.isFinite(item.numericValue))
    .sort((left, right) => String(left.observation_date).localeCompare(String(right.observation_date)));

  if (series.length === 0) {
    return <p className="subtle chart-empty">No time-series observations are available.</p>;
  }

  const width = 640;
  const height = 190;
  const paddingX = 26;
  const paddingTop = 18;
  const paddingBottom = 34;
  const values = series.map((item) => item.numericValue);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueSpan = maxValue - minValue || 1;
  const chartWidth = width - paddingX * 2;
  const chartHeight = height - paddingTop - paddingBottom;
  const points = series.map((item, index) => {
    const x = series.length === 1
      ? width / 2
      : paddingX + (index / (series.length - 1)) * chartWidth;
    const y = paddingTop + ((maxValue - item.numericValue) / valueSpan) * chartHeight;
    return { ...item, x, y };
  });

  return (
    <div className="timeseries-chart">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${series.length} time-series observations from ${series[0].observation_date} to ${series[series.length - 1].observation_date}`}
      >
        <line className="chart-gridline" x1={paddingX} x2={width - paddingX} y1={paddingTop} y2={paddingTop} />
        <line className="chart-gridline" x1={paddingX} x2={width - paddingX} y1={paddingTop + chartHeight} y2={paddingTop + chartHeight} />
        {points.length > 1 ? (
          <polyline
            className="chart-line"
            points={points.map((point) => `${point.x},${point.y}`).join(" ")}
          />
        ) : null}
        {points.map((point) => (
          <circle key={`${point.observation_date}-${point.value}`} className="chart-point" cx={point.x} cy={point.y} r="4">
            <title>{`${point.observation_date}: ${formatObservationValue(point.numericValue)}`}</title>
          </circle>
        ))}
        <text className="chart-label" x={paddingX} y={height - 8}>{series[0].observation_date}</text>
        <text className="chart-label chart-label-end" x={width - paddingX} y={height - 8}>{series[series.length - 1].observation_date}</text>
        <text className="chart-value-label" x={paddingX} y={paddingTop - 5}>{formatObservationValue(maxValue)}</text>
        <text className="chart-value-label" x={paddingX} y={paddingTop + chartHeight - 5}>{formatObservationValue(minValue)}</text>
      </svg>
    </div>
  );
}

function buildChoroplethModel(
  observations,
  joinKey,
  distribution = null,
  missingValueLabel = "No observation",
) {
  if (!Array.isArray(observations) || observations.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [{ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel }],
      minValue: null,
      maxValue: null,
      usesDistribution: false,
    };
  }

  const keyedValues = [];
  const keyedMap = new Map();

  for (const item of observations) {
    const joinValue = observationJoinValue(item, joinKey);
    const numericValue = Number(item.value);
    if (!joinValue || !Number.isFinite(numericValue)) {
      continue;
    }

    keyedMap.set(String(joinValue), numericValue);
  }

  const values = [...keyedMap.values()];
  if (values.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [{ color: CHOROPLETH_FALLBACK_COLOR, label: missingValueLabel }],
      minValue: null,
      maxValue: null,
      usesDistribution: false,
    };
  }

  const apiBins = distributionBins(distribution);
  const usesDistribution = apiBins.length > 0;
  const minValue = usesDistribution ? apiBins[0].lowerBound : Math.min(...values);
  const maxValue = usesDistribution ? apiBins[apiBins.length - 1].upperBound : Math.max(...values);
  const span = maxValue - minValue;

  for (const [key, numericValue] of keyedMap.entries()) {
    keyedValues.push(
      key,
      usesDistribution
        ? colorForDistributionValue(numericValue, apiBins)
        : colorForValue(numericValue, minValue, maxValue),
    );
  }

  const legendItems = usesDistribution
    ? apiBins.map((bin, index) => ({
        color: bin.color,
        label: apiBins.length === 1
          ? "All numeric values"
          : index === 0
            ? `Up to ${formatLegendValue(bin.upperBound)}`
            : index === apiBins.length - 1
              ? `${formatLegendValue(bin.lowerBound)} and above`
              : `${formatLegendValue(bin.lowerBound)} - ${formatLegendValue(bin.upperBound)}`,
        count: bin.count,
      }))
    : CHOROPLETH_PALETTE.map((color, index) => {
    if (span <= 0) {
      return {
        color,
        label: formatLegendValue(minValue),
      };
    }

    const start = minValue + (span * index) / CHOROPLETH_PALETTE.length;
    const end = index === CHOROPLETH_PALETTE.length - 1
      ? maxValue
      : minValue + (span * (index + 1)) / CHOROPLETH_PALETTE.length;

    return {
      color,
      label: `${formatLegendValue(start)} - ${formatLegendValue(end)}`,
    };
  });

  legendItems.push({
    color: CHOROPLETH_FALLBACK_COLOR,
    label: missingValueLabel,
  });

  return {
    expression: ["match", ["to-string", ["get", joinKey]], ...keyedValues, CHOROPLETH_FALLBACK_COLOR],
    legendItems,
    minValue,
    maxValue,
    usesDistribution,
  };
}

function buildChoroplethMatchExpression(
  observations,
  joinKey,
  distribution = null,
  missingValueLabel = "No observation",
) {
  return buildChoroplethModel(
    observations,
    joinKey,
    distribution,
    missingValueLabel,
  ).expression;
}

export default function ExplorerPage() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);

  const [apiHealth, setApiHealth] = useState({
    state: "loading",
    message: "checking /api/health",
  });
  const [tilesHealth, setTilesHealth] = useState({
    state: "loading",
    message: "checking /tiles/catalog",
  });
  const [metrics, setMetrics] = useState([]);
  const [metricsError, setMetricsError] = useState("");
  const [selectedDataset, setSelectedDataset] = useState(DEFAULT_ACS_DATASET);
  const [selectedMetric, setSelectedMetric] = useState("");
  const [states, setStates] = useState([]);
  const [countyGeographies, setCountyGeographies] = useState([]);
  const [geographiesError, setGeographiesError] = useState("");
  const [selectedStateFips, setSelectedStateFips] = useState("");

  const [observationStatus, setObservationStatus] = useState({
    state: "idle",
    message: "selecting metric",
  });
  const [observations, setObservations] = useState([]);
  const [distribution, setDistribution] = useState(null);
  const [distributionStatus, setDistributionStatus] = useState({
    state: "idle",
    message: "waiting for metric",
  });
  const [tileMetadata, setTileMetadata] = useState(null);
  const [activeSourceLayer, setActiveSourceLayer] = useState(null);
  const [mapReady, setMapReady] = useState(false);
  const [hoveredCounty, setHoveredCounty] = useState(null);
  const [selectedGeoId, setSelectedGeoId] = useState("");
  const [timeseries, setTimeseries] = useState([]);
  const [timeseriesStatus, setTimeseriesStatus] = useState({
    state: "idle",
    message: "Click a county to load its history.",
  });
  const [activeTab, setActiveTab] = useState("chart");
  const [saveStatus, setSaveStatus] = useState("");

  const datasetMetrics = useMemo(
    () => metrics.filter((metric) => metricDataset(metric.metric_code) === selectedDataset),
    [metrics, selectedDataset],
  );
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
  const observationIndex = useMemo(
    () => buildObservationIndex(observations, tileMetadata?.joinKey || "geo_id"),
    [observations, tileMetadata],
  );
  const selectedObservation = useMemo(
    () => observations.find((item) => item.geo_id === selectedGeoId) || null,
    [observations, selectedGeoId],
  );
  const selectedCountyGeography = useMemo(
    () => countyGeographies.find((item) => item.geo_id === selectedGeoId) || null,
    [countyGeographies, selectedGeoId],
  );
  const selectedCounty =
    selectedObservation || timeseries[timeseries.length - 1] || selectedCountyGeography || null;
  const selectedCountyHasObservation = Boolean(selectedObservation || timeseries.length > 0);
  const geographyIndex = useMemo(
    () => buildObservationIndex(countyGeographies, tileMetadata?.joinKey || "geo_id"),
    [countyGeographies, tileMetadata],
  );
  const missingValueLabel = selectedDataset === "acs1"
    ? "Not published in ACS1"
    : "No observation";

  useEffect(() => {
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
  }, [datasetMetrics, metrics, selectedDataset, selectedMetric]);

  useEffect(() => {
    let cancelled = false;

    async function bootstrap() {
      try {
        const healthResponse = await fetch("/api/health", { cache: "no-store" });
        if (!healthResponse.ok) {
          throw new Error(`status ${healthResponse.status}`);
        }
        const payload = await healthResponse.json();
        if (!cancelled) {
          setApiHealth({ state: "ok", message: payload.status || "ok" });
        }
      } catch (error) {
        if (!cancelled) {
          setApiHealth({ state: "bad", message: error.message });
        }
      }

      try {
        const items = await fetchAllCatalogItems("/api/catalog/metrics", {
          source_code: "CENSUS_ACS",
          active_only: "true",
          dashboard_suitability: "PUBLIC_SAFE",
        });

        if (!cancelled) {
          setMetrics(items);
          const params = new URLSearchParams(window.location.search);
          const requestedMetric = params.get("metric");
          const requestedState = params.get("state");
          const requestedGeo = params.get("geo");
          if (requestedMetric && items.some((item) => item.metric_code === requestedMetric)) {
            setSelectedDataset(metricDataset(requestedMetric) || DEFAULT_ACS_DATASET);
            setSelectedMetric(requestedMetric);
          }
          if (requestedState) setSelectedStateFips(requestedState);
          if (requestedGeo) setSelectedGeoId(requestedGeo);
        }
      } catch (error) {
        if (!cancelled) {
          setMetricsError(error.message || "Unable to load metrics.");
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
            message: error.message || "catalog unavailable",
          });
        }
      }
    }

    bootstrap();

    return () => {
      cancelled = true;
    };
  }, []);

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
    if (!selectedMetric) {
      return;
    }

    let cancelled = false;
    setObservationStatus({ state: "loading", message: `loading ${selectedMetric}` });

    async function loadObservations() {
      try {
        const query = new URLSearchParams({
          metric_code: selectedMetric,
          geo_level: "COUNTY",
          limit: "4000",
        });
        if (selectedStateFips) {
          query.set("state_fips", selectedStateFips);
        }
        const response = await fetch(`/api/observations/latest?${query.toString()}`, {
          cache: "no-store",
        });

        if (!response.ok) {
          throw new Error(`status ${response.status}`);
        }

        const payload = await response.json();
        const items = Array.isArray(payload.items) ? payload.items : [];

        if (!cancelled) {
          setObservations(items);
          setObservationStatus({
            state: "ok",
            message: items.length > 0
              ? `loaded ${items.length} county records`
              : `0 counties published for this selection`,
          });
        }
      } catch (error) {
        if (!cancelled) {
          setObservations([]);
          setObservationStatus({ state: "bad", message: error.message });
        }
      }
    }

    loadObservations();

    return () => {
      cancelled = true;
    };
  }, [selectedMetric, selectedStateFips]);

  useEffect(() => {
    if (!selectedMetric) {
      return;
    }

    let cancelled = false;
    setDistribution(null);
    setDistributionStatus({ state: "loading", message: "loading API bins" });

    async function loadDistribution() {
      try {
        const query = new URLSearchParams({
          metric_code: selectedMetric,
          geo_level: "COUNTY",
          bin_count: String(CHOROPLETH_PALETTE.length),
        });
        if (selectedStateFips) {
          query.set("state_fips", selectedStateFips);
        }
        const response = await fetch(`/api/distribution/bins?${query.toString()}`, {
          cache: "no-store",
        });

        if (!response.ok) {
          throw new Error(`status ${response.status}`);
        }

        const payload = await response.json();
        if (Number(payload.total) === 0) {
          if (!cancelled) {
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

        if (!cancelled) {
          setDistribution(payload);
          setDistributionStatus({
            state: "ok",
            message: `${payload.bin_count} API bins across ${payload.total} records`,
          });
        }
      } catch (error) {
        if (!cancelled) {
          setDistribution(null);
          setDistributionStatus({
            state: "warn",
            message: `${error.message}; using local fallback`,
          });
        }
      }

      try {
        const [stateItems, countyItems] = await Promise.all([
          fetchAllCatalogItems("/api/catalog/geographies", { geo_level: "STATE" }),
          fetchAllCatalogItems("/api/catalog/geographies", { geo_level: "COUNTY" }),
        ]);

        if (!cancelled) {
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
        if (!cancelled) {
          setGeographiesError(error.message || "Unable to load geography selectors.");
        }
      }
    }

    loadDistribution();

    return () => {
      cancelled = true;
    };
  }, [selectedMetric, selectedStateFips]);

  useEffect(() => {
    if (!selectedMetric || !selectedGeoId) {
      setTimeseries([]);
      setTimeseriesStatus({
        state: "idle",
        message: "Click a county to load its history.",
      });
      return;
    }

    let cancelled = false;
    setTimeseries([]);
    setTimeseriesStatus({ state: "loading", message: "Loading county history..." });

    async function loadTimeseries() {
      try {
        const query = new URLSearchParams({
          metric_code: selectedMetric,
          geo_id: selectedGeoId,
          limit: "1000",
        });
        const response = await fetch(`/api/observations/timeseries?${query.toString()}`, {
          cache: "no-store",
        });

        if (!response.ok) {
          throw new Error(`status ${response.status}`);
        }

        const payload = await response.json();
        const items = Array.isArray(payload.items) ? payload.items : [];
        if (!cancelled) {
          setTimeseries(items);
          setTimeseriesStatus({
            state: "ok",
            message: `${items.length} historical observation${items.length === 1 ? "" : "s"}`,
          });
        }
      } catch (error) {
        if (!cancelled) {
          setTimeseries([]);
          setTimeseriesStatus({
            state: "bad",
            message: error.message || "Unable to load county history.",
          });
        }
      }
    }

    loadTimeseries();

    return () => {
      cancelled = true;
    };
  }, [selectedMetric, selectedGeoId]);

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

    map.on("load", async () => {
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

    const handleCountyMove = (event) => {
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

    const handleCountyClick = (event) => {
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
        setSelectedGeoId(county.geo_id);
      }
    };

    const removeChoropleth = () => {
      if (map.getLayer("choropleth-selected")) {
        map.removeLayer("choropleth-selected");
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

    const addChoropleth = async (sourceLayer) => {
      if (!sourceLayer) {
        return;
      }

      const featureCollection = await loadPreviewTileFeatures(tileMetadata.tileTemplate, sourceLayer);
      if (cancelled) {
        return;
      }

      removeChoropleth();

      map.addSource("choropleth", {
        type: "geojson",
        data: featureCollection,
      });

      map.addLayer(
        {
          id: "choropleth-fill",
          type: "fill",
          source: "choropleth",
          filter: COUNTY_TILE_FILTER,
          paint: {
            "fill-color": buildChoroplethMatchExpression(
              observations,
              tileMetadata.joinKey,
              distribution,
              missingValueLabel,
            ),
            "fill-opacity": 0.95,
          },
        },
        "obs-points",
      );

      map.addLayer(
        {
          id: "choropleth-outline",
          type: "line",
          source: "choropleth",
          filter: COUNTY_TILE_FILTER,
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
          filter: buildSelectionFilter(selectedJoinValue, tileMetadata.joinKey),
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

    addChoropleth(currentSourceLayer).catch((error) => {
      if (!cancelled) {
        setTilesHealth({
          state: "warn",
          message: error.message || "tile preview render failed",
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
    observations,
    observationIndex,
    geographyIndex,
    distribution,
    missingValueLabel,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) {
      return;
    }

    const source = map.getSource("obs");
    if (!source) {
      return;
    }

    const features = observations
      .map((item) => observationToFeature(item))
      .filter(Boolean);

    source.setData({
      type: "FeatureCollection",
      features,
    });

    if (map.getLayer("choropleth-fill") && tileMetadata?.joinKey) {
      map.setPaintProperty(
        "choropleth-fill",
        "fill-color",
        buildChoroplethMatchExpression(
          observations,
          tileMetadata.joinKey,
          distribution,
          missingValueLabel,
        ),
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
    if (!map || !mapReady || !selectedStateFips || counties.length === 0) {
      return;
    }

    const bounds = new maplibregl.LngLatBounds();
    for (const county of counties) {
      const longitude = Number(county.longitude);
      const latitude = Number(county.latitude);
      if (Number.isFinite(longitude) && Number.isFinite(latitude)) {
        bounds.extend([longitude, latitude]);
      }
    }

    if (!bounds.isEmpty()) {
      map.fitBounds(bounds, { padding: 45, maxZoom: 7, duration: 700 });
    }
  }, [counties, mapReady, selectedStateFips]);

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
      buildSelectionFilter(selectedJoinValue, tileMetadata.joinKey),
    );
  }, [mapReady, observations, selectedCountyGeography, selectedGeoId, tileMetadata]);

  const selectedMetricMeta = metrics.find((metric) => metric.metric_code === selectedMetric);
  const choroplethModel = useMemo(
    () => buildChoroplethModel(
      observations,
      tileMetadata?.joinKey || "geo_id",
      distribution,
      missingValueLabel,
    ),
    [observations, tileMetadata, distribution, missingValueLabel],
  );

  function pillClass(status) {
    if (status === "ok") {
      return "pill ok";
    }
    if (status === "warn" || status === "loading" || status === "idle") {
      return "pill warn";
    }
    return "pill bad";
  }

  const apiQuery = selectedMetric
    ? `/api/observations/latest?metric_code=${encodeURIComponent(selectedMetric)}&geo_level=COUNTY${selectedStateFips ? `&state_fips=${selectedStateFips}` : ""}&limit=5000`
    : "Select a metric to generate an API query.";

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
      dataset: selectedDataset,
      geoLevel: "COUNTY",
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
    const escape = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const blob = new Blob([[headings, ...rows].map((row) => row.map(escape).join(",")).join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${selectedMetric.replaceAll(":", "-")}-county-latest.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  return (
    <main
      className="dashboard"
      data-testid="dashboard"
      data-selected-dataset={selectedDataset}
      data-selected-metric={selectedMetric}
      data-metric-count={metrics.length}
      data-county-count={countyGeographies.length}
    >
      <header className="explorer-heading">
        <div><div className="section-kicker">Analytical workbench</div><h1>Indicator Explorer</h1><p>Build a source-visible county view, inspect its observations, and save the configuration for reuse.</p></div>
        <div className="command-row"><button className="button secondary" type="button" onClick={exportCsv} disabled={observations.length === 0}><Download size={15} /> Export CSV</button><button className="button primary" type="button" onClick={handleSaveChart} disabled={!selectedMetric}><Save size={15} /> Save view</button></div>
      </header>
      {saveStatus ? <div className="save-toast" role="status">{saveStatus}</div> : null}

      <section className="status-row">
        <span className={pillClass(apiHealth.state)} data-testid="api-status">
          API: <strong>{apiHealth.message}</strong>
        </span>
        <span className={pillClass(tilesHealth.state)} data-testid="tiles-status">
          Tiles: <strong>{tilesHealth.message}</strong>
        </span>
        <span className={pillClass(observationStatus.state)} data-testid="observations-status">
          Observations: <strong>{observationStatus.message}</strong>
        </span>
        <span className={pillClass(distributionStatus.state)} data-testid="distribution-status">
          Distribution: <strong>{distributionStatus.message}</strong>
        </span>
      </section>

      <div className="workspace-tabs" role="tablist" aria-label="Explorer views">
        {["chart", "table", "metadata", "api query", "notes"].map((tab) => <button role="tab" aria-selected={activeTab === tab} className={activeTab === tab ? "active" : ""} type="button" onClick={() => setActiveTab(tab)} key={tab}>{tab}</button>)}
      </div>

      <section className="grid">
        <article className="card">
          <h2>Data &amp; Geography</h2>
          <div className="selector-grid">
            <div className="control-group">
              <label htmlFor="dataset-select">ACS dataset</label>
              <select
                id="dataset-select"
                className="select"
                data-testid="dataset-select"
                value={selectedDataset}
                onChange={(event) => setSelectedDataset(event.target.value)}
              >
                <option value="acs5">ACS 5-year — complete county coverage</option>
                <option value="acs1">ACS 1-year — partial county coverage</option>
              </select>
            </div>

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
              >
                <option value="">All states</option>
                {states.map((state) => (
                  <option value={state.state_fips} key={state.geo_id}>
                    {state.state_name}
                  </option>
                ))}
              </select>
            </div>

            <div className="control-group">
              <label htmlFor="county-select">County</label>
              <select
                id="county-select"
                className="select"
                data-testid="county-select"
                value={counties.some((county) => county.geo_id === selectedGeoId) ? selectedGeoId : ""}
                onChange={(event) => setSelectedGeoId(event.target.value)}
                disabled={!selectedStateFips || counties.length === 0}
              >
                <option value="">
                  {selectedStateFips ? "All counties" : "Select a state first"}
                </option>
                {counties.map((county) => (
                  <option value={county.geo_id} key={county.geo_id}>
                    {county.county_name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {metricsError ? <p className="subtle">Metrics error: {metricsError}</p> : null}
          {geographiesError ? <p className="subtle">Geographies error: {geographiesError}</p> : null}
          {selectedMetricMeta ? (
            <p className="metric-meta">
              Source: {selectedMetricMeta.source_code} | Dataset: {selectedDataset.toUpperCase()} | Loaded catalog: {metrics.length.toLocaleString()} metrics
            </p>
          ) : null}
          <p className={`coverage-note ${selectedDataset === "acs1" ? "partial" : "complete"}`}>
            {selectedDataset === "acs1"
              ? "ACS 1-year county coverage is partial: Census publishes counties with populations of 65,000 or more. Uncolored counties are not published in ACS1."
              : "ACS 5-year estimates provide complete county coverage and are the default for nationwide county maps."}
          </p>

          <section className="county-panel" aria-live="polite">
            <div className="county-panel-header">
              <div>
                <div className="eyebrow">Selected county</div>
                <h3>{selectedCounty ? observationName(selectedCounty) : "Choose a county on the map"}</h3>
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
                        ? selectedCounty.period || selectedCounty.observation_date || "-"
                        : "Not published"}
                    </dd>
                  </div>
                  <div>
                    <dt>Source</dt>
                    <dd>{selectedCountyHasObservation ? selectedCounty.source || selectedCounty.source_code || "-" : "CENSUS_ACS"}</dd>
                  </div>
                  <div>
                    <dt>Dataset</dt>
                    <dd>{selectedCounty.dataset || selectedCounty.dataset_code || selectedDataset}</dd>
                  </div>
                  <div>
                    <dt>Margin of error</dt>
                    <dd>{selectedCountyHasObservation ? marginOfErrorText(selectedCounty) : "Not published"}</dd>
                  </div>
                  <div>
                    <dt>Geography ID</dt>
                    <dd>{selectedCounty.geo_id}</dd>
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
                Hover for a quick read; click a county to pin its details and fetch its time series.
              </p>
            )}
          </section>
        </article>

        <article className="card workspace-panel" data-active={activeTab === "chart"}>
          <h2>{selectedMetricMeta ? displayMetricName(selectedMetricMeta) : "County map"}</h2>
          <p className="subtle">Latest county estimates, joined to Martin vector geometry by the discovered geography key.</p>
          <div className="map-shell">
            <div className="map-canvas" data-testid="map-canvas" ref={mapContainerRef} />
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
                      {hoveredCounty.observation.period || hoveredCounty.observation.observation_date} · {hoveredCounty.observation.source || hoveredCounty.observation.source_code}
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
                    <td>{item.county_name || item.state_name || item.geo_id}</td>
                    <td>{item.geo_level || "-"}</td>
                    <td>{item.observation_date}</td>
                    <td>{item.metric_code}</td>
                    <td>{item.value ?? "-"}</td>
                    <td>{item.units || "-"}</td>
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
          <SourceNote source={selectedMetricMeta?.source_code} dataset={selectedDataset.toUpperCase()} metric={selectedMetricMeta ? `${displayMetricName(selectedMetricMeta)} (${selectedMetricMeta.metric_code})` : null} geography={selectedStateFips ? "Counties in selected state" : "United States counties"} period={observations[0]?.period || observations[0]?.observation_date} updatedAt={selectedMetricMeta?.updated_at} caveats={selectedMetricMeta?.caveats || (selectedDataset === "acs1" ? "ACS 1-year county estimates are available only for counties meeting the Census population threshold." : "ACS 5-year estimates favor geographic coverage over single-year currency.")} />
        </article>
        <article className="card span-2 workspace-panel" data-active={activeTab === "api query"}>
          <div className="section-kicker">Reproducible request</div><h2>API Query</h2><p className="subtle">This endpoint reproduces the observation set currently used by the map.</p><code className="api-query">GET {apiQuery}</code>
        </article>
        <article className="card span-2 workspace-panel" data-active={activeTab === "notes"}>
          <div className="section-kicker">Interpretation notes</div><h2>Use this view carefully</h2><p>Raw county population is highly skewed. The map uses API-calculated distribution bins, reports missing observations separately, and preserves margin-of-error context in county details.</p><p className="subtle">Transformation: raw value. Geography: county. Dataset: {selectedDataset.toUpperCase()}. Color treatment: five distribution-backed intervals with a local fallback only when the distribution endpoint is unavailable.</p>
        </article>
      </section>
    </main>
  );
}
