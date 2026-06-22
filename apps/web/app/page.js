"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { VectorTile } from "@mapbox/vector-tile";
import maplibregl from "maplibre-gl";
import Protobuf from "pbf";

const COUNTY_TILE_FILTER = ["has", "county_fips"];
const CHOROPLETH_FALLBACK_COLOR = "#9fb0ba";
const CHOROPLETH_PALETTE = ["#f7e6a1", "#a8d08d", "#4f9f68", "#2f80b7", "#5e4fa2"];

function normalizeMetricCode(code) {
  return typeof code === "string" ? code.toLowerCase() : "";
}

function pickPreferredMetric(metrics) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return "";
  }

  const populationMetric = metrics.find((item) => {
    const code = normalizeMetricCode(item.metric_code);
    const name = normalizeMetricCode(item.metric_display_name);
    return code.includes("pop") || name.includes("population");
  });

  return (populationMetric || metrics[0]).metric_code;
}

function metricOptions(metrics) {
  return (metrics || []).map((metric) => ({
    value: metric.metric_code,
    label: `${metric.metric_display_name} (${metric.metric_code})`,
    source: metric.source_code,
  }));
}

function metricSupportsCounty(metric) {
  if (!metric || !Array.isArray(metric.valid_geo_grains)) {
    return false;
  }

  return metric.valid_geo_grains.some(
    (grain) => typeof grain === "string" && grain.toLowerCase() === "county",
  );
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

function formatLegendValue(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    notation: Math.abs(value) >= 10000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 10000 ? 1 : 0,
  }).format(value);
}

function buildChoroplethModel(observations, joinKey) {
  if (!Array.isArray(observations) || observations.length === 0) {
    return {
      expression: ["literal", CHOROPLETH_FALLBACK_COLOR],
      legendItems: [],
      minValue: null,
      maxValue: null,
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
      legendItems: [],
      minValue: null,
      maxValue: null,
    };
  }

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = maxValue - minValue;

  for (const [key, numericValue] of keyedMap.entries()) {
    keyedValues.push(key, colorForValue(numericValue, minValue, maxValue));
  }

  const legendItems = CHOROPLETH_PALETTE.map((color, index) => {
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

  return {
    expression: ["match", ["to-string", ["get", joinKey]], ...keyedValues, CHOROPLETH_FALLBACK_COLOR],
    legendItems,
    minValue,
    maxValue,
  };
}

function buildChoroplethMatchExpression(observations, joinKey) {
  return buildChoroplethModel(observations, joinKey).expression;
}

export default function HomePage() {
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
  const [selectedMetric, setSelectedMetric] = useState("");

  const [observationStatus, setObservationStatus] = useState({
    state: "idle",
    message: "selecting metric",
  });
  const [observations, setObservations] = useState([]);
  const [tileMetadata, setTileMetadata] = useState(null);
  const [activeSourceLayer, setActiveSourceLayer] = useState(null);
  const [mapReady, setMapReady] = useState(false);

  const countyCapableMetrics = useMemo(
    () => metrics.filter((metric) => metricSupportsCounty(metric)),
    [metrics],
  );
  const options = useMemo(
    () => metricOptions(countyCapableMetrics.length > 0 ? countyCapableMetrics : metrics),
    [countyCapableMetrics, metrics],
  );

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
        const metricsResponse = await fetch("/api/catalog/metrics?limit=25", {
          cache: "no-store",
        });

        if (!metricsResponse.ok) {
          throw new Error(`status ${metricsResponse.status}`);
        }

        const payload = await metricsResponse.json();
        const items = Array.isArray(payload.items) ? payload.items : [];
        const countyItems = items.filter((metric) => metricSupportsCounty(metric));
        const preferredItems = countyItems.length > 0 ? countyItems : items;

        if (!cancelled) {
          setMetrics(items);
          setSelectedMetric(pickPreferredMetric(preferredItems));
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
        const response = await fetch(`/api/observations/latest?${query.toString()}`, {
          cache: "no-store",
        });

        if (!response.ok) {
          throw new Error(`status ${response.status}`);
        }

        const payload = await response.json();
        let items = Array.isArray(payload.items) ? payload.items : [];
        let usedFallback = false;

        if (items.length === 0) {
          const fallbackQuery = new URLSearchParams({
            metric_code: selectedMetric,
            limit: "4000",
          });
          const fallbackResponse = await fetch(
            `/api/observations/latest?${fallbackQuery.toString()}`,
            {
              cache: "no-store",
            },
          );

          if (!fallbackResponse.ok) {
            throw new Error(`status ${fallbackResponse.status}`);
          }

          const fallbackPayload = await fallbackResponse.json();
          const fallbackItems = Array.isArray(fallbackPayload.items)
            ? fallbackPayload.items
            : [];

          if (fallbackItems.length > 0) {
            const countyFallbackItems = fallbackItems.filter((item) => isCountyObservation(item));
            items = countyFallbackItems.length > 0 ? countyFallbackItems : fallbackItems;
            usedFallback = true;
          }
        }

        if (!cancelled) {
          setObservations(items);
          setObservationStatus({
            state: "ok",
            message: usedFallback
              ? `loaded ${items.length} records (fallback without geo_level)`
              : `loaded ${items.length} records`,
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
  }, [selectedMetric]);

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
    const currentSourceLayer = activeSourceLayer || tileMetadata.sourceLayer;

    const removeChoropleth = () => {
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
            "fill-color": buildChoroplethMatchExpression(observations, tileMetadata.joinKey),
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
    };
  }, [mapReady, tileMetadata, activeSourceLayer, observations]);

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
        buildChoroplethMatchExpression(observations, tileMetadata.joinKey),
      );
    }

    if (features.length > 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const feature of features) {
        const [lng, lat] = feature.geometry.coordinates;
        bounds.extend([lng, lat]);
      }
      map.fitBounds(bounds, { padding: 30, maxZoom: 7, duration: 800 });
    }
  }, [mapReady, observations, tileMetadata]);

  const selectedMetricMeta = metrics.find((metric) => metric.metric_code === selectedMetric);
  const choroplethModel = useMemo(
    () => buildChoroplethModel(observations, tileMetadata?.joinKey || "geo_id"),
    [observations, tileMetadata],
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

  return (
    <main className="dashboard">
      <section className="hero">
        <h1>Population ETL Local Dashboard</h1>
        <p>App Router frontend with same-origin API and tile proxies for quick local iteration.</p>
      </section>

      <section className="status-row">
        <span className={pillClass(apiHealth.state)}>
          API: <strong>{apiHealth.message}</strong>
        </span>
        <span className={pillClass(tilesHealth.state)}>
          Tiles: <strong>{tilesHealth.message}</strong>
        </span>
        <span className={pillClass(observationStatus.state)}>
          Observations: <strong>{observationStatus.message}</strong>
        </span>
      </section>

      <section className="grid">
        <article className="card">
          <h2>Metric Selector</h2>
          <div className="controls">
            <label htmlFor="metric-select">Metric</label>
            <select
              id="metric-select"
              className="select"
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
          {metricsError ? <p className="subtle">Metrics error: {metricsError}</p> : null}
          {selectedMetricMeta ? (
            <p className="metric-meta">
              Source: {selectedMetricMeta.source_code} | Suitability: {selectedMetricMeta.dashboard_suitability}
            </p>
          ) : null}
        </article>

        <article className="card">
          <h2>Map Preview</h2>
          <p className="subtle">Choropleth polygons are joined to observations by discovered tile join key; low-opacity points remain for diagnostics.</p>
          <div className="map-shell">
            <div className="map-canvas" ref={mapContainerRef} />
            {choroplethModel.legendItems.length > 0 ? (
              <div className="map-legend" aria-label="Choropleth value legend">
                <div className="legend-title">Value</div>
                {choroplethModel.legendItems.map((item) => (
                  <div className="legend-row" key={`${item.color}-${item.label}`}>
                    <span className="legend-swatch" style={{ backgroundColor: item.color }} />
                    <span>{item.label}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </article>

        <article className="card span-2">
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
      </section>
    </main>
  );
}
