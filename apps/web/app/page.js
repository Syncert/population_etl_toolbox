"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";

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

function observationToFeature(item) {
  if (
    !item ||
    typeof item.geo_longitude !== "number" ||
    typeof item.geo_latitude !== "number"
  ) {
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
      coordinates: [item.geo_longitude, item.geo_latitude],
    },
  };
}

async function discoverTileTemplate() {
  const catalogResponse = await fetch("/tiles/catalog", { cache: "no-store" });
  if (!catalogResponse.ok) {
    throw new Error(`Tiles catalog unavailable (${catalogResponse.status})`);
  }

  const catalog = await catalogResponse.json();
  const candidates = [];

  if (Array.isArray(catalog)) {
    for (const entry of catalog) {
      if (typeof entry === "string") {
        candidates.push(entry);
      } else if (entry && typeof entry.id === "string") {
        candidates.push(entry.id);
      }
    }
  } else if (catalog && typeof catalog === "object") {
    if (Array.isArray(catalog.collections)) {
      for (const entry of catalog.collections) {
        if (entry && typeof entry.id === "string") {
          candidates.push(entry.id);
        }
      }
    }

    for (const key of Object.keys(catalog)) {
      if (key !== "collections") {
        candidates.push(key);
      }
    }
  }

  const uniqueCandidates = [...new Set(candidates)].filter(Boolean);

  for (const id of uniqueCandidates) {
    const tileJsonPaths = [
      `/tiles/${id}`,
      `/tiles/${id}.json`,
      `/tiles/${id}/tilejson.json`,
    ];

    for (const path of tileJsonPaths) {
      try {
        const tileJsonResponse = await fetch(path, { cache: "no-store" });
        if (!tileJsonResponse.ok) {
          continue;
        }

        const tileJson = await tileJsonResponse.json();
        if (Array.isArray(tileJson.tiles) && tileJson.tiles.length > 0) {
          return tileJson.tiles[0];
        }
      } catch {
        // Try next tilejson path.
      }
    }
  }

  throw new Error("No tile template discovered from /tiles/catalog");
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
  const [mapReady, setMapReady] = useState(false);

  const options = useMemo(() => metricOptions(metrics), [metrics]);

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

        if (!cancelled) {
          setMetrics(items);
          setSelectedMetric(pickPreferredMetric(items));
        }
      } catch (error) {
        if (!cancelled) {
          setMetricsError(error.message || "Unable to load metrics.");
        }
      }

      try {
        await discoverTileTemplate();
        if (!cancelled) {
          setTilesHealth({ state: "ok", message: "catalog reachable" });
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
    if (!selectedMetric) {
      return;
    }

    let cancelled = false;
    setObservationStatus({ state: "loading", message: `loading ${selectedMetric}` });

    async function loadObservations() {
      try {
        const query = new URLSearchParams({
          metric_code: selectedMetric,
          limit: "25",
        });
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
            message: `loaded ${items.length} records`,
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
              "background-color": "#e8eef2",
            },
          },
        ],
      },
      center: [-98.5795, 39.8283],
      zoom: 3,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", async () => {
      setMapReady(true);

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
          "circle-radius": 5,
          "circle-opacity": 0.75,
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 1,
        },
      });

      try {
        const tileTemplate = await discoverTileTemplate();

        if (!map.getSource("basemap")) {
          map.addSource("basemap", {
            type: "raster",
            tiles: [tileTemplate],
            tileSize: 256,
          });

          map.addLayer(
            {
              id: "basemap-layer",
              type: "raster",
              source: "basemap",
              paint: {
                "raster-opacity": 0.8,
              },
            },
            "obs-points",
          );
        }
      } catch {
        // Keep fallback background if tiles are unavailable.
      }
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded() || !mapReady) {
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

    if (features.length > 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const feature of features) {
        const [lng, lat] = feature.geometry.coordinates;
        bounds.extend([lng, lat]);
      }
      map.fitBounds(bounds, { padding: 30, maxZoom: 7, duration: 800 });
    }
  }, [mapReady, observations]);

  const selectedMetricMeta = metrics.find((metric) => metric.metric_code === selectedMetric);

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
          <p className="subtle">Points come from latest observations and render over discovered /tiles catalog layers when available.</p>
          <div className="map-shell" ref={mapContainerRef} />
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
