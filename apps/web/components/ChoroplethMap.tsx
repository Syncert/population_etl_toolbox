"use client";

// A read-only choropleth over the Martin vector boundary.
//
// The colouring logic is not new: this renders `buildChoroplethModel` from
// lib/explorerViewModel — the same model the explorer uses, with the same
// join key discovery and the same rule that a row without a usable number is
// left uncoloured rather than coloured as zero. What is local here is the
// MapLibre wiring for a presentation with no hover, selection, or extrusion,
// which is all a comparison map needs.
//
// The legend states what is being coloured, including whether the value is
// API-derived, and the caller always renders a table alongside so the map is
// never the only way to retrieve a value.

import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import type { ExpressionSpecification, FilterSpecification } from "maplibre-gl";
import { buildChoroplethModel, tileFilterForGeoLevel } from "../lib/explorerViewModel";
import type { ObservationRow } from "../lib/explorerViewModel";
import type { discoverTileMetadata } from "../lib/tiles";

type TileMetadata = Awaited<ReturnType<typeof discoverTileMetadata>>;

export default function ChoroplethMap({
  rows,
  tileMetadata,
  geoLevel,
  legendTitle,
  missingLabel = "Not published on both sides",
  testId = "comparison-map",
}: {
  rows: ObservationRow[];
  tileMetadata: TileMetadata | null;
  geoLevel: string;
  legendTitle: string;
  missingLabel?: string;
  testId?: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [ready, setReady] = useState(false);

  const model = useMemo(
    () => buildChoroplethModel(rows, tileMetadata?.joinKey || "geo_id", null, missingLabel),
    [rows, tileMetadata, missingLabel],
  );

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {},
        layers: [
          { id: "background", type: "background", paint: { "background-color": "#dfe8ed" } },
        ],
      },
      center: [-98.5795, 39.8283],
      zoom: 3,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("load", () => setReady(true));
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
      setReady(false);
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready || !tileMetadata) {
      return;
    }

    const sourceId = "comparison-boundary";
    const layerId = "comparison-choropleth";
    if (!map.getSource(sourceId)) {
      map.addSource(sourceId, {
        type: "vector",
        tiles: [new URL(tileMetadata.tileTemplate, window.location.origin).toString()],
        minzoom: 0,
        maxzoom: 12,
      });
    }
    if (!map.getLayer(layerId)) {
      map.addLayer({
        id: layerId,
        type: "fill",
        source: sourceId,
        "source-layer": tileMetadata.sourceLayer,
        paint: {
          "fill-color": model.expression as unknown as ExpressionSpecification,
          "fill-opacity": 0.85,
          "fill-outline-color": "#ffffff",
        },
      });
    } else {
      map.setPaintProperty(
        layerId,
        "fill-color",
        model.expression as unknown as ExpressionSpecification,
      );
    }
    map.setFilter(
      layerId,
      tileFilterForGeoLevel(geoLevel) as unknown as FilterSpecification,
    );
  }, [ready, tileMetadata, model, geoLevel]);

  return (
    <div className="map-shell">
      <div
        className="map-canvas"
        data-testid={testId}
        data-map-ready={ready ? "true" : "false"}
        data-colored-values={model.valueCount}
        ref={containerRef}
        role="region"
        aria-label={`${legendTitle}. The comparison table lists every value, including the geographies this map leaves uncoloured.`}
      />
      {model.legendItems.length > 0 ? (
        <div className="map-legend" aria-label={`${legendTitle} legend`}>
          <div className="legend-title">{legendTitle}</div>
          {model.legendItems.map((item) => (
            <div className="legend-row" key={`${item.color}-${item.label}`}>
              <span className="legend-swatch" style={{ backgroundColor: item.color }} />
              <span>{item.label}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
